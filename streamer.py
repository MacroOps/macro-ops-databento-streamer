#!/usr/bin/env python3
"""
Databento Live Streaming Service

Connects to Databento's live TCP gateway (port 13000) and streams
real-time CME futures data to the Node.js backend.

Uses the official databento-python library for binary protocol handling.
"""

import os
import sys
import json
import time
import signal
import logging
import requests
from datetime import datetime, timezone
from threading import Thread, Event
from collections import defaultdict

import databento as db

# Configuration
DATABENTO_API_KEY = os.environ.get("DATABENTO_API_KEY")
BACKEND_URL = os.environ.get("BACKEND_URL", "https://macro-ops-backend.fly.dev")
PUSH_ENDPOINT = f"{BACKEND_URL}/api/live-data"

# Symbols to stream (continuous front-month contracts)
SYMBOLS = [
    "ES.FUT", "NQ.FUT", "CL.FUT", "GC.FUT", "SI.FUT",
    "ZB.FUT", "ZN.FUT", "ZT.FUT", "NG.FUT", "YM.FUT",
    "RTY.FUT", "KE.FUT", "ZC.FUT", "ZS.FUT"
]

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Graceful shutdown
shutdown_event = Event()

def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)


class DatabentoStreamer:
    """Streams live data from Databento and pushes to Node.js backend."""
    
    def __init__(self):
        self.client = None
        self.bars = defaultdict(dict)  # symbol -> {open, high, low, close, volume}
        self.quotes = {}  # symbol -> latest quote
        self.last_push = time.time()
        self.push_interval = 1.0  # Push updates every 1 second
        self.records_received = 0
        self.ohlcv_processed = 0
        self.last_stats_time = time.time()
        self.symbol_map = {}  # instrument_id -> symbol name
        
    def start(self):
        """Start the live streaming connection."""
        if not DATABENTO_API_KEY:
            logger.error("DATABENTO_API_KEY environment variable not set")
            sys.exit(1)
            
        logger.info("=" * 60)
        logger.info("Databento Live Streamer Starting")
        logger.info(f"Backend URL: {BACKEND_URL}")
        logger.info(f"Symbols: {', '.join(SYMBOLS)}")
        logger.info("=" * 60)
        
        # Test backend connectivity
        self._test_backend()
        
        while not shutdown_event.is_set():
            try:
                self._connect_and_stream()
            except Exception as e:
                logger.error(f"Stream error: {e}")
                if not shutdown_event.is_set():
                    logger.info("Reconnecting in 5 seconds...")
                    time.sleep(5)
    
    def _test_backend(self):
        """Test connectivity to the Node.js backend."""
        try:
            res = requests.get(f"{BACKEND_URL}/health", timeout=10)
            if res.ok:
                logger.info(f"Backend connectivity OK: {BACKEND_URL}")
            else:
                logger.warning(f"Backend returned {res.status_code}")
        except Exception as e:
            logger.warning(f"Backend connectivity test failed: {e}")
    
    def _connect_and_stream(self):
        """Connect to Databento live gateway and process messages."""
        logger.info("Connecting to Databento live gateway...")
        
        # Create live client
        self.client = db.Live(key=DATABENTO_API_KEY)
        
        # Subscribe to OHLCV-1m bars for aggregated price data
        logger.info("Subscribing to ohlcv-1m schema...")
        self.client.subscribe(
            dataset="GLBX.MDP3",
            schema="ohlcv-1m",
            symbols=SYMBOLS,
            stype_in="parent",
        )
        
        logger.info("Subscription active, waiting for data...")
        
        # Process incoming records
        for record in self.client:
            if shutdown_event.is_set():
                break
                
            self.records_received += 1
            self._process_record(record)
            
            # Periodically push aggregated data to backend
            if time.time() - self.last_push >= self.push_interval:
                self._push_to_backend()
                self.last_push = time.time()
            
            # Log stats every 60 seconds
            if time.time() - self.last_stats_time >= 60:
                logger.info(f"Stats: {self.records_received} records received, {self.ohlcv_processed} OHLCV processed, {len(self.bars)} bars, {len(self.quotes)} quotes buffered")
                self.last_stats_time = time.time()
        
        logger.info("Closing Databento connection...")
        self.client.stop()
    
    def _process_record(self, record):
        """Process a single record from Databento."""
        try:
            # Get record type
            record_type = type(record).__name__
            
            if record_type in ("OhlcvMsg", "OHLCVMsg"):
                self._process_ohlcv(record)
            elif record_type in ("MboMsg", "Mbp1Msg", "MBOMsg", "MBP1Msg"):
                self._process_quote(record)
            elif record_type in ("TradeMsg", "TRADEMsg"):
                self._process_trade(record)
            elif record_type == "ErrorMsg":
                logger.error(f"Databento error: {record.err}")
            elif record_type == "SystemMsg":
                logger.info(f"Databento system: {record.msg}")
            elif record_type in ("SymbolMappingMsg", "SymbolMappingMsgV2"):
                in_sym = getattr(record, 'stype_in_symbol', None)
                out_sym = getattr(record, 'stype_out_symbol', None)
                iid = getattr(record, 'instrument_id', None)
                if out_sym and iid is not None:
                    clean = self._clean_symbol(out_sym)
                    if clean:
                        self.symbol_map[iid] = clean
                        logger.info(f"Symbol mapping: {in_sym} -> {out_sym} (id={iid}) -> {clean}")
                    else:
                        logger.info(f"Symbol mapping (unclean): {in_sym} -> {out_sym} (id={iid})")
                else:
                    logger.info(f"Symbol mapping: {in_sym} -> {out_sym} (id={iid})")
            else:
                logger.info(f"Unknown record type: {record_type} attrs={[a for a in dir(record) if not a.startswith('_')]}")
                
        except Exception as e:
            logger.error(f"Error processing record: {e}")
    
    def _process_ohlcv(self, record):
        """Process OHLCV bar record."""
        try:
            # Get symbol from record
            symbol = self._get_symbol(record)
            if not symbol:
                logger.warning(f"OHLCV record with no symbol: instrument_id={getattr(record, 'instrument_id', '?')} raw_symbol={getattr(record, 'symbol', '?')} pretty={getattr(record, 'pretty_symbol', '?')} close={getattr(record, 'close', '?')}")
                return
            
            # Extract OHLCV values (prices are in fixed-point, divide by 1e9)
            open_price = record.open / 1e9
            high_price = record.high / 1e9
            low_price = record.low / 1e9
            close_price = record.close / 1e9
            volume = record.volume
            
            # Skip invalid prices
            if open_price <= 0 or close_price <= 0:
                return
            
            # Get timestamp
            ts_event = record.ts_event  # nanoseconds since epoch
            dt = datetime.fromtimestamp(ts_event / 1e9, tz=timezone.utc)
            
            # Store bar data
            self.bars[symbol] = {
                "symbol": symbol,
                "time": dt.isoformat(),
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "close": close_price,
                "volume": volume,
                "timestamp": int(time.time() * 1000),
            }
            
            # Also update quote from latest bar
            self.quotes[symbol] = {
                "symbol": symbol,
                "price": close_price,
                "open": open_price,
                "high": high_price,
                "low": low_price,
                "volume": volume,
                "timestamp": int(time.time() * 1000),
                "source": "databento_live",
            }
            
            self.ohlcv_processed += 1
            if self.ohlcv_processed <= 5 or self.ohlcv_processed % 100 == 0:
                logger.info(f"OHLCV {symbol}: O={open_price:.2f} H={high_price:.2f} L={low_price:.2f} C={close_price:.2f} V={volume}")
            
        except Exception as e:
            logger.error(f"Error processing OHLCV: {e}")
    
    def _process_quote(self, record):
        """Process quote/BBO record."""
        try:
            symbol = self._get_symbol(record)
            if not symbol:
                return
            
            # Extract bid/ask (prices in fixed-point)
            bid_price = record.bid_px_00 / 1e9 if hasattr(record, 'bid_px_00') else None
            ask_price = record.ask_px_00 / 1e9 if hasattr(record, 'ask_px_00') else None
            
            if bid_price and ask_price and bid_price > 0 and ask_price > 0:
                mid_price = (bid_price + ask_price) / 2
                
                self.quotes[symbol] = {
                    "symbol": symbol,
                    "price": mid_price,
                    "bid": bid_price,
                    "ask": ask_price,
                    "timestamp": int(time.time() * 1000),
                    "source": "databento_live",
                }
                
                logger.debug(f"Quote {symbol}: Bid={bid_price:.2f} Ask={ask_price:.2f} Mid={mid_price:.2f}")
                
        except Exception as e:
            logger.error(f"Error processing quote: {e}")
    
    def _process_trade(self, record):
        """Process trade record."""
        try:
            symbol = self._get_symbol(record)
            if not symbol:
                return
            
            price = record.price / 1e9
            size = record.size
            
            if price > 0:
                self.quotes[symbol] = {
                    "symbol": symbol,
                    "price": price,
                    "size": size,
                    "timestamp": int(time.time() * 1000),
                    "source": "databento_live",
                }
                
                logger.debug(f"Trade {symbol}: {size} @ {price:.2f}")
                
        except Exception as e:
            logger.error(f"Error processing trade: {e}")
    
    def _get_symbol(self, record):
        """Extract clean symbol from record using instrument_id -> symbol map."""
        try:
            instrument_id = getattr(record, 'instrument_id', None)
            
            # Check our local symbol map first
            if instrument_id is not None and instrument_id in self.symbol_map:
                return self.symbol_map[instrument_id]
            
            # Try to get symbol from the client's symbology map
            if self.client and instrument_id is not None:
                try:
                    sym_map = self.client.symbology_map
                    if sym_map and instrument_id in sym_map:
                        raw_symbol = sym_map[instrument_id]
                        clean = self._clean_symbol(raw_symbol)
                        if clean:
                            self.symbol_map[instrument_id] = clean
                            logger.info(f"Mapped instrument {instrument_id} -> {raw_symbol} -> {clean}")
                            return clean
                except Exception as e:
                    logger.debug(f"Symbology map lookup failed: {e}")
            
            # Try attributes on the record directly
            for attr in ('pretty_symbol', 'symbol'):
                raw_symbol = getattr(record, attr, None)
                if raw_symbol and str(raw_symbol).strip():
                    clean = self._clean_symbol(str(raw_symbol).strip())
                    if clean and instrument_id is not None:
                        self.symbol_map[instrument_id] = clean
                    return clean
            
            return None
        except Exception as e:
            logger.error(f"Symbol extraction error: {e}")
            return None
    
    def _clean_symbol(self, raw_symbol):
        """Clean a raw symbol string to its root (e.g., 'ESM6' -> 'ES')."""
        if not raw_symbol:
            return None
        # If it's a parent symbol like "ES.FUT", extract root
        if '.FUT' in raw_symbol:
            return raw_symbol.split('.')[0]
        # Clean up specific contract (e.g., "ESM6" -> "ES", "ESH26" -> "ES")
        cleaned = raw_symbol.rstrip('0123456789')
        if cleaned and cleaned[-1] in 'FGHJKMNQUVXZ':
            cleaned = cleaned[:-1]
        return cleaned if cleaned else raw_symbol
    
    def _push_to_backend(self):
        """Push aggregated data to Node.js backend."""
        if not self.bars and not self.quotes:
            return
        
        payload = {
            "bars": list(self.bars.values()),
            "quotes": list(self.quotes.values()),
            "timestamp": int(time.time() * 1000),
            "source": "databento_live_streamer",
        }
        
        try:
            res = requests.post(
                PUSH_ENDPOINT,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=5
            )
            
            if res.ok:
                bar_count = len(payload["bars"])
                quote_count = len(payload["quotes"])
                if bar_count > 0 or quote_count > 0:
                    logger.info(f"Pushed {bar_count} bars, {quote_count} quotes to backend")
            else:
                logger.warning(f"Backend push failed: {res.status_code} {res.text[:100]}")
                
        except requests.exceptions.Timeout:
            logger.warning("Backend push timeout")
        except Exception as e:
            logger.error(f"Backend push error: {e}")


def main():
    """Main entry point."""
    streamer = DatabentoStreamer()
    streamer.start()


if __name__ == "__main__":
    main()
