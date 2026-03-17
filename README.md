# Databento Live Streamer

Python sidecar that connects to Databento's live TCP gateway (port 13000) and streams real-time CME futures data to the Node.js backend.

## Architecture
- Connects to `equs-mini.lsg.databento.com:13000` via TCP binary protocol
- Uses official `databento-python` library
- Pushes OHLCV bars and quotes to `macro-ops-backend` via `POST /api/live-data`
- Runs as a Fly.io worker process (no HTTP service)

## Environment Variables
- `DATABENTO_API_KEY` - Databento API key (set as Fly.io secret)
- `BACKEND_URL` - Node.js backend URL (default: `https://macro-ops-backend.fly.dev`)
