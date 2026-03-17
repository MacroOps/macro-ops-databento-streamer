# Python Databento Live Streamer
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies required by databento
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY streamer.py .

# Run the streamer
CMD ["python", "-u", "streamer.py"]
