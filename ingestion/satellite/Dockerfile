FROM python:3.12-slim

WORKDIR /app

# Install Python deps first (before apt to avoid system package conflicts)
COPY ingestion/satellite/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install system dependencies for rasterio, PostGIS, and OSM tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev libgdal-dev \
    osm2pgsql && \
    rm -rf /var/lib/apt/lists/*

COPY shared/ ./shared/
COPY ingestion/satellite/src/ ./src/

# Kickstart with OSM sync on boot (no auth needed)
CMD ["python", "-u", "src/scheduler.py"]