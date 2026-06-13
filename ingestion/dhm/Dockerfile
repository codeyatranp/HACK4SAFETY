FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for rasterio and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev libgdal-dev && \
    rm -rf /var/lib/apt/lists/*

COPY ingestion/dhm/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ ./shared/
COPY ingestion/dhm/src/ ./src/

CMD ["python", "-u", "src/connector.py"]