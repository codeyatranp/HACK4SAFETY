FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for PostGIS client
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY processing/engine/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy shared modules and processing engine code
COPY shared/ shared/
COPY processing/engine/ processing/engine/

CMD ["python", "processing/engine/src/engine/risk_engine.py"]