FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY processing/gis/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/ shared/
COPY processing/gis/ processing/gis/

CMD ["python", "-c", "from processing.gis.src.zone_mapper import GISZoneMapper; m = GISZoneMapper(); m.connect(); print('GIS Zone Mapper ready — invoked by risk-engine')"]