# GeoGuard — Data Ingestion System

**IoT-Based Landslide Detection & Early Warning System for Nepal**

Built by Team CodeYatra | Software Lead: Mohsin Raja

---

## Quick Start

```bash
cd geoguard

# Copy environment configuration
cp .env.example .env

# Start all services
docker-compose up -d
```

This starts:
- **IoT Sensor Data API** (Node.js + Express) — `http://localhost:3000`
- **DHM Rainfall Connector** (Python) — auto-starts fetching every 15 min
- **Satellite Data Fetcher** (Python) — auto-starts all scheduled fetches
- **InfluxDB** — `http://localhost:8086` (time-series store)
- **PostgreSQL + PostGIS** — `localhost:5432` (spatial store)
- **Mosquitto MQTT** — `mqtt://localhost:1883`

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   DATA INGESTION LAYER                       │
├───────────────┬───────────────────┬─────────────────────────┤
│ IoT Sensor    │ Satellite &       │ DHM Rainfall            │
│ Data API      │ Open Data Fetcher │ API Connector           │
│ (Node.js)     │ (Python async)    │ (Python)                │
│ POST /sensor  │ NASA GPM, ESA S1  │ Nepal DHM → zones       │
│ + MQTT fallbk │ Sentinel-2, OSM   │ 15-min polling          │
└───────┬───────┴─────────┬─────────┴───────────┬─────────────┘
        │                 │                      │
        ▼                 ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│                VALIDATION & STORAGE                          │
│  • Schema validation, range checks, duplicate detection      │
│  • InfluxDB (time-series, 90-day hot)                        │
│  • PostgreSQL + PostGIS (spatial/GIS, zone mappings)         │
│  • S3 (cold archive, optional)                               │
└─────────────────────────────┬───────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 RISK SCORE ENGINE (consumer)                 │
│         Reads unified data every 15 min → risk scores       │
└─────────────────────────────────────────────────────────────┘
```

---

## Components

### 1. IoT Sensor Data API (`ingestion/api/`)

The hardware interface point. Receives sensor readings and forwards them to storage.

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/v1/sensor/data` | POST | Receive sensor readings (auth required) |
| `/api/v1/sensor/register` | POST | Register a new sensor node |
| `/api/v1/sensor/data/:id` | GET | Query sensor history |
| `/api/v1/sensor/status` | GET | Network health dashboard data |
| `/api/v1/sensor/:id/mode` | PUT | Switch normal/elevated mode |

**Auth:** `X-Device-Key` header with sensor-specific API key.

**Validation pipeline:** Auth → JSON Schema → Range checks → Nepal geo-boundary → Duplicate detection → Temporal freshness

### 2. DHM Rainfall Connector (`ingestion/dhm/`)

Fetches rainfall data from Nepal Department of Hydrology & Meteorology every 15 minutes.

**Current state:** Simulation mode — generates realistic Nepal monsoon rainfall patterns. No official DHM API exists yet.

**In simulation mode:**
- Generates realistic rainfall values accounting for monsoon season and elevation effects
- Maps DHM stations to GeoGuard zones via haversine distance
- Pre-computes rolling windows (1hr, 6hr, 24hr, 72hr)
- Writes to InfluxDB as `dhm_rainfall` measurement
- Zones without DHM coverage get `satellite_proxy` entries

### 3. Satellite & Open Data Fetcher (`ingestion/satellite/`)

| Source | Interval | Data | Auth Needed? |
|--------|----------|------|-------------|
| **NASA GPM IMERG** | 30 min | Global rainfall | Yes — Earthdata |
| **Sentinel-1 SAR** | 6 days | Ground deformation | Yes — Copernicus |
| **Sentinel-2** | 5 days | Vegetation (NDVI) | Yes — Copernicus |
| **OpenStreetMap** | Daily | Roads, settlements | No — open data |

**Without credentials:** Sources gracefully degrade to simulation/dry-run mode.

---

## What Works Now (Self-Contained)

Everything below runs **without any external API credentials**:

| Component | Status | Notes |
|-----------|--------|-------|
| IoT Sensor API | **Fully functional** | Accepts sensor data, validates, stores in InfluxDB |
| DHM Connector | **Functional (simulation)** | Generates realistic Nepal monsoon rainfall |
| NASA GPM | **Functional (simulation)** | Generates rainfall patterns for all 15 zones |
| Sentinel-1/2 | **Dry-run** | Logs setup guidance |
| OSM Sync | **Functional** | Downloads Nepal OSM daily (no auth needed) |
| InfluxDB | **Fully functional** | Time-series store, 90-day retention |
| PostgreSQL + PostGIS | **Fully functional** | Spatial store, all tables created |
| MQTT Broker | **Fully functional** | Real-time data forwarding |

---

## What You Need to Provide

### NASA Earthdata Credentials (for real GPM rainfall)
1. Register at https://urs.earthdata.nasa.gov
2. Add to `.env`:
   ```
   NASA_EARTHDATA_USERNAME=your_username
   NASA_EARTHDATA_PASSWORD=your_password
   ```

### ESA Copernicus Credentials (for real Sentinel data)
1. Register at https://dataspace.copernicus.eu
2. Add to `.env`:
   ```
   COPERNICUS_USERNAME=your_username
   COPERNICUS_PASSWORD=your_password
   ```

### DHM API Endpoint (for real Nepal rainfall data)
- No official developer API exists yet. The connector runs in simulation mode until DHM provides an endpoint.
- If you obtain a DHM data source (CSV, FTP, etc.), update `DHM_API_URL` in `.env`.

### AWS S3 Credentials (optional, for cold archive)
- Only needed if you want 90+ day data archiving
- Set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_S3_BUCKET` in `.env`

---

## Testing the API

```bash
# Register a sensor
curl -X POST http://localhost:3000/api/v1/sensor/register \
  -H "Content-Type: application/json" \
  -d '{"sensor_id":"SN-LD-001","zone_id":"SINDHUPALCHOK-05","lat":27.85,"lng":85.78}'

# Save the returned api_key, then send data
curl -X POST http://localhost:3000/api/v1/sensor/data \
  -H "Content-Type: application/json" \
  -H "X-Device-Key: <api_key>" \
  -d '{
    "sensor_id":"SN-LD-001",
    "timestamp":"2026-06-11T10:30:00Z",
    "tilt_deg":2.3,
    "moisture_pct":67.5,
    "vibration_g":0.12,
    "rainfall_mm":15.2,
    "battery_pct":85.0,
    "gps_lat":27.7172,
    "gps_lng":85.3240
  }'

# Query sensor history
curl http://localhost:3000/api/v1/sensor/data/SN-LD-001?hours=24

# Check sensor network status
curl http://localhost:3000/api/v1/sensor/status
```

---

## Project Structure

```
geoguard/
├── docker-compose.yml            # All services orchestrated
├── .env.example                  # Environment template
├── .env                          # Your configuration (gitignored)
│
├── shared/                       # Shared between all components
│   ├── config/
│   │   ├── nepal_zones.json      # 15 priority zones with GPS
│   │   └── bounding_box.py       # Nepal coordinate validation
│   ├── models/
│   │   └── unified.py            # UnifiedRiskInput dataclass
│   └── validation/
│       └── sensor_schema.json    # JSON Schema for sensor payload
│
├── ingestion/
│   ├── api/                      # Node.js IoT Sensor API
│   │   ├── package.json
│   │   ├── Dockerfile
│   │   └── src/
│   │       ├── app.js            # Express entry point
│   │       ├── routes/sensor.js  # All API endpoints
│   │       ├── middleware/
│   │       │   ├── auth.js       # Device key validation
│   │       │   └── validator.js  # Schema + range + geo checks
│   │       ├── services/
│   │       │   ├── ingest.service.js  # PostgreSQL operations
│   │       │   ├── influx.service.js  # InfluxDB writes
│   │       │   └── s3.service.js     # Cold archive
│   │       └── mqtt/
│   │           ├── broker.js     # MQTT connect/publish/sub
│   │           └── mosquitto.conf
│   │
│   ├── dhm/                      # Python DHM Connector
│   │   ├── requirements.txt
│   │   ├── Dockerfile
│   │   └── src/
│   │       └── connector.py      # Full DHM ingestion pipeline
│   │
│   └── satellite/                # Python Satellite Fetcher
│       ├── requirements.txt
│       ├── Dockerfile
│       └── src/
│           ├── scheduler.py      # APScheduler orchestration
│           ├── fetchers/
│           │   ├── nasa_gpm.py   # NASA GPM IMERG rainfall
│           │   ├── sentinel1.py  # ESA Sentinel-1 SAR
│           │   ├── sentinel2.py  # ESA Sentinel-2 vegetation
│           │   └── osm_sync.py   # OpenStreetMap daily sync
│           └── processors/
│               └── crop.py       # Raster crop + zonal stats
│
└── scripts/
    ├── setup-postgis.sql         # PostgreSQL + PostGIS init
    └── setup-influx.sh           # InfluxDB setup
```

---

## Next Steps (Phase 1A Complete → Phase 1B)

- [x] Project scaffolding & Docker Compose
- [x] IoT Sensor API (fully functional)
- [x] DHM Connector (simulation mode)
- [x] Satellite Fetcher skeleton (simulation for GPM, dry-run for Sentinel)
- [x] Nepal zone definitions (15 high-priority zones)
- [x] PostgreSQL + PostGIS schema
- [x] MQTT broker setup
- [ ] Register for NASA Earthdata & Copernicus accounts
- [ ] Confirm DHM data source (API or alternative)
- [ ] Build Risk Score Engine (Phase 1: rule-based SWI)
- [ ] Build Nepal Police Dashboard (React.js + Leaflet)
- [ ] Build Alert System (WhatsApp + SMS)