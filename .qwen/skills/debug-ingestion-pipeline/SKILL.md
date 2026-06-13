---
name: debug-ingestion-pipeline
description: Systematic approach to debugging and verifying Docker-based data ingestion pipelines — check logs, fix code/env mismatches, seed empty tables, then validate end-to-end
source: auto-skill
extracted_at: '2026-06-11T16:23:40.542Z'
---

# Debugging & Verifying Data Ingestion Pipelines

## Context
This skill was extracted from debugging GeoGuard's multi-source ingestion stack (sensor API, DHM connector, satellite fetcher, OSM sync) running in Docker Compose. The approach applies to any multi-container data pipeline that writes to InfluxDB + PostgreSQL + MQTT.

## Procedure

### 1. Check Container Health First
After `docker compose up -d`, immediately verify with:
- `docker compose ps` — all containers should be `Up` or `Healthy`
- `docker compose logs --tail=30 <service>` for each ingestion service — look for crashes, exceptions, or credential warnings

### 2. Identify Three Common Bug Classes

**A. Module/import crashes** (Node.js `MODULE_NOT_FOUND`, Python `ImportError`)
- Look for top-level imports of optional/conditional dependencies — especially S3, AWS, or other service clients that may not be in package.json
- Fix: remove unused top-level imports entirely, or move conditional ones to lazy `require()` / conditional imports inside functions that check env vars first
- Example: `const { S3Client } = require("@aws-sdk/client-s3")` was imported at top-level of sensor route file but never used there — S3 functionality was already handled by a separate `s3.service.js` module that does lazy loading. The fix was simply removing the unused import, NOT adding the package to dependencies.
- Key insight: check if the import is actually USED in the file before adding the missing package. Often the import is leftover/unused and should just be removed.

**B. InfluxDB field type conflicts** (`unprocessable entity` / `field type conflict`)
- InfluxDB enforces strict field type consistency per measurement — once a field is stored as `float`, all writes must be `float`
- Common cause: simulation code writes `round(x, 1)` (float) but fallback/proxy code writes `0` (integer)
- Fix: use `0.0` explicitly for all numeric fields, and set defaults to `0.0` not `0`
- After fixing code, must drop the conflicting measurement or wipe the bucket (old data retains the wrong schema)

**C. Environment variable name mismatches**
- `.env` key names must exactly match what the code reads via `os.getenv()` / `process.env`
- Common pattern: `.env.example` says `COPERNICUS_USERNAME` but someone wrote `COPERNICUS_EMAIL` in `.env`
- Fix: align `.env` key names to match what code reads; verify by checking `os.getenv("COPERNICUS_USERNAME")` in the fetcher code

### 3. Seed Empty Reference Tables
- Docker init scripts often create schema (tables, indexes) but don't insert reference data
- Foreign key constraints will silently block inserts if referenced rows don't exist (e.g., `sensors.zone_id → zones.zone_id`)
- Generate seed SQL from JSON config files using Python, then pipe into PostgreSQL via `docker exec -i <container> psql`
- Always include `ON CONFLICT DO UPDATE` so the seed script is re-runnable
- When adding new columns (e.g., `sensor_types JSONB`), use `ALTER TABLE` on the running database before rebuilding API containers — otherwise INSERT queries referencing new columns will crash

### 4. End-to-End Validation Per Source

For each ingestion source, validate the full data path:

| Source | Validate |
|---|---|
| **Sensor API** | Register sensor → POST data → GET data back → check `/status` endpoint |
| **Scheduled fetchers** | Check container logs for successful writes → query InfluxDB with Flux |
| **PostgreSQL writers** | `docker exec <pg-container> psql -c "SELECT count(*) FROM <table>"` |
| **MQTT publishers** | `docker exec <mosquitto-container> mosquitto_sub -t "<topic>" -v` |

### 5. Query InfluxDB via Flux
Use the correct Flux syntax:
```
from(bucket:"sensor-data")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "dhm_rainfall")
  |> limit(n:5)
```
Note: `limit(n:5)` not `limit(5)` — the latter causes a parse error.

### 6. Timestamp Awareness
- Scheduled fetchers (APScheduler) won't produce data until their next trigger time
- NASA GPM: 30 min, DHM: 15 min, Sentinel: 1-6 hr, OSM: daily at 03:00 UTC
- For immediate testing, check if the scheduler has `next_run_time` offset (GeoGuard uses 5-10 second offset on first run)

## Key Lessons
- Always check logs before assuming containers are "working" — `docker compose ps` showing "Up" doesn't mean the service isn't crashing internally
- InfluxDB type conflicts are silent data loss — points are dropped, not errored at the application level
- Empty reference tables are a silent blocker — sensor registration will succeed but FK violations cause subtle downstream failures
- Seed scripts should be idempotent (`ON CONFLICT DO UPDATE`) and kept alongside schema scripts