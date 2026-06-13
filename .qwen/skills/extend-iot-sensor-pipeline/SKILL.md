---
name: extend-iot-sensor-pipeline
description: Pattern for adding new IoT sensor types to an existing ingestion pipeline — update JSON schema, validator ranges, InfluxDB writer fields, PostgreSQL registration, and route handler in coordinated order
source: auto-skill
extracted_at: '2026-06-11T17:13:52.425Z'
---

# Extending IoT Sensor Data Pipeline with New Sensor Types

## Context
This skill was extracted from adding real hardware sensor support to GeoGuard's IoT ingestion API. The user had four physical sensors: rain detection (boolean), moisture (percentage), wind speed (km/h), and tiltometer (degrees). The existing pipeline only supported `tilt_deg`, `moisture_pct`, `rainfall_mm`, `vibration_g`. Adding new fields required coordinated changes across 6 files in the data flow chain.

## The 6-File Update Chain

When adding new sensor fields, you must update all files in the exact order below. Missing any one causes silent data loss or validation failures:

### 1. `shared/validation/sensor_schema.json` — JSON Schema
- Add new properties with appropriate types (`number`, `boolean`)
- Set realistic `minimum`/`maximum` ranges
- For boolean sensors (e.g., rain detection): use `"type": "boolean"`
- For numeric sensors (e.g., wind speed): use `"type": "number"` with `minimum: 0, maximum: <safe_limit>`
- Keep `additionalProperties: false` — this means any field NOT in the schema is rejected

### 2. `middleware/validator.js` — Range Checks & Anomaly Detection
- Add each new field to the `RANGES` object with warning thresholds
- Boolean fields: mark with `{ type: "boolean" }` and skip them in the numeric range-check loop
- Numeric fields: set `{ min, max, warn_max, warn_min }` where `warn_max` is the "anomaly" threshold (e.g., wind_speed_kmh warn_max=80 means >80km/h gets flagged)
- Update the range-check loop to handle boolean fields: `if (range.type === "boolean") continue`

### 3. `services/influx.service.js` — InfluxDB Writer
- Add each new field as either `.floatField()` or `.booleanField()` on the Point builder
- **Critical**: boolean fields MUST use `.booleanField()` (e.g., `booleanField("rain_detected", data.rain_detected || false)`)
- **Critical**: numeric defaults MUST be `0` not `0.0` for the `||` fallback — but InfluxDB will store as float if the first write is float. Be consistent.
- The `flags` tag uses `.join(",")` on an array — ensure `(data.flags || [])` handles undefined

### 4. `services/ingest.service.js` — PostgreSQL Registration
- Add `sensor_types` parameter to `registerSensor()` — stores what sensors a node carries as JSONB
- Default value: `["rain_detection", "moisture", "wind_speed", "tilt"]`
- Include in the INSERT query: `INSERT INTO sensors (..., sensor_types, ...) VALUES (..., $6, ...)`
- Use `ON CONFLICT (sensor_id) DO UPDATE SET sensor_types = $6` so re-registration updates capabilities

### 5. `routes/sensor.js` — Route Handler
- Extract `sensor_types` from `req.body` in the register endpoint
- Pass it through to `registerSensor()`
- Return it in the response JSON so hardware team knows what was registered
- No changes needed on the data POST endpoint — it already accepts the full schema via `validateSensorData`

### 6. PostgreSQL Schema — ALTER TABLE
- Add the new column: `ALTER TABLE sensors ADD COLUMN sensor_types JSONB DEFAULT '["rain_detection","moisture","wind_speed","tilt"]';`
- Run via `docker exec -i <postgres-container> psql -U <user> -d <db> -c "ALTER TABLE ..."`
- This must happen BEFORE rebuilding the API container

## Rebuild & Test Sequence

After all code changes:

1. **ALTER TABLE first** (on running PostgreSQL) — add new columns before API restart
2. **Rebuild sensor-api container**: `docker compose build sensor-api`
3. **Restart**: `docker compose up -d sensor-api`
4. **Register a sensor with sensor_types**:
   ```bash
   curl -X POST http://localhost:3000/api/v1/sensor/register \
     -H "Content-Type: application/json" \
     -d '{"sensor_id":"HW-001","zone_id":"SINDHUPALCHOK-05","lat":27.85,"lng":85.78,"sensor_types":["rain_detection","moisture","wind_speed","tilt"]}'
   ```
5. **Send data with all sensor fields**:
   ```bash
   curl -X POST http://localhost:3000/api/v1/sensor/data \
     -H "Content-Type: application/json" \
     -H "X-Device-Key: <key_from_step_4>" \
     -d '{"sensor_id":"HW-001","timestamp":"...","rain_detected":true,"moisture_pct":65,"wind_speed_kmh":12.5,"tilt_deg":2.3,"battery_pct":92,"gps_lat":27.85,"gps_lng":85.78}'
   ```
6. **Verify InfluxDB**: query `sensor_reading` measurement — new fields should appear
7. **Verify PostgreSQL**: `SELECT sensor_id, sensor_types FROM sensors;`

## Sensor Type Mapping for GeoGuard Hardware

| Physical Sensor | API Field | Type | InfluxDB Method | Range/Thresholds |
|---|---|---|---|---|
| Rain detection sensor | `rain_detected` | boolean | `.booleanField()` | true/false (no range check) |
| Moisture sensor | `moisture_pct` | number | `.floatField()` | 0-100, no warn threshold |
| Wind speed sensor | `wind_speed_kmh` | number | `.floatField()` | 0-200, warn_max=80 |
| Tiltometer | `tilt_deg` | number | `.floatField()` | 0-90, warn_max=45 |
| Battery | `battery_pct` | number | `.floatField()` | 0-100, warn_min=15 |

## Key Lessons

- **additionalProperties: false** in JSON Schema means any undeclared field is rejected with 400 — ALL new fields must be in the schema
- **Boolean fields need special handling** at every layer: schema (`type: boolean`), validator (skip range checks), InfluxDB (`booleanField` not `floatField`), defaults (`false` not `0`)
- **sensor_types JSONB** lets each hardware node declare its capabilities — useful for the Risk Engine to know which data streams to expect per zone
- **Always rebuild after schema changes** — the Node.js app caches the compiled Ajv validator; changes to `sensor_schema.json` won't take effect until container rebuild
- **ALTER TABLE before rebuild** — if the INSERT query references a column that doesn't exist yet, the API will crash on registration