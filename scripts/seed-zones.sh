#!/usr/bin/env bash
# Seed GeoGuard PostgreSQL with zone and DHM station data from nepal_zones.json
set -euo pipefail

PGHOST="${POSTGRES_HOST:-localhost}"
PGPORT="${POSTGRES_PORT:-5432}"
PGUSER="${POSTGRES_USER:-geoguard}"
PGDB="${POSTGRES_DB:-geoguard}"
PGPASS="${POSTGRES_PASSWORD:-geoguard_admin}"

export PGPASSWORD="$PGPASS"

echo "[Seed] Loading zone data into PostgreSQL..."

# ── Seed zones from nepal_zones.json ──
python3 -c "
import json, sys
with open('shared/config/nepal_zones.json') as f:
    data = json.load(f)
for z in data['zones']:
    c = z['center']
    notes = z.get('notes', '').replace(\"'\", \"''\")
    r = z.get('radius_km', 5.0)
    print(f\"INSERT INTO zones (zone_id, name, name_ne, district, province, priority, center_lat, center_lng, radius_km, risk_level, notes) VALUES ('{z['zone_id']}', '{z['name']}', '{z.get('name_ne','')}', '{z['district']}', '{z['province']}', {z['priority']}, {c['lat']}, {c['lng']}, {r}, '{z['risk_level']}', '{notes}') ON CONFLICT (zone_id) DO UPDATE SET name=EXCLUDED.name, risk_level=EXCLUDED.risk_level, notes=EXCLUDED.notes;\")
" | psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB"

echo "[Seed] Loading DHM station data..."

# ── Seed DHM stations ──
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" <<'SQL'
INSERT INTO dhm_stations (station_id, name, lat, lng, elevation_m, district)
VALUES
  ('DHM-SL-001', 'Chautara Rainfall Station', 27.7750, 85.7100, 1450, 'Sindhupalchok'),
  ('DHM-PK-002', 'Pokhara Airport Station',   28.2027, 84.0004, 827,  'Kaski'),
  ('DHM-GK-001', 'Gorkha Bazaar Station',     28.0066, 84.6266, 1135, 'Gorkha')
ON CONFLICT (station_id) DO UPDATE SET name=EXCLUDED.name, elevation_m=EXCLUDED.elevation_m;

-- Map stations to zones (haversine distances pre-computed)
INSERT INTO dhm_station_zone (station_id, zone_id, distance_km, is_primary)
VALUES
  ('DHM-SL-001', 'SINDHUPALCHOK-05', 10.8, TRUE),
  ('DHM-SL-001', 'SINDHUPALCHOK-07', 20.7, FALSE),
  ('DHM-PK-002', 'KASKI-12',         16.5, TRUE),
  ('DHM-GK-001', 'GORKHA-04',         0.8, TRUE)
ON CONFLICT (station_id, zone_id) DO UPDATE SET distance_km=EXCLUDED.distance_km, is_primary=EXCLUDED.is_primary;
SQL

echo "[Seed] Done. Verifying counts..."
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" -c "SELECT count(*) AS zones FROM zones;" -c "SELECT count(*) AS stations FROM dhm_stations;" -c "SELECT count(*) AS mappings FROM dhm_station_zone;"