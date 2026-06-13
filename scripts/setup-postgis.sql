-- GeoGuard PostgreSQL + PostGIS initialization

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── Zone definitions ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS zones (
    zone_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    name_ne VARCHAR(200),
    district VARCHAR(100) NOT NULL,
    province VARCHAR(100) NOT NULL,
    priority INTEGER NOT NULL DEFAULT 99,
    center_lat DOUBLE PRECISION NOT NULL,
    center_lng DOUBLE PRECISION NOT NULL,
    radius_km DOUBLE PRECISION DEFAULT 5.0,
    risk_level VARCHAR(20) DEFAULT 'LOW',
    notes TEXT,
    geom GEOMETRY(Polygon, 4326)
);

-- ── DHM station metadata ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS dhm_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(200),
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    elevation_m DOUBLE PRECISION,
    district VARCHAR(100),
    geom GEOMETRY(Point, 4326)
);

-- Station-to-zone mapping
CREATE TABLE IF NOT EXISTS dhm_station_zone (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    station_id VARCHAR(50) REFERENCES dhm_stations(station_id),
    zone_id VARCHAR(50) REFERENCES zones(zone_id),
    distance_km DOUBLE PRECISION,
    is_primary BOOLEAN DEFAULT FALSE,
    UNIQUE(station_id, zone_id)
);

-- ── Satellite data catalog ───────────────────────────────────
CREATE TABLE IF NOT EXISTS satellite_data (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    zone_id VARCHAR(50) REFERENCES zones(zone_id),
    source VARCHAR(50) NOT NULL,  -- 'nasa_gpm', 'sentinel1', 'sentinel2'
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    data_type VARCHAR(50) NOT NULL,  -- 'rainfall', 'deformation', 'ndvi'
    value DOUBLE PRECISION,
    metadata JSONB,
    is_stale BOOLEAN DEFAULT FALSE,
    UNIQUE(zone_id, source, data_type, timestamp)
);

-- ── OSM road segments (for route optimizer) ──────────────────
CREATE TABLE IF NOT EXISTS osm_roads (
    osm_id BIGINT PRIMARY KEY,
    name VARCHAR(200),
    highway_type VARCHAR(50),
    surface VARCHAR(50),
    maxspeed INTEGER,
    risk_score DOUBLE PRECISION DEFAULT 0,
    geom GEOMETRY(LineString, 4326)
);

-- ── OSM settlements ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS osm_settlements (
    osm_id BIGINT PRIMARY KEY,
    name VARCHAR(200),
    population INTEGER,
    geom GEOMETRY(Point, 4326)
);

-- ── Sensor registry ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sensors (
    sensor_id VARCHAR(50) PRIMARY KEY,
    zone_id VARCHAR(50) REFERENCES zones(zone_id),
    api_key VARCHAR(200) UNIQUE NOT NULL,
    lat DOUBLE PRECISION,
    lng DOUBLE PRECISION,
    deployed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_seen TIMESTAMP WITH TIME ZONE,
    battery_pct DOUBLE PRECISION,
    status VARCHAR(20) DEFAULT 'inactive',  -- active, inactive, offline, maintenance
    mode VARCHAR(20) DEFAULT 'normal'       -- normal, elevated
);

-- ── Audit log ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_log (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    sensor_id VARCHAR(50),
    action VARCHAR(50) NOT NULL,  -- 'data_received', 'validation_failed', 'anomaly_flagged', 'duplicate'
    details JSONB
);

-- ── Indexes ──────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_zones_geom ON zones USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_dhm_stations_geom ON dhm_stations USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_satellite_data_zone_ts ON satellite_data(zone_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_osm_roads_geom ON osm_roads USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_sensors_zone ON sensors(zone_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_ts ON audit_log(timestamp);

-- ── Seed: insert DHM station-to-zone mappings ────────────────
-- (These will be populated by the DHM connector at runtime)

-- ═══════════════════════════════════════════════════════════════
-- PROCESSING LAYER — Risk Scores & Alerts
-- ═══════════════════════════════════════════════════════════════

-- ── Risk scores — current state (upsert per zone) ────────────
CREATE TABLE IF NOT EXISTS risk_scores_current (
    zone_id VARCHAR(50) PRIMARY KEY REFERENCES zones(zone_id),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    risk_level VARCHAR(20) NOT NULL,  -- LOW | MODERATE | HIGH | CRITICAL
    primary_driver VARCHAR(200),
    confidence DOUBLE PRECISION DEFAULT 0.5,
    recommended_action TEXT,
    recommended_action_ne TEXT,
    rainfall_subscore DOUBLE PRECISION DEFAULT 0,
    ground_condition_subscore DOUBLE PRECISION DEFAULT 0,
    static_risk_subscore DOUBLE PRECISION DEFAULT 0,
    satellite_subscore DOUBLE PRECISION DEFAULT 0,
    soil_moisture_pct DOUBLE PRECISION,
    ground_tilt_deg DOUBLE PRECISION,
    vibration_g DOUBLE PRECISION,
    rainfall_1hr_mm DOUBLE PRECISION,
    rainfall_6hr_mm DOUBLE PRECISION,
    rainfall_24hr_mm DOUBLE PRECISION,
    rainfall_72hr_mm DOUBLE PRECISION,
    slope_angle_deg DOUBLE PRECISION DEFAULT 0,
    ndvi_index DOUBLE PRECISION,
    deformation_flag BOOLEAN DEFAULT FALSE
);

-- ── Risk scores — history log (append-only) ──────────────────
CREATE TABLE IF NOT EXISTS risk_scores_history (
    id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    zone_id VARCHAR(50) NOT NULL REFERENCES zones(zone_id),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    risk_score DOUBLE PRECISION NOT NULL,
    risk_level VARCHAR(20) NOT NULL,
    primary_driver VARCHAR(200),
    confidence DOUBLE PRECISION DEFAULT 0.5,
    recommended_action TEXT,
    recommended_action_ne TEXT
);

-- ── Alerts — threshold breach notifications ──────────────────
CREATE TABLE IF NOT EXISTS alerts (
    alert_id VARCHAR(200) PRIMARY KEY,
    zone_id VARCHAR(50) NOT NULL REFERENCES zones(zone_id),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    metric VARCHAR(50) NOT NULL,  -- tilt | vibration | moisture | rainfall_instant | battery
    level VARCHAR(20) NOT NULL,    -- MODERATE | HIGH | CRITICAL
    value DOUBLE PRECISION,
    threshold_breached DOUBLE PRECISION,
    label VARCHAR(200),
    label_ne VARCHAR(200),
    sensor_id VARCHAR(50),
    recommended_action TEXT,
    recommended_action_ne TEXT,
    sensor_data JSONB,
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP WITH TIME ZONE
);

-- ── Processing indexes ────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_risk_history_zone_ts ON risk_scores_history(zone_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_risk_history_level ON risk_scores_history(risk_level, timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_zone_ts ON alerts(zone_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_level ON alerts(level, timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_unack ON alerts(zone_id, level) WHERE acknowledged = FALSE;
