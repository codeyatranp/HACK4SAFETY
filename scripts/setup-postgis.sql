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

-- ═══════════════════════════════════════════════════════════════
-- EXTERNAL SOURCE TABLES — Dashboard live integrations
-- ═══════════════════════════════════════════════════════════════

-- ── DHM official station readings ──────────────────────────────
CREATE TABLE IF NOT EXISTS dhm_station_readings (
    station_id VARCHAR(100) PRIMARY KEY,
    station_name VARCHAR(255),
    district VARCHAR(120),
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    elevation_m DOUBLE PRECISION,
    rain_1hr DOUBLE PRECISION,
    rain_3hr DOUBLE PRECISION,
    rain_6hr DOUBLE PRECISION,
    rain_12hr DOUBLE PRECISION,
    rain_24hr DOUBLE PRECISION,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    source VARCHAR(60) NOT NULL DEFAULT 'dhm',
    warning_level VARCHAR(20) NOT NULL DEFAULT 'safe',
    status VARCHAR(20) NOT NULL DEFAULT 'online', -- online | offline
    raw_payload JSONB
);

-- ── NASA COOLR / global landslide catalog (Nepal subset) ─────
CREATE TABLE IF NOT EXISTS landslide_catalog (
    event_id VARCHAR(120) PRIMARY KEY,
    event_date TIMESTAMP WITH TIME ZONE,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    district VARCHAR(120),
    province VARCHAR(120),
    type VARCHAR(120),
    fatalities INTEGER DEFAULT 0,
    injuries INTEGER DEFAULT 0,
    trigger VARCHAR(80),
    source_url TEXT,
    imported_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    raw_payload JSONB
);

-- ── BIPAD incidents ────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bipad_incidents (
    bipad_id VARCHAR(120) PRIMARY KEY,
    title TEXT,
    hazard VARCHAR(80),
    district_id VARCHAR(60),
    district_name VARCHAR(120),
    province VARCHAR(120),
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    deaths INTEGER DEFAULT 0,
    missing INTEGER DEFAULT 0,
    injured INTEGER DEFAULT 0,
    families_affected INTEGER DEFAULT 0,
    incident_date TIMESTAMP WITH TIME ZONE,
    verified BOOLEAN DEFAULT FALSE,
    source_url TEXT,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    raw_payload JSONB
);

-- ── BIPAD active alerts ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bipad_alerts (
    alert_id VARCHAR(120) PRIMARY KEY,
    title TEXT,
    hazard VARCHAR(80),
    district_id VARCHAR(60),
    district_name VARCHAR(120),
    province VARCHAR(120),
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    severity VARCHAR(30),
    status VARCHAR(30),
    alert_date TIMESTAMP WITH TIME ZONE,
    expiry_date TIMESTAMP WITH TIME ZONE,
    source_url TEXT,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    raw_payload JSONB
);

CREATE INDEX IF NOT EXISTS idx_dhm_station_readings_fetched_at ON dhm_station_readings(fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_dhm_station_readings_warning ON dhm_station_readings(warning_level, rain_24hr DESC);
CREATE INDEX IF NOT EXISTS idx_landslide_catalog_event_date ON landslide_catalog(event_date DESC);
CREATE INDEX IF NOT EXISTS idx_landslide_catalog_trigger ON landslide_catalog(trigger);
CREATE INDEX IF NOT EXISTS idx_landslide_catalog_lat_lon ON landslide_catalog(lat, lon);
CREATE INDEX IF NOT EXISTS idx_bipad_incidents_date_hazard ON bipad_incidents(incident_date DESC, hazard);
CREATE INDEX IF NOT EXISTS idx_bipad_alerts_status_date ON bipad_alerts(status, alert_date DESC);
