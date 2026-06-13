const { Pool } = require("pg");

const pool = new Pool({
  host: process.env.POSTGRES_HOST || "localhost",
  port: parseInt(process.env.POSTGRES_PORT || "5432"),
  user: process.env.POSTGRES_USER || "geoguard",
  password: process.env.POSTGRES_PASSWORD || "geoguard_admin",
  database: process.env.POSTGRES_DB || "geoguard",
});

/**
 * Validate device API key from the X-Device-Key header.
 * Looks up the key in the sensors table.
 */
async function validateDeviceKey(apiKey) {
  if (!apiKey) return null;

  const result = await pool.query(
    "SELECT sensor_id, zone_id, status, mode FROM sensors WHERE api_key = $1",
    [apiKey]
  );

  if (result.rows.length === 0) return null;

  const sensor = result.rows[0];
  return {
    sensor_id: sensor.sensor_id,
    zone_id: sensor.zone_id,
    status: sensor.status,
    mode: sensor.mode,
  };
}

/**
 * Register a new sensor with a generated API key.
 */
async function registerSensor(sensorId, zoneId, lat, lng, sensorTypes) {
  const apiKey = `sk_${sensorId}_${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;

  const typesJson = JSON.stringify(sensorTypes || ["rain_detection", "moisture", "wind_speed", "tilt"]);

  await pool.query(
    `INSERT INTO sensors (sensor_id, zone_id, api_key, lat, lng, sensor_types, status)
     VALUES ($1, $2, $3, $4, $5, $6, 'active')
     ON CONFLICT (sensor_id) DO UPDATE SET sensor_types = $6, zone_id = $2`,
    [sensorId, zoneId, apiKey, lat, lng, typesJson]
  );

  return { sensor_id: sensorId, api_key: apiKey, sensor_types: sensorTypes };
}

/**
 * Update sensor last_seen and battery status.
 */
async function updateSensorStatus(sensorId, batteryPct) {
  await pool.query(
    "UPDATE sensors SET last_seen = NOW(), battery_pct = $1, status = 'active' WHERE sensor_id = $2",
    [batteryPct, sensorId]
  );
}

/**
 * Switch sensor mode between normal and elevated.
 */
async function setSensorMode(sensorId, mode) {
  await pool.query(
    "UPDATE sensors SET mode = $1 WHERE sensor_id = $2",
    [mode, sensorId]
  );
}

module.exports = {
  pool,
  validateDeviceKey,
  registerSensor,
  updateSensorStatus,
  setSensorMode,
};
