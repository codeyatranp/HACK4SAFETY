const Ajv = require("ajv");
const addFormats = require("ajv-formats");
const sensorSchema = require("../../shared/validation/sensor_schema.json");

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);

const validate = ajv.compile(sensorSchema);

// Nepal bounding box for geo-validation
const NEPAL_BOUNDS = {
  north: 30.4,
  south: 26.3,
  east: 88.2,
  west: 80.0,
};

// Expected value ranges for anomaly detection
const RANGES = {
  rain_detected: { type: "boolean" },
  rainfall_mm: { min: 0, max: 500, warn_max: 200 },
  moisture_pct: { min: 0, max: 100 },
  wind_speed_kmh: { min: 0, max: 200, warn_max: 80 },
  tilt_deg: { min: 0, max: 90, warn_max: 45 },
  vibration_g: { min: 0, max: 10, warn_max: 2 },
  battery_pct: { min: 0, max: 100, warn_min: 15 },
};

/**
 * Validation middleware.
 * 1. JSON Schema validation (rejects if invalid)
 * 2. Range checks (accepts but flags anomalies)
 * 3. Nepal geo-boundary check
 * 4. Timestamp freshness check
 * Returns flags array on req.flags
 */
function validateSensorData(req, res, next) {
  const flags = [];
  const data = req.body;

  // Step 1: JSON Schema validation
  const valid = validate(data);
  if (!valid) {
    return res.status(400).json({
      error: "Validation failed",
      detail: "Payload does not match expected schema",
      errors: validate.errors.map((e) => ({
        field: e.instancePath || e.schemaPath,
        message: e.message,
      })),
    });
  }

  // Step 2: Range checks (anomaly flagging)
  for (const [field, range] of Object.entries(RANGES)) {
    if (data[field] === undefined) continue;
    if (range.type === "boolean") continue;  // boolean fields have no range

    if (data[field] > range.warn_max || data[field] < (range.warn_min || range.min)) {
      flags.push(`anomaly:${field}_out_of_expected_range`);
    }
  }

  // Step 3: Geo-boundary check
  if (data.gps_lat !== undefined && data.gps_lng !== undefined) {
    const { gps_lat: lat, gps_lng: lng } = data;
    if (
      lat < NEPAL_BOUNDS.south ||
      lat > NEPAL_BOUNDS.north ||
      lng < NEPAL_BOUNDS.west ||
      lng > NEPAL_BOUNDS.east
    ) {
      flags.push("out_of_zone:sensor_outside_nepal");
    }
  }

  // Step 4: Timestamp freshness (within ±5 minutes)
  if (data.timestamp) {
    const readingTime = new Date(data.timestamp).getTime();
    const serverTime = Date.now();
    const diffSec = Math.abs(serverTime - readingTime) / 1000;

    if (diffSec > 300) {
      flags.push(`clock_drift:${Math.round(diffSec)}s_difference`);
    }
  }

  req.flags = flags;
  next();
}

/**
 * Duplicate detection — checks if same sensor_id + timestamp was received within 5s.
 */
const recentReadings = new Map(); // key: "sensor_id:rounded_ts", value: timestamp

function detectDuplicate(req, res, next) {
  const { sensor_id } = req.body;
  const timestamp = req.body.timestamp || new Date().toISOString();

  // Round timestamp to 5-second window
  const tsMs = new Date(timestamp).getTime();
  const windowKey = `${sensor_id}:${Math.floor(tsMs / 5000) * 5000}`;

  if (recentReadings.has(windowKey)) {
    req.isDuplicate = true;
    req.flags = req.flags || [];
    req.flags.push("duplicate");
  } else {
    req.isDuplicate = false;
    recentReadings.set(windowKey, Date.now());

    // Clean old entries (older than 10 seconds)
    const now = Date.now();
    for (const [key, ts] of recentReadings.entries()) {
      if (now - ts > 10000) {
        recentReadings.delete(key);
      }
    }
  }

  next();
}

module.exports = { validateSensorData, detectDuplicate };
