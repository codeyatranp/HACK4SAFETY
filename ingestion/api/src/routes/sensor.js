const express = require("express");
const router = express.Router();
const { deviceAuth } = require("../middleware/auth");
const { validateSensorData, detectDuplicate } = require("../middleware/validator");
const {
  writeSensorReading,
  queryRecentReadings,
} = require("../services/influx.service");
const {
  registerSensor,
  updateSensorStatus,
  setSensorMode,
  pool,
} = require("../services/ingest.service");
const { publishToMQTT } = require("../mqtt/broker");

// ── POST /api/v1/sensor/data ────────────────────────────────
// Receive sensor readings from hardware team
router.post("/data", deviceAuth, validateSensorData, detectDuplicate, async (req, res) => {
  try {
    const data = {
      ...req.body,
      zone_id: req.sensor.zone_id,
      flags: req.flags,
    };

    // Write to InfluxDB (time-series)
    await writeSensorReading(data);

    // Update sensor status in PostgreSQL
    await updateSensorStatus(data.sensor_id, data.battery_pct);

    // Forward to MQTT for real-time consumers
    await publishToMQTT(`geoguard/sensor/data/${data.sensor_id}`, data);

    // Log audit entry
    await pool.query(
      `INSERT INTO audit_log (sensor_id, action, details)
       VALUES ($1, $2, $3)`,
      [
        data.sensor_id,
        req.isDuplicate ? "duplicate" : "data_received",
        JSON.stringify({ flags: data.flags, zone_id: data.zone_id }),
      ]
    );

    res.status(200).json({
      status: "accepted",
      sensor_id: data.sensor_id,
      received_at: new Date().toISOString(),
      validation: req.flags.length > 0 ? "passed_with_flags" : "passed",
      flags: req.flags,
      is_duplicate: req.isDuplicate,
    });
  } catch (err) {
    console.error("[Sensor Data Error]", err.message);
    res.status(500).json({ error: "Failed to process sensor data" });
  }
});

// ── GET /api/v1/sensor/data/:sensor_id ──────────────────────
// Query sensor history
router.get("/data/:sensor_id", async (req, res) => {
  try {
    const { sensor_id } = req.params;
    const hours = parseInt(req.query.hours) || 24;

    const readings = await queryRecentReadings(sensor_id, hours);

    res.json({
      sensor_id,
      hours,
      count: readings.length,
      readings,
    });
  } catch (err) {
    console.error("[Query Error]", err.message);
    res.status(500).json({ error: "Failed to query sensor data" });
  }
});

// ── POST /api/v1/sensor/register ─────────────────────────────
// Register a new sensor node
router.post("/register", async (req, res) => {
  try {
    const { sensor_id, zone_id, lat, lng, sensor_types } = req.body;

    if (!sensor_id || !zone_id) {
      return res.status(400).json({
        error: "Missing required fields",
        detail: "sensor_id and zone_id are required",
      });
    }

    const result = await registerSensor(sensor_id, zone_id, lat, lng, sensor_types);

    res.status(201).json({
      message: "Sensor registered successfully",
      sensor_id: result.sensor_id,
      api_key: result.api_key,
      sensor_types: result.sensor_types,
      note: "Store this API key securely — it cannot be retrieved again",
    });
  } catch (err) {
    console.error("[Register Error]", err.message);
    res.status(500).json({ error: "Failed to register sensor" });
  }
});

// ── GET /api/v1/sensor/status ───────────────────────────────
// Network health dashboard data
router.get("/status", async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        s.sensor_id,
        s.zone_id,
        z.name AS zone_name,
        s.status,
        s.mode,
        s.battery_pct,
        s.last_seen,
        s.deployed_at,
        CASE 
          WHEN s.last_seen IS NULL THEN 999999
          ELSE EXTRACT(EPOCH FROM (NOW() - s.last_seen))
        END AS seconds_since_last_ping
      FROM sensors s
      LEFT JOIN zones z ON s.zone_id = z.zone_id
      ORDER BY seconds_since_last_ping DESC
    `);

    const summary = {
      total: result.rows.length,
      active: result.rows.filter((r) => r.status === "active").length,
      offline: result.rows.filter((r) => r.status === "offline").length,
      maintenance: result.rows.filter((r) => r.status === "maintenance").length,
    };

    res.json({ summary, sensors: result.rows });
  } catch (err) {
    console.error("[Status Error]", err.message);
    res.status(500).json({ error: "Failed to get sensor status" });
  }
});

// ── PUT /api/v1/sensor/:sensor_id/mode ───────────────────────
// Switch normal/elevated mode
router.put("/sensor/:sensor_id/mode", async (req, res) => {
  try {
    const { sensor_id } = req.params;
    const { mode } = req.body;

    if (!["normal", "elevated"].includes(mode)) {
      return res.status(400).json({
        error: "Invalid mode",
        detail: "Mode must be 'normal' or 'elevated'",
      });
    }

    await setSensorMode(sensor_id, mode);

    res.json({
      sensor_id,
      mode,
      message: `Sensor mode set to ${mode} (${mode === "elevated" ? "10s interval" : "60s interval"})`,
    });
  } catch (err) {
    console.error("[Mode Error]", err.message);
    res.status(500).json({ error: "Failed to update sensor mode" });
  }
});

module.exports = router;
