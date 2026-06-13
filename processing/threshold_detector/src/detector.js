/**
 * GeoGuard Event Threshold Detector — MQTT stream processor.
 *
 * Monitors incoming sensor data via MQTT in real-time.
 * When any threshold is breached, triggers immediate alerts
 * BEFORE the 15-minute Risk Engine scoring cycle completes.
 *
 * This provides real-time response capability for critical events
 * like sudden ground tilt, extreme vibration, or rainfall bursts.
 *
 * Architecture:
 *   MQTT consumer → threshold evaluation → alert trigger → MQTT publish + PostgreSQL
 */
require("dotenv").config();
const mqtt = require("mqtt");
const { Pool } = require("pg");
const THRESHOLDS = require("./rules");
const AlertTrigger = require("./alert_trigger");

const MQTT_HOST = process.env.MQTT_HOST || "mosquitto";
const MQTT_PORT = parseInt(process.env.MQTT_PORT || "1883", 10);
const MQTT_TOPIC = process.env.MQTT_TOPIC || "geoguard/sensor/data/+";

const POSTGRES_HOST = process.env.POSTGRES_HOST || "localhost";
const POSTGRES_PORT = parseInt(process.env.POSTGRES_PORT || "5432", 10);
const POSTGRES_USER = process.env.POSTGRES_USER || "geoguard";
const POSTGRES_PASSWORD = process.env.POSTGRES_PASSWORD || "geoguard_admin";
const POSTGRES_DB = process.env.POSTGRES_DB || "geoguard";

// ── PostgreSQL connection ──────────────────────────────────────
const pgPool = new Pool({
  host: POSTGRES_HOST,
  port: POSTGRES_PORT,
  user: POSTGRES_USER,
  password: POSTGRES_PASSWORD,
  database: POSTGRES_DB,
  max: 5,
  idleTimeoutMillis: 30000,
});

pgPool.on("error", (err) => {
  console.error("[THRESHOLD-DETECTOR] PostgreSQL pool error:", err.message);
});

// ── Zone mapping cache (sensor_id → zone_id) ──────────────────
const sensorZoneMap = new Map();

async function loadSensorZoneMap() {
  try {
    const result = await pgPool.query(
      "SELECT sensor_id, zone_id FROM sensors WHERE status = 'active'"
    );
    result.rows.forEach((row) => sensorZoneMap.set(row.sensor_id, row.zone_id));
    console.log(
      `[THRESHOLD-DETECTOR] Loaded ${sensorZoneMap.size} sensor-zone mappings`
    );
  } catch (err) {
    console.error("[THRESHOLD-DETECTOR] Failed to load sensor zone map:", err.message);
  }
}

// ── Threshold evaluation ───────────────────────────────────────
function evaluateThresholds(data) {
  /** Evaluate sensor data against all thresholds. Returns list of breaches. */
  const breaches = [];

  // ── Tilt ─────────────────────────────────────────────────────
  if (data.tilt_deg !== undefined && data.tilt_deg !== null) {
    if (data.tilt_deg >= THRESHOLDS.tilt.critical) {
      breaches.push({ metric: "tilt", level: "CRITICAL", value: data.tilt_deg });
    } else if (data.tilt_deg >= THRESHOLDS.tilt.high) {
      breaches.push({ metric: "tilt", level: "HIGH", value: data.tilt_deg });
    } else if (data.tilt_deg >= THRESHOLDS.tilt.moderate) {
      breaches.push({ metric: "tilt", level: "MODERATE", value: data.tilt_deg });
    }
  }

  // ── Vibration ─────────────────────────────────────────────────
  if (data.vibration_g !== undefined && data.vibration_g !== null) {
    if (data.vibration_g >= THRESHOLDS.vibration.critical) {
      breaches.push({ metric: "vibration", level: "CRITICAL", value: data.vibration_g });
    } else if (data.vibration_g >= THRESHOLDS.vibration.high) {
      breaches.push({ metric: "vibration", level: "HIGH", value: data.vibration_g });
    } else if (data.vibration_g >= THRESHOLDS.vibration.moderate) {
      breaches.push({ metric: "vibration", level: "MODERATE", value: data.vibration_g });
    }
  }

  // ── Soil moisture ─────────────────────────────────────────────
  if (data.moisture_pct !== undefined && data.moisture_pct !== null) {
    if (data.moisture_pct >= THRESHOLDS.moisture.critical) {
      breaches.push({ metric: "moisture", level: "CRITICAL", value: data.moisture_pct });
    } else if (data.moisture_pct >= THRESHOLDS.moisture.high) {
      breaches.push({ metric: "moisture", level: "HIGH", value: data.moisture_pct });
    } else if (data.moisture_pct >= THRESHOLDS.moisture.moderate) {
      breaches.push({ metric: "moisture", level: "MODERATE", value: data.moisture_pct });
    }
  }

  // ── Rainfall (instant) ────────────────────────────────────────
  if (data.rainfall_mm !== undefined && data.rainfall_mm !== null) {
    if (data.rainfall_mm >= THRESHOLDS.rainfall_instant.critical) {
      breaches.push({ metric: "rainfall_instant", level: "CRITICAL", value: data.rainfall_mm });
    } else if (data.rainfall_mm >= THRESHOLDS.rainfall_instant.high) {
      breaches.push({ metric: "rainfall_instant", level: "HIGH", value: data.rainfall_mm });
    } else if (data.rainfall_mm >= THRESHOLDS.rainfall_instant.moderate) {
      breaches.push({ metric: "rainfall_instant", level: "MODERATE", value: data.rainfall_mm });
    }
  }

  // ── Battery (operational — not a risk alert, maintenance only) ──
  if (data.battery_pct !== undefined && data.battery_pct !== null) {
    if (data.battery_pct <= THRESHOLDS.battery.critical) {
      breaches.push({ metric: "battery", level: "CRITICAL", value: data.battery_pct });
    } else if (data.battery_pct <= THRESHOLDS.battery.low) {
      breaches.push({ metric: "battery", level: "MODERATE", value: data.battery_pct });
    }
  }

  return breaches;
}

// ── MQTT client ─────────────────────────────────────────────────
const mqttClient = mqtt.connect(`mqtt://${MQTT_HOST}:${MQTT_PORT}`, {
  clientId: "geoguard-threshold-detector",
  clean: true,
  reconnectPeriod: 5000,
});

const alertTrigger = new AlertTrigger(mqttClient, pgPool);

mqttClient.on("connect", () => {
  console.log(`[THRESHOLD-DETECTOR] Connected to MQTT broker at ${MQTT_HOST}:${MQTT_PORT}`);
  mqttClient.subscribe(MQTT_TOPIC, { qos: 1 }, (err) => {
    if (err) {
      console.error("[THRESHOLD-DETECTOR] MQTT subscribe failed:", err.message);
    } else {
      console.log(`[THRESHOLD-DETECTOR] Subscribed to ${MQTT_TOPIC}`);
    }
  });
});

mqttClient.on("message", (topic, message) => {
  try {
    const data = JSON.parse(message.toString());

    // ── Resolve zone_id ──────────────────────────────────────
    const zoneId = sensorZoneMap.get(data.sensor_id) || data.zone_id || "UNKNOWN";

    // ── Evaluate thresholds ──────────────────────────────────
    const breaches = evaluateThresholds(data);

    if (breaches.length === 0) {
      return; // No threshold breach — normal data, skip
    }

    // ── Trigger alerts for each breach ───────────────────────
    for (const breach of breaches) {
      const thresholdConfig = THRESHOLDS[breach.metric];
      alertTrigger.trigger(zoneId, breach.metric, breach.level, data, thresholdConfig);
    }

  } catch (err) {
    console.error("[THRESHOLD-DETECTOR] Message processing failed:", err.message);
  }
});

mqttClient.on("error", (err) => {
  console.error("[THRESHOLD-DETECTOR] MQTT error:", err.message);
});

mqttClient.on("close", () => {
  console.log("[THRESHOLD-DETECTOR] MQTT connection closed");
});

// ── Startup ─────────────────────────────────────────────────────
async function start() {
  await loadSensorZoneMap();

  // Refresh sensor-zone mapping every 5 minutes
  setInterval(loadSensorZoneMap, 5 * 60 * 1000);

  console.log(
    "[THRESHOLD-DETECTOR] Started. Monitoring sensor data for threshold breaches."
  );
}

start().catch((err) => {
  console.error("[THRESHOLD-DETECTOR] Startup failed:", err.message);
  process.exit(1);
});