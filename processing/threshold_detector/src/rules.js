/**
 * GeoGuard Threshold Definitions
 *
 * Nepal-adapted thresholds for immediate alert triggering.
 * These thresholds are checked in real-time against incoming sensor data
 * via MQTT, BEFORE the 15-minute Risk Engine scoring cycle.
 *
 * When any threshold is breached, the Threshold Detector publishes an
 * immediate alert to MQTT and inserts an alert record in PostgreSQL.
 */

const THRESHOLDS = {
  // ── Ground tilt (degrees) ──────────────────────────────────
  tilt: {
    moderate: 3.0,   // Slow movement detected — alert District EOC
    high: 5.0,       // Accelerating deformation — alert ward leaders
    critical: 8.0,   // Rapid ground displacement — evacuate immediately
    label: "Ground tilt anomaly",
    label_ne: "जमिन झुक असामान्य",
  },

  // ── Vibration (g-force) ────────────────────────────────────
  vibration: {
    moderate: 0.15,
    high: 0.30,
    critical: 0.50,
    label: "Vibration anomaly",
    label_ne: "भूकम्पी असामान्य",
  },

  // ── Soil moisture (%) ──────────────────────────────────────
  moisture: {
    moderate: 55.0,  // Elevated saturation — monitor
    high: 75.0,      // Critical saturation — prepare evacuation
    critical: 90.0,  // Near-full saturation — imminent failure
    label: "Soil saturation critical",
    label_ne: "माटो स्याचुरेसन क्रिटिकल",
  },

  // ── Rainfall rate (mm in last reading) ──────────────────────
  rainfall_instant: {
    moderate: 15.0,  // Moderate rain
    high: 30.0,      // Heavy rain
    critical: 55.0,  // Extreme monsoon burst
    label: "Extreme rainfall burst",
    label_ne: "चरम वर्षा",
  },

  // ── Battery (%) — operational threshold ──────────────────────
  battery: {
    low: 20.0,       // Battery low — maintenance alert
    critical: 5.0,   // Battery critical — sensor may go offline
    label: "Sensor battery critical",
    label_ne: "सेन्सर ब्याट्री क्रिटिकल",
  },
};

module.exports = THRESHOLDS;