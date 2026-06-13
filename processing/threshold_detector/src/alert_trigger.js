/**
 * GeoGuard Alert Trigger — publishes immediate alerts when thresholds breach.
 *
 * When the Threshold Detector identifies a threshold breach, this module:
 * 1. Publishes an alert to MQTT (geoguard/alerts/immediate)
 * 2. Inserts an alert record into PostgreSQL (alerts table)
 * 3. Logs the alert with severity level
 *
 * Alert suppression: same zone + same metric gets max 1 alert per 30 min,
 * unless risk level escalates (e.g., moderate → high).
 */

const MQTT_ALERT_TOPIC = process.env.MQTT_ALERT_TOPIC || "geoguard/alerts/immediate";

// ── Alert suppression tracking ────────────────────────────────
const recentAlerts = new Map(); // zone_id:metric → { level, timestamp }

const SUPPRESSION_WINDOW_MS = 30 * 60 * 1000; // 30 minutes

class AlertTrigger {
  constructor(mqttClient, pgPool) {
    this.mqttClient = mqttClient;
    this.pgPool = pgPool;
  }

  /**
   * Trigger an alert for a zone if not suppressed.
   *
   * @param {string} zoneId - Zone identifier
   * @param {string} metric - Which threshold was breached (tilt, vibration, etc.)
   * @param {string} level - Alert level: MODERATE | HIGH | CRITICAL
   * @param {object} data - Full sensor data payload
   * @param {object} thresholdConfig - Threshold definition with labels
   */
  async trigger(zoneId, metric, level, data, thresholdConfig) {
    const key = `${zoneId}:${metric}`;
    const now = Date.now();
    const previous = recentAlerts.get(key);

    // ── Suppression check ──────────────────────────────────
    if (previous) {
      const timeSinceLast = now - previous.timestamp;
      const levelOrder = { MODERATE: 1, HIGH: 2, CRITICAL: 3 };
      const currentOrder = levelOrder[level] || 0;
      const previousOrder = levelOrder[previous.level] || 0;

      // Suppress if within window AND not an escalation
      if (timeSinceLast < SUPPRESSION_WINDOW_MS && currentOrder <= previousOrder) {
        return; // Suppressed
      }
    }

    // ── Build alert payload ─────────────────────────────────
    const metricToField = {
      tilt: "tilt_deg",
      vibration: "vibration_g",
      moisture: "moisture_pct",
      rainfall_instant: "rainfall_mm",
      battery: "battery_pct",
    };
    const dataField = metricToField[metric] || metric;
    const alert = {
      alert_id: `ALT-${zoneId}-${metric}-${now}`,
      zone_id: zoneId,
      timestamp: new Date().toISOString(),
      metric: metric,
      level: level,
      value: data[dataField] ?? null,
      threshold_breached: thresholdConfig[level.toLowerCase()],
      label: thresholdConfig.label,
      label_ne: thresholdConfig.label_ne,
      sensor_id: data.sensor_id || "UNKNOWN",
      recommended_action: this._getAction(level),
      recommended_action_ne: this._getActionNe(level),
      sensor_data: data,
    };

    // ── Publish to MQTT ─────────────────────────────────────
    if (this.mqttClient && this.mqttClient.connected) {
      this.mqttClient.publish(MQTT_ALERT_TOPIC, JSON.stringify(alert), { qos: 1 });
    }

    // ── Insert into PostgreSQL ───────────────────────────────
    if (this.pgPool) {
      try {
        await this.pgPool.query(
          `INSERT INTO alerts (
            alert_id, zone_id, timestamp, metric, level,
            value, threshold_breached, label, label_ne,
            sensor_id, recommended_action, recommended_action_ne, sensor_data
          ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
          ON CONFLICT (alert_id) DO NOTHING`,
          [
            alert.alert_id, alert.zone_id, alert.timestamp,
            alert.metric, alert.level, alert.value,
            alert.threshold_breached, alert.label, alert.label_ne,
            alert.sensor_id, alert.recommended_action,
            alert.recommended_action_ne, JSON.stringify(alert.sensor_data),
          ]
        );
      } catch (err) {
        console.error("[ALERT-TRIGGER] PostgreSQL insert failed:", err.message);
      }
    }

    // ── Update suppression tracking ──────────────────────────
    recentAlerts.set(key, { level, timestamp: now });

    console.log(
      `[ALERT-TRIGGER] Zone ${zoneId}: ${level} ${metric} alert triggered ` +
      `(value=${alert.value}, threshold=${alert.threshold_breached})`
    );
  }

  _getAction(level) {
    const actions = {
      MODERATE: "Monitor closely. Alert District EOC.",
      HIGH: "Prepare evacuation. Alert ward leaders and police.",
      CRITICAL: "Evacuate immediately. All alert channels activated.",
    };
    return actions[level] || "Monitor.";
  }

  _getActionNe(level) {
    const actions = {
      MODERATE: "नजिकबाट अवलोकन गर्नुहोस्। जिल्ला EOC लाई सूचना।",
      HIGH: "स्थानान्तरणको तयारी। वडा अधिकारी र प्रहरीलाई सूचना।",
      CRITICAL: "तत्काल स्थानान्तरण। सबै सूचना माध्यम सक्रिय।",
    };
    return actions[level] || "अवलोकन गर्नुहोस्।";
  }
}

module.exports = AlertTrigger;