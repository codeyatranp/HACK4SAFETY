const mqtt = require("mqtt");

let mqttClient = null;

/**
 * Connect to the Mosquitto MQTT broker.
 * Used for real-time forwarding of sensor data.
 */
function connectMQTT() {
  const host = process.env.MQTT_HOST || "localhost";
  const port = parseInt(process.env.MQTT_PORT || "1883");

  mqttClient = mqtt.connect(`mqtt://${host}:${port}`, {
    clientId: `geoguard-api-${Date.now()}`,
    clean: true,
    connectTimeout: 5000,
    reconnectPeriod: 3000,
  });

  mqttClient.on("connect", () => {
    console.log("[MQTT] Connected to broker");
  });

  mqttClient.on("error", (err) => {
    console.error("[MQTT Error]", err.message);
  });

  mqttClient.on("close", () => {
    console.log("[MQTT] Disconnected from broker");
  });
}

/**
 * Publish a message to an MQTT topic.
 */
async function publishToMQTT(topic, payload) {
  if (!mqttClient || !mqttClient.connected) {
    console.warn("[MQTT] Not connected, skipping publish");
    return;
  }

  try {
    mqttClient.publish(topic, JSON.stringify(payload), { qos: 0 });
  } catch (err) {
    console.error("[MQTT Publish Error]", err.message);
  }
}

/**
 * Subscribe to an MQTT topic with a callback.
 */
async function subscribeToMQTT(topic, callback) {
  if (!mqttClient || !mqttClient.connected) {
    console.warn("[MQTT] Not connected, skipping subscribe");
    return;
  }

  mqttClient.subscribe(topic, { qos: 1 }, (err) => {
    if (err) {
      console.error("[MQTT Subscribe Error]", err.message);
      return;
    }
    console.log(`[MQTT] Subscribed to ${topic}`);
  });

  mqttClient.on("message", (receivedTopic, message) => {
    if (receivedTopic === topic) {
      try {
        const data = JSON.parse(message.toString());
        callback(data);
      } catch (err) {
        console.error("[MQTT Parse Error]", err.message);
      }
    }
  });
}

module.exports = {
  connectMQTT,
  publishToMQTT,
  subscribeToMQTT,
  getClient: () => mqttClient,
};
