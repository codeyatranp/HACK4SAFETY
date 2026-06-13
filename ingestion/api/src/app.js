const express = require("express");
const cors = require("cors");
const helmet = require("helmet");
const sensorRoutes = require("./routes/sensor");
const { connectMQTT } = require("./mqtt/broker");

const app = express();
const PORT = process.env.SENSOR_API_PORT || 3000;

// ── Middleware ───────────────────────────────────────────────
app.use(helmet());
app.use(cors({ origin: "*" })); // Tighten in production
app.use(express.json({ limit: "1mb" }));

// ── Health check ─────────────────────────────────────────────
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "geoguard-sensor-api",
    timestamp: new Date().toISOString(),
    uptime: process.uptime(),
  });
});

// ── Routes ───────────────────────────────────────────────────
app.use("/api/v1/sensor", sensorRoutes);

// ── 404 handler ──────────────────────────────────────────────
app.use((req, res) => {
  res.status(404).json({ error: "Not found" });
});

// ── Error handler ────────────────────────────────────────────
app.use((err, req, res, _next) => {
  console.error("[API Error]", err.message);
  res.status(500).json({ error: "Internal server error" });
});

// ── Start ────────────────────────────────────────────────────
app.listen(PORT, "0.0.0.0", () => {
  console.log(`[Sensor API] Listening on port ${PORT}`);
  connectMQTT();
});

module.exports = app;
