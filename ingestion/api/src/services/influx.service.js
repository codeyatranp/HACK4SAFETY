const { InfluxDB, Point } = require("@influxdata/influxdb-client");

const influxDB = new InfluxDB({
  url: process.env.INFLUXDB_URL || "http://localhost:8086",
  token: process.env.INFLUXDB_TOKEN || "geoguard-influx-token",
});

const writeApi = influxDB.getWriteApi(
  process.env.INFLUXDB_ORG || "geoguard",
  process.env.INFLUXDB_BUCKET || "sensor-data"
);

const BUCKET = process.env.INFLUXDB_BUCKET || "sensor-data";

/**
 * Write a sensor reading to InfluxDB.
 */
async function writeSensorReading(data) {
  const point = new Point("sensor_reading")
    .tag("sensor_id", data.sensor_id)
    .tag("zone_id", data.zone_id || "")
    .tag("flags", (data.flags || []).join(","))
    .booleanField("rain_detected", data.rain_detected || false)
    .floatField("rainfall_mm", data.rainfall_mm || 0)
    .floatField("moisture_pct", data.moisture_pct || 0)
    .floatField("wind_speed_kmh", data.wind_speed_kmh || 0)
    .floatField("tilt_deg", data.tilt_deg || 0)
    .floatField("vibration_g", data.vibration_g || 0)
    .floatField("battery_pct", data.battery_pct || 0)
    .floatField("gps_lat", data.gps_lat || 0)
    .floatField("gps_lng", data.gps_lng || 0)
    .timestamp(new Date(data.timestamp));

  writeApi.writePoint(point);

  // Flush periodically (batching handled by InfluxDB client)
  await writeApi.flush();
}

/**
 * Query recent readings for a sensor.
 */
async function queryRecentReadings(sensorId, hours = 24) {
  const queryApi = influxDB.getQueryApi(
    process.env.INFLUXDB_ORG || "geoguard"
  );

  const query = `
    from(bucket: "${BUCKET}")
      |> range(start: -${hours}h)
      |> filter(fn: (r) => r._measurement == "sensor_reading")
      |> filter(fn: (r) => r.sensor_id == "${sensorId}")
      |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
      |> sort(columns: ["_time"], desc: true)
      |> limit(n: 1000)
  `;

  const results = [];
  await new Promise((resolve, reject) => {
    queryApi.queryRows(query, {
      next(row, tableMeta) {
        const o = tableMeta.toObject(row);
        results.push(o);
      },
      error(err) {
        reject(err);
      },
      complete() {
        resolve();
      },
    });
  });

  return results;
}

module.exports = {
  writeSensorReading,
  queryRecentReadings,
};
