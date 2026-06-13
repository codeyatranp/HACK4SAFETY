const { validateDeviceKey } = require("../services/ingest.service");

/**
 * Auth middleware — validates X-Device-Key header against sensor registry.
 */
async function deviceAuth(req, res, next) {
  const apiKey = req.headers["x-device-key"];

  if (!apiKey) {
    return res.status(401).json({
      error: "Missing authentication",
      detail: "X-Device-Key header is required",
    });
  }

  const sensor = await validateDeviceKey(apiKey);

  if (!sensor) {
    return res.status(401).json({
      error: "Invalid device key",
      detail: "This sensor is not registered",
    });
  }

  // Attach sensor context to the request
  req.sensor = sensor;
  next();
}

module.exports = { deviceAuth };
