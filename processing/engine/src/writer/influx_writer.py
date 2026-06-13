"""
InfluxDB writer for risk score outputs.

Writes RiskScoreOutput to InfluxDB as a time-series measurement
enabling historical risk score queries and trend analysis on the dashboard.
"""
import os
import logging
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from processing.engine.src.models.risk_output import RiskScoreOutput

logger = logging.getLogger("risk-engine")

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "geoguard-influx-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "geoguard")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor-data")


class RiskInfluxWriter:
    """Writes risk scores to InfluxDB for time-series tracking."""

    def __init__(self):
        self.client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG,
        )
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def close(self):
        self.client.close()

    def write_risk_score(self, output: RiskScoreOutput):
        """Write a single zone's risk score to InfluxDB."""
        try:
            point = (
                Point("risk_score")
                .tag("zone_id", output.zone_id)
                .tag("risk_level", output.risk_level)
                .tag("primary_driver", output.primary_driver)
                .field("risk_score", output.risk_score)
                .field("confidence", output.confidence)
                .field("rainfall_subscore", output.rainfall_subscore)
                .field("ground_condition_subscore", output.ground_condition_subscore)
                .field("static_risk_subscore", output.static_risk_subscore)
                .field("satellite_subscore", output.satellite_subscore)
                .time(datetime.now(timezone.utc))
            )

            # Optional fields — only write if available
            if output.soil_moisture_pct is not None:
                point.field("soil_moisture_pct", output.soil_moisture_pct)
            if output.ground_tilt_deg is not None:
                point.field("ground_tilt_deg", output.ground_tilt_deg)
            if output.vibration_g is not None:
                point.field("vibration_g", output.vibration_g)
            if output.rainfall_1hr_mm is not None:
                point.field("rainfall_1hr_mm", output.rainfall_1hr_mm)
            if output.rainfall_6hr_mm is not None:
                point.field("rainfall_6hr_mm", output.rainfall_6hr_mm)
            if output.rainfall_24hr_mm is not None:
                point.field("rainfall_24hr_mm", output.rainfall_24hr_mm)
            if output.rainfall_72hr_mm is not None:
                point.field("rainfall_72hr_mm", output.rainfall_72hr_mm)
            if output.ndvi_index is not None:
                point.field("ndvi_index", output.ndvi_index)
            if output.deformation_flag:
                point.field("deformation_flag", True)

            self.write_api.write(
                bucket=INFLUXDB_BUCKET,
                org=INFLUXDB_ORG,
                record=point,
            )

            logger.debug(f"Risk score written to InfluxDB for zone {output.zone_id}")

        except Exception as e:
            logger.error(f"InfluxDB write failed for zone {output.zone_id}: {e}")

    def write_batch(self, outputs: list[RiskScoreOutput]):
        """Write risk scores for all zones in a single batch."""
        for output in outputs:
            self.write_risk_score(output)