"""
Data aggregator for the Risk Score Engine.

Merges time-series data from InfluxDB with static zone data from
PostgreSQL/PostGIS into UnifiedRiskInput objects ready for scoring.
"""
import os
import sys
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from shared.models.unified import UnifiedRiskInput
from processing.engine.src.data_reader.influx_reader import InfluxReader
from processing.engine.src.data_reader.postgis_reader import PostGISReader

logger = logging.getLogger("risk-engine")


class DataAggregator:
    """
    Aggregates data from multiple sources into UnifiedRiskInput per zone.

    Reads from:
    - InfluxDB: sensor readings, DHM rainfall, satellite data (dynamic)
    - PostgreSQL/PostGIS: slope angle, historical frequency, zone metadata (static)

    Produces a complete UnifiedRiskInput for each zone every 15 minutes.
    """

    def __init__(self):
        self.influx = InfluxReader()
        self.postgis = PostGISReader()
        self.postgis.connect()

    def close(self):
        self.influx.close()
        self.postgis.close()

    def aggregate_for_zone(self, zone_id: str) -> UnifiedRiskInput:
        """
        Build a UnifiedRiskInput for a single zone by merging all data sources.

        Priority order for conflicting data:
        1. Sensor data (ground truth when available)
        2. DHM data (Nepal-specific rainfall, more accurate than satellite)
        3. Satellite data (coverage for zones without sensors/DHM stations)
        4. Static defaults (baseline values from zone config)
        """
        # ── Read dynamic data from InfluxDB ──────────────────
        influx_data = self.influx.read_all_for_zone(zone_id)

        # ── Read static data from PostgreSQL ──────────────────
        static_data = self.postgis.read_zone_static_data(zone_id)

        # ── Merge into UnifiedRiskInput ──────────────────────
        input = UnifiedRiskInput(
            zone_id=zone_id,
            timestamp=datetime.now(timezone.utc),
            source=influx_data.get("source", "satellite"),

            # Ground conditions (from sensor)
            soil_moisture_pct=influx_data.get("soil_moisture_pct"),
            ground_tilt_deg=influx_data.get("ground_tilt_deg"),
            vibration_g=influx_data.get("vibration_g"),

            # Rainfall (merged from DHM/sensor/satellite)
            rainfall_1hr_mm=influx_data.get("rainfall_1hr_mm"),
            rainfall_6hr_mm=influx_data.get("rainfall_6hr_mm"),
            rainfall_24hr_mm=influx_data.get("rainfall_24hr_mm"),
            rainfall_72hr_mm=influx_data.get("rainfall_72hr_mm"),
            rainfall_source=influx_data.get("rainfall_source", "satellite_proxy"),

            # Satellite-derived
            ndvi_index=influx_data.get("ndvi_index"),
            deformation_flag=influx_data.get("deformation_flag", False),
            deformation_mm=influx_data.get("deformation_mm"),

            # Static (from PostgreSQL/defaults)
            slope_angle_deg=static_data.get("slope_angle_deg", 30.0),
            historical_frequency=static_data.get("historical_frequency", 3.0),

            # Metadata
            confidence=0.5,
            data_freshness_sec=influx_data.get("data_freshness_sec", 999999),
            sensor_battery_pct=influx_data.get("sensor_battery_pct"),
        )

        return input

    def aggregate_all_zones(self) -> list[UnifiedRiskInput]:
        """
        Build UnifiedRiskInput for all configured zones.

        Called every 15 minutes by the Risk Engine scheduler.
        """
        zone_ids = self.postgis.get_all_zone_ids()
        inputs = []

        for zone_id in zone_ids:
            try:
                input = self.aggregate_for_zone(zone_id)
                inputs.append(input)
                logger.info(
                    f"Zone {zone_id}: source={input.source}, "
                    f"rainfall_source={input.rainfall_source}, "
                    f"moisture={input.soil_moisture_pct}, "
                    f"tilt={input.ground_tilt_deg}, "
                    f"slope={input.slope_angle_deg}"
                )
            except Exception as e:
                logger.error(f"Failed to aggregate data for zone {zone_id}: {e}")

        return inputs