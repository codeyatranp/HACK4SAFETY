"""
InfluxDB data reader for the Risk Score Engine.

Reads sensor time-series data, DHM rainfall, and satellite rainfall
from InfluxDB to construct UnifiedRiskInput per zone.
"""
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.query_api import QueryApi

logger = logging.getLogger("risk-engine")

INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://localhost:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "geoguard-influx-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "geoguard")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "sensor-data")


class InfluxReader:
    """Reads time-series data from InfluxDB for risk score computation."""

    def __init__(self):
        self.client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG,
        )
        self.query_api = self.client.query_api()

    def close(self):
        self.client.close()

    def _execute_flux(self, flux: str) -> list[dict]:
        """Execute a Flux query and return results as list of dicts."""
        try:
            tables = self.query_api.query(flux, org=INFLUXDB_ORG)
            results = []
            for table in tables:
                for record in table.records:
                    row = {}
                    for key in record.values.keys():
                        row[key] = record.values[key]
                    results.append(row)
            return results
        except Exception as e:
            logger.error(f"InfluxDB query failed: {e}")
            return []

    def read_latest_sensor_data(self, zone_id: str, window_minutes: int = 15) -> dict:
        """
        Read the most recent sensor reading for a zone.

        Returns dict with sensor fields: tilt_deg, moisture_pct,
        vibration_g, rainfall_mm, battery_pct, gps_lat, gps_lng.
        """
        flux = (
            f'from(bucket: "{INFLUXDB_BUCKET}")'
            f' |> range(start: -{window_minutes}m)'
            f' |> filter(fn: (r) => r._measurement == "sensor_reading")'
            f' |> filter(fn: (r) => r.zone_id == "{zone_id}")'
            f' |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
            f' |> sort(columns: ["_time"], desc: true)'
            f' |> limit(n: 1)'
        )

        results = self._execute_flux(flux)
        if not results:
            return {}

        row = results[0]
        return {
            "soil_moisture_pct": row.get("moisture_pct"),
            "ground_tilt_deg": row.get("tilt_deg"),
            "vibration_g": row.get("vibration_g"),
            "rainfall_1hr_mm": row.get("rainfall_mm"),
            "sensor_battery_pct": row.get("battery_pct"),
            "timestamp": row.get("_time"),
            "data_freshness_sec": (
                (datetime.now(timezone.utc) - datetime.fromisoformat(row["_time"].replace("Z", "+00:00"))).total_seconds()
                if row.get("_time")
                else 999999
            ),
        }

    def read_dhm_rainfall(self, zone_id: str) -> dict:
        """
        Read the most recent DHM rainfall data for a zone.

        Returns dict with rainfall accumulations and source flag.
        """
        flux = (
            f'from(bucket: "{INFLUXDB_BUCKET}")'
            f' |> range(start: -30m)'
            f' |> filter(fn: (r) => r._measurement == "dhm_rainfall")'
            f' |> filter(fn: (r) => r.zone_id == "{zone_id}")'
            f' |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
            f' |> sort(columns: ["_time"], desc: true)'
            f' |> limit(n: 1)'
        )

        results = self._execute_flux(flux)
        if not results:
            return {"rainfall_source": "satellite_proxy"}

        row = results[0]
        source = row.get("source", "unknown")

        return {
            "rainfall_1hr_mm": row.get("rainfall_1hr_mm"),
            "rainfall_6hr_mm": row.get("rainfall_6hr_mm"),
            "rainfall_24hr_mm": row.get("rainfall_24hr_mm"),
            "rainfall_72hr_mm": row.get("rainfall_72hr_mm"),
            "river_level_m": row.get("river_level_m"),
            "rainfall_source": source,
        }

    def read_satellite_data(self, zone_id: str, hours: int = 168) -> dict:
        """
        Read latest satellite-derived data for a zone (NDVI, deformation).

        Looks back up to 7 days since satellite data is periodic, not real-time.
        """
        flux = (
            f'from(bucket: "{INFLUXDB_BUCKET}")'
            f' |> range(start: -{hours}h)'
            f' |> filter(fn: (r) => r._measurement == "satellite_data")'
            f' |> filter(fn: (r) => r.zone_id == "{zone_id}")'
            f' |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
            f' |> sort(columns: ["_time"], desc: true)'
            f' |> limit(n: 1)'
        )

        results = self._execute_flux(flux)
        if not results:
            return {}

        row = results[0]
        return {
            "ndvi_index": row.get("ndvi"),
            "deformation_flag": row.get("deformation_flag", False),
            "deformation_mm": row.get("deformation_mm"),
        }

    def read_satellite_rainfall(self, zone_id: str, hours: int = 72) -> dict:
        """
        Read latest NASA GPM rainfall for a zone.

        NASA ingestion writes rainfall to satellite_rainfall. The
        satellite_data measurement is reserved for slower metadata like
        NDVI/deformation, so rainfall needs its own lookup.
        """
        flux = (
            f'from(bucket: "{INFLUXDB_BUCKET}")'
            f' |> range(start: -{hours}h)'
            f' |> filter(fn: (r) => r._measurement == "satellite_rainfall")'
            f' |> filter(fn: (r) => r.zone_id == "{zone_id}")'
            f' |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
            f' |> sort(columns: ["_time"], desc: true)'
            f' |> limit(n: 1)'
        )

        results = self._execute_flux(flux)
        if not results:
            return {}

        row = results[0]
        return {
            "rainfall_1hr_mm": row.get("rainfall_1hr_mm"),
            "rainfall_6hr_mm": row.get("rainfall_6hr_mm"),
            "rainfall_24hr_mm": row.get("rainfall_24hr_mm"),
            "rainfall_72hr_mm": row.get("rainfall_72hr_mm"),
            "rainfall_source": row.get("source", "nasa_gpm"),
        }

    def read_all_for_zone(self, zone_id: str) -> dict:
        """
        Read all available time-series data for a zone.

        Merges sensor, DHM, and satellite data into a single dict
        ready for UnifiedRiskInput construction.
        """
        sensor = self.read_latest_sensor_data(zone_id)
        dhm = self.read_dhm_rainfall(zone_id)
        satellite_rainfall = self.read_satellite_rainfall(zone_id)
        satellite = self.read_satellite_data(zone_id)

        merged = {}

        # ── Sensor data (ground conditions) ──────────────────
        if sensor:
            merged["soil_moisture_pct"] = sensor.get("soil_moisture_pct")
            merged["ground_tilt_deg"] = sensor.get("ground_tilt_deg")
            merged["vibration_g"] = sensor.get("vibration_g")
            merged["sensor_battery_pct"] = sensor.get("sensor_battery_pct")
            merged["data_freshness_sec"] = sensor.get("data_freshness_sec", 999999)
            merged["source"] = "sensor"

            # Sensor provides its own rainfall (local gauge)
            sensor_rain = sensor.get("rainfall_1hr_mm")
            if sensor_rain is not None:
                merged["rainfall_1hr_mm"] = sensor_rain
                merged["rainfall_source"] = "sensor"

        # ── DHM rainfall (overrides sensor if available) ─────
        if dhm and dhm.get("rainfall_source") != "satellite_proxy":
            # DHM data is more authoritative than local sensor gauge
            merged["rainfall_1hr_mm"] = dhm.get("rainfall_1hr_mm")
            merged["rainfall_6hr_mm"] = dhm.get("rainfall_6hr_mm")
            merged["rainfall_24hr_mm"] = dhm.get("rainfall_24hr_mm")
            merged["rainfall_72hr_mm"] = dhm.get("rainfall_72hr_mm")
            merged["rainfall_source"] = dhm.get("rainfall_source", "dhm")
        elif dhm and dhm.get("rainfall_source") == "satellite_proxy":
            # No DHM station for this zone — satellite is rainfall proxy
            merged["rainfall_source"] = "satellite_proxy"

        # ── NASA GPM rainfall for satellite-proxy zones ───────
        if (
            satellite_rainfall
            and merged.get("rainfall_source") in (None, "satellite_proxy")
        ):
            merged["rainfall_1hr_mm"] = satellite_rainfall.get("rainfall_1hr_mm")
            merged["rainfall_6hr_mm"] = satellite_rainfall.get("rainfall_6hr_mm")
            merged["rainfall_24hr_mm"] = satellite_rainfall.get("rainfall_24hr_mm")
            merged["rainfall_72hr_mm"] = satellite_rainfall.get("rainfall_72hr_mm")
            merged["rainfall_source"] = satellite_rainfall.get("rainfall_source", "nasa_gpm")

        # ── Satellite-derived data ────────────────────────────
        if satellite:
            merged["ndvi_index"] = satellite.get("ndvi_index")
            merged["deformation_flag"] = satellite.get("deformation_flag", False)
            merged["deformation_mm"] = satellite.get("deformation_mm")
            # Mark source as merged if we have both sensor and satellite
            if merged.get("source") == "sensor":
                merged["source"] = "merged"

        # ── Defaults for missing data ─────────────────────────
        if "source" not in merged:
            merged["source"] = "satellite"
        if "data_freshness_sec" not in merged:
            merged["data_freshness_sec"] = 999999
        if "rainfall_source" not in merged:
            merged["rainfall_source"] = "satellite_proxy"

        return merged
