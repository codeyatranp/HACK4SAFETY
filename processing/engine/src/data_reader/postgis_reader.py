"""
PostgreSQL + PostGIS data reader for the Risk Score Engine.

Reads static zone data: slope angle, historical frequency,
zone metadata, and infrastructure (roads, settlements) for
risk overlay calculations.
"""
import os
import json
import logging
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("risk-engine")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "geoguard")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "geoguard_admin")
POSTGRES_DB = os.getenv("POSTGRES_DB", "geoguard")

# Load zone definitions from JSON config
ZONES_CONFIG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", "..",
    "shared", "config", "nepal_zones.json",
)
try:
    with open(ZONES_CONFIG_PATH) as f:
        ZONES = json.load(f)["zones"]
except FileNotFoundError:
    logger.warning("nepal_zones.json not found — loading zones from PostgreSQL at runtime")
    ZONES = []

# ── Default static values per zone (when PostGIS data not yet loaded) ──
# Slope angles are estimated from SRTM DEM analysis of Nepal hill districts
DEFAULT_SLOPE_ANGLES = {
    "SINDHUPALCHOK-05": 42.0,  # Bhotekoshi corridor — steep
    "SINDHUPALCHOK-07": 38.0,
    "KASKI-12": 32.0,  # Pokhara valley slopes
    "MYAGDI-03": 35.0,
    "BAGLUNG-06": 33.0,
    "GORKHA-04": 40.0,  # 2015 earthquake affected — unstable slopes
    "NUWAKOT-02": 30.0,
    "DHADHING-08": 35.0,
    "RUKUM-01": 28.0,  # Remote, moderate slopes
    "JAJARKOT-03": 30.0,
    "TEHRI-02": 25.0,
    "OKHALDHUNGA-04": 32.0,
    "PALPA-05": 28.0,
    "BHOJPUR-03": 35.0,
    "RAMECHAP-01": 34.0,
}

# Historical landslide frequency (events per 25yr period, estimated from NDRRMA data)
DEFAULT_HISTORICAL_FREQ = {
    "SINDHUPALCHOK-05": 12.0,  # Highest in Nepal
    "SINDHUPALCHOK-07": 9.0,
    "KASKI-12": 5.0,
    "MYAGDI-03": 4.0,
    "BAGLUNG-06": 3.0,
    "GORKHA-04": 7.0,  # Post-2015 earthquake elevated
    "NUWAKOT-02": 4.0,
    "DHADHING-08": 5.0,
    "RUKUM-01": 2.0,
    "JAJARKOT-03": 3.0,
    "TEHRI-02": 2.0,
    "OKHALDHUNGA-04": 3.0,
    "PALPA-05": 2.0,
    "BHOJPUR-03": 3.0,
    "RAMECHAP-01": 4.0,
}


class PostGISReader:
    """Reads static zone data and spatial information from PostgreSQL+PostGIS."""

    def __init__(self):
        self.conn = None

    def connect(self):
        """Establish PostgreSQL connection."""
        try:
            self.conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB,
                cursor_factory=RealDictCursor,
            )
            logger.info("PostgreSQL connection established")
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")
            self.conn = None

    def close(self):
        if self.conn:
            self.conn.close()

    def read_zone_static_data(self, zone_id: str) -> dict:
        """
        Read static zone data: slope angle, historical frequency.

        Falls back to hardcoded defaults if PostGIS data not loaded yet
        (DEM data requires satellite processing pipeline to populate).
        """
        result = {
            "slope_angle_deg": DEFAULT_SLOPE_ANGLES.get(zone_id, 30.0),
            "historical_frequency": DEFAULT_HISTORICAL_FREQ.get(zone_id, 3.0),
            "zone_name": "",
            "zone_name_ne": "",
            "district": "",
            "province": "",
            "center_lat": 0.0,
            "center_lng": 0.0,
        }

        # Try to read from PostgreSQL (DEM-processed slope data)
        if self.conn:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(
                        "SELECT zone_id, name, name_ne, district, province, "
                        "center_lat, center_lng, risk_level "
                        "FROM zones WHERE zone_id = %s",
                        (zone_id,),
                    )
                    row = cur.fetchone()
                    if row:
                        result["zone_name"] = row["name"]
                        result["zone_name_ne"] = row.get("name_ne", "")
                        result["district"] = row["district"]
                        result["province"] = row["province"]
                        result["center_lat"] = row["center_lat"]
                        result["center_lng"] = row["center_lng"]

                    # Check for DEM-derived slope angle in satellite_data table
                    cur.execute(
                        "SELECT value FROM satellite_data "
                        "WHERE zone_id = %s AND data_type = 'slope_angle' "
                        "ORDER BY timestamp DESC LIMIT 1",
                        (zone_id,),
                    )
                    slope_row = cur.fetchone()
                    if slope_row:
                        result["slope_angle_deg"] = slope_row["value"]

                    # Check for historical frequency
                    cur.execute(
                        "SELECT value FROM satellite_data "
                        "WHERE zone_id = %s AND data_type = 'historical_frequency' "
                        "ORDER BY timestamp DESC LIMIT 1",
                        (zone_id,),
                    )
                    freq_row = cur.fetchone()
                    if freq_row:
                        result["historical_frequency"] = freq_row["value"]

            except Exception as e:
                logger.warning(f"PostgreSQL query failed for zone {zone_id}: {e}")

        # Supplement from nepal_zones.json if DB didn't have data
        zone_config = next(
            (z for z in ZONES if z["zone_id"] == zone_id), None
        )
        if zone_config and not result["zone_name"]:
            result["zone_name"] = zone_config["name"]
            result["zone_name_ne"] = zone_config.get("name_ne", "")
            result["district"] = zone_config["district"]
            result["province"] = zone_config["province"]
            result["center_lat"] = zone_config["center"]["lat"]
            result["center_lng"] = zone_config["center"]["lng"]

        return result

    def read_affected_infrastructure(self, zone_id: str, radius_km: float = 5.0) -> list[str]:
        """
        Find OSM infrastructure (roads, settlements) within a zone's impact radius.

        Uses PostGIS ST_DWithin for spatial proximity query.
        Returns list of OSM feature descriptions for the risk output.
        """
        # Get zone center coordinates
        zone_data = self.read_zone_static_data(zone_id)
        lat = zone_data.get("center_lat", 0)
        lng = zone_data.get("center_lng", 0)

        if not lat or not lng:
            return []

        infrastructure = []

        if self.conn:
            try:
                with self.conn.cursor() as cur:
                    # Find nearby roads
                    cur.execute(
                        "SELECT osm_id, name, highway_type FROM osm_roads "
                        "WHERE ST_DWithin(geom, "
                        "ST_MakePoint(%s, %s)::geography, %s * 1000) "
                        "ORDER BY name",
                        (lng, lat, radius_km),
                    )
                    roads = cur.fetchall()
                    for road in roads:
                        label = road["name"] or f"Road #{road['osm_id']}"
                        infrastructure.append(f"Road: {label} ({road['highway_type']})")

                    # Find nearby settlements
                    cur.execute(
                        "SELECT osm_id, name, population FROM osm_settlements "
                        "WHERE ST_DWithin(geom, "
                        "ST_MakePoint(%s, %s)::geography, %s * 1000) "
                        "ORDER BY population DESC NULLS LAST LIMIT 10",
                        (lng, lat, radius_km),
                    )
                    settlements = cur.fetchall()
                    for stl in settlements:
                        label = stl["name"] or f"Settlement #{stl['osm_id']}"
                        pop = stl.get("population", "?")
                        infrastructure.append(f"Settlement: {label} (pop: {pop})")

            except Exception as e:
                logger.warning(f"Infrastructure query failed for {zone_id}: {e}")

        return infrastructure

    def get_all_zone_ids(self) -> list[str]:
        """Get all zone IDs from the configuration."""
        if ZONES:
            return [z["zone_id"] for z in ZONES]

        # Fall back to PostgreSQL if config file not available
        if self.conn:
            try:
                with self.conn.cursor() as cur:
                    cur.execute("SELECT zone_id FROM zones ORDER BY priority")
                    return [row["zone_id"] for row in cur.fetchall()]
            except Exception as e:
                logger.warning(f"Failed to query zones from PostgreSQL: {e}")

        return list(DEFAULT_SLOPE_ANGLES.keys())