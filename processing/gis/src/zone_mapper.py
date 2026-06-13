"""
GeoGuard GIS Zone Mapper — Spatial operations for risk overlay.

Provides PostGIS-based spatial joins between risk zones and:
- OSM road segments → road risk scoring
- OSM settlements → impact zone identification
- Zone boundaries → province/district filtering
- Risk area polygons → evacuation corridor mapping (Phase 2 prep)
"""
import os
import logging
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger("gis-mapper")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "geoguard")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "geoguard_admin")
POSTGRES_DB = os.getenv("POSTGRES_DB", "geoguard")


class GISZoneMapper:
    """
    Performs spatial operations between GeoGuard zones and OSM/geospatial data.

    Used by:
    - Risk Engine: identify affected infrastructure for risk output
    - Route Optimizer: spatial join risk scores to road segments
    - Dashboard: zone boundary polygons for map visualization
    """

    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                dbname=POSTGRES_DB,
                cursor_factory=RealDictCursor,
            )
            logger.info("GIS Zone Mapper — PostgreSQL connection established")
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")

    def close(self):
        if self.conn:
            self.conn.close()

    def update_road_risk_overlay(self, risk_outputs: list[dict]):
        """
        Update risk scores on OSM road segments based on zone risk scores.

        For each road segment that intersects a zone's risk area,
        update the road's risk_score field to the zone's current risk score.

        This enables the Route Optimizer to avoid high-risk roads.
        """
        if not self.conn:
            logger.warning("No PostgreSQL connection — skipping road risk overlay")
            return

        try:
            with self.conn.cursor() as cur:
                for output in risk_outputs:
                    zone_id = output["zone_id"]
                    risk_score = output["risk_score"]

                    # Get zone center and radius
                    cur.execute(
                        "SELECT center_lat, center_lng, radius_km "
                        "FROM zones WHERE zone_id = %s",
                        (zone_id,),
                    )
                    zone_row = cur.fetchone()
                    if not zone_row:
                        continue

                    lat = zone_row["center_lat"]
                    lng = zone_row["center_lng"]
                    radius_km = zone_row["radius_km"]

                    # Update risk scores for roads within zone impact radius
                    cur.execute(
                        """
                        UPDATE osm_roads
                        SET risk_score = %s
                        WHERE ST_DWithin(
                            geom,
                            ST_MakePoint(%s, %s)::geography,
                            %s * 1000
                        )
                        """,
                        (risk_score, lng, lat, radius_km),
                    )

                    updated = cur.rowcount
                    logger.debug(
                        f"Zone {zone_id} (risk={risk_score:.1f}): "
                        f"{updated} road segments updated"
                    )

                self.conn.commit()
                logger.info(f"Road risk overlay updated for {len(risk_outputs)} zones")

        except Exception as e:
            logger.error(f"Road risk overlay failed: {e}")
            if self.conn:
                self.conn.rollback()

    def get_zone_impact_summary(self, zone_id: str) -> dict:
        """
        Get a complete impact summary for a zone.

        Returns counts of affected roads, settlements, and infrastructure
        within the zone's impact radius.
        """
        if not self.conn:
            return {"roads": 0, "settlements": 0, "population_affected": 0}

        try:
            with self.conn.cursor() as cur:
                # Get zone geometry
                cur.execute(
                    "SELECT center_lat, center_lng, radius_km, name, name_ne "
                    "FROM zones WHERE zone_id = %s",
                    (zone_id,),
                )
                zone = cur.fetchone()
                if not zone:
                    return {"roads": 0, "settlements": 0, "population_affected": 0}

                lat = zone["center_lat"]
                lng = zone["center_lng"]
                radius_km = zone["radius_km"]

                # Count affected roads
                cur.execute(
                    """
                    SELECT COUNT(*) as road_count,
                           ARRAY_AGG(name) FILTER (WHERE name IS NOT NULL) as road_names
                    FROM osm_roads
                    WHERE ST_DWithin(
                        geom,
                        ST_MakePoint(%s, %s)::geography,
                        %s * 1000
                    )
                    """,
                    (lng, lat, radius_km),
                )
                roads = cur.fetchone()

                # Count affected settlements + total population
                cur.execute(
                    """
                    SELECT COUNT(*) as settlement_count,
                           COALESCE(SUM(population), 0) as total_population
                    FROM osm_settlements
                    WHERE ST_DWithin(
                        geom,
                        ST_MakePoint(%s, %s)::geography,
                        %s * 1000
                    )
                    """,
                    (lng, lat, radius_km),
                )
                settlements = cur.fetchone()

                return {
                    "zone_name": zone["name"],
                    "zone_name_ne": zone.get("name_ne", ""),
                    "roads": roads["road_count"] if roads else 0,
                    "road_names": roads["road_names"] if roads else [],
                    "settlements": settlements["settlement_count"] if settlements else 0,
                    "population_affected": settlements["total_population"] if settlements else 0,
                }

        except Exception as e:
            logger.warning(f"Impact summary query failed for {zone_id}: {e}")
            return {"roads": 0, "settlements": 0, "population_affected": 0}

    def get_zones_by_risk_level(self, risk_level: str) -> list[dict]:
        """
        Get all zones currently at a given risk level.

        Used by dashboard and alert system for filtering.
        """
        if not self.conn:
            return []

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT rc.zone_id, rc.risk_score, rc.risk_level,
                           rc.primary_driver, rc.confidence,
                           rc.recommended_action, rc.recommended_action_ne,
                           z.name, z.name_ne, z.district, z.province,
                           z.center_lat, z.center_lng
                    FROM risk_scores_current rc
                    JOIN zones z ON rc.zone_id = z.zone_id
                    WHERE rc.risk_level = %s
                    ORDER BY rc.risk_score DESC
                    """,
                    (risk_level,),
                )
                return [dict(row) for row in cur.fetchall()]

        except Exception as e:
            logger.warning(f"Zones by risk level query failed: {e}")
            return []

    def get_all_zone_risk_map(self) -> list[dict]:
        """
        Get current risk state for all zones — for dashboard map rendering.

        Returns zone_id, risk_score, risk_level, center coords, name
        for every zone with a current risk assessment.
        """
        if not self.conn:
            return []

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT rc.zone_id, rc.risk_score, rc.risk_level,
                           rc.primary_driver, rc.confidence,
                           rc.recommended_action, rc.recommended_action_ne,
                           rc.rainfall_subscore, rc.ground_condition_subscore,
                           rc.static_risk_subscore, rc.satellite_subscore,
                           z.name, z.name_ne, z.district, z.province,
                           z.center_lat, z.center_lng
                    FROM risk_scores_current rc
                    JOIN zones z ON rc.zone_id = z.zone_id
                    ORDER BY rc.risk_score DESC
                    """,
                )
                return [dict(row) for row in cur.fetchall()]

        except Exception as e:
            logger.warning(f"All zone risk map query failed: {e}")
            return []