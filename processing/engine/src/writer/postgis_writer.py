"""
PostgreSQL writer for risk score outputs.

Writes current risk state to PostgreSQL for dashboard real-time queries
and alert system lookups. Stores the latest risk per zone + history log.
"""
import os
import logging
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor

from processing.engine.src.models.risk_output import RiskScoreOutput

logger = logging.getLogger("risk-engine")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "geoguard")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "geoguard_admin")
POSTGRES_DB = os.getenv("POSTGRES_DB", "geoguard")


class RiskPostGISWriter:
    """Writes risk scores to PostgreSQL for current-state and historical queries."""

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
            logger.info("Risk PostgreSQL connection established")
        except Exception as e:
            logger.error(f"PostgreSQL connection failed: {e}")

    def close(self):
        if self.conn:
            self.conn.close()

    def write_risk_score(self, output: RiskScoreOutput):
        """
        Write risk score to PostgreSQL.

        Updates two tables:
        - risk_scores_current: latest risk per zone (upsert)
        - risk_scores_history: full log for trend analysis
        """
        if not self.conn:
            logger.warning("No PostgreSQL connection — skipping write")
            return

        try:
            with self.conn.cursor() as cur:
                # ── Upsert current risk state ─────────────────
                cur.execute(
                    """
                    INSERT INTO risk_scores_current (
                        zone_id, timestamp, risk_score, risk_level,
                        primary_driver, confidence, recommended_action,
                        recommended_action_ne, rainfall_subscore,
                        ground_condition_subscore, static_risk_subscore,
                        satellite_subscore, soil_moisture_pct,
                        ground_tilt_deg, vibration_g, rainfall_1hr_mm,
                        rainfall_6hr_mm, rainfall_24hr_mm, rainfall_72hr_mm,
                        slope_angle_deg, ndvi_index, deformation_flag
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s
                    )
                    ON CONFLICT (zone_id) DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
                        risk_score = EXCLUDED.risk_score,
                        risk_level = EXCLUDED.risk_level,
                        primary_driver = EXCLUDED.primary_driver,
                        confidence = EXCLUDED.confidence,
                        recommended_action = EXCLUDED.recommended_action,
                        recommended_action_ne = EXCLUDED.recommended_action_ne,
                        rainfall_subscore = EXCLUDED.rainfall_subscore,
                        ground_condition_subscore = EXCLUDED.ground_condition_subscore,
                        static_risk_subscore = EXCLUDED.static_risk_subscore,
                        satellite_subscore = EXCLUDED.satellite_subscore,
                        soil_moisture_pct = EXCLUDED.soil_moisture_pct,
                        ground_tilt_deg = EXCLUDED.ground_tilt_deg,
                        vibration_g = EXCLUDED.vibration_g,
                        rainfall_1hr_mm = EXCLUDED.rainfall_1hr_mm,
                        rainfall_6hr_mm = EXCLUDED.rainfall_6hr_mm,
                        rainfall_24hr_mm = EXCLUDED.rainfall_24hr_mm,
                        rainfall_72hr_mm = EXCLUDED.rainfall_72hr_mm,
                        slope_angle_deg = EXCLUDED.slope_angle_deg,
                        ndvi_index = EXCLUDED.ndvi_index,
                        deformation_flag = EXCLUDED.deformation_flag
                    """,
                    (
                        output.zone_id,
                        output.timestamp,
                        output.risk_score,
                        output.risk_level,
                        output.primary_driver,
                        output.confidence,
                        output.recommended_action,
                        output.recommended_action_ne,
                        output.rainfall_subscore,
                        output.ground_condition_subscore,
                        output.static_risk_subscore,
                        output.satellite_subscore,
                        output.soil_moisture_pct,
                        output.ground_tilt_deg,
                        output.vibration_g,
                        output.rainfall_1hr_mm,
                        output.rainfall_6hr_mm,
                        output.rainfall_24hr_mm,
                        output.rainfall_72hr_mm,
                        output.slope_angle_deg,
                        output.ndvi_index,
                        output.deformation_flag,
                    ),
                )

                # ── Append to history log ──────────────────────
                cur.execute(
                    """
                    INSERT INTO risk_scores_history (
                        zone_id, timestamp, risk_score, risk_level,
                        primary_driver, confidence, recommended_action,
                        recommended_action_ne
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        output.zone_id,
                        output.timestamp,
                        output.risk_score,
                        output.risk_level,
                        output.primary_driver,
                        output.confidence,
                        output.recommended_action,
                        output.recommended_action_ne,
                    ),
                )

                self.conn.commit()
                logger.debug(f"Risk score written to PostgreSQL for zone {output.zone_id}")

        except Exception as e:
            logger.error(f"PostgreSQL write failed for zone {output.zone_id}: {e}")
            if self.conn:
                self.conn.rollback()

    def write_batch(self, outputs: list[RiskScoreOutput]):
        """Write risk scores for all zones."""
        for output in outputs:
            self.write_risk_score(output)
