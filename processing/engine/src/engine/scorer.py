"""
Risk Score Engine — main scorer that produces RiskScoreOutput per zone.

Takes UnifiedRiskInput, computes composite risk score using the SWI model,
identifies primary/secondary drivers, calculates confidence, and produces
the complete RiskScoreOutput consumed by dashboard, alerts, and BIPAD.
"""
import os
import sys
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from shared.models.unified import UnifiedRiskInput
from processing.engine.src.models.swi_model import (
    compute_risk_score,
    identify_primary_driver,
    compute_confidence,
    compute_rainfall_subscore,
    compute_ground_condition_subscore,
    compute_static_risk_subscore,
    compute_satellite_subscore,
)
from processing.engine.src.models.risk_output import RiskScoreOutput
from processing.engine.src.data_reader.postgis_reader import PostGISReader

logger = logging.getLogger("risk-engine")


class RiskScorer:
    """
    Computes RiskScoreOutput from UnifiedRiskInput.

    This is the core processing component that takes all available data
    for a zone and produces the actionable risk assessment.
    """

    def __init__(self):
        self.postgis = PostGISReader()
        self.postgis.connect()

    def close(self):
        self.postgis.close()

    def score_zone(self, input: UnifiedRiskInput) -> RiskScoreOutput:
        """
        Produce a complete risk assessment for a zone.

        Steps:
        1. Compute composite risk score (0-100) via SWI model
        2. Identify primary driver and secondary factors
        3. Calculate model confidence
        4. Find affected infrastructure via PostGIS
        5. Assemble RiskScoreOutput
        """
        # ── 1. Composite risk score ──────────────────────────
        composite_score = compute_risk_score(input)

        # ── 2. Primary driver + secondary factors ────────────
        primary_driver, secondary = identify_primary_driver(input)

        # ── 3. Confidence ────────────────────────────────────
        confidence = compute_confidence(input)

        # ── 4. Affected infrastructure ───────────────────────
        affected = self.postgis.read_affected_infrastructure(input.zone_id)

        # ── 5. Sub-scores for dashboard drill-down ──────────
        rainfall_sub, _ = compute_rainfall_subscore(
            input.rainfall_1hr_mm,
            input.rainfall_6hr_mm,
            input.rainfall_24hr_mm,
            input.rainfall_72hr_mm,
        )
        ground_sub, _ = compute_ground_condition_subscore(
            input.ground_tilt_deg,
            input.vibration_g,
            input.soil_moisture_pct,
        )
        static_sub = compute_static_risk_subscore(
            input.slope_angle_deg,
            input.historical_frequency,
            input.ndvi_index,
        )
        satellite_sub, _ = compute_satellite_subscore(
            input.deformation_flag,
            input.deformation_mm,
            input.ndvi_index,
        )

        # ── 6. Assemble output ──────────────────────────────
        output = RiskScoreOutput(
            zone_id=input.zone_id,
            timestamp=datetime.now(timezone.utc),
            risk_score=composite_score,
            risk_level="",  # set by __post_init__
            primary_driver=primary_driver,
            secondary_factors=secondary,
            affected_infrastructure=affected,
            confidence=confidence,
            rainfall_subscore=rainfall_sub,
            ground_condition_subscore=ground_sub,
            static_risk_subscore=static_sub,
            satellite_subscore=satellite_sub,
            # Input snapshot for audit
            soil_moisture_pct=input.soil_moisture_pct,
            ground_tilt_deg=input.ground_tilt_deg,
            vibration_g=input.vibration_g,
            rainfall_1hr_mm=input.rainfall_1hr_mm,
            rainfall_6hr_mm=input.rainfall_6hr_mm,
            rainfall_24hr_mm=input.rainfall_24hr_mm,
            rainfall_72hr_mm=input.rainfall_72hr_mm,
            slope_angle_deg=input.slope_angle_deg,
            ndvi_index=input.ndvi_index,
            deformation_flag=input.deformation_flag,
        )

        logger.info(
            f"Zone {input.zone_id}: risk_score={output.risk_score:.1f} "
            f"({output.risk_level}), driver={output.primary_driver}, "
            f"confidence={output.confidence:.2f}"
        )

        return output