"""
Soil Water Index (SWI) rule-based risk scoring model — Phase 1.

Adapted from Japan MLIT's SWI methodology for Nepal's context.
Computes a composite risk score (0-100) per zone using weighted sub-scores
from rainfall, ground conditions, static risk, and satellite data.
"""
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Optional

# Set up import paths — same pattern as DHM/satellite ingestion services
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

from shared.models.unified import UnifiedRiskInput
from processing.engine.src.models.thresholds import (
    RAINFALL_THRESHOLDS,
    GROUND_CONDITION_THRESHOLDS,
    STATIC_RISK_THRESHOLDS,
    VIBRATION_THRESHOLD,
)


# ── Soil Water Index Computation ────────────────────────────────
def compute_soil_wetness_index(
    rainfall_1hr: Optional[float],
    rainfall_6hr: Optional[float],
    rainfall_24hr: Optional[float],
    rainfall_72hr: Optional[float],
    soil_moisture_pct: Optional[float],
) -> float:
    """
    Compute the Soil Wetness Index (SWI) — a measure of how saturated the soil is.

    Based on Japan MLIT methodology adapted for Nepal:
    - Soil moisture directly measures saturation
    - Rainfall accumulation indicates prolonged wetting
    - The 72hr window captures antecedent saturation from prior rainfall
    - Weighted combination reflects both immediate and cumulative effects

    Returns a 0-1 normalized index where:
    - 0.0 = Completely dry soil
    - 0.5 = Moderately saturated
    - 1.0 = Fully saturated (landslide likely)
    """
    # ── Soil moisture (direct saturation) ─────────────────────────
    if soil_moisture_pct is not None:
        moisture_score = soil_moisture_pct / 100.0
    else:
        # If no soil moisture reading, estimate from rainfall accumulation
        # Assume 40% moisture per 100mm of 72hr accumulated rainfall
        if rainfall_72hr is not None:
            moisture_score = min(1.0, rainfall_72hr / 250.0)
        elif rainfall_24hr is not None:
            moisture_score = min(1.0, rainfall_24hr / 150.0)
        elif rainfall_6hr is not None:
            moisture_score = min(1.0, rainfall_6hr / 80.0)
        elif rainfall_1hr is not None:
            moisture_score = min(1.0, rainfall_1hr / 30.0)
        else:
            moisture_score = 0.2  # Default assumption: slightly moist

    # ── Rainfall accumulation (progressive wetting) ──────────────────
    rain_scores = []
    if rainfall_1hr is not None:
        rain_scores.append(("1hr", min(1.0, rainfall_1hr / RAINFALL_THRESHOLDS["1hr"]["moderate"])))
    if rainfall_6hr is not None:
        rain_scores.append(("6hr", min(1.0, rainfall_6hr / RAINFALL_THRESHOLDS["6hr"]["moderate"])))
    if rainfall_24hr is not None:
        rain_scores.append(("24hr", min(1.0, rainfall_24hr / RAINFALL_THRESHOLDS["24hr"]["moderate"])))
    if rainfall_72hr is not None:
        rain_scores.append(("72hr", min(1.0, rainfall_72hr / RAINFALL_THRESHOLDS["72hr"]["moderate"])))

    # Weight: longer windows have more impact on soil saturation
    rain_weight = 0.0
    for window, score in rain_scores:
        weight = RAINFALL_THRESHOLDS[window]["weight"]
        rain_weight += score * weight

    # ── Combined SWI ──────────────────────────────────────────
    # Soil moisture provides immediate saturation measure
    # Rainfall accumulation provides progressive wetting measure
    # The combination captures both current and antecedent conditions
    swi = (0.4 * moisture_score) + (0.6 * rain_weight)

    return max(0.0, min(1.0, swi))


# ── Sub-score Computation ──────────────────────────────────────
def compute_rainfall_subscore(
    rainfall_1hr: Optional[float],
    rainfall_6hr: Optional[float],
    rainfall_24hr: Optional[float],
    rainfall_72hr: Optional[float],
) -> tuple[float, str]:
    """Compute rainfall contribution to risk score (0-100) and identify primary driver."""
    scores = {}

    if rainfall_1hr is not None:
        scores["1hr"] = min(100.0, rainfall_1hr / RAINFALL_THRESHOLDS["1hr"]["high"] * 100)
    if rainfall_6hr is not None:
        scores["6hr"] = min(100.0, rainfall_6hr / RAINFALL_THRESHOLDS["6hr"]["high"] * 100)
    if rainfall_24hr is not None:
        scores["24hr"] = min(100.0, rainfall_24hr / RAINFALL_THRESHOLDS["24hr"]["high"] * 100)
    if rainfall_72hr is not None:
        scores["72hr"] = min(100.0, rainfall_72hr / RAINFALL_THRESHOLDS["72hr"]["high"] * 100)

    if not scores:
        return 0.0, "No rainfall data"

    # Find the window with highest contribution (primary driver)
    primary_window = max(scores, key=lambda k: scores[k])
    primary_score = scores[primary_window]

    # Weighted average across all available windows
    total_weight = sum(
        RAINFALL_THRESHOLDS[w]["weight"] for w in scores
    )
    weighted_score = sum(
        scores[w] * RAINFALL_THRESHOLDS[w]["weight"] for w in scores
    ) / total_weight

    return weighted_score, f"Rainfall {primary_window} accumulated"


def compute_ground_condition_subscore(
    tilt_deg: Optional[float],
    vibration_g: Optional[float],
    soil_moisture_pct: Optional[float],
) -> tuple[float, str]:
    """Compute ground condition contribution to risk score (0-100) and primary driver."""
    scores = {}

    if tilt_deg is not None:
        scores["tilt"] = min(100.0, tilt_deg / GROUND_CONDITION_THRESHOLDS["tilt"]["critical"] * 100)
    if vibration_g is not None:
        scores["vibration"] = min(100.0, vibration_g / VIBRATION_THRESHOLD["critical"] * 100)
    if soil_moisture_pct is not None:
        scores["moisture"] = min(100.0, soil_moisture_pct / GROUND_CONDITION_THRESHOLDS["moisture"]["critical"] * 100)

    if not scores:
        return 0.0, "No ground condition data"

    primary_factor = max(scores, key=lambda k: scores[k])
    primary_score = scores[primary_factor]

    # Ground conditions are equally weighted
    weighted_score = sum(scores.values()) / len(scores)

    driver_labels = {
        "tilt": "Ground tilt anomaly detected",
        "vibration": "Vibration anomaly detected",
        "moisture": "Soil saturation critical",
    }
    return weighted_score, driver_labels[primary_factor]


def compute_static_risk_subscore(
    slope_angle_deg: float,
    historical_frequency: float,
    ndvi_index: Optional[float],
) -> float:
    """Compute static (baseline) risk from permanent zone characteristics."""
    slope_score = min(100.0, slope_angle_deg / STATIC_RISK_THRESHOLDS["slope_angle"]["critical"] * 100)

    freq_score = min(100.0, historical_frequency / STATIC_RISK_THRESHOLDS["historical_frequency"]["critical"] * 100)

    if ndvi_index is not None:
        # Low NDVI = eroded slope = higher risk
        ndvi_score = max(0.0, (1.0 - ndvi_index) * 100)
    else:
        ndvi_score = 0.0

    # Weighted combination: slope and frequency are primary, NDVI secondary
    static_score = (
        0.5 * slope_score
        + 0.3 * freq_score
        + 0.2 * ndvi_score
    )

    return static_score


def compute_satellite_subscore(
    deformation_flag: bool,
    deformation_mm: Optional[float],
    ndvi_index: Optional[float],
) -> tuple[float, str]:
    """Compute satellite-derived contribution to risk."""
    score = 0.0
    driver = "No satellite data"

    if deformation_flag:
        score += 60.0
        driver = "Ground deformation detected (Sentinel-1 SAR)"
        if deformation_mm is not None:
            score += min(40.0, deformation_mm / 10.0 * 40)

    if ndvi_index is not None and ndvi_index < 0.3:
        vegetation_loss = (0.3 - ndvi_index) / 0.3 * 30
        score += vegetation_loss
        if deformation_flag:
            driver = "Deformation + vegetation loss"
        else:
            driver = f"Vegetation loss detected (NDVI={ndvi_index:.2f})"

    return score, driver


# ── Composite Risk Score ──────────────────────────────────────
def compute_risk_score(input: UnifiedRiskInput) -> float:
    """
    Compute the composite risk score (0-100) for a zone using the SWI model.

    Weighted combination of:
    - Rainfall sub-score (0-100) — dynamic, highest weight
    - Ground condition sub-score (0-100) — dynamic, medium weight
    - Static risk sub-score (0-100) — baseline, medium weight
    - Satellite sub-score (0-100) — supplementary, lower weight

    The weighting reflects that rainfall is the primary landslide trigger in Nepal,
    ground conditions amplify that risk, static factors set the baseline,
    and satellite data provides additional evidence.
    """
    # ── Sub-scores ─────────────────────────────────────────────
    rainfall_sub, rainfall_driver = compute_rainfall_subscore(
        input.rainfall_1hr_mm,
        input.rainfall_6hr_mm,
        input.rainfall_24hr_mm,
        input.rainfall_72hr_mm,
    )

    ground_sub, ground_driver = compute_ground_condition_subscore(
        input.ground_tilt_deg,
        input.vibration_g,
        input.soil_moisture_pct,
    )

    static_sub = compute_static_risk_subscore(
        input.slope_angle_deg,
        input.historical_frequency,
        input.ndvi_index,
    )

    satellite_sub, satellite_driver = compute_satellite_subscore(
        input.deformation_flag,
        input.deformation_mm,
        input.ndvi_index,
    )

    # ── Weighted composite ─────────────────────────────────────
    # Rainfall is primary trigger → highest weight
    # Ground conditions amplify → medium weight
    # Static factors set baseline → medium weight
    # Satellite provides supplementary evidence → lower weight
    composite = (
        0.40 * rainfall_sub
        + 0.25 * ground_sub
        + 0.25 * static_sub
        + 0.10 * satellite_sub
    )

    return composite


def identify_primary_driver(input: UnifiedRiskInput) -> tuple[str, list[str]]:
    """
    Identify the primary risk driver and list secondary factors.

    Returns the factor with highest sub-score contribution as the primary driver,
    and all factors above a minimum threshold as secondary factors.
    """
    drivers = []

    rainfall_sub, rainfall_driver = compute_rainfall_subscore(
        input.rainfall_1hr_mm,
        input.rainfall_6hr_mm,
        input.rainfall_24hr_mm,
        input.rainfall_72hr_mm,
    )
    ground_sub, ground_driver = compute_ground_condition_subscore(
        input.ground_tilt_deg,
        input.vibration_g,
        input.soil_moisture_pct,
    )
    static_sub = compute_static_risk_subscore(
        input.slope_angle_deg,
        input.historical_frequency,
        input.ndvi_index,
    )
    satellite_sub, satellite_driver = compute_satellite_subscore(
        input.deformation_flag,
        input.deformation_mm,
        input.ndvi_index,
    )

    sub_scores = {
        "rainfall": rainfall_sub,
        "ground": ground_sub,
        "static": static_sub,
        "satellite": satellite_sub,
    }

    driver_labels = {
        "rainfall": rainfall_driver,
        "ground": ground_driver,
        "static": "High slope angle / historical frequency",
        "satellite": satellite_driver,
    }

    # Primary driver = highest sub-score category
    primary_category = max(sub_scores, key=lambda k: sub_scores[k])
    primary_driver = driver_labels[primary_category]

    # Secondary factors = categories with significant contribution (>25)
    secondary = [
        driver_labels[cat]
        for cat, score in sub_scores.items()
        if cat != primary_category and score > 25
    ]

    return primary_driver, secondary


def compute_confidence(input: UnifiedRiskInput) -> float:
    """
    Compute model confidence (0-1) based on data availability and freshness.

    Higher confidence when:
    - More data sources are available (sensor, DHM, satellite)
    - Data is recent (low data_freshness_sec)
    - Multiple independent signals agree (rainfall + tilt + deformation)
    """
    confidence = 0.0

    # ── Source availability ──────────────────────────────────────
    has_sensor = input.source in ("sensor", "merged")
    has_dhm = input.rainfall_source in ("dhm", "sensor")
    has_satellite = input.source in ("satellite", "merged") or input.deformation_flag

    if has_sensor:
        confidence += 0.35
    if has_dhm:
        confidence += 0.25
    if has_satellite:
        confidence += 0.15

    # ── Data freshness ──────────────────────────────────────────
    if input.data_freshness_sec < 900:  # < 15 min
        confidence += 0.15
    elif input.data_freshness_sec < 3600:  # < 1 hr
        confidence += 0.10
    elif input.data_freshness_sec < 14400:  # < 4 hr
        confidence += 0.05

    # ── Signal agreement ────────────────────────────────────────
    # If multiple independent signals indicate risk, boost confidence
    signals = 0
    if input.rainfall_72hr_mm and input.rainfall_72hr_mm > 50:
        signals += 1
    if input.soil_moisture_pct and input.soil_moisture_pct > 70:
        signals += 1
    if input.ground_tilt_deg and input.ground_tilt_deg > 5:
        signals += 1
    if input.deformation_flag:
        signals += 1

    if signals >= 3:
        confidence += 0.10
    elif signals >= 2:
        confidence += 0.05

    return min(1.0, confidence)