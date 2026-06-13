"""
Nepal-adapted Soil Water Index (SWI) thresholds for Phase 1 rule-based risk scoring.

Based on Japan MLIT's Soil Water Index methodology, adapted for Nepal's
geological context: steeper slopes, monsoon-dominated rainfall patterns,
different soil types, and higher historical landslide frequency in the
Himalayan corridor.

The SWI concept: soil saturation accumulates from rainfall and depletes
over time. When saturation exceeds a threshold that depends on slope
angle and soil type, landslide risk increases dramatically.

Nepal adaptation key differences from Japan:
  - Higher base slope angles (30-60° typical in hill districts vs Japan's 20-40°)
  - Intense monsoon bursts (150mm/hr possible vs Japan's typical 50mm/hr)
  - Residual instability from 2015 Gorkha earthquake in many zones
  - Less vegetation cover on steep slopes → higher erosion baseline
"""

# ── Rainfall Thresholds (mm) ─────────────────────────────────
# Values derived from DHM historical records + NDRRMA incident correlation
# Monsoon (Jun-Sep) values are higher; non-monsoon uses same thresholds
# but lower actual values will naturally produce lower scores

RAINFALL_THRESHOLDS = {
    "1hr": {
        "low": 5.0,
        "moderate": 15.0,
        "high": 30.0,
        "critical": 55.0,
        "weight": 0.10,  # Short window — immediate burst indicator
    },
    "6hr": {
        "low": 20.0,
        "moderate": 50.0,
        "high": 80.0,
        "critical": 120.0,
        "weight": 0.20,  # Medium window — sustained rainfall
    },
    "24hr": {
        "low": 40.0,
        "moderate": 100.0,
        "high": 180.0,
        "critical": 280.0,
        "weight": 0.30,  # Day window — cumulative effect
    },
    "72hr": {
        "low": 80.0,
        "moderate": 200.0,
        "high": 350.0,
        "critical": 500.0,
        "weight": 0.40,  # 3-day window — antecedent saturation (primary SWI factor)
    },
}

# ── Ground Condition Thresholds ───────────────────────────────
# Grouped thresholds for tilt, moisture, vibration — used by ground sub-scorer

GROUND_CONDITION_THRESHOLDS = {
    "tilt": {
        "low": 1.0,
        "moderate": 3.0,
        "high": 5.0,
        "critical": 8.0,
    },
    "moisture": {
        "low": 30.0,
        "moderate": 55.0,
        "high": 75.0,
        "critical": 90.0,
    },
}

VIBRATION_THRESHOLD = {
    "low": 0.05,
    "moderate": 0.15,
    "high": 0.30,
    "critical": 0.50,
}

# ── Static Risk Thresholds ────────────────────────────────────
# Used for baseline risk from permanent zone characteristics

STATIC_RISK_THRESHOLDS = {
    "slope_angle": {
        "low": 15.0,
        "moderate": 30.0,
        "high": 45.0,
        "critical": 60.0,
    },
    "historical_frequency": {
        "low": 1.0,
        "moderate": 5.0,
        "high": 10.0,
        "critical": 15.0,
    },
}

# ── NDVI (Vegetation) Thresholds ─────────────────────────────
# Lower NDVI = less vegetation = higher erosion risk

NDVI_THRESHOLDS = {
    "healthy": 0.6,
    "moderate": 0.4,
    "sparse": 0.2,
    "bare": 0.0,
}

# ── Slope Angle Multiplier ────────────────────────────────────
# Higher slope = higher base risk. Used as multiplier on dynamic scores.

SLOPE_RISK_MULTIPLIER = {
    "flat": (0, 15, 0.3),
    "gentle": (15, 30, 0.7),
    "steep": (30, 45, 1.2),
    "very_steep": (45, 60, 1.8),
    "extreme": (60, 90, 2.5),
}

# ── Historical Frequency Scale ─────────────────────────────────
# Zones with more historical landslides get a base risk boost

HISTORICAL_FREQUENCY_SCALE = {
    "rare": 0.0,
    "occasional": 0.2,
    "frequent": 0.5,
    "very_frequent": 1.0,
}

# ── Deformation Flag Contribution ─────────────────────────────

DEFORMATION_RISK_BONUS = {
    False: 0.0,
    True: 15.0,
}

# ── Scoring Weight Distribution ───────────────────────────────
# How much each factor category contributes to the composite 0-100 score

SCORING_WEIGHTS = {
    "rainfall": 0.40,
    "ground_conditions": 0.25,
    "static_risk": 0.20,
    "satellite": 0.15,
}

# ── Data Freshness Penalty ────────────────────────────────────

FRESHNESS_THRESHOLDS = {
    "fresh": 900,
    "acceptable": 3600,
    "stale": 14400,
    "very_stale": 86400,
}

FRESHNESS_CONFIDENCE = {
    "fresh": 1.0,
    "acceptable": 0.7,
    "stale": 0.4,
    "very_stale": 0.1,
}

# ── Immediate Alert Thresholds (for Threshold Detector) ──────
# These trigger alerts *immediately* without waiting for the 15-min cycle

IMMEDIATE_ALERT_TRIGGERS = {
    "tilt_critical": 8.0,
    "vibration_critical": 0.50,
    "rainfall_1hr_critical": 55.0,
    "moisture_critical": 90.0,
}