"""
Risk Score Engine output model.

Produced every 15 minutes per zone. This is the primary output
consumed by the dashboard, alert system, and BIPAD reporter.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


# ── Risk Level Definitions ────────────────────────────────────
RISK_LEVELS = {
    "LOW": {"min": 0, "max": 25, "color": "green"},
    "MODERATE": {"min": 26, "max": 50, "color": "yellow"},
    "HIGH": {"min": 51, "max": 75, "color": "orange"},
    "CRITICAL": {"min": 76, "max": 100, "color": "red"},
}


def classify_risk(score: float) -> str:
    """Map a 0-100 risk score to its risk level string."""
    if score <= 25:
        return "LOW"
    elif score <= 50:
        return "MODERATE"
    elif score <= 75:
        return "HIGH"
    else:
        return "CRITICAL"


def risk_color(level: str) -> str:
    """Return the color tag for a risk level."""
    return RISK_LEVELS[level]["color"]


# ── Recommended Actions ───────────────────────────────────────
RECOMMENDED_ACTIONS = {
    "LOW": "No action required. Continue routine monitoring.",
    "MODERATE": "Monitor closely. Alert District EOC. Review sensor data trend.",
    "HIGH": "Prepare evacuation. Send WhatsApp alert to ward leaders and police. "
            "Advise community to avoid high-risk routes.",
    "CRITICAL": "Evacuate immediately. All alert channels activated. "
                "Auto-report to BIPAD. Dispatch police to affected zone.",
}

# Nepali translations
RECOMMENDED_ACTIONS_NE = {
    "LOW": "कुनै कार्य आवश्यक छैन। नियमित अवलोकन गर्नुहोस्।",
    "MODERATE": "नजिकबाट अवलोकन गर्नुहोस्। जिल्ला EOC लाई सूचना दिनुहोस्।",
    "HIGH": "स्थानान्तरणको तयारी गर्नुहोस्। वडा अधिकारी र प्रहरीलाई WhatsApp सूचना पठाउनुहोस्।",
    "CRITICAL": "तत्काल स्थानान्तरण गर्नुहोस्। सबै सूचना माध्यम सक्रिय। BIPAD लाई स्वतः सूचना।",
}


@dataclass
class RiskScoreOutput:
    """
    Complete risk assessment for a single zone.

    Produced by the Risk Score Engine every 15 minutes per zone.
    This is the canonical output consumed by all downstream systems.
    """
    zone_id: str
    timestamp: datetime
    risk_score: float  # 0-100
    risk_level: str  # LOW | MODERATE | HIGH | CRITICAL
    primary_driver: str  # e.g. "Rainfall 72hr accumulated"
    secondary_factors: list[str] = field(default_factory=list)
    affected_infrastructure: list[str] = field(default_factory=list)
    confidence: float = 0.5  # 0-1 model certainty
    predicted_peak_time: Optional[datetime] = None
    recommended_action: str = ""
    recommended_action_ne: str = ""

    # ── Sub-scores (for dashboard drill-down) ────────────────
    rainfall_subscore: float = 0.0  # 0-100
    ground_condition_subscore: float = 0.0  # 0-100
    static_risk_subscore: float = 0.0  # 0-100
    satellite_subscore: float = 0.0  # 0-100

    # ── Input snapshot (for audit/debug) ──────────────────────
    soil_moisture_pct: Optional[float] = None
    ground_tilt_deg: Optional[float] = None
    vibration_g: Optional[float] = None
    rainfall_1hr_mm: Optional[float] = None
    rainfall_6hr_mm: Optional[float] = None
    rainfall_24hr_mm: Optional[float] = None
    rainfall_72hr_mm: Optional[float] = None
    slope_angle_deg: float = 0.0
    ndvi_index: Optional[float] = None
    deformation_flag: bool = False

    def __post_init__(self):
        self.risk_level = classify_risk(self.risk_score)
        self.recommended_action = RECOMMENDED_ACTIONS[self.risk_level]
        self.recommended_action_ne = RECOMMENDED_ACTIONS_NE[self.risk_level]

    def to_dict(self) -> dict:
        """Serialize to dict for InfluxDB/JSON/API transport."""
        return {
            "zone_id": self.zone_id,
            "timestamp": self.timestamp.isoformat(),
            "risk_score": round(self.risk_score, 1),
            "risk_level": self.risk_level,
            "risk_color": risk_color(self.risk_level),
            "primary_driver": self.primary_driver,
            "secondary_factors": self.secondary_factors,
            "affected_infrastructure": self.affected_infrastructure,
            "confidence": round(self.confidence, 2),
            "predicted_peak_time": (
                self.predicted_peak_time.isoformat()
                if self.predicted_peak_time
                else None
            ),
            "recommended_action": self.recommended_action,
            "recommended_action_ne": self.recommended_action_ne,
            "rainfall_subscore": round(self.rainfall_subscore, 1),
            "ground_condition_subscore": round(self.ground_condition_subscore, 1),
            "static_risk_subscore": round(self.static_risk_subscore, 1),
            "satellite_subscore": round(self.satellite_subscore, 1),
            "soil_moisture_pct": self.soil_moisture_pct,
            "ground_tilt_deg": self.ground_tilt_deg,
            "vibration_g": self.vibration_g,
            "rainfall_1hr_mm": self.rainfall_1hr_mm,
            "rainfall_6hr_mm": self.rainfall_6hr_mm,
            "rainfall_24hr_mm": self.rainfall_24hr_mm,
            "rainfall_72hr_mm": self.rainfall_72hr_mm,
            "slope_angle_deg": self.slope_angle_deg,
            "ndvi_index": self.ndvi_index,
            "deformation_flag": self.deformation_flag,
        }