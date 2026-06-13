"""
Unified data model consumed by the Risk Score Engine.
All three ingestion streams converge to this format.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class UnifiedRiskInput:
    """
    Unified data point for the Risk Score Engine.
    Produced every 15 minutes per zone by the ingestion layer.
    """

    zone_id: str  # e.g. "SINDHUPALCHOK-05"
    timestamp: datetime  # ISO 8601
    source: str  # "sensor" | "satellite" | "dhm" | "merged"

    # Ground conditions (from IoT sensors or satellite proxy)
    soil_moisture_pct: Optional[float] = None
    ground_tilt_deg: Optional[float] = None
    vibration_g: Optional[float] = None

    # Rainfall (all sources merged, source flagged)
    rainfall_1hr_mm: Optional[float] = None
    rainfall_6hr_mm: Optional[float] = None
    rainfall_24hr_mm: Optional[float] = None
    rainfall_72hr_mm: Optional[float] = None
    rainfall_source: str = "satellite_proxy"  # "dhm" | "nasa_gpm" | "sensor" | "satellite_proxy"

    # Satellite-derived
    ndvi_index: Optional[float] = None  # Sentinel-2 vegetation
    deformation_flag: bool = False  # Sentinel-1 SAR
    deformation_mm: Optional[float] = None  # displacement magnitude

    # Static (from DEM / pre-loaded)
    slope_angle_deg: float = 0.0  # SRTM/ALOS DEM
    historical_frequency: float = 0.0  # ISRO BHUVAN historical data

    # Metadata
    confidence: float = 0.5  # data quality score (0-1)
    data_freshness_sec: int = 999999  # age of newest data point
    sensor_battery_pct: Optional[float] = None  # only for sensor source

    def to_dict(self) -> dict:
        """Serialize to dict for InfluxDB/JSON transport."""
        return {
            "zone_id": self.zone_id,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "soil_moisture_pct": self.soil_moisture_pct,
            "ground_tilt_deg": self.ground_tilt_deg,
            "vibration_g": self.vibration_g,
            "rainfall_1hr_mm": self.rainfall_1hr_mm,
            "rainfall_6hr_mm": self.rainfall_6hr_mm,
            "rainfall_24hr_mm": self.rainfall_24hr_mm,
            "rainfall_72hr_mm": self.rainfall_72hr_mm,
            "rainfall_source": self.rainfall_source,
            "ndvi_index": self.ndvi_index,
            "deformation_flag": self.deformation_flag,
            "deformation_mm": self.deformation_mm,
            "slope_angle_deg": self.slope_angle_deg,
            "historical_frequency": self.historical_frequency,
            "confidence": self.confidence,
            "data_freshness_sec": self.data_freshness_sec,
            "sensor_battery_pct": self.sensor_battery_pct,
        }

    @classmethod
    def for_zone(cls, zone_id: str, slope_angle: float = 30.0, historical: float = 0.1):
        """Create a minimal baseline input for a zone (satellite-only)."""
        return cls(
            zone_id=zone_id,
            timestamp=datetime.utcnow(),
            source="satellite",
            slope_angle_deg=slope_angle,
            historical_frequency=historical,
        )