"""
Satellite data processing utilities.
Crops raster data to Nepal and extracts zonal statistics.
"""
import json
import os
import logging
from typing import Optional

logger = logging.getLogger("satellite-processor")


def crop_to_nepal_bounds(dataset, crop_epsg: int = 4326):
    """
    Crop a raster/xarray dataset to Nepal's bounding box.
    
    Phase 2: implemented with rioxarray when satellite data is available.
    Phase 1: returns dataset unchanged.
    """
    try:
        import rioxarray  # noqa: F401

        from shared.config.bounding_box import NEPAL_BOUNDS

        cropped = dataset.rio.clip_box(
            minx=NEPAL_BOUNDS["west"],
            miny=NEPAL_BOUNDS["south"],
            maxx=NEPAL_BOUNDS["east"],
            maxy=NEPAL_BOUNDS["north"],
        )
        return cropped
    except ImportError:
        logger.debug("rioxarray not installed — skipping crop")
        return dataset


def extract_zonal_stats(zone_id: str, dataset, stat: str = "mean") -> Optional[float]:
    """
    Extract zonal statistics for a single GeoGuard zone from a raster.
    
    Phase 2: implemented with rasterio/zonal when available.
    Phase 1: returns None.
    """
    try:
        # Import zone geometry from config
        zones_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "shared", "config", "nepal_zones.json",
        )
        with open(zones_path) as f:
            zones = {z["zone_id"]: z for z in json.load(f)["zones"]}

        zone = zones.get(zone_id)
        if not zone:
            return None

        # For Phase 1: return None (satellite data not yet integrated)
        # TODO Phase 2: use rasterstats.zonal_stats with zone polygon
        return None

    except Exception as e:
        logger.error(f"Zonal stats error for {zone_id}: {e}")
        return None