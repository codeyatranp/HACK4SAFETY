"""
GeoGuard Satellite & Open Data Fetcher — Main Scheduler

Orchestrates periodic fetches from NASA GPM, ESA Sentinel-1/2,
OSM, and ISRO BHUVAN. Each source runs on its own schedule.

Real data path: when credentials exist, fetches from actual APIs.
Simulation path: when credentials are missing, generates realistic data.
"""
import os
import sys
import json
import logging
import asyncio
from datetime import datetime, timezone, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [Satellite] %(message)s")
logger = logging.getLogger("satellite-fetcher")

# Lazy imports — fetchers may not all work without credentials
try:
    from fetchers.nasa_gpm import NASAGPMFetcher
    GPM_AVAILABLE = True
except ImportError:
    GPM_AVAILABLE = False
    logger.warning("NASA GPM fetcher not available (missing dependencies)")

try:
    from fetchers.sentinel1 import Sentinel1Fetcher
    S1_AVAILABLE = True
except ImportError:
    S1_AVAILABLE = False
    logger.warning("Sentinel-1 fetcher not available")

try:
    from fetchers.sentinel2 import Sentinel2Fetcher
    S2_AVAILABLE = True
except ImportError:
    S2_AVAILABLE = False
    logger.warning("Sentinel-2 fetcher not available")

try:
    from fetchers.osm_sync import OSMSyncFetcher
    OSM_AVAILABLE = True
except ImportError:
    OSM_AVAILABLE = False
    logger.warning("OSM sync fetcher not available")


class SatelliteFetcher:
    """Main scheduler for all satellite and open data sources."""

    def __init__(self):
        self.scheduler = AsyncIOScheduler()

        # Check what credentials are available
        self.has_nasa = bool(os.getenv("NASA_EARTHDATA_USERNAME"))
        self.has_copernicus = bool(os.getenv("COPERNICUS_USERNAME"))

    async def fetch_nasa_gpm(self):
        """Fetch NASA GPM IMERG rainfall (30-min interval)."""
        if not GPM_AVAILABLE:
            logger.debug("NASA GPM: not installed — skipping")
            return

        logger.info("NASA GPM: starting fetch cycle...")
        try:
            fetcher = NASAGPMFetcher()
            await fetcher.fetch_and_process()
            logger.info("NASA GPM: fetch complete")
        except Exception as e:
            logger.error(f"NASA GPM fetch failed: {e}")

    async def fetch_sentinel1(self):
        """Fetch ESA Sentinel-1 SAR ground deformation (1-hr check, 6-day actual)."""
        if not S1_AVAILABLE:
            logger.debug("Sentinel-1: not installed — skipping")
            return

        logger.info("Sentinel-1: starting catalogue search...")
        try:
            fetcher = Sentinel1Fetcher()
            await fetcher.fetch_and_process()
            logger.info("Sentinel-1: search complete")
        except Exception as e:
            logger.error(f"Sentinel-1 fetch failed: {e}")

    async def fetch_sentinel2(self):
        """Fetch ESA Sentinel-2 vegetation/land cover (6-hr check, 5-day actual)."""
        if not S2_AVAILABLE:
            logger.debug("Sentinel-2: not installed — skipping")
            return

        logger.info("Sentinel-2: starting catalogue search...")
        try:
            fetcher = Sentinel2Fetcher()
            await fetcher.fetch_and_process()
            logger.info("Sentinel-2: search complete")
        except Exception as e:
            logger.error(f"Sentinel-2 fetch failed: {e}")

    async def fetch_osm(self):
        """Sync OpenStreetMap Nepal extract (daily)."""
        if not OSM_AVAILABLE:
            logger.debug("OSM sync: not installed — skipping")
            return

        logger.info("OSM: starting daily sync...")
        try:
            fetcher = OSMSyncFetcher()
            await fetcher.fetch_and_process()
            logger.info("OSM: sync complete")
        except Exception as e:
            logger.error(f"OSM sync failed: {e}")

    async def all_sources_status(self):
        """Log status of all satellite sources."""
        logger.info("=" * 60)
        logger.info("Satellite Data Fetcher Status:")
        logger.info(f"  NASA GPM IMERG:     {'ENABLED (real)' if self.has_nasa else 'SIMULATION MODE'}")
        logger.info(f"  Sentinel-1 SAR:     {'ENABLED (real)' if self.has_copernicus else 'NOT CONFIGURED'}")
        logger.info(f"  Sentinel-2 Optical: {'ENABLED (real)' if self.has_copernicus else 'NOT CONFIGURED'}")
        logger.info(f"  OSM Sync:           ALWAYS ENABLED (no auth required)")
        logger.info("=" * 60)

    def start(self):
        """Start the scheduler with all jobs."""
        # NASA GPM: every 30 minutes
        self.scheduler.add_job(
            self.fetch_nasa_gpm,
            "interval",
            minutes=30,
            id="nasa_gpm",
        )

        # Sentinel-1 SAR: check every hour
        self.scheduler.add_job(
            self.fetch_sentinel1,
            "interval",
            hours=1,
            id="sentinel1",
        )

        # Sentinel-2: check every 6 hours
        self.scheduler.add_job(
            self.fetch_sentinel2,
            "interval",
            hours=6,
            id="sentinel2",
        )

        # OSM: daily at 03:00 UTC
        self.scheduler.add_job(
            self.fetch_osm,
            "cron",
            hour=3,
            minute=0,
            id="osm_daily",
        )

        # Status report: every hour
        self.scheduler.add_job(
            self.all_sources_status,
            "interval",
            hours=1,
            id="status_report",
            next_run_time=datetime.now(timezone.utc) + timedelta(seconds=10),
        )

        self.scheduler.start()
        mode_nasa = "REAL DATA" if self.has_nasa else "SIMULATION"
        mode_s1 = "REAL SEARCH" if self.has_copernicus else "NOT CONFIGURED"
        mode_s2 = "REAL SEARCH" if self.has_copernicus else "NOT CONFIGURED"

        logger.info("Satellite Data Fetcher started.")
        logger.info(f"  NASA GPM:    every 30 min  ({mode_nasa})")
        logger.info(f"  Sentinel-1:  every 1 hr   ({mode_s1})")
        logger.info(f"  Sentinel-2:  every 6 hr   ({mode_s2})")
        logger.info(f"  OSM Sync:    daily 03:00 UTC")

        try:
            asyncio.get_event_loop().run_forever()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Satellite Data Fetcher shutting down.")
            self.scheduler.shutdown()


if __name__ == "__main__":
    fetcher = SatelliteFetcher()
    fetcher.start()