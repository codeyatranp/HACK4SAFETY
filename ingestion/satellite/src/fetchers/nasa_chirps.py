"""
NASA CHIRPS Daily Rainfall Fetcher

Fetches historical daily rainfall data from NASA's Climate Hazards Group
InfraRed Precipitation with Stations (CHIRPS) dataset at 0.05° resolution.
CHIRPS provides merged satellite and station-based rainfall estimates
for the entire globe with 30+ years of historical data.

CHIRPS v2.0 file URL pattern:
  https://data.chc.ucsb.edu/products/CHIRPS-2.0/global-daily/netcdf/daily/p05/{yyyy}/chirps-v2.0.{yyyy}.days_p05.nc
"""
import os
import json
import logging
import tempfile
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("nasa-chirps")

NEPAL_BOUNDS = {"north": 30.4, "south": 26.3, "east": 88.2, "west": 80.0}

class NASAChirpsFetcher:
    """
    NASA CHIRPS daily rainfall data fetcher.
    Uses the yearly aggregate NetCDF file for reliability and speed.
    """

    def __init__(self):
        self.influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        self.influx_token = os.getenv("INFLUXDB_TOKEN", "geoguard-influx-token")
        self.influx_org = os.getenv("INFLUXDB_ORG", "geoguard")
        self.influx_bucket = os.getenv("INFLUXDB_BUCKET", "sensor-data")
        
        self._client: Optional[httpx.AsyncClient] = None

    def _load_zones(self):
        zones_path = os.getenv("ZONES_CONFIG_PATH") or os.path.join(
            os.path.dirname(__file__), "..", "..", "..",
            "shared", "config", "nepal_zones.json",
        )
        if not os.path.exists(zones_path):
            alt_path = "/app/shared/config/nepal_zones.json"
            if os.path.exists(alt_path):
                zones_path = alt_path

        with open(zones_path) as f:
            return json.load(f)["zones"]

    async def _ensure_authenticated(self) -> bool:
        """Simple activation check (CHIRPS doesn't strictly require login for public files)."""
        if self._client:
            return True
            
        self._client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=120.0,
            headers={
                "User-Agent": "GeoGuard-Nepal/1.0 (Research Project; contact@geoguard.org)"
            }
        )
        return True

    async def _close_client(self):
        if self._client:
            await self._client.aclose()
            self._client = None

    def _build_chirps_url(self, dt: datetime) -> str:
        """
        Build URL for CHIRPS v2.0 yearly daily rainfall NetCDF file.
        """
        yyyy = dt.strftime("%Y")
        return f"https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05/chirps-v2.0.{yyyy}.days_p05.nc"

    async def _download_chirps_file(self, url: str) -> Optional[str]:
        """Download CHIRPS NetCDF file with caching."""
        if not await self._ensure_authenticated():
            return None

        cache_dir = os.path.join(tempfile.gettempdir(), "geoguard_cache")
        os.makedirs(cache_dir, exist_ok=True)
        filename = os.path.basename(url)
        cache_path = os.path.join(cache_dir, filename)

        if os.path.exists(cache_path):
            # Yearly file is stable, but re-download if older than 7 days
            mtime = datetime.fromtimestamp(os.path.getmtime(cache_path))
            if (datetime.now() - mtime).days < 7:
                logger.info(f"CHIRPS: Using cached file {cache_path}")
                return cache_path

        try:
            if not self._client:
                return None
                
            logger.info(f"CHIRPS: Downloading {url}...")
            async with self._client.stream("GET", url) as resp:
                if resp.status_code == 200:
                    with open(cache_path, "wb") as f:
                        async for chunk in resp.iter_bytes():
                            f.write(chunk)
                    logger.info(f"CHIRPS: Downloaded {os.path.getsize(cache_path)/(1024*1024):.1f} MB")
                    return cache_path
                else:
                    logger.error(f"CHIRPS: Download failed {resp.status_code}")
                    return None
        except Exception as e:
            logger.error(f"CHIRPS: Download exception: {e}")
            return None

    def _process_chirps_netcdf(self, filepath: str, target_date: datetime = None) -> dict:
        """Process NetCDF and extract zonal rainfall stats for a specific date."""
        try:
            import xarray as xr
            import numpy as np

            ds = xr.open_dataset(filepath, engine="netcdf4")

            # Subset to Nepal for performance
            nepal_ds = ds.sel(
                lat=slice(NEPAL_BOUNDS["south"], NEPAL_BOUNDS["north"]),
                lon=slice(NEPAL_BOUNDS["west"], NEPAL_BOUNDS["east"])
            )

            if target_date and "time" in nepal_ds.coords:
                # Find nearest time (usually daily at 00:00)
                # target_date is UTC
                target_np = np.datetime64(target_date.strftime("%Y-%m-%d"))
                try:
                    nepal_ds = nepal_ds.sel(time=target_np, method="nearest")
                except:
                    logger.warning(f"CHIRPS: Could not select exact date {target_date}, using latest available")
                    nepal_ds = nepal_ds.isel(time=-1)

            precip_var = None
            for v in ["precip", "precipitation", "precipitation_amount"]:
                if v in nepal_ds.data_vars:
                    precip_var = v
                    break
            
            if not precip_var:
                logger.error(f"CHIRPS: No precip var in {list(nepal_ds.data_vars)}")
                ds.close()
                return {}

            precip = nepal_ds[precip_var]
            zones = self._load_zones()
            zone_data = {}

            for zone in zones:
                zlat, zlng = zone["center"]["lat"], zone["center"]["lng"]
                radius = zone.get("radius_km", 5.0)
                lat_r = radius / 111.0
                lon_r = radius / (111.0 * np.cos(np.radians(zlat)))

                try:
                    subset = precip.sel(
                        lat=slice(zlat - lat_r, zlat + lat_r),
                        lon=slice(zlng - lon_r, zlng + lon_r)
                    )
                    val = float(subset.mean().values)
                except:
                    val = float(precip.sel(lat=zlat, lon=zlng, method="nearest").values)

                # CHIRPS is daily mm
                daily_val = round(max(0.0, val), 1)
                zone_data[zone["zone_id"]] = {
                    "1hr_mm": round(daily_val / 24, 1), # Approx hourly
                    "6hr_mm": round(daily_val / 4, 1),
                    "24hr_mm": daily_val,
                    "72hr_mm": daily_val * 3 # Very rough estimate if only one day processed
                }

            ds.close()
            return zone_data
        except Exception as e:
            logger.error(f"CHIRPS: Processing error: {e}")
            return {}

    async def fetch_and_process(self, days_back: int = 30):
        """Main pipeline - fetch CHIRPS data for the past N days."""
        await self._ensure_authenticated()
        
        now = datetime.now(timezone.utc)
        url = self._build_chirps_url(now)
        
        filepath = await self._download_chirps_file(url)
        if not filepath:
            return

        written = 0
        for day_offset in range(days_back):
            target = now - timedelta(days=day_offset)
            data = self._process_chirps_netcdf(filepath, target)
            if data:
                for zid, vals in data.items():
                    self._write_to_influxdb(zid, vals, source="nasa_chirps", timestamp=target)
                    written += 1
                logger.info(f"CHIRPS: SUCCESS - Processed {len(data)} zones for {target.date()}")
        
        await self._close_client()
        return written

    def _write_to_influxdb(self, zone_id: str, rainfall: dict, source: str, timestamp: datetime = None):
        """Write to InfluxDB."""
        try:
            from influxdb_client import InfluxDBClient, Point
            from influxdb_client.client.write_api import SYNCHRONOUS

            with InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org) as client:
                write_api = client.write_api(write_options=SYNCHRONOUS)
                point = Point("satellite_rainfall") \
                    .tag("zone_id", zone_id) \
                    .tag("source", source) \
                    .field("rainfall_1hr_mm", float(rainfall.get("1hr_mm", 0.0))) \
                    .field("rainfall_6hr_mm", float(rainfall.get("6hr_mm", 0.0))) \
                    .field("rainfall_24hr_mm", float(rainfall.get("24hr_mm", 0.0))) \
                    .field("rainfall_72hr_mm", float(rainfall.get("72hr_mm", 0.0))) \
                    .time(timestamp or datetime.now(timezone.utc))
                
                write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=point)
        except Exception as e:
            logger.error(f"CHIRPS: Influx write failed for {zone_id}: {e}")

    async def _simulate_and_write(self, days_back: int):
        """No-op for real-only requirement, but kept for interface compatibility."""
        pass
