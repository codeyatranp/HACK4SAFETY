"""
NASA GPM IMERG Rainfall Fetcher

Fetches global precipitation data from NASA's Global Precipitation
Measurement (GPM) IMERG product at 0.1° resolution every 30 minutes.

GPM IMERG v07 file URL pattern:
  https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.07/{yyyy}/{mm}/3IMERGHH.{yyyymmdd}-S{hhmm00}-E{hhmm59}.{version}.nc4
"""
import os
import json
import random
import logging
import tempfile
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("nasa-gpm")

# NASA GES DISC endpoints
GPM_BASE_URL = "https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3"
EARTHDATA_LOGIN_URL = "https://urs.earthdata.nasa.gov"
EARTHDATA_AUTH_URL = "https://urs.earthdata.nasa.gov/oauth/authorize"

NEPAL_BOUNDS = {"north": 30.4, "south": 26.3, "east": 88.2, "west": 80.0}

class NASAGPMFetcher:
    """
    NASA GPM IMERG rainfall data fetcher.
    Uses session-based auth with Earthdata Login cookies.
    """

    def __init__(self):
        self.username = os.getenv("NASA_EARTHDATA_USERNAME")
        self.password = os.getenv("NASA_EARTHDATA_PASSWORD")
        self.env_token = os.getenv("NASA_EARTHDATA_TOKEN")
        
        self.influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        self.influx_token = os.getenv("INFLUXDB_TOKEN", "geoguard-influx-token")
        self.influx_org = os.getenv("INFLUXDB_ORG", "geoguard")
        self.influx_bucket = os.getenv("INFLUXDB_BUCKET", "sensor-data")
        
        self._token: Optional[str] = self.env_token
        self._client: Optional[httpx.AsyncClient] = None
        self._authenticated = False

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
        """Authenticate with Earthdata Login and maintain session cookies."""
        if self._authenticated and self._client:
            return True

        if not self.username or not self.password:
            logger.error("NASA Earthdata: no credentials available")
            return False

        try:
            self._client = httpx.AsyncClient(
                follow_redirects=True,
                timeout=60.0,
                cookies={}
            )

            auth_url = f"{EARTHDATA_AUTH_URL}?client_id=e2WVk8Pw6weeLUKZYOxvTQ&response_type=code&redirect_uri=https%3A%2F%2Fgpm1.gesdisc.eosdis.nasa.gov%2Fdata-redirect"
            
            resp = await self._client.get(auth_url)
            
            if "login" in str(resp.url).lower() or resp.status_code == 200:
                login_data = {
                    "username": self.username,
                    "password": self.password,
                }
                resp = await self._client.post(str(resp.url), data=login_data)
                
                if "urs.earthdata.nasa.gov" in str(resp.url) and resp.status_code == 200:
                    if "error" in resp.text.lower() or "invalid" in resp.text.lower():
                        logger.error("NASA Earthdata: login failed - invalid credentials")
                        return False

            self._authenticated = True
            logger.info("NASA Earthdata: authenticated successfully")
            return True

        except Exception as e:
            logger.error(f"NASA Earthdata authentication error: {e}")
            return False

    async def _close_client(self):
        if self._client:
            await self._client.aclose()
            self._client = None
            self._authenticated = False

    def _build_gpm_url(self, dt: datetime, product: str = "GPM_3IMERGHHE.07") -> str:
        """
        Build URL for GPM IMERG v07.
        Example: 3B-HHR-E.MS.MRG.3IMERG.20240612-S130000-E132959.0780.V07B.HDF5
        """
        # Round down to nearest 30 mins
        dt = dt.replace(second=0, microsecond=0)
        if dt.minute >= 30:
            start_dt = dt.replace(minute=30)
        else:
            start_dt = dt.replace(minute=0)
            
        end_dt = start_dt + timedelta(minutes=29, seconds=59)

        yyyy = start_dt.strftime("%Y")
        doy = start_dt.strftime("%j")
        date_part = start_dt.strftime("%Y%m%d")
        start_part = f"S{start_dt.strftime('%H%M%S')}"
        end_part = f"E{end_dt.strftime('%H%M%S')}"

        # Granule is minutes from start of day
        granule = f"{(start_dt.hour * 60 + start_dt.minute):04d}"

        prefix = "3B-HHR.MS.MRG.3IMERG"
        if "3IMERGHHE" in product:
            prefix = "3B-HHR-E.MS.MRG.3IMERG"
        elif "3IMERGHHL" in product:
            prefix = "3B-HHR-L.MS.MRG.3IMERG"

        filename = f"{prefix}.{date_part}-{start_part}-{end_part}.{granule}.V07C.HDF5"
        return f"{GPM_BASE_URL}/{product}/{yyyy}/{doy}/{filename}"

    async def _download_gpm_file(self, url: str) -> Optional[str]:
        """Download using authenticated session with Earthdata cookies."""
        if not await self._ensure_authenticated():
            logger.error("GPM: Authentication failed")
            return None

        try:
            if not self._client:
                return None
                
            resp = await self._client.get(url)
            
            if resp.status_code == 200:
                if b"<!DOCTYPE html>" in resp.content[:200]:
                    logger.error("GPM: Download returned HTML (likely login page) instead of data.")
                    return None
                    
                tmp_dir = tempfile.mkdtemp(prefix="geoguard_gpm_")
                filepath = os.path.join(tmp_dir, "gpm_imerg.hdf5")
                with open(filepath, "wb") as f:
                    f.write(resp.content)
                logger.info(f"GPM: Downloaded {len(resp.content)/(1024*1024):.1f} MB from {url}")
                return filepath
            elif resp.status_code == 401:
                logger.error(f"GPM: Auth failed (401) for {url} - session may have expired")
                self._authenticated = False
                return None
            elif resp.status_code == 404:
                logger.warning(f"GPM: File not found (404) at {url}")
                return None
            else:
                logger.error(f"GPM: Download failed {resp.status_code}")
                return None
        except Exception as e:
            logger.error(f"GPM: Download exception: {e}")
            return None

    def _process_gpm_netcdf(self, filepath: str) -> dict:
        """Process NetCDF4/HDF5 and extract zonal stats."""
        try:
            import xarray as xr
            import numpy as np

            # engine="netcdf4" is required for HDF5 files from NASA
            ds = xr.open_dataset(filepath, engine="netcdf4")

            # Look for precipitation variable
            precip_var = None
            for v in ["precipitation", "precipitationCal", "precipitationUncal"]:
                if v in ds.data_vars:
                    precip_var = v
                    break
            
            if not precip_var:
                logger.error(f"GPM: No precip var in {list(ds.data_vars)}")
                ds.close()
                return {}

            precip = ds[precip_var]

            # GPM lon is usually 0-360, but check
            if "lon" in precip.coords:
                if float(precip.lon.max()) > 180:
                    precip = precip.assign_coords(lon=((precip.lon + 180) % 360 - 180))
                    precip = precip.sortby("lon")

            # Crop to Nepal
            nepal_precip = precip.sel(
                lat=slice(NEPAL_BOUNDS["south"], NEPAL_BOUNDS["north"]),
                lon=slice(NEPAL_BOUNDS["west"], NEPAL_BOUNDS["east"])
            )

            zones = self._load_zones()
            zone_data = {}

            for zone in zones:
                zlat, zlng = zone["center"]["lat"], zone["center"]["lng"]
                # Use slightly larger radius for 0.1 deg satellite data
                radius = zone.get("radius_km", 10.0)
                lat_r = radius / 111.0
                lon_r = radius / (111.0 * np.cos(np.radians(zlat)))

                try:
                    subset = nepal_precip.sel(
                        lat=slice(zlat - lat_r, zlat + lat_r),
                        lon=slice(zlng - lon_r, zlng + lon_r)
                    )
                    val = float(subset.mean().values)
                except:
                    # Fallback to nearest point
                    val = float(nepal_precip.sel(lat=zlat, lon=zlng, method="nearest").values)

                # Rainfall is rate (mm/hr), but we store as accumulation estimate
                rainfall_1hr = round(max(0.0, val), 1)
                zone_data[zone["zone_id"]] = {
                    "1hr_mm": rainfall_1hr,
                    "24hr_mm": round(rainfall_1hr * 12, 1) # Simple multiplier for sim consistency
                }

            ds.close()
            return zone_data
        except Exception as e:
            logger.error(f"GPM: Processing error: {e}")
            return {}
        finally:
            if os.path.exists(filepath):
                try:
                    os.unlink(filepath)
                    os.rmdir(os.path.dirname(filepath))
                except: pass

    async def fetch_and_process(self):
        """Main pipeline."""
        if not self.username and not self.env_token:
            logger.warning("GPM: No credentials - simulating")
            await self._simulate_and_write()
            return

        # Products: Early (4h delay), Late (14h delay)
        products = ["GPM_3IMERGHHE.07", "GPM_3IMERGHHL.07"]
        now = datetime.now(timezone.utc)
        
        # Search back up to 24 hours
        for hours_back in range(4, 25):
            target = now - timedelta(hours=hours_back)
            for prod in products:
                url = self._build_gpm_url(target, prod)
                logger.info(f"GPM: Trying {prod} at {target.isoformat()}")
                
                filepath = await self._download_gpm_file(url)
                if filepath:
                    data = self._process_gpm_netcdf(filepath)
                    if data:
                        for zid, vals in data.items():
                            self._write_to_influxdb(zid, vals, source=f"nasa_gpm_{prod.split('.')[0].lower()}", timestamp=target)
                        logger.info(f"GPM: SUCCESS - Processed {len(data)} zones from {prod}")
                        await self._close_client()
                        return

        logger.warning("GPM: No real data found - falling back to simulation")
        await self._simulate_and_write()
        await self._close_client()

    async def _simulate_and_write(self, days_back: int = 1):
        """Realistic simulation fallback with backfill support."""
        zones = self._load_zones()
        
        logger.info(f"GPM: Simulating {len(zones)} zones for {days_back} days")
        
        for zone in zones:
            for day_offset in range(days_back):
                target_time = datetime.now(timezone.utc) - timedelta(days=day_offset)
                # June–September: monsoon season (higher values)
                month = target_time.month
                is_monsoon = 6 <= month <= 9
                
                base = random.uniform(2, 6) if is_monsoon else random.uniform(0, 1)
                rainfall = {
                    "1hr_mm": round(max(0, random.gauss(base, base*0.3)), 1),
                    "24hr_mm": round(max(0, random.gauss(base*10, base*2)), 1)
                }
                self._write_to_influxdb(
                    zone["zone_id"], 
                    rainfall, 
                    source="nasa_gpm_simulated", 
                    timestamp=target_time
                )

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
                    .field("rainfall_24hr_mm", float(rainfall.get("24hr_mm", 0.0))) \
                    .time(timestamp or datetime.now(timezone.utc))
                
                write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=point)
        except Exception as e:
            logger.error(f"GPM: Influx write failed for {zone_id}: {e}")
