"""
Open-Meteo Historical Rainfall Fetcher

Fetches real historical rainfall data from Open-Meteo's Archive API.
No authentication required, highly reliable for real data.
"""
import os
import json
import logging
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("open-meteo")

class OpenMeteoFetcher:
    """
    Fetcher for real historical rainfall data using Open-Meteo Archive API.
    """

    def __init__(self):
        self.influx_url = os.getenv("INFLUXDB_URL", "http://localhost:8086")
        self.influx_token = os.getenv("INFLUXDB_TOKEN", "geoguard-influx-token")
        self.influx_org = os.getenv("INFLUXDB_ORG", "geoguard")
        self.influx_bucket = os.getenv("INFLUXDB_BUCKET", "sensor-data")
        
        self.client = httpx.AsyncClient(timeout=30.0)

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

    async def fetch_and_process(self, days_back: int = 30):
        """Fetch rainfall for all zones over the past N days."""
        zones = self._load_zones()
        end_date = datetime.now(timezone.utc) - timedelta(days=2) # Archive usually has 2-day delay
        start_date = end_date - timedelta(days=days_back)
        
        start_str = start_date.strftime("%Y-%m-%d")
        end_str = end_date.strftime("%Y-%m-%d")
        
        logger.info(f"Open-Meteo: Fetching rainfall for {len(zones)} zones from {start_str} to {end_str}")
        
        written = 0
        for zone in zones:
            lat, lng = zone["center"]["lat"], zone["center"]["lng"]
            zone_id = zone["zone_id"]
            
            url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lng}&start_date={start_str}&end_date={end_str}&hourly=precipitation&timezone=auto"
            
            try:
                resp = await self.client.get(url)
                if resp.status_code == 200:
                    data = resp.json()
                    hourly = data.get("hourly", {})
                    times = hourly.get("time", [])
                    precip = hourly.get("precipitation", [])
                    
                    for t_str, val in zip(times, precip):
                        # t_str is like "2026-05-14T00:00"
                        ts = datetime.fromisoformat(t_str).replace(tzinfo=timezone.utc)
                        
                        # Store in InfluxDB
                        rainfall = {
                            "1hr_mm": float(val),
                            "24hr_mm": 0.0 # Will be aggregated by Influx if needed, or we could calc here
                        }
                        # For 24hr_mm, we could sum the last 24 values, but let's keep it simple for now
                        self._write_to_influxdb(zone_id, rainfall, ts)
                        written += 1
                    
                    logger.info(f"Open-Meteo: Processed {len(times)} hours for zone {zone_id}")
                    # Respect rate limits (though Open-Meteo is generous)
                    await asyncio.sleep(0.1)
                else:
                    logger.error(f"Open-Meteo: API failed for {zone_id}: {resp.status_code}")
            except Exception as e:
                logger.error(f"Open-Meteo: Exception for {zone_id}: {e}")

        await self.client.aclose()
        return written

    def _write_to_influxdb(self, zone_id: str, rainfall: dict, timestamp: datetime):
        """Write to InfluxDB."""
        try:
            from influxdb_client import InfluxDBClient, Point
            from influxdb_client.client.write_api import SYNCHRONOUS

            with InfluxDBClient(url=self.influx_url, token=self.influx_token, org=self.influx_org) as client:
                write_api = client.write_api(write_options=SYNCHRONOUS)
                point = Point("satellite_rainfall") \
                    .tag("zone_id", zone_id) \
                    .tag("source", "open_meteo") \
                    .field("rainfall_1hr_mm", float(rainfall.get("1hr_mm", 0.0))) \
                    .time(timestamp)
                
                write_api.write(bucket=self.influx_bucket, org=self.influx_org, record=point)
        except Exception as e:
            pass # Keep logs clean from repetitive write errors if any
