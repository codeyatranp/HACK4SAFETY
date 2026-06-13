#!/usr/bin/env python3
"""
Backfill GeoGuard with real Nepal source data and recompute dashboard state.

By default this script avoids simulated fallback data. Pass --allow-simulated
only when you explicitly want development-mode data.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "ingestion" / "satellite" / "src"))
sys.path.insert(0, str(ROOT / "ingestion" / "dhm" / "src"))

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import psycopg2

from fetchers.copernicus_auth import CopernicusAuth
from fetchers.nasa_gpm import NASAGPMFetcher
from fetchers.sentinel1 import NEPAL_BBOX as S1_BBOX, Sentinel1Fetcher
from fetchers.sentinel2 import NEPAL_BBOX as S2_BBOX, Sentinel2Fetcher
from processing.engine.src.engine.risk_engine import RiskEngine

load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BACKFILL] %(message)s")
logger = logging.getLogger("geoguard-backfill")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--step-hours", type=int, default=6)
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--include-osm", action="store_true")
    parser.add_argument("--allow-simulated", action="store_true")
    parser.add_argument("--skip-risk", action="store_true")
    return parser.parse_args()


def pg_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER", "geoguard"),
        password=os.getenv("POSTGRES_PASSWORD", "geoguard_admin"),
        dbname=os.getenv("POSTGRES_DB", "geoguard"),
    )


def cleanup_postgres() -> None:
    sql_path = ROOT / "scripts" / "cleanup-runtime-data.sql"
    logger.info("Cleaning Postgres runtime data")
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(sql_path.read_text())


def cleanup_influx() -> None:
    logger.info("Cleaning InfluxDB GeoGuard measurements")
    client = InfluxDBClient(
        url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
        token=os.getenv("INFLUXDB_TOKEN", "geoguard-influx-token"),
        org=os.getenv("INFLUXDB_ORG", "geoguard"),
    )
    delete_api = client.delete_api()
    bucket = os.getenv("INFLUXDB_BUCKET", "sensor-data")
    org = os.getenv("INFLUXDB_ORG", "geoguard")
    for measurement in (
        "sensor_reading",
        "dhm_rainfall",
        "satellite_rainfall",
        "satellite_data",
        "risk_score",
    ):
        delete_api.delete(
            start="1970-01-01T00:00:00Z",
            stop=utc_now().isoformat(),
            predicate=f'_measurement="{measurement}"',
            bucket=bucket,
            org=org,
        )
    client.close()


def write_gpm_to_influx(zone_id: str, rainfall: dict, timestamp: datetime) -> None:
    client = InfluxDBClient(
        url=os.getenv("INFLUXDB_URL", "http://localhost:8086"),
        token=os.getenv("INFLUXDB_TOKEN", "geoguard-influx-token"),
        org=os.getenv("INFLUXDB_ORG", "geoguard"),
    )
    write_api = client.write_api(write_options=SYNCHRONOUS)
    point = (
        Point("satellite_rainfall")
        .tag("zone_id", zone_id)
        .tag("source", "nasa_gpm")
        .field("rainfall_1hr_mm", float(rainfall.get("1hr_mm", 0.0)))
        .field("rainfall_6hr_mm", float(rainfall.get("6hr_mm", 0.0)))
        .field("rainfall_24hr_mm", float(rainfall.get("24hr_mm", 0.0)))
        .field("rainfall_72hr_mm", float(rainfall.get("72hr_mm", 0.0)))
        .time(timestamp)
    )
    write_api.write(
        bucket=os.getenv("INFLUXDB_BUCKET", "sensor-data"),
        org=os.getenv("INFLUXDB_ORG", "geoguard"),
        record=point,
    )
    client.close()


async def backfill_nasa_gpm(days: int, step_hours: int, allow_simulated: bool) -> int:
    fetcher = NASAGPMFetcher()
    if not fetcher.username or not fetcher.password:
        if allow_simulated:
            await fetcher._simulate_and_write()
        else:
            logger.warning("NASA credentials missing; skipping simulated fallback")
        return 0

    token = await fetcher._get_earthdata_token()
    if not token:
        if allow_simulated:
            await fetcher._simulate_and_write()
        else:
            logger.warning("NASA auth failed; skipping simulated fallback")
        return 0

    written = 0
    cursor = utc_now() - timedelta(days=days)
    end = utc_now()
    while cursor <= end:
        url = fetcher._build_gpm_url(cursor)
        logger.info("NASA GPM: %s", url)
        filepath = await fetcher._download_gpm_file(url, token)
        if filepath:
            zone_data = fetcher._process_gpm_netcdf(filepath)
            for zone_id, rainfall in zone_data.items():
                write_gpm_to_influx(zone_id, rainfall, cursor)
                written += 1
            logger.info("NASA GPM: wrote %s zones for %s", len(zone_data), cursor.isoformat())
        cursor += timedelta(hours=step_hours)
    return written


async def backfill_sentinel_catalog(days: int) -> tuple[int, int]:
    if not os.getenv("COPERNICUS_USERNAME") or not os.getenv("COPERNICUS_PASSWORD"):
        logger.warning("Copernicus credentials missing; skipping Sentinel-1/2")
        return (0, 0)

    start = (utc_now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    end = utc_now().strftime("%Y-%m-%dT23:59:59Z")
    zones = json.loads((ROOT / "shared" / "config" / "nepal_zones.json").read_text())["zones"]

    auth = CopernicusAuth()
    try:
        s1_products = await auth.search_products(
            collection="SENTINEL-1",
            bbox=S1_BBOX,
            start_date=start,
            end_date=end,
            attributes={"productType": "GRD", "processingMode": "IW"},
            limit=100,
        )
        s1_coverage = Sentinel1Fetcher()._map_products_to_zones(s1_products, zones)
        Sentinel1Fetcher._write_coverage_flags(s1_coverage)

        s2_products = await auth.search_products(
            collection="SENTINEL-2",
            bbox=S2_BBOX,
            start_date=start,
            end_date=end,
            attributes={"productType": "S2MSI2A", "processingMode": "OPER"},
            limit=100,
        )
        s2_coverage = Sentinel2Fetcher()._map_products_to_zones(s2_products, zones)
        Sentinel2Fetcher._write_coverage_flags(s2_coverage)
        return (len(s1_coverage), len(s2_coverage))
    finally:
        await auth.close()


async def run_dhm_if_real(allow_simulated: bool) -> None:
    if not os.getenv("DHM_API_URL") and not allow_simulated:
        logger.warning("DHM_API_URL is not configured; skipping DHM simulated fallback")
        return
    from connector import DHMConnector

    await DHMConnector().fetch_and_store()


async def run_osm() -> None:
    from fetchers.osm_sync import OSMSyncFetcher

    await OSMSyncFetcher().fetch_and_process()


async def main() -> None:
    args = parse_args()
    if args.cleanup:
        cleanup_postgres()
        cleanup_influx()

    await run_dhm_if_real(args.allow_simulated)
    gpm_rows = await backfill_nasa_gpm(args.days, args.step_hours, args.allow_simulated)
    s1_zones, s2_zones = await backfill_sentinel_catalog(args.days)

    if args.include_osm:
        await run_osm()

    if not args.skip_risk:
        logger.info("Running one risk scoring cycle")
        engine = RiskEngine()
        await engine.run_scoring_cycle()
        engine._shutdown()

    logger.info(
        "Backfill complete: nasa_gpm_rows=%s sentinel1_zones=%s sentinel2_zones=%s",
        gpm_rows,
        s1_zones,
        s2_zones,
    )


if __name__ == "__main__":
    asyncio.run(main())
