"""
ESA Sentinel-1 SAR Ground Deformation Fetcher

Real data path:
  - Authenticate with Copernicus Data Space Ecosystem (CDSE)
  - Search for recent S1 IW GRD products over Nepal zones
  - Extract product metadata (orbit, date, footprint)
  - Flag zones with recent SAR coverage for potential InSAR analysis
  - Write coverage metadata to PostgreSQL

Phase 2 (future):
  - Download SLC products for InSAR displacement analysis
  - Compute ground deformation per zone
  - Flag zones with >5mm displacement
"""
import os
import json
import logging
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("sentinel-1")

NEPAL_BBOX = (80.0, 26.3, 88.2, 30.4)


class Sentinel1Fetcher:
    """
    ESA Sentinel-1 SAR data fetcher.

    Phase 1 (with credentials): search CDSE catalogue for recent SAR
    products, record coverage metadata per zone.

    Phase 2 (with InSAR processing): detect ground deformation by
    comparing SAR imagery against baseline reference.
    """

    def __init__(self):
        self.username = os.getenv("COPERNICUS_USERNAME")
        self.password = os.getenv("COPERNICUS_PASSWORD")

    async def fetch_and_process(self):
        if not self.username or not self.password:
            logger.info(
                "Sentinel-1: no Copernicus credentials — skipping. "
                "Register at https://dataspace.copernicus.eu to enable."
            )
            return

        logger.info("Sentinel-1: searching CDSE catalogue for recent SAR products...")

        try:
            from src.fetchers.copernicus_auth import CopernicusAuth

            auth = CopernicusAuth()
            if not auth.is_configured:
                logger.warning("Sentinel-1: CDSE auth not configured")
                return

            # Search for recent S1 IW products over Nepal (last 7 days)
            now = datetime.now(timezone.utc)
            start = (now - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00Z")
            end = now.strftime("%Y-%m-%dT23:59:59Z")

            products = await auth.search_products(
                collection="SENTINEL-1",
                bbox=NEPAL_BBOX,
                start_date=start,
                end_date=end,
                attributes={"productType": "GRD", "processingMode": "IW"},
                limit=20,
            )

            if not products:
                logger.info("Sentinel-1: no recent GRD products found over Nepal")
                await auth.close()
                return

            # Map products to GeoGuard zones based on footprint overlap
            zones_path = os.getenv("ZONES_CONFIG_PATH") or os.path.join(
                os.path.dirname(__file__), "..", "..", "..",
                "shared", "config", "nepal_zones.json",
            )
            with open(zones_path) as f:
                zones = json.load(f)["zones"]

            coverage_zones = self._map_products_to_zones(products, zones)

            # Write coverage metadata to PostgreSQL
            self._write_coverage_flags(coverage_zones)

            logger.info(
                f"Sentinel-1: found {len(products)} products, "
                f"covering {len(coverage_zones)} zones"
            )

            await auth.close()

        except ImportError:
            logger.warning("Sentinel-1: copernicus_auth module not available")
        except Exception as e:
            logger.error(f"Sentinel-1 fetch failed: {e}")

    def _map_products_to_zones(self, products: list[dict], zones: list[dict]) -> list[dict]:
        """
        Map Sentinel-1 product footprints to GeoGuard zones.
        A zone is covered if its center point falls within the product footprint.
        """
        coverage = []

        for zone in zones:
            zlat = zone["center"]["lat"]
            zlng = zone["center"]["lng"]

            for product in products:
                footprint = product.get("footprint")
                if not footprint:
                    continue

                # Simple bounding box check from footprint coordinates
                coords = footprint.get("coordinates", [])
                if not coords:
                    continue

                # Handle potential MultiPolygon nesting
                if footprint.get("type") == "MultiPolygon":
                    flat_coords = []
                    for poly in coords:
                        for ring in poly:
                            flat_coords.extend(ring)
                else:
                    # Regular Polygon
                    flat_coords = []
                    for ring in coords:
                        flat_coords.extend(ring)

                if not flat_coords:
                    continue

                lngs = [c[0] for c in flat_coords if isinstance(c, (list, tuple)) and len(c) >= 2]
                lats = [c[1] for c in flat_coords if isinstance(c, (list, tuple)) and len(c) >= 2]

                if not lats or not lngs:
                    continue

                if min(lats) <= zlat <= max(lats) and min(lngs) <= zlng <= max(lngs):
                    coverage.append({
                        "zone_id": zone["zone_id"],
                        "product_name": product["name"],
                        "product_date": product["date"],
                        "product_type": product.get("product_type", "GRD"),
                        "orbit_direction": product.get("orbit_direction", ""),
                        "polarisation": product.get("polarisation", ""),
                        "available_for_insar": product.get("product_type") == "SLC",
                    })
                    break  # One product per zone is enough for coverage tracking

        return coverage

    @staticmethod
    def _write_coverage_flags(coverage_zones: list[dict]):
        """
        Write SAR coverage metadata to PostgreSQL satellite_data table.
        Stores product availability per zone for the Risk Engine to reference.
        """
        try:
            import psycopg2

            pg_host = os.getenv("POSTGRES_HOST", "localhost")
            if pg_host == "postgres":
                pg_host = "localhost"

            conn = psycopg2.connect(
                host=pg_host,
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "geoguard"),
                password=os.getenv("POSTGRES_PASSWORD", "geoguard_admin"),
                dbname=os.getenv("POSTGRES_DB", "geoguard"),
            )
            cur = conn.cursor()

            for zone in coverage_zones:
                cur.execute(
                    """INSERT INTO satellite_data (zone_id, source, timestamp, data_type, value, metadata)
                       VALUES (%s, 'sentinel1', %s, 'sar_coverage', %s, %s)
                       ON CONFLICT (zone_id, source, data_type, timestamp) DO UPDATE
                       SET value = EXCLUDED.value, metadata = EXCLUDED.metadata""",
                    (
                        zone["zone_id"],
                        datetime.now(timezone.utc),
                        1.0,  # coverage flag: zone has SAR data available
                        json.dumps({
                            "product_name": zone["product_name"],
                            "product_date": zone["product_date"],
                            "product_type": zone["product_type"],
                            "orbit_direction": zone.get("orbit_direction", ""),
                            "polarisation": zone.get("polarisation", ""),
                            "available_for_insar": zone.get("available_for_insar", False),
                            "source": "sentinel-1",
                        }),
                    ),
                )

            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"Sentinel-1: wrote coverage for {len(coverage_zones)} zones to PostgreSQL")

        except Exception as e:
            logger.error(f"Sentinel-1 PostgreSQL write failed: {e}")

    @staticmethod
    def _write_deformation_flags(deformation_zones: list[dict]):
        """
        Write deformation flags to PostgreSQL.
        Phase 2: called after InSAR processing detects ground movement.

        Args:
            deformation_zones: [{"zone_id": str, "displacement_mm": float}, ...]
        """
        try:
            import psycopg2

            pg_host = os.getenv("POSTGRES_HOST", "localhost")
            if pg_host == "postgres":
                pg_host = "localhost"

            conn = psycopg2.connect(
                host=pg_host,
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "geoguard"),
                password=os.getenv("POSTGRES_PASSWORD", "geoguard_admin"),
                dbname=os.getenv("POSTGRES_DB", "geoguard"),
            )
            cur = conn.cursor()

            for zone in deformation_zones:
                cur.execute(
                    """INSERT INTO satellite_data (zone_id, source, timestamp, data_type, value, metadata)
                       VALUES (%s, 'sentinel1', %s, 'deformation', %s, %s)
                       ON CONFLICT (zone_id, source, data_type, timestamp) DO UPDATE
                       SET value = EXCLUDED.value, metadata = EXCLUDED.metadata""",
                    (
                        zone["zone_id"],
                        datetime.now(timezone.utc),
                        zone["displacement_mm"],
                        json.dumps({"source": "sentinel-1", "version": "v1"}),
                    ),
                )

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Sentinel-1 PostgreSQL write failed: {e}")