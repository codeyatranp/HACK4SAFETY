"""
ESA Sentinel-2 Vegetation & Land Cover Fetcher

Real data path:
  - Authenticate with Copernicus Data Space Ecosystem (CDSE)
  - Search for recent S2 Level-2A products over Nepal (cloud cover < 30%)
  - Map product coverage to GeoGuard zones
  - Store product availability metadata in PostgreSQL
  - Flag zones with recent clear-sky optical coverage

Phase 2 (future):
  - Download B4 (Red) and B8 (NIR) bands via CDSE band-specific API
  - Compute NDVI = (NIR - Red) / (NIR + Red) per zone
  - Compare vs baseline → detect vegetation loss (erosion indicator)
  - Write NDVI values to PostgreSQL
"""
import os
import json
import logging
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("sentinel-2")

NEPAL_BBOX = (80.0, 26.3, 88.2, 30.4)


class Sentinel2Fetcher:
    """
    ESA Sentinel-2 optical data fetcher.

    Phase 1 (with credentials): search CDSE catalogue for recent
    cloud-free L2A products, record zone coverage metadata.

    Phase 2 (with NDVI processing): download individual bands,
    compute per-zone NDVI, detect vegetation loss.
    """

    def __init__(self):
        self.username = os.getenv("COPERNICUS_USERNAME")
        self.password = os.getenv("COPERNICUS_PASSWORD")

    async def fetch_and_process(self):
        if not self.username or not self.password:
            logger.info(
                "Sentinel-2: no Copernicus credentials — skipping. "
                "Register at https://dataspace.copernicus.eu to enable."
            )
            return

        logger.info("Sentinel-2: searching CDSE catalogue for recent optical products...")

        try:
            from src.fetchers.copernicus_auth import CopernicusAuth

            auth = CopernicusAuth()
            if not auth.is_configured:
                logger.warning("Sentinel-2: CDSE auth not configured")
                return

            # Search for recent S2 L2A products over Nepal (last 5 days, cloud < 30%)
            now = datetime.now(timezone.utc)
            start = (now - timedelta(days=5)).strftime("%Y-%m-%dT00:00:00Z")
            end = now.strftime("%Y-%m-%dT23:59:59Z")

            products = await auth.search_products(
                collection="SENTINEL-2",
                bbox=NEPAL_BBOX,
                start_date=start,
                end_date=end,
                attributes={"productType": "S2MSI2A", "processingMode": "OPER"},
                limit=20,
            )

            if not products:
                logger.info("Sentinel-2: no recent L2A products found over Nepal")
                await auth.close()
                return

            # Filter for low cloud cover
            clear_products = []
            for p in products:
                cc = p.get("cloud_cover")
                if cc is not None:
                    try:
                        if float(cc) <= 30:
                            clear_products.append(p)
                    except (ValueError, TypeError):
                        clear_products.append(p)
                else:
                    clear_products.append(p)

            logger.info(
                f"Sentinel-2: found {len(products)} products, "
                f"{len(clear_products)} with cloud cover ≤ 30%"
            )

            # Map products to zones
            zones_path = os.getenv("ZONES_CONFIG_PATH") or os.path.join(
                os.path.dirname(__file__), "..", "..", "..",
                "shared", "config", "nepal_zones.json",
            )
            with open(zones_path) as f:
                zones = json.load(f)["zones"]

            coverage_zones = self._map_products_to_zones(clear_products, zones)

            # Write coverage metadata to PostgreSQL
            self._write_coverage_flags(coverage_zones)

            logger.info(
                f"Sentinel-2: {len(clear_products)} clear products "
                f"cover {len(coverage_zones)} zones"
            )

            await auth.close()

        except ImportError:
            logger.warning("Sentinel-2: copernicus_auth module not available")
        except Exception as e:
            logger.error(f"Sentinel-2 fetch failed: {e}")

    def _map_products_to_zones(self, products: list[dict], zones: list[dict]) -> list[dict]:
        """
        Map Sentinel-2 product footprints to GeoGuard zones.
        """
        coverage = []

        for zone in zones:
            zlat = zone["center"]["lat"]
            zlng = zone["center"]["lng"]

            best_product = None
            best_cloud = 100.0

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
                    cc = product.get("cloud_cover")
                    try:
                        cloud = float(cc) if cc else 100.0
                    except (ValueError, TypeError):
                        cloud = 100.0

                    if cloud < best_cloud:
                        best_cloud = cloud
                        best_product = product

            if best_product:
                coverage.append({
                    "zone_id": zone["zone_id"],
                    "product_name": best_product["name"],
                    "product_date": best_product["date"],
                    "cloud_cover_pct": best_cloud,
                    "available_for_ndvi": True,
                })

        return coverage

    @staticmethod
    def _write_coverage_flags(coverage_zones: list[dict]):
        """
        Write optical coverage metadata to PostgreSQL.
        Stores product availability per zone for NDVI computation reference.
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
                       VALUES (%s, 'sentinel2', %s, 'optical_coverage', %s, %s)
                       ON CONFLICT (zone_id, source, data_type, timestamp) DO UPDATE
                       SET value = EXCLUDED.value, metadata = EXCLUDED.metadata""",
                    (
                        zone["zone_id"],
                        datetime.now(timezone.utc),
                        1.0,
                        json.dumps({
                            "product_name": zone["product_name"],
                            "product_date": zone["product_date"],
                            "cloud_cover_pct": zone.get("cloud_cover_pct", 0),
                            "available_for_ndvi": zone.get("available_for_ndvi", True),
                            "source": "sentinel-2",
                        }),
                    ),
                )

            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"Sentinel-2: wrote coverage for {len(coverage_zones)} zones to PostgreSQL")

        except Exception as e:
            logger.error(f"Sentinel-2 PostgreSQL write failed: {e}")

    @staticmethod
    def _write_ndvi(zone_ndvi: list[dict]):
        """
        Write NDVI values to PostgreSQL.
        Phase 2: called after NDVI computation from downloaded bands.

        Args:
            zone_ndvi: [{"zone_id": str, "ndvi": float, "change_pct": float}, ...]
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

            for zone in zone_ndvi:
                cur.execute(
                    """INSERT INTO satellite_data (zone_id, source, timestamp, data_type, value, metadata)
                       VALUES (%s, 'sentinel2', %s, 'ndvi', %s, %s)
                       ON CONFLICT (zone_id, source, data_type, timestamp) DO UPDATE
                       SET value = EXCLUDED.value, metadata = EXCLUDED.metadata""",
                    (
                        zone["zone_id"],
                        datetime.now(timezone.utc),
                        zone["ndvi"],
                        json.dumps({
                            "source": "sentinel-2",
                            "change_pct": zone.get("change_pct"),
                        }),
                    ),
                )

            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            logger.error(f"Sentinel-2 PostgreSQL write failed: {e}")