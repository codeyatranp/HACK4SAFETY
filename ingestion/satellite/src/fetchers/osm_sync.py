"""
OpenStreetMap Daily Sync Fetcher

Downloads the latest Nepal extract from Geofabrik/Overpass API
and loads it into PostGIS for the Police Route Optimizer
and impact zone mapping.

No authentication required — OSM is free and open.
"""
import os
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("osm-sync")

# Geofabrik Nepal extract (updated daily)
NEPAL_OSM_URL = "https://download.geofabrik.de/asia/nepal-latest.osm.pbf"
# Overpass API alternative URL for real-time queries
OVERPASS_URL = "https://overpass-api.de/api/interpreter"


class OSMSyncFetcher:
    """
    OpenStreetMap Nepal data syncer.
    
    Downloads the latest Nepal road network, settlements, and
    infrastructure data daily. Loads into PostGIS for spatial
    queries by the Risk Engine and Route Optimizer.
    
    No credentials needed — downloads from free, publicly maintained APIs.
    """

    def __init__(self):
        self.download_dir = Path("/tmp/geoguard/osm")
        self.download_dir.mkdir(parents=True, exist_ok=True)

    async def fetch_and_process(self):
        """
        Download and load OSM data into PostGIS.
        """
        logger.info("OSM: fetching Nepal extract from Geofabrik...")

        pbf_path = self.download_dir / "nepal-latest.osm.pbf"

        # Download Nepal PBF file
        await self._download_nepal_pbf(pbf_path)

        # Load into PostGIS using osm2pgsql
        await self._load_into_postgis(pbf_path)

        # Extract key infrastructure for route optimizer
        await self._index_infrastructure()

        logger.info("OSM: sync complete")

    async def _download_nepal_pbf(self, output_path: Path):
        """Download Nepal OSM extract from Geofabrik."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(NEPAL_OSM_URL) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        output_path.write_bytes(content)
                        file_size_mb = len(content) / (1024 * 1024)
                        logger.info(
                            f"OSM: downloaded Nepal extract "
                            f"({file_size_mb:.1f} MB)"
                        )
                    else:
                        logger.error(
                            f"OSM download failed: HTTP {resp.status}"
                        )
        except Exception as e:
            logger.error(f"OSM download error: {e}")

    async def _load_into_postgis(self, pbf_path: Path):
        """
        Load the PBF file into PostgreSQL using osm2pgsql.
        
        Requires osm2pgsql installed on the host.
        Phase 1: skip if osm2pgsql not available (logs warning).
        """
        import subprocess

        osm2pgsql = "/usr/bin/osm2pgsql"
        if not Path(osm2pgsql).exists():
            logger.warning(
                "OSM: osm2pgsql not found — skipping PostGIS load. "
                "Install with: apt-get install osm2pgsql"
            )
            return

        try:
            # Use 127.0.0.1 to force TCP/IP connection (avoids peer auth issues with localhost)
            pg_host = os.getenv("POSTGRES_HOST", "localhost")
            if pg_host == "localhost":
                pg_host = "127.0.0.1"
            
            cmd = [
                osm2pgsql,
                "--create",
                "--slim",
                "--cache=500",
                "-H", pg_host,
                "-P", os.getenv("POSTGRES_PORT", "5432"),
                "-U", os.getenv("POSTGRES_USER", "geoguard"),
                "-d", os.getenv("POSTGRES_DB", "geoguard"),
                "--prefix=osm",
                str(pbf_path),
            ]

            env = {**os.environ, "PGPASSWORD": os.getenv("POSTGRES_PASSWORD", "geoguard_admin")}
            result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=600)

            if result.returncode == 0:
                logger.info("OSM: loaded into PostGIS successfully")
            else:
                logger.error(f"OSM osm2pgsql failed: {result.stderr[:200]}")

        except FileNotFoundError:
            logger.warning("OSM: osm2pgsql not installed")
        except Exception as e:
            logger.error(f"OSM load error: {e}")

    async def _index_infrastructure(self):
        """
        Extract and index key infrastructure features:
        - Roads for route optimizer
        - Settlements for impact zone mapping
        - Police stations, hospitals for emergency response
        """
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                user=os.getenv("POSTGRES_USER", "geoguard"),
                password=os.getenv("POSTGRES_PASSWORD", "geoguard_admin"),
                dbname=os.getenv("POSTGRES_DB", "geoguard"),
            )
            cur = conn.cursor()

            # Sync roads into our structured table
            cur.execute("""
                INSERT INTO osm_roads (osm_id, name, highway_type, geom)
                SELECT 
                    osm_id * -1 AS osm_id,
                    name,
                    highway,
                    way AS geom
                FROM osm_line
                WHERE highway IS NOT NULL
                ON CONFLICT (osm_id) DO UPDATE
                SET name = EXCLUDED.name, highway_type = EXCLUDED.highway_type, geom = EXCLUDED.geom
            """)

            # Sync settlements
            cur.execute("""
                INSERT INTO osm_settlements (osm_id, name, population, geom)
                SELECT 
                    osm_id * -1 AS osm_id,
                    name,
                    NULLIF(tags->>'population', '')::INTEGER,
                    way AS geom
                FROM osm_point
                WHERE place IN ('city', 'town', 'village', 'hamlet')
                ON CONFLICT (osm_id) DO UPDATE
                SET name = EXCLUDED.name, population = EXCLUDED.population, geom = EXCLUDED.geom
            """)

            conn.commit()
            logger.info(
                f"OSM: indexed {cur.rowcount} roads and settlements"
            )
            cur.close()
            conn.close()

        except Exception as e:
            logger.warning(f"OSM infrastructure indexing skipped: {e}")