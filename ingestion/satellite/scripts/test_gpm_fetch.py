import os
import sys
import asyncio
import logging
from datetime import datetime, timezone, timedelta

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fetchers.nasa_gpm import NASAGPMFetcher

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("test-gpm")

async def test_fetch():
    logger.info("Starting NASA GPM real data fetch test...")
    fetcher = NASAGPMFetcher()
    
    # Check credentials
    if not fetcher.username or not fetcher.password:
        logger.error("No NASA Earthdata credentials found in environment!")
        return

    logger.info(f"Using Username: {fetcher.username}")

    # Ensure authentication
    auth_ok = await fetcher._ensure_authenticated()
    if auth_ok:
        logger.info("Successfully authenticated with Earthdata.")
    else:
        logger.warning("Authentication failed.")

    # Try a few products and times
    now = datetime.now(timezone.utc)
    # Search back from 4h to 10h ago
    for hours_back in range(4, 11):
        target_time = now - timedelta(hours=hours_back)
        for product in ["GPM_3IMERGHHE.07", "GPM_3IMERGHHL.07"]:
            url = fetcher._build_gpm_url(target_time, product=product)
            logger.info(f"Testing URL ({hours_back}h ago): {url}")
            
            filepath = await fetcher._download_gpm_file(url)
            if filepath:
                logger.info(f"Successfully downloaded file to {filepath}")
                zone_data = fetcher._process_gpm_netcdf(filepath)
                if zone_data:
                    logger.info(f"Successfully processed NetCDF! Found data for {len(zone_data)} zones.")
                    # Print first 3 zones
                    count = 0
                    for zid, data in zone_data.items():
                        logger.info(f"  Zone {zid}: {data}")
                        count += 1
                        if count >= 3: break
                    
                    logger.info("TEST PASSED: Real GPM data is accessible and processable.")
                    return
                else:
                    logger.error("Failed to process downloaded NetCDF file.")
            else:
                logger.warning(f"File not found or download failed for {url}")

    logger.error("TEST FAILED: Could not fetch any real GPM data in the tested windows.")

if __name__ == "__main__":
    asyncio.run(test_fetch())
