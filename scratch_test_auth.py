import os
import asyncio
import sys
import tempfile
from dotenv import load_dotenv
import httpx
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/app/src")
load_dotenv("/home/theamal/dev/Geogaurd/geoguard/.env")

async def test_gpm_download_manual_redirect():
    username = os.getenv("NASA_EARTHDATA_USERNAME")
    password = os.getenv("NASA_EARTHDATA_PASSWORD")
    
    print("Testing GPM Download with manual redirect handling...")
    
    # We disable automatic redirect following so we can inspect and handle each step
    client = httpx.AsyncClient(
        follow_redirects=False,
        timeout=60.0,
        cookies={}
    )
    
    from fetchers.nasa_gpm import NASAGPMFetcher
    fetcher = NASAGPMFetcher()
    now = datetime.now(timezone.utc)
    target = now - timedelta(hours=12)
    url = fetcher._build_gpm_url(target)
    print("Target URL:", url)
    
    try:
        current_url = url
        headers = {}
        
        for step in range(10):  # limit redirects
            print(f"Step {step}: GET {current_url}")
            resp = await client.get(current_url, headers=headers)
            print("  Status code:", resp.status_code)
            
            if resp.status_code in (301, 302, 303, 307, 308):
                next_url = resp.headers.get("Location")
                if not next_url:
                    print("  Redirect status but no Location header!")
                    break
                    
                # If next_url is relative, build full url
                if next_url.startswith("/"):
                    from urllib.parse import urljoin
                    next_url = urljoin(current_url, next_url)
                    
                print("  Redirect Location:", next_url)
                
                # Check if we are redirecting to urs.earthdata.nasa.gov
                if "urs.earthdata.nasa.gov" in next_url:
                    print("  Detected redirect to Earthdata Login. Supplying Basic Auth.")
                    # Attach basic auth
                    client.auth = httpx.BasicAuth(username, password)
                else:
                    # Clear basic auth for non-auth domains
                    client.auth = None
                    
                current_url = next_url
            elif resp.status_code == 200:
                if b"<!DOCTYPE html>" in resp.content[:200]:
                    print("  SUCCESS but returned HTML page (still login form).")
                    print(resp.content[:300].decode('utf-8', errors='ignore'))
                else:
                    print(f"  SUCCESS! Downloaded file size: {len(resp.content)} bytes")
                break
            else:
                print(f"  Request failed with status {resp.status_code}")
                print(resp.text[:300])
                break
                
    except Exception as e:
        print("Exception:", e)
    finally:
        await client.aclose()

async def main():
    await test_gpm_download_manual_redirect()

if __name__ == "__main__":
    asyncio.run(main())
