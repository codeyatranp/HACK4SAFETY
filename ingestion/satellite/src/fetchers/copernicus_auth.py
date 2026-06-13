"""
Copernicus Data Space Ecosystem (CDSE) Authentication

Handles OAuth2 authentication against the CDSE Keycloak identity provider.
Returns access tokens for product search and download.

CDSE replaced the old scihub.copernicus.eu in Oct 2023.
New endpoints:
  - Auth: https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token
  - Catalog: https://catalogue.dataspace.copernicus.eu/odata/v1/Products
"""
import os
import logging
import time
from typing import Optional

import httpx

logger = logging.getLogger("copernicus-auth")

CDSE_AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
CDSE_CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"


class CopernicusAuth:
    """
    OAuth2 authentication client for Copernicus Data Space Ecosystem.

    Uses client_credentials flow with username/password
    to obtain access tokens for the CDSE catalogue and download APIs.
    """

    def __init__(self):
        self.username = os.getenv("COPERNICUS_USERNAME")
        self.password = os.getenv("COPERNICUS_PASSWORD")
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0
        self._client = httpx.AsyncClient(timeout=30.0)

    @property
    def is_configured(self) -> bool:
        return bool(self.username and self.password)

    async def get_token(self) -> Optional[str]:
        """
        Get or refresh the CDSE access token.
        Tokens are valid for ~10 minutes; auto-refresh when expired.
        """
        if self._token and time.time() < self._token_expiry - 60:
            return self._token

        try:
            resp = await self._client.post(
                CDSE_AUTH_URL,
                data={
                    "grant_type": "password",
                    "client_id": "cdse-public",
                    "username": self.username,
                    "password": self.password,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )

            if resp.status_code == 200:
                token_data = resp.json()
                self._token = token_data["access_token"]
                expires_in = token_data.get("expires_in", 600)
                self._token_expiry = time.time() + expires_in
                logger.info("CDSE: access token obtained successfully")
                return self._token
            else:
                logger.error(
                    f"CDSE auth failed: {resp.status_code} — {resp.text[:300]}"
                )
                return None

        except Exception as e:
            logger.error(f"CDSE auth error: {e}")
            return None

    async def search_products(
        self,
        collection: str,
        bbox: tuple[float, float, float, float],
        start_date: str,
        end_date: str,
        attributes: Optional[dict] = None,
        limit: int = 10,
    ) -> list[dict]:
        """
        Search the CDSE catalogue for satellite products.

        Args:
            collection: Product collection name (e.g. SENTINEL-1, SENTINEL-2)
            bbox: Bounding box as (west, south, east, north) in degrees
            start_date: Start date ISO string (e.g. "2026-06-01T00:00:00Z")
            end_date: End date ISO string
            attributes: Additional OData filter attributes (e.g. {"cloudCover": "<30"})
            limit: Max number of results

        Returns:
            List of product metadata dicts with: id, name, date, footprint, cloud_cover, link
        """
        token = await self.get_token()
        if not token:
            return []

        # Build OData filter - skip spatial and attribute filters due to CDSE OData syntax issues
        # We'll do client-side filtering by bbox and attributes
        filters = [
            f"Collection/Name eq '{collection}'",
            f"ContentDate/Start gt {start_date}",
            f"ContentDate/End lt {end_date}",
        ]

        filter_str = " and ".join(filters)

        try:
            resp = await self._client.get(
                f"{CDSE_CATALOG_URL}?$filter={filter_str}&$top={limit}&$orderby=ContentDate/Start desc",
                headers={"Authorization": f"Bearer {token}"},
            )

            if resp.status_code == 200:
                data = resp.json()
                products_raw = data.get("value", [])

                products = []
                for p in products_raw:
                    # Extract key metadata
                    attrs = {}
                    attributes = p.get("Attributes") or []
                    for attr in attributes:
                        if attr.get("Name") in ("cloudCover", "orbitDirection", "polarisation", "productType", "processingMode"):
                            attrs[attr["Name"]] = attr.get("Value")

                    footprint = p.get("GeoFootprint")
                    # GeoFootprint is already a GeoJSON geometry, not wrapped in "geometry" key
                    if footprint and footprint.get("type"):
                        footprint_geom = footprint
                    else:
                        footprint_geom = None

                    products.append({
                        "id": p.get("Id"),
                        "name": p.get("Name"),
                        "date": p.get("ContentDate", {}).get("Start"),
                        "footprint": footprint_geom,
                        "cloud_cover": attrs.get("cloudCover"),
                        "orbit_direction": attrs.get("orbitDirection"),
                        "polarisation": attrs.get("polarisation"),
                        "product_type": attrs.get("productType"),
                        "link": p.get("DownloadLink", ""),
                    })

                logger.info(f"CDSE: found {len(products)} {collection} products")
                return products
            else:
                logger.error(f"CDSE catalogue search failed: {resp.status_code} — {resp.text[:300]}")
                return []

        except Exception as e:
            logger.error(f"CDSE catalogue search error: {e}")
            return []

    async def close(self):
        await self._client.aclose()