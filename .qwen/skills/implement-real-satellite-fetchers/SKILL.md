---
name: implement-real-satellite-fetchers
description: Pattern for implementing real satellite data ingestion from NASA GPM IMERG and Copernicus CDSE — auth flows, API search, NetCDF processing, and graceful simulation fallback
source: auto-skill
extracted_at: '2026-06-11T16:47:07.553Z'
---

# Implementing Real Satellite Data Fetchers

## Context
This skill was extracted from implementing real data ingestion for GeoGuard, replacing simulation/dry-run stubs with actual API calls to NASA GES DISC (GPM IMERG rainfall) and Copernicus Data Space Ecosystem (Sentinel-1/2 satellite imagery). The pattern applies to any project that needs to fetch satellite or open-data from authenticated APIs with graceful fallback.

## Architecture Pattern

### Simulation → Real Data Migration
Each fetcher follows the same structure:
```
fetch_and_process():
  if credentials exist → try real data path
  if real path fails → fall back to simulation (with logged warning)
  if no credentials → simulation only
```
This ensures the system never breaks — real data is an upgrade, not a requirement.

### NASA GPM IMERG (Rainfall NetCDF)

**Auth flow**: Token-based, not cookie-based
1. POST to `https://urs.earthdata.nasa.gov/api/v2/token` with `{"username": ..., "password": ...}`
2. Response contains `token` or `access_token` — use in `Authorization: Bearer <token>` header
3. Token expires; cache it and re-request when stale

**Data download**:
- URL pattern: `https://gpm1.gesdisc.eosdis.nasa.gov/data/GPM_L3/GPM_3IMERGHH.07/{yyyy}/{mm}/3IMERGHH.{YYYYMMDD}-S{HHMM00}-E{HHMM59}.07.nc4`
- Round to nearest half-hour (IMERG is half-hourly at :00 and :30)
- Try current, then previous 30-min, then previous hour (3 fallback attempts)
- Download full NetCDF4 file (~10MB) to temp directory

**Processing with xarray/netCDF4**:
1. Open with `xarray.open_dataset(filepath)`
2. Find precipitation variable (`precipitation`, `precipitationCal`, or fallback)
3. **Handle lon convention**: IMERG uses 0-360°; convert to -180 to 180 with `(lon + 180) % 360 - 180` and `sortby("lon")`
4. Crop to Nepal bounds with `.sel(lat=slice(...), lon=slice(...))`
5. Extract per-zone stats: select grid cells within radius of each zone center (lat_radius = km/111, lng_radius = km/(111*cos(lat)))
6. Compute mean precipitation rate → convert to mm accumulation
7. Always write float values to InfluxDB (never `0`, always `0.0`) to avoid type conflicts

**Cleanup**: Delete temp file and directory after processing (success or failure)

### Copernicus CDSE (Sentinel-1/2 Product Search)

**Auth flow**: OAuth2 via Keycloak (CDSE replaced old scihub in Oct 2023)
1. POST to `https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token`
2. Body: `grant_type=password, client_id=cdse-public, username=..., password=...`
3. Content-Type: `application/x-www-form-urlencoded` (NOT JSON)
4. Response: `access_token` valid for ~10 minutes; cache with expiry timestamp
5. Auto-refresh when `time.time() > expiry - 60`

**Catalogue search** (OData v1):
- Endpoint: `https://catalogue.dataspace.copernicus.eu/odata/v1/Products`
- Filter syntax: OData with special CSC functions
  - `Collection/Name eq 'SENTINEL-1'` or `'SENTINEL-2'`
  - `ContentDate/Start gt <ISO date>` / `ContentDate/End lt <ISO date>`
  - `OData.CSC.Intersects(area=Geography'POLYGON((...))')` for geographic filter
  - `Attributes/OData.CSC.StringAttribute/Name eq '<key>' and Value eq '<val>'` for attributes
- Response `value` array contains products with `Id`, `Name`, `ContentDate`, `GeoFootprint`, `Attributes`, `DownloadLink`

**Product-to-zone mapping**:
1. Extract footprint polygon coordinates from `GeoFootprint.geometry.coordinates`
2. For each zone center point, check if it falls within the footprint bounding box
3. For Sentinel-2: pick the product with lowest cloud cover per zone
4. Store as `satellite_data` rows with `data_type='sar_coverage'` or `'optical_coverage'`

**Attribute filtering**:
- Sentinel-1: `productType=GRD`, `processingMode=IW`
- Sentinel-2: `productType=S2MSI2A`, `processingMode=OPER`; then filter `cloudCover <= 30`

## Key Implementation Details

### Shared Auth Module
Create `copernicus_auth.py` as a shared module used by both Sentinel-1 and Sentinel-2:
- Single `CopernicusAuth` class with token caching
- `search_products()` method that both fetchers call
- `is_configured` property to check credentials
- `httpx.AsyncClient` for async HTTP (don't mix with `aiohttp`)

### InfluxDB Writes — Always Floats
```python
.field("rainfall_1hr_mm", data.get("rainfall_1hr_mm", 0.0))  # NOT 0
```
InfluxDB enforces field type consistency per measurement. Once a field exists as float, integer writes are silently dropped.

### PostgreSQL Writes — Use ON CONFLICT
```sql
INSERT INTO satellite_data (zone_id, source, timestamp, data_type, value, metadata)
VALUES (...)
ON CONFLICT (zone_id, source, data_type, timestamp) DO UPDATE
SET value = EXCLUDED.value, metadata = EXCLUDED.metadata
```
This makes scheduled re-fetches safe — they update rather than error on duplicate timestamps.

### Scheduler Credential Detection
```python
self.has_nasa = bool(os.getenv("NASA_EARTHDATA_USERNAME"))
self.has_copernicus = bool(os.getenv("COPERNICUS_USERNAME"))
```
Log the mode at startup so it's visible:
```
NASA GPM: every 30 min (REAL DATA)
Sentinel-1: every 1 hr (REAL SEARCH)
```

### Async Coordination
- NASA GPM: uses `aiohttp.ClientSession` (compatible with APScheduler's `AsyncIOScheduler`)
- Copernicus: uses `httpx.AsyncClient` (also async-compatible)
- Don't mix the two in the same fetcher class

## Common Pitfalls

1. **Earthdata token API returns different keys**: sometimes `token`, sometimes `access_token` — handle both
2. **IMERG lon convention**: 0-360° longitude must be converted to -180/180 before Nepal crop
3. **CDSE auth content type**: must be `x-www-form-urlencoded`, not JSON
4. **Sentinel search attributes**: CDSE OData attribute filtering uses nested syntax, not simple key=value
5. **Credential env var names must match code exactly**: `COPERNICUS_USERNAME` not `COPERNICUS_EMAIL`
6. **NetCDF variable names vary by version**: GPM v07 uses `precipitationCal`; probe all variants
7. **Cloud cover attribute**: may be None/missing in some products — handle gracefully