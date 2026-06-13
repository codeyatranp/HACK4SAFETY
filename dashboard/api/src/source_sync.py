import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional

import httpx


class ExternalSourceSync:
    """
    Synchronizes live external data sources used by dashboard command endpoints.

    Sources:
      - DHM station rainfall (GeoPortal REST with HTML fallback)
      - NASA COOLR landslide catalog (ArcGIS REST)
      - BIPAD incidents/alerts (token-auth API when configured)
    """

    DHM_GEOJSON_URL = (
        "https://geoportal.dhm.gov.np/server/rest/services/Hydrology/"
        "HMS_Rainfall/FeatureServer/0/query?where=1%3D1&outFields=*&returnGeometry=true&f=json"
    )
    DHM_SCRAPE_URL = "https://www.dhm.gov.np/hydrology/rainfall-watch-map"
    COOLR_QUERY_URL = (
        "https://maps.nccs.nasa.gov/arcgis/rest/services/landslide_v2/MapServer/0/query"
    )
    DEFAULT_BIPAD_API_BASE = "https://bipadportal.gov.np/api/v1"

    def __init__(
        self,
        get_pg_conn: Callable[[], Any],
        logger: Optional[logging.Logger] = None,
    ):
        self._get_pg_conn = get_pg_conn
        self.logger = logger or logging.getLogger("dashboard-api")
        self._sync_lock = asyncio.Lock()
        self._status_lock = asyncio.Lock()
        self._status: dict[str, dict[str, Any]] = {}

        self.bipad_api_base = os.getenv(
            "BIPAD_API_BASE", self.DEFAULT_BIPAD_API_BASE
        ).rstrip("/")
        self.bipad_username = os.getenv("BIPAD_USERNAME", "").strip()
        self.bipad_password = os.getenv("BIPAD_PASSWORD", "").strip()
        self.bipad_token = os.getenv("BIPAD_API_TOKEN", "").strip() or None

    async def update_status(
        self,
        source: str,
        state: str,
        records: int = 0,
        detail: Optional[str] = None,
    ):
        now_iso = datetime.now(timezone.utc).isoformat()
        async with self._status_lock:
            entry = self._status.get(source, {})
            entry["source"] = source
            entry["state"] = state
            entry["records"] = records
            entry["detail"] = detail
            entry["last_attempt"] = now_iso
            if state == "ok":
                entry["last_success"] = now_iso
            self._status[source] = entry

    async def get_status_snapshot(self) -> dict[str, dict[str, Any]]:
        async with self._status_lock:
            return dict(self._status)

    def ensure_schema(self):
        """
        Ensure external integration tables exist.
        This is safe to run repeatedly on API startup.
        """
        ddl = """
        CREATE TABLE IF NOT EXISTS dhm_station_readings (
            station_id VARCHAR(100) PRIMARY KEY,
            station_name VARCHAR(255),
            district VARCHAR(120),
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            elevation_m DOUBLE PRECISION,
            rain_1hr DOUBLE PRECISION,
            rain_3hr DOUBLE PRECISION,
            rain_6hr DOUBLE PRECISION,
            rain_12hr DOUBLE PRECISION,
            rain_24hr DOUBLE PRECISION,
            fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            source VARCHAR(60) NOT NULL DEFAULT 'dhm',
            warning_level VARCHAR(20) NOT NULL DEFAULT 'safe',
            status VARCHAR(20) NOT NULL DEFAULT 'online',
            raw_payload JSONB
        );
        CREATE TABLE IF NOT EXISTS landslide_catalog (
            event_id VARCHAR(120) PRIMARY KEY,
            event_date TIMESTAMP WITH TIME ZONE,
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            district VARCHAR(120),
            province VARCHAR(120),
            type VARCHAR(120),
            fatalities INTEGER DEFAULT 0,
            injuries INTEGER DEFAULT 0,
            trigger VARCHAR(80),
            source_url TEXT,
            imported_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            raw_payload JSONB
        );
        CREATE TABLE IF NOT EXISTS bipad_incidents (
            bipad_id VARCHAR(120) PRIMARY KEY,
            title TEXT,
            hazard VARCHAR(80),
            district_id VARCHAR(60),
            district_name VARCHAR(120),
            province VARCHAR(120),
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            deaths INTEGER DEFAULT 0,
            missing INTEGER DEFAULT 0,
            injured INTEGER DEFAULT 0,
            families_affected INTEGER DEFAULT 0,
            incident_date TIMESTAMP WITH TIME ZONE,
            verified BOOLEAN DEFAULT FALSE,
            source_url TEXT,
            fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            raw_payload JSONB
        );
        CREATE TABLE IF NOT EXISTS bipad_alerts (
            alert_id VARCHAR(120) PRIMARY KEY,
            title TEXT,
            hazard VARCHAR(80),
            district_id VARCHAR(60),
            district_name VARCHAR(120),
            province VARCHAR(120),
            lat DOUBLE PRECISION,
            lon DOUBLE PRECISION,
            severity VARCHAR(30),
            status VARCHAR(30),
            alert_date TIMESTAMP WITH TIME ZONE,
            expiry_date TIMESTAMP WITH TIME ZONE,
            source_url TEXT,
            fetched_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            raw_payload JSONB
        );
        CREATE INDEX IF NOT EXISTS idx_dhm_station_readings_fetched_at ON dhm_station_readings(fetched_at DESC);
        CREATE INDEX IF NOT EXISTS idx_dhm_station_readings_warning ON dhm_station_readings(warning_level, rain_24hr DESC);
        CREATE INDEX IF NOT EXISTS idx_landslide_catalog_event_date ON landslide_catalog(event_date DESC);
        CREATE INDEX IF NOT EXISTS idx_landslide_catalog_trigger ON landslide_catalog(trigger);
        CREATE INDEX IF NOT EXISTS idx_landslide_catalog_lat_lon ON landslide_catalog(lat, lon);
        CREATE INDEX IF NOT EXISTS idx_bipad_incidents_date_hazard ON bipad_incidents(incident_date DESC, hazard);
        CREATE INDEX IF NOT EXISTS idx_bipad_alerts_status_date ON bipad_alerts(status, alert_date DESC);
        """
        conn = self._get_pg_conn()
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value is None or value == "":
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any, default: int = 0) -> int:
        try:
            if value is None or value == "":
                return default
            return int(float(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _pick_ci(record: dict[str, Any], keys: list[str]) -> Any:
        lowered = {str(k).lower(): v for k, v in (record or {}).items()}
        for key in keys:
            if key.lower() in lowered:
                value = lowered[key.lower()]
                if value is not None and value != "":
                    return value
        return None

    @staticmethod
    def _parse_timestamp(value: Any) -> datetime:
        now = datetime.now(timezone.utc)
        if value is None or value == "":
            return now
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)

        if isinstance(value, (int, float)):
            # ArcGIS style timestamps are often milliseconds.
            ts = float(value)
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            try:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                return now

        if isinstance(value, str):
            s = value.strip()
            try:
                if s.endswith("Z"):
                    s = s.replace("Z", "+00:00")
                return datetime.fromisoformat(s).astimezone(timezone.utc)
            except Exception:
                pass
            for fmt in (
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M",
                "%Y-%m-%d",
                "%d/%m/%Y %H:%M",
                "%m/%d/%Y %H:%M",
            ):
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.replace(tzinfo=timezone.utc)
                except Exception:
                    continue
        return now

    @staticmethod
    def _warning_level(rain_1hr: Optional[float], rain_3hr: Optional[float], rain_6hr: Optional[float], rain_12hr: Optional[float], rain_24hr: Optional[float], status: str) -> str:
        if status == "offline":
            return "offline"
        r1 = rain_1hr or 0.0
        r3 = rain_3hr or 0.0
        r6 = rain_6hr or 0.0
        r12 = rain_12hr or 0.0
        r24 = rain_24hr or 0.0
        if r12 >= 120 or r24 >= 140:
            return "danger"
        if r1 >= 60 or r3 >= 80 or r6 >= 100:
            return "warning"
        if r1 >= 30 or r24 >= 80:
            return "watch"
        return "safe"

    @staticmethod
    def _station_id_from_name(name: str, fallback: str = "dhm-station") -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", (name or "").strip().lower()).strip("-")
        return slug[:96] if slug else fallback

    async def _fetch_dhm_geoportal(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(self.DHM_GEOJSON_URL)
            resp.raise_for_status()
            payload = resp.json()

        features = payload.get("features", [])
        rows: list[dict[str, Any]] = []
        now = datetime.now(timezone.utc)
        for feature in features:
            attrs = feature.get("attributes", {}) or {}
            geom = feature.get("geometry", {}) or {}
            station_name = self._pick_ci(
                attrs,
                [
                    "station_name",
                    "station",
                    "name",
                    "stname",
                    "station_n",
                ],
            )
            station_id = self._pick_ci(
                attrs,
                ["station_id", "station_code", "stn_code", "id", "objectid"],
            )
            if not station_id:
                station_id = self._station_id_from_name(
                    str(station_name or ""), f"dhm-{len(rows)+1}"
                )

            lat = self._safe_float(
                self._pick_ci(attrs, ["lat", "latitude", "y", "station_lat"])
            )
            lon = self._safe_float(
                self._pick_ci(attrs, ["lon", "lng", "longitude", "x", "station_lon"])
            )
            if lat is None:
                lat = self._safe_float(geom.get("y"))
            if lon is None:
                lon = self._safe_float(geom.get("x"))

            rain_1hr = self._safe_float(
                self._pick_ci(attrs, ["rain_1hr", "rainfall_1hr", "precip_1h", "r1h"])
            )
            rain_3hr = self._safe_float(
                self._pick_ci(attrs, ["rain_3hr", "rainfall_3hr", "precip_3h", "r3h"])
            )
            rain_6hr = self._safe_float(
                self._pick_ci(attrs, ["rain_6hr", "rainfall_6hr", "precip_6h", "r6h"])
            )
            rain_12hr = self._safe_float(
                self._pick_ci(attrs, ["rain_12hr", "rainfall_12hr", "precip_12h", "r12h"])
            )
            rain_24hr = self._safe_float(
                self._pick_ci(attrs, ["rain_24hr", "rainfall_24hr", "precip_24h", "r24h"])
            )
            fetched_at = self._parse_timestamp(
                self._pick_ci(attrs, ["obs_time", "timestamp", "datetime", "last_update"])
            )
            status = "offline" if (now - fetched_at) > timedelta(hours=3) else "online"
            warning_level = self._warning_level(
                rain_1hr, rain_3hr, rain_6hr, rain_12hr, rain_24hr, status
            )
            rows.append(
                {
                    "station_id": str(station_id),
                    "station_name": str(station_name or station_id),
                    "district": self._pick_ci(attrs, ["district", "district_name"]),
                    "lat": lat,
                    "lon": lon,
                    "elevation_m": self._safe_float(
                        self._pick_ci(attrs, ["elevation", "elevation_m", "elev"])
                    ),
                    "rain_1hr": rain_1hr,
                    "rain_3hr": rain_3hr,
                    "rain_6hr": rain_6hr,
                    "rain_12hr": rain_12hr,
                    "rain_24hr": rain_24hr,
                    "fetched_at": fetched_at,
                    "warning_level": warning_level,
                    "status": status,
                    "source": "dhm_geoportal",
                    "raw_payload": attrs,
                }
            )
        return rows

    async def _fetch_dhm_html_fallback(self) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(self.DHM_SCRAPE_URL)
            resp.raise_for_status()
            html = resp.text

        # Minimal table parser fallback (intentionally lightweight).
        rows: list[dict[str, Any]] = []
        tr_blocks = re.findall(r"<tr[^>]*>(.*?)</tr>", html, flags=re.IGNORECASE | re.DOTALL)
        for tr in tr_blocks:
            cells_raw = re.findall(r"<t[dh][^>]*>(.*?)</t[dh]>", tr, flags=re.IGNORECASE | re.DOTALL)
            if len(cells_raw) < 6:
                continue
            cells = [
                re.sub(r"<[^>]+>", "", c, flags=re.DOTALL).strip()
                for c in cells_raw
            ]
            station_name = cells[0] if len(cells) > 0 else None
            district = cells[1] if len(cells) > 1 else None
            if not station_name:
                continue
            rain_1hr = self._safe_float(cells[2]) if len(cells) > 2 else None
            rain_3hr = self._safe_float(cells[3]) if len(cells) > 3 else None
            rain_6hr = self._safe_float(cells[4]) if len(cells) > 4 else None
            rain_24hr = self._safe_float(cells[5]) if len(cells) > 5 else None
            fetched_at = datetime.now(timezone.utc)
            warning_level = self._warning_level(
                rain_1hr, rain_3hr, rain_6hr, None, rain_24hr, "online"
            )
            rows.append(
                {
                    "station_id": self._station_id_from_name(station_name),
                    "station_name": station_name,
                    "district": district,
                    "lat": None,
                    "lon": None,
                    "elevation_m": None,
                    "rain_1hr": rain_1hr,
                    "rain_3hr": rain_3hr,
                    "rain_6hr": rain_6hr,
                    "rain_12hr": None,
                    "rain_24hr": rain_24hr,
                    "fetched_at": fetched_at,
                    "warning_level": warning_level,
                    "status": "online",
                    "source": "dhm_scrape",
                    "raw_payload": {"cells": cells},
                }
            )
        return rows

    async def sync_dhm(self) -> int:
        async with self._sync_lock:
            try:
                stations = await self._fetch_dhm_geoportal()
                source = "geoportal"
            except Exception as geo_err:
                self.logger.warning(f"DHM geoportal sync failed, trying HTML fallback: {geo_err}")
                try:
                    stations = await self._fetch_dhm_html_fallback()
                    source = "html"
                except Exception as scrape_err:
                    await self.update_status(
                        "dhm", "error", 0, f"geoportal={geo_err}; html={scrape_err}"
                    )
                    raise

            if not stations:
                await self.update_status("dhm", "error", 0, "no station rows returned")
                return 0

            conn = self._get_pg_conn()
            with conn.cursor() as cur:
                for row in stations:
                    cur.execute(
                        """
                        INSERT INTO dhm_station_readings (
                            station_id, station_name, district, lat, lon, elevation_m,
                            rain_1hr, rain_3hr, rain_6hr, rain_12hr, rain_24hr,
                            fetched_at, source, warning_level, status, raw_payload
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s::jsonb
                        )
                        ON CONFLICT (station_id) DO UPDATE SET
                            station_name = EXCLUDED.station_name,
                            district = EXCLUDED.district,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            elevation_m = EXCLUDED.elevation_m,
                            rain_1hr = EXCLUDED.rain_1hr,
                            rain_3hr = EXCLUDED.rain_3hr,
                            rain_6hr = EXCLUDED.rain_6hr,
                            rain_12hr = EXCLUDED.rain_12hr,
                            rain_24hr = EXCLUDED.rain_24hr,
                            fetched_at = EXCLUDED.fetched_at,
                            source = EXCLUDED.source,
                            warning_level = EXCLUDED.warning_level,
                            status = EXCLUDED.status,
                            raw_payload = EXCLUDED.raw_payload
                        """,
                        (
                            row["station_id"],
                            row["station_name"],
                            row.get("district"),
                            row.get("lat"),
                            row.get("lon"),
                            row.get("elevation_m"),
                            row.get("rain_1hr"),
                            row.get("rain_3hr"),
                            row.get("rain_6hr"),
                            row.get("rain_12hr"),
                            row.get("rain_24hr"),
                            row.get("fetched_at"),
                            row.get("source"),
                            row.get("warning_level"),
                            row.get("status"),
                            json.dumps(row.get("raw_payload") or {}),
                        ),
                    )
            conn.commit()
            await self.update_status("dhm", "ok", len(stations), f"source={source}")
            self.logger.info(f"DHM sync complete: {len(stations)} stations ({source})")
            return len(stations)

    @staticmethod
    def _normalize_trigger(value: Any) -> str:
        if value is None:
            return "unknown"
        if isinstance(value, bool):
            return "rainfall" if value else "other"
        text = str(value).strip().lower()
        if not text:
            return "unknown"
        if "rain" in text:
            return "rainfall"
        if "earthquake" in text or "seismic" in text:
            return "earthquake"
        return text

    async def sync_coolr(self) -> int:
        async with self._sync_lock:
            params = {
                "where": "country_name='Nepal'",
                "outFields": "*",
                "returnGeometry": "true",
                "f": "json",
                "resultRecordCount": "5000",
            }
            async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
                resp = await client.get(self.COOLR_QUERY_URL, params=params)
                resp.raise_for_status()
                payload = resp.json()

            features = payload.get("features", [])
            if not features:
                await self.update_status("coolr", "error", 0, "no features in ArcGIS response")
                return 0

            rows: list[dict[str, Any]] = []
            for f in features:
                attrs = f.get("attributes", {}) or {}
                geom = f.get("geometry", {}) or {}
                event_id = self._pick_ci(attrs, ["event_id", "id", "objectid", "oid"])
                event_date = self._parse_timestamp(
                    self._pick_ci(attrs, ["event_date", "eventDate", "date"])
                )
                lat = self._safe_float(self._pick_ci(attrs, ["latitude", "lat", "y"])) or self._safe_float(geom.get("y"))
                lon = self._safe_float(self._pick_ci(attrs, ["longitude", "lon", "x"])) or self._safe_float(geom.get("x"))
                if not event_id:
                    event_id = f"{event_date.date().isoformat()}-{lat}-{lon}"

                rows.append(
                    {
                        "event_id": str(event_id),
                        "event_date": event_date,
                        "lat": lat,
                        "lon": lon,
                        "district": self._pick_ci(attrs, ["district", "district_name"]),
                        "province": self._pick_ci(attrs, ["province", "province_name", "region"]),
                        "type": self._pick_ci(attrs, ["landslide_type", "type", "event_type"]),
                        "fatalities": self._safe_int(
                            self._pick_ci(attrs, ["fatality_count", "fatalities", "deaths"]), 0
                        ),
                        "injuries": self._safe_int(
                            self._pick_ci(attrs, ["injury_count", "injuries"]), 0
                        ),
                        "trigger": self._normalize_trigger(
                            self._pick_ci(attrs, ["rainfall_trigger", "trigger"])
                        ),
                        "source_url": self._pick_ci(attrs, ["source_link", "source_url", "url"]),
                        "raw_payload": attrs,
                    }
                )

            conn = self._get_pg_conn()
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        """
                        INSERT INTO landslide_catalog (
                            event_id, event_date, lat, lon, district, province,
                            type, fatalities, injuries, trigger, source_url,
                            imported_at, raw_payload
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            NOW(), %s::jsonb
                        )
                        ON CONFLICT (event_id) DO UPDATE SET
                            event_date = EXCLUDED.event_date,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            district = EXCLUDED.district,
                            province = EXCLUDED.province,
                            type = EXCLUDED.type,
                            fatalities = EXCLUDED.fatalities,
                            injuries = EXCLUDED.injuries,
                            trigger = EXCLUDED.trigger,
                            source_url = EXCLUDED.source_url,
                            imported_at = NOW(),
                            raw_payload = EXCLUDED.raw_payload
                        """,
                        (
                            row["event_id"],
                            row["event_date"],
                            row.get("lat"),
                            row.get("lon"),
                            row.get("district"),
                            row.get("province"),
                            row.get("type"),
                            row.get("fatalities", 0),
                            row.get("injuries", 0),
                            row.get("trigger"),
                            row.get("source_url"),
                            json.dumps(row.get("raw_payload") or {}),
                        ),
                    )
            conn.commit()
            await self.update_status("coolr", "ok", len(rows), "source=arcgis")
            self.logger.info(f"NASA COOLR sync complete: {len(rows)} Nepal events")
            return len(rows)

    async def _refresh_bipad_token(self, client: httpx.AsyncClient) -> Optional[str]:
        if not self.bipad_username or not self.bipad_password:
            return self.bipad_token
        login_url = f"{self.bipad_api_base}/auth/login/"
        resp = await client.post(
            login_url,
            json={"username": self.bipad_username, "password": self.bipad_password},
            headers={"Accept": "application/json"},
            timeout=20.0,
        )
        if resp.status_code >= 400:
            self.logger.warning(f"BIPAD auth failed ({resp.status_code})")
            return self.bipad_token
        payload = resp.json()
        token = payload.get("token") or payload.get("key") or payload.get("access")
        if token:
            self.bipad_token = token
        return self.bipad_token

    async def _bipad_get(self, client: httpx.AsyncClient, path: str, params: Optional[dict[str, Any]] = None) -> Any:
        if not self.bipad_token and not (self.bipad_username and self.bipad_password):
            raise RuntimeError("BIPAD credentials/token not configured")

        token = self.bipad_token or await self._refresh_bipad_token(client)
        if not token:
            raise RuntimeError("BIPAD token unavailable")

        url = f"{self.bipad_api_base}/{path.lstrip('/')}"
        headers = {"Authorization": f"Token {token}", "Accept": "application/json"}
        resp = await client.get(url, params=params or {}, headers=headers, timeout=25.0)
        if resp.status_code == 401 and (self.bipad_username and self.bipad_password):
            token = await self._refresh_bipad_token(client)
            if token:
                headers["Authorization"] = f"Token {token}"
                resp = await client.get(url, params=params or {}, headers=headers, timeout=25.0)
        resp.raise_for_status()
        return resp.json()

    @staticmethod
    def _items_from_payload(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for key in ("results", "data", "items", "incidents", "alerts"):
                if isinstance(payload.get(key), list):
                    return [x for x in payload[key] if isinstance(x, dict)]
        return []

    @staticmethod
    def _value_from_nested(record: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in record and record[key] not in (None, ""):
                return record[key]
        return None

    async def sync_bipad_incidents(self, days: int = 30) -> int:
        async with self._sync_lock:
            if not self.bipad_token and not (self.bipad_username and self.bipad_password):
                await self.update_status("bipad_incidents", "skipped", 0, "credentials not configured")
                return 0

            now = datetime.now(timezone.utc)
            params = {
                "format": "json",
                "date_from": (now - timedelta(days=days)).date().isoformat(),
                "date_to": now.date().isoformat(),
            }

            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                payload = await self._bipad_get(client, "/incident/", params=params)
            items = self._items_from_payload(payload)
            if not items:
                await self.update_status("bipad_incidents", "ok", 0, "empty payload")
                return 0

            rows: list[dict[str, Any]] = []
            for item in items:
                district_val = self._value_from_nested(item, "district_name", "district")
                district_name = district_val.get("title") if isinstance(district_val, dict) else district_val
                district_id = self._value_from_nested(item, "district_id", "district")
                if isinstance(district_id, dict):
                    district_id = district_id.get("id")
                hazard = self._value_from_nested(item, "hazard", "hazard_type")
                if isinstance(hazard, dict):
                    hazard = hazard.get("title") or hazard.get("name")

                point = self._value_from_nested(item, "point", "location", "geom")
                lat = self._safe_float(self._value_from_nested(item, "lat", "latitude"))
                lon = self._safe_float(self._value_from_nested(item, "lon", "lng", "longitude"))
                if isinstance(point, dict):
                    lat = lat if lat is not None else self._safe_float(point.get("lat") or point.get("latitude"))
                    lon = lon if lon is not None else self._safe_float(point.get("lon") or point.get("lng") or point.get("longitude"))

                incident_date = self._parse_timestamp(
                    self._value_from_nested(item, "incident_on", "incident_date", "date", "created_at")
                )
                bipad_id = self._value_from_nested(item, "id", "bipad_id", "incident_id")
                if not bipad_id:
                    bipad_id = f"{incident_date.isoformat()}-{lat}-{lon}"

                rows.append(
                    {
                        "bipad_id": str(bipad_id),
                        "title": self._value_from_nested(item, "title", "name"),
                        "hazard": str(hazard or "unknown").lower(),
                        "district_id": str(district_id) if district_id is not None else None,
                        "district_name": district_name,
                        "province": self._value_from_nested(item, "province", "province_name"),
                        "lat": lat,
                        "lon": lon,
                        "deaths": self._safe_int(
                            self._value_from_nested(item, "loss_human_death", "deaths"), 0
                        ),
                        "missing": self._safe_int(
                            self._value_from_nested(item, "loss_human_missing", "missing"), 0
                        ),
                        "injured": self._safe_int(
                            self._value_from_nested(item, "loss_human_injured", "injured"), 0
                        ),
                        "families_affected": self._safe_int(
                            self._value_from_nested(item, "loss_family_affected", "families_affected"), 0
                        ),
                        "incident_date": incident_date,
                        "verified": bool(self._value_from_nested(item, "verified", "is_verified") or False),
                        "source_url": self._value_from_nested(item, "url", "source_url"),
                        "raw_payload": item,
                    }
                )

            conn = self._get_pg_conn()
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        """
                        INSERT INTO bipad_incidents (
                            bipad_id, title, hazard, district_id, district_name, province,
                            lat, lon, deaths, missing, injured, families_affected,
                            incident_date, verified, source_url, fetched_at, raw_payload
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, NOW(), %s::jsonb
                        )
                        ON CONFLICT (bipad_id) DO UPDATE SET
                            title = EXCLUDED.title,
                            hazard = EXCLUDED.hazard,
                            district_id = EXCLUDED.district_id,
                            district_name = EXCLUDED.district_name,
                            province = EXCLUDED.province,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            deaths = EXCLUDED.deaths,
                            missing = EXCLUDED.missing,
                            injured = EXCLUDED.injured,
                            families_affected = EXCLUDED.families_affected,
                            incident_date = EXCLUDED.incident_date,
                            verified = EXCLUDED.verified,
                            source_url = EXCLUDED.source_url,
                            fetched_at = NOW(),
                            raw_payload = EXCLUDED.raw_payload
                        """,
                        (
                            row["bipad_id"],
                            row.get("title"),
                            row.get("hazard"),
                            row.get("district_id"),
                            row.get("district_name"),
                            row.get("province"),
                            row.get("lat"),
                            row.get("lon"),
                            row.get("deaths", 0),
                            row.get("missing", 0),
                            row.get("injured", 0),
                            row.get("families_affected", 0),
                            row.get("incident_date"),
                            row.get("verified", False),
                            row.get("source_url"),
                            json.dumps(row.get("raw_payload") or {}),
                        ),
                    )
            conn.commit()
            await self.update_status("bipad_incidents", "ok", len(rows), "source=bipad_api")
            self.logger.info(f"BIPAD incidents sync complete: {len(rows)} rows")
            return len(rows)

    async def sync_bipad_alerts(self) -> int:
        async with self._sync_lock:
            if not self.bipad_token and not (self.bipad_username and self.bipad_password):
                await self.update_status("bipad_alerts", "skipped", 0, "credentials not configured")
                return 0

            params = {"format": "json", "status": "active"}
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                payload = await self._bipad_get(client, "/alert/", params=params)
            items = self._items_from_payload(payload)
            if not items:
                await self.update_status("bipad_alerts", "ok", 0, "empty payload")
                return 0

            rows: list[dict[str, Any]] = []
            for item in items:
                district_val = self._value_from_nested(item, "district_name", "district")
                district_name = district_val.get("title") if isinstance(district_val, dict) else district_val
                district_id = self._value_from_nested(item, "district_id", "district")
                if isinstance(district_id, dict):
                    district_id = district_id.get("id")
                hazard = self._value_from_nested(item, "hazard", "hazard_type")
                if isinstance(hazard, dict):
                    hazard = hazard.get("title") or hazard.get("name")

                point = self._value_from_nested(item, "point", "location", "geom")
                lat = self._safe_float(self._value_from_nested(item, "lat", "latitude"))
                lon = self._safe_float(self._value_from_nested(item, "lon", "lng", "longitude"))
                if isinstance(point, dict):
                    lat = lat if lat is not None else self._safe_float(point.get("lat") or point.get("latitude"))
                    lon = lon if lon is not None else self._safe_float(point.get("lon") or point.get("lng") or point.get("longitude"))

                alert_date = self._parse_timestamp(
                    self._value_from_nested(item, "alert_date", "issued_at", "created_at")
                )
                expiry_date = self._parse_timestamp(
                    self._value_from_nested(item, "expiry_date", "expires_at", "valid_till")
                )
                alert_id = self._value_from_nested(item, "id", "alert_id")
                if not alert_id:
                    alert_id = f"{alert_date.isoformat()}-{lat}-{lon}"

                rows.append(
                    {
                        "alert_id": str(alert_id),
                        "title": self._value_from_nested(item, "title", "name"),
                        "hazard": str(hazard or "unknown").lower(),
                        "district_id": str(district_id) if district_id is not None else None,
                        "district_name": district_name,
                        "province": self._value_from_nested(item, "province", "province_name"),
                        "lat": lat,
                        "lon": lon,
                        "severity": str(
                            self._value_from_nested(item, "severity", "level") or "unknown"
                        ).lower(),
                        "status": str(
                            self._value_from_nested(item, "status") or "active"
                        ).lower(),
                        "alert_date": alert_date,
                        "expiry_date": expiry_date,
                        "source_url": self._value_from_nested(item, "url", "source_url"),
                        "raw_payload": item,
                    }
                )

            conn = self._get_pg_conn()
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        """
                        INSERT INTO bipad_alerts (
                            alert_id, title, hazard, district_id, district_name, province,
                            lat, lon, severity, status, alert_date, expiry_date,
                            source_url, fetched_at, raw_payload
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, NOW(), %s::jsonb
                        )
                        ON CONFLICT (alert_id) DO UPDATE SET
                            title = EXCLUDED.title,
                            hazard = EXCLUDED.hazard,
                            district_id = EXCLUDED.district_id,
                            district_name = EXCLUDED.district_name,
                            province = EXCLUDED.province,
                            lat = EXCLUDED.lat,
                            lon = EXCLUDED.lon,
                            severity = EXCLUDED.severity,
                            status = EXCLUDED.status,
                            alert_date = EXCLUDED.alert_date,
                            expiry_date = EXCLUDED.expiry_date,
                            source_url = EXCLUDED.source_url,
                            fetched_at = NOW(),
                            raw_payload = EXCLUDED.raw_payload
                        """,
                        (
                            row["alert_id"],
                            row.get("title"),
                            row.get("hazard"),
                            row.get("district_id"),
                            row.get("district_name"),
                            row.get("province"),
                            row.get("lat"),
                            row.get("lon"),
                            row.get("severity"),
                            row.get("status"),
                            row.get("alert_date"),
                            row.get("expiry_date"),
                            row.get("source_url"),
                            json.dumps(row.get("raw_payload") or {}),
                        ),
                    )
            conn.commit()
            await self.update_status("bipad_alerts", "ok", len(rows), "source=bipad_api")
            self.logger.info(f"BIPAD alerts sync complete: {len(rows)} rows")
            return len(rows)
