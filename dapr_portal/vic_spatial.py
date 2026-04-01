"""Victorian open spatial data: Esri MapServer queries and GeoServer WFS (bounded, cached)."""

from __future__ import annotations

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

import httpx
from shapely.geometry import shape

# ---
# Canonical public endpoints (verify on Data Vic if services move).
# Planning zones: spatial.planning.vic.gov.au planning_scheme_zones MapServer.
# Dataset context: https://discover.data.vic.gov.au/ (Vicmap Planning / planning scheme zones).
PLANNING_SCHEME_ZONES_MAPSERVER = (
    "https://spatial.planning.vic.gov.au/gis/rest/services/planning_scheme_zones/MapServer"
)
# Layer IDs: 11=IN1Z, 12=IN2Z, 13=IN3Z (Industrial 1/2/3). Group layer 10 has no queryable geometry.
DEFAULT_INDUSTRIAL_MAP_LAYER_IDS = (11, 12, 13)

# UDP industrial land 2022 — GeoServer WFS on DELWP Open Data Platform.
# Dataset: "Urban Development Program - Industrial Land 2022" (Data Vic).
WFS_BASE_URL = "https://opendata.maps.vic.gov.au/geoserver/wfs"
UDP_INDUSTRIAL_TYPENAME = "open-data-platform:ind2022"

# Rough Melbourne metro bounding box (WGS84): minLon, minLat, maxLon, maxLat
DEFAULT_METRO_MELBOURNE_BBOX = (144.55, -38.20, 145.65, -37.50)

DEFAULT_VIC_CACHE_DIR = Path(
    os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache"))
) / "dapr-portal-cli" / "vicmap"


def parse_bbox(s: str) -> tuple[float, float, float, float]:
    """Parse ``minLon,minLat,maxLon,maxLat`` (WGS84)."""
    parts = [p.strip() for p in s.split(",")]
    if len(parts) != 4:
        raise ValueError("bbox must be minLon,minLat,maxLon,maxLat")
    try:
        vals = tuple(float(x) for x in parts)
    except ValueError as e:
        raise ValueError("bbox values must be numbers") from e
    min_lon, min_lat, max_lon, max_lat = vals
    if min_lon >= max_lon or min_lat >= max_lat:
        raise ValueError("bbox min must be strictly less than max")
    if not (-180 <= min_lon <= 180 and -180 <= max_lon <= 180):
        raise ValueError("longitude out of range")
    if not (-90 <= min_lat <= 90 and -90 <= max_lat <= 90):
        raise ValueError("latitude out of range")
    return vals


def _cache_key_parts(*parts: str) -> str:
    h = hashlib.sha256("|".join(parts).encode()).hexdigest()[:32]
    return h


def _read_json_cache(path: Path, ttl: float | None) -> Any | None:
    if not path.is_file():
        return None
    if ttl is not None and time.time() - path.stat().st_mtime > ttl:
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _write_json_cache(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)


def query_mapserver_layer_geojson(
    client: httpx.Client,
    mapserver_base: str,
    layer_id: int,
    bbox: tuple[float, float, float, float],
    *,
    where: str = "1=1",
    out_fields: str = "OBJECTID,ZONE_CODE,LGA,SCHEME_CODE,ZONE_DESCRIPTION",
    page_size: int = 2000,
    max_features: int | None = None,
    cache_dir: Path = DEFAULT_VIC_CACHE_DIR,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> dict[str, Any]:
    """
    Paginated MapServer query returning a single GeoJSON FeatureCollection (merged).
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    geom = f"{min_lon},{min_lat},{max_lon},{max_lat}"
    key = _cache_key_parts(
        mapserver_base,
        str(layer_id),
        geom,
        where,
        out_fields,
        str(page_size),
        str(max_features if max_features is not None else -1),
    )
    cache_path = cache_dir / f"esri_{key}.json"
    if disk_cache and not refresh:
        cached = _read_json_cache(cache_path, cache_ttl_seconds)
        if cached is not None:
            return cached

    base = mapserver_base.rstrip("/") + f"/{layer_id}/query"
    all_features: list[dict] = []
    offset = 0
    while True:
        params = {
            "f": "geojson",
            "where": where,
            "geometry": geom,
            "geometryType": "esriGeometryEnvelope",
            "inSR": "4326",
            "outSR": "4326",
            "outFields": out_fields,
            "returnGeometry": "true",
            "resultOffset": str(offset),
            "resultRecordCount": str(page_size),
        }
        r = client.get(base, params=params, headers={"User-Agent": _ua()})
        r.raise_for_status()
        chunk = r.json()
        feats = chunk.get("features") or []
        all_features.extend(feats)
        if max_features is not None and len(all_features) >= max_features:
            all_features = all_features[:max_features]
            break
        if len(feats) < page_size:
            break
        offset += page_size

    fc = {"type": "FeatureCollection", "features": all_features}
    if disk_cache:
        _write_json_cache(cache_path, fc)
    return fc


def wfs_getfeature_geojson(
    client: httpx.Client,
    type_name: str,
    bbox: tuple[float, float, float, float],
    *,
    cql_filter: str | None = None,
    page_size: int = 1000,
    max_features: int | None = None,
    cache_dir: Path = DEFAULT_VIC_CACHE_DIR,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> dict[str, Any]:
    """WFS 2.0 GetFeature with bbox; returns GeoJSON FeatureCollection."""
    min_lon, min_lat, max_lon, max_lat = bbox
    bbox_arg = f"{min_lon},{min_lat},{max_lon},{max_lat},EPSG:4326"
    key = _cache_key_parts(
        WFS_BASE_URL,
        type_name,
        bbox_arg,
        cql_filter or "",
        str(page_size),
        str(max_features if max_features is not None else -1),
    )
    cache_path = cache_dir / f"wfs_{key}.json"
    if disk_cache and not refresh:
        cached = _read_json_cache(cache_path, cache_ttl_seconds)
        if cached is not None:
            return cached

    all_features: list[dict] = []
    start = 0
    while True:
        params: dict[str, str] = {
            "service": "wfs",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": type_name,
            "outputFormat": "application/json",
            "srsName": "EPSG:4326",
            "bbox": bbox_arg,
            "count": str(page_size),
            "startIndex": str(start),
        }
        if cql_filter:
            params["CQL_FILTER"] = cql_filter
        r = client.get(WFS_BASE_URL, params=params, headers={"User-Agent": _ua()})
        r.raise_for_status()
        chunk = r.json()
        feats = chunk.get("features") or []
        all_features.extend(feats)
        if max_features is not None and len(all_features) >= max_features:
            all_features = all_features[:max_features]
            break
        if len(feats) < page_size:
            break
        start += page_size

    fc = {"type": "FeatureCollection", "features": all_features}
    if disk_cache:
        _write_json_cache(cache_path, fc)
    return fc


def feature_collection_to_site_rows(
    features: list[dict],
    *,
    name_prefix: str,
    id_prop_keys: tuple[str, ...] = ("OBJECTID",),
) -> list[dict[str, Any]]:
    """
    Each feature -> {name, lat, lon, properties} for CSV / scout.
    Uses representative_point() inside polygon/multipolygon.
    """
    rows: list[dict[str, Any]] = []
    for feat in features:
        geom = feat.get("geometry")
        props = dict(feat.get("properties") or {})
        if not geom:
            continue
        try:
            g = shape(geom)
        except Exception:
            continue
        if g.is_empty:
            continue
        try:
            pt = g.representative_point()
        except Exception:
            continue
        lon, lat = float(pt.x), float(pt.y)
        parts = [name_prefix]
        for k in id_prop_keys:
            if k in props and props[k] is not None:
                parts.append(f"{k}={props[k]}")
        if feat.get("id") is not None:
            parts.append(f"fid={feat['id']}")
        name = "|".join(parts)
        rows.append({"name": name, "lat": lat, "lon": lon, "properties": props})
    return rows


def rows_to_csv_text(rows: list[dict[str, Any]]) -> str:
    import csv
    import io

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=["name", "lat", "lon"], extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow({"name": r["name"], "lat": r["lat"], "lon": r["lon"]})
    return buf.getvalue()


def zone_codes_to_layer_ids(zone_codes: list[str]) -> tuple[int, ...]:
    """Map ZONE_CODE tokens to MapServer layer ids."""
    m = {"IN1Z": 11, "IN2Z": 12, "IN3Z": 13}
    ids: list[int] = []
    for z in zone_codes:
        z = z.strip().upper()
        if z not in m:
            raise ValueError(f"unknown zone code {z!r}; expected one of {sorted(m)}")
        lid = m[z]
        if lid not in ids:
            ids.append(lid)
    return tuple(ids)


def _ua() -> str:
    return "dapr-portal-cli/0.1 (+Data Vic spatial)"
