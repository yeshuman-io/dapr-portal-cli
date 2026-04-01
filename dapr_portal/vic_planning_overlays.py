"""Point-in-polygon checks against spatial.planning.vic.gov.au MapServer layers (WGS84)."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

import httpx

from dapr_portal.vic_spatial import PLANNING_SCHEME_ZONES_MAPSERVER

# Bushfire: DELWP Bushfire Prone Areas / BMO service (spatial.planning.vic.gov.au).
# Layer 0 = BMO, Layer 1 = BPA (Bushfire Prone Areas) — both queried; hit if either matches.
BUSHFIRE_PRONE_MAPSERVER = (
    "https://spatial.planning.vic.gov.au/gis/rest/services/bushfire_prone_areas/MapServer"
)
BUSHFIRE_BMO_LAYER_ID = 0
BUSHFIRE_BPA_LAYER_ID = 1

# Flood-related planning signal: Urban Floodway Zone (UFZ) in planning_scheme_zones.
FLOOD_UFZ_LAYER_ID = 44


def _overlay_cache_path(
    cache_dir: Path, service: str, layer_id: int, lon: float, lat: float
) -> Path:
    key = hashlib.sha256(
        f"{service}|{layer_id}|{lon:.7f}|{lat:.7f}".encode()
    ).hexdigest()[:28]
    return cache_dir / f"overlay_{key}.json"


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


def query_layer_intersect_count(
    client: httpx.Client,
    mapserver_base: str,
    layer_id: int,
    lon: float,
    lat: float,
    *,
    cache_dir: Path | None = None,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> int:
    """Return feature count intersecting point (EPSG:4326)."""
    if cache_dir is not None and disk_cache:
        cpath = _overlay_cache_path(cache_dir, mapserver_base, layer_id, lon, lat)
        if not refresh:
            hit = _read_json_cache(cpath, cache_ttl_seconds)
            if hit is not None:
                return int(hit["count"])

    base = mapserver_base.rstrip("/") + f"/{layer_id}/query"
    params = {
        "f": "json",
        "geometry": f"{lon},{lat}",
        "geometryType": "esriGeometryPoint",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "returnCountOnly": "true",
    }
    r = client.get(
        base,
        params=params,
        headers={"User-Agent": "dapr-portal-cli/0.1 (+spatial.planning.vic.gov.au)"},
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"overlay query error: {data.get('error')}")
    count = int(data.get("count", 0))
    if cache_dir is not None and disk_cache:
        _write_json_cache(
            _overlay_cache_path(cache_dir, mapserver_base, layer_id, lon, lat),
            {"count": count},
        )
    return count


def query_bushfire_hits(
    client: httpx.Client,
    lon: float,
    lat: float,
    **kwargs: Any,
) -> tuple[bool, bool, bool]:
    """
    Returns (bushfire_overlay_hit, bmo_hit, bpa_hit).
    bushfire_overlay_hit is True if either BMO or BPA polygon contains the point.
    """
    bmo = query_layer_intersect_count(
        client, BUSHFIRE_PRONE_MAPSERVER, BUSHFIRE_BMO_LAYER_ID, lon, lat, **kwargs
    )
    bpa = query_layer_intersect_count(
        client, BUSHFIRE_PRONE_MAPSERVER, BUSHFIRE_BPA_LAYER_ID, lon, lat, **kwargs
    )
    return (bmo > 0 or bpa > 0), bmo > 0, bpa > 0


def query_floodway_hit(
    client: httpx.Client,
    lon: float,
    lat: float,
    **kwargs: Any,
) -> bool:
    """True if point lies in an Urban Floodway Zone (UFZ) polygon."""
    n = query_layer_intersect_count(
        client,
        PLANNING_SCHEME_ZONES_MAPSERVER,
        FLOOD_UFZ_LAYER_ID,
        lon,
        lat,
        **kwargs,
    )
    return n > 0
