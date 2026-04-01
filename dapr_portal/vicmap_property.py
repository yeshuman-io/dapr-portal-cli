"""Vicmap Property parcel lookup via public Esri FeatureServer (CC BY — Data Vic)."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

import httpx

# Vicmap Property REST / FeatureServer (ArcGIS Online AP-Southeast).
# Dataset: Vicmap Property REST API — https://discover.data.vic.gov.au/dataset/vicmap-property-rest-api
# Parcel map polygons layer:
VICMAP_PARCEL_FEATURE_LAYER = (
    "https://services-ap1.arcgis.com/P744lA0wf4LlBZ84/arcgis/rest/services/"
    "Vicmap_Parcel/FeatureServer/0"
)

DEFAULT_PARCEL_OUT_FIELDS = (
    "OBJECTID,parcel_spi,parcel_pfi,parcel_lot_number,parcel_plan_number,"
    "parcel_lga_code,parcel_status,parcel_crown_status,parcel_multi,parcel_road"
)


def _parcel_cache_path(cache_dir: Path, lon: float, lat: float, out_fields: str) -> Path:
    key = hashlib.sha256(
        f"{lon:.7f}|{lat:.7f}|{out_fields}".encode()
    ).hexdigest()[:32]
    return cache_dir / f"parcel_point_{key}.json"


def _read_cache(path: Path, ttl: float | None) -> Any | None:
    if not path.is_file():
        return None
    if ttl is not None and time.time() - path.stat().st_mtime > ttl:
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)


def query_parcels_at_point(
    client: httpx.Client,
    lon: float,
    lat: float,
    *,
    layer_url: str = VICMAP_PARCEL_FEATURE_LAYER,
    out_fields: str = DEFAULT_PARCEL_OUT_FIELDS,
    max_records: int = 20,
    return_geometry: bool = False,
    cache_dir: Path | None = None,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> list[dict[str, Any]]:
    """
    Return parcel attribute dicts for all polygons intersecting the point (WGS84).
    A single location can match multiple parcel features (e.g. complex boundaries).
    """
    if cache_dir is not None and disk_cache:
        cpath = _parcel_cache_path(cache_dir, lon, lat, out_fields)
        if not refresh:
            hit = _read_cache(cpath, cache_ttl_seconds)
            if hit is not None:
                return hit

    base = layer_url.rstrip("/") + "/query"
    params = {
        "f": "geojson",
        "geometryType": "esriGeometryPoint",
        "geometry": f"{lon},{lat}",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": out_fields,
        "returnGeometry": "true" if return_geometry else "false",
        "resultRecordCount": str(max_records),
    }
    r = client.get(
        base,
        params=params,
        headers={"User-Agent": "dapr-portal-cli/0.1 (+Vicmap Property)"},
    )
    r.raise_for_status()
    data = r.json()
    feats = data.get("features") or []
    rows: list[dict[str, Any]] = []
    for f in feats:
        props = dict(f.get("properties") or {})
        rows.append(props)

    if cache_dir is not None and disk_cache:
        _write_cache(_parcel_cache_path(cache_dir, lon, lat, out_fields), rows)
    return rows


def parcels_to_flat_row(
    site_name: str | None,
    lat: float,
    lon: float,
    parcels: list[dict[str, Any]],
) -> dict[str, Any]:
    """First intersecting parcel attributes + match count (for CSV)."""
    row: dict[str, Any] = {
        "site_name": site_name or "",
        "lat": lat,
        "lon": lon,
        "parcel_match_count": len(parcels),
    }
    if parcels:
        row.update(parcels[0])
    return row
