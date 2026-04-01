"""Vicmap Address point features via public Esri FeatureServer (CC BY — Data Vic)."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any

import httpx

# Vicmap Address REST API — https://discover.data.vic.gov.au/dataset/vicmap-address-rest-api
VICMAP_ADDRESS_FEATURE_LAYER = (
    "https://services-ap1.arcgis.com/P744lA0wf4LlBZ84/arcgis/rest/services/"
    "Vicmap_Address/FeatureServer/0"
)

DEFAULT_ADDRESS_OUT_FIELDS = (
    "OBJECTID,property_pfi,ezi_address,is_primary,locality_name,postcode"
)

# Vicmap Address points rarely sit on industrial seed coordinates; buffer search by default.
DEFAULT_ADDRESS_POINT_BUFFER_M = 100.0


def _where_property_pfi(property_pfi: str) -> str:
    safe = str(property_pfi).replace("'", "''")
    return f"property_pfi = '{safe}'"


def _address_cache_path(cache_dir: Path, property_pfi: str, out_fields: str) -> Path:
    key = hashlib.sha256(f"{property_pfi}|{out_fields}".encode()).hexdigest()[:32]
    return cache_dir / f"address_pfi_{key}.json"


def _address_point_cache_path(
    cache_dir: Path, lon: float, lat: float, out_fields: str, buffer_m: float
) -> Path:
    key = hashlib.sha256(
        f"{lon:.7f}|{lat:.7f}|{out_fields}|{buffer_m}".encode()
    ).hexdigest()[:32]
    return cache_dir / f"address_point_{key}.json"


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


def query_addresses_for_property_pfi(
    client: httpx.Client,
    property_pfi: str | None,
    *,
    layer_url: str = VICMAP_ADDRESS_FEATURE_LAYER,
    out_fields: str = DEFAULT_ADDRESS_OUT_FIELDS,
    max_records: int = 50,
    cache_dir: Path | None = None,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> list[dict[str, Any]]:
    """
    Return address attribute dicts for Vicmap Address points linked to parcel property_pfi.
    """
    if property_pfi is None or not str(property_pfi).strip():
        return []

    pfi_s = str(property_pfi).strip()
    if cache_dir is not None and disk_cache:
        cpath = _address_cache_path(cache_dir, pfi_s, out_fields)
        if not refresh:
            hit = _read_cache(cpath, cache_ttl_seconds)
            if hit is not None:
                return hit

    base = layer_url.rstrip("/") + "/query"
    params = {
        "f": "geojson",
        "where": _where_property_pfi(pfi_s),
        "outFields": out_fields,
        "returnGeometry": "false",
        "resultRecordCount": str(max_records),
    }
    r = client.get(
        base,
        params=params,
        headers={"User-Agent": "dapr-portal-cli/0.1 (+Vicmap Address)"},
    )
    r.raise_for_status()
    data = r.json()
    feats = data.get("features") or []
    rows: list[dict[str, Any]] = []
    for f in feats:
        props = dict(f.get("properties") or {})
        rows.append(props)

    if cache_dir is not None and disk_cache:
        _write_cache(_address_cache_path(cache_dir, pfi_s, out_fields), rows)
    return rows


def query_addresses_at_point(
    client: httpx.Client,
    lon: float,
    lat: float,
    *,
    layer_url: str = VICMAP_ADDRESS_FEATURE_LAYER,
    out_fields: str = DEFAULT_ADDRESS_OUT_FIELDS,
    max_records: int = 15,
    buffer_meters: float = DEFAULT_ADDRESS_POINT_BUFFER_M,
    cache_dir: Path | None = None,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> list[dict[str, Any]]:
    """
    Address points within ``buffer_meters`` of (lon, lat) in WGS84.
    Use when ``property_pfi`` join returns nothing (e.g. industrial parcels).
    """
    if cache_dir is not None and disk_cache:
        cpath = _address_point_cache_path(cache_dir, lon, lat, out_fields, buffer_meters)
        if not refresh:
            hit = _read_cache(cpath, cache_ttl_seconds)
            if hit is not None:
                return hit

    base = layer_url.rstrip("/") + "/query"
    params: dict[str, str] = {
        "f": "geojson",
        "geometryType": "esriGeometryPoint",
        "geometry": f"{lon},{lat}",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
        "outFields": out_fields,
        "returnGeometry": "false",
        "resultRecordCount": str(max_records),
    }
    if buffer_meters and buffer_meters > 0:
        params["distance"] = str(buffer_meters)
        params["units"] = "esriSRUnit_Meter"

    r = client.get(
        base,
        params=params,
        headers={"User-Agent": "dapr-portal-cli/0.1 (+Vicmap Address)"},
    )
    r.raise_for_status()
    data = r.json()
    feats = data.get("features") or []
    rows: list[dict[str, Any]] = []
    for f in feats:
        props = dict(f.get("properties") or {})
        rows.append(props)

    if cache_dir is not None and disk_cache:
        _write_cache(
            _address_point_cache_path(cache_dir, lon, lat, out_fields, buffer_meters),
            rows,
        )
    return rows


def query_addresses_for_site(
    client: httpx.Client,
    lon: float,
    lat: float,
    parcel_pfi: str | None,
    *,
    layer_url: str = VICMAP_ADDRESS_FEATURE_LAYER,
    out_fields: str = DEFAULT_ADDRESS_OUT_FIELDS,
    max_pfi_matches: int = 50,
    point_fallback: bool = True,
    max_point_matches: int = 15,
    point_buffer_meters: float = DEFAULT_ADDRESS_POINT_BUFFER_M,
    cache_dir: Path | None = None,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> tuple[list[dict[str, Any]], str | None]:
    """
    Try ``property_pfi`` join first; if empty and ``point_fallback``, buffered point query.
    Returns (rows, source) where source is ``property_pfi``, ``point_intersect``, or None.
    """
    rows = query_addresses_for_property_pfi(
        client,
        parcel_pfi,
        layer_url=layer_url,
        out_fields=out_fields,
        max_records=max_pfi_matches,
        cache_dir=cache_dir,
        cache_ttl_seconds=cache_ttl_seconds,
        refresh=refresh,
        disk_cache=disk_cache,
    )
    if rows:
        return rows, "property_pfi"
    if point_fallback:
        rows = query_addresses_at_point(
            client,
            lon,
            lat,
            layer_url=layer_url,
            out_fields=out_fields,
            max_records=max_point_matches,
            buffer_meters=point_buffer_meters,
            cache_dir=cache_dir,
            cache_ttl_seconds=cache_ttl_seconds,
            refresh=refresh,
            disk_cache=disk_cache,
        )
        if rows:
            return rows, "point_intersect"
    return [], None


def pick_preferred_address_row(addresses: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Prefer is_primary Y; else first row."""
    if not addresses:
        return None
    for row in addresses:
        v = row.get("is_primary")
        if v is not None and str(v).strip().upper() in ("Y", "1", "T", "TRUE"):
            return row
    return addresses[0]


def address_summary_fields(addresses: list[dict[str, Any]]) -> dict[str, Any]:
    """ezi_address, counts, and ambiguous flag for CSV/report flattening."""
    row = pick_preferred_address_row(addresses)
    n = len(addresses)
    return {
        "ezi_address": (row.get("ezi_address") if row else None) or "",
        "address_match_count": n,
        "address_ambiguous": n > 1,
    }
