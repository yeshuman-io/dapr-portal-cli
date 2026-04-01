"""Paginated Vicmap Address FeatureServer queries by WGS84 bounding box (CC BY — Data Vic)."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Iterator

import httpx

from dapr_portal.vicmap_address import DEFAULT_ADDRESS_OUT_FIELDS, VICMAP_ADDRESS_FEATURE_LAYER


def _page_cache_path(
    cache_dir: Path,
    bbox: tuple[float, float, float, float],
    out_fields: str,
    offset: int,
    page_size: int,
) -> Path:
    key = hashlib.sha256(
        f"{bbox}|{out_fields}|{offset}|{page_size}".encode()
    ).hexdigest()[:32]
    return cache_dir / "address_bulk" / f"page_{key}.json"


def _read_json(path: Path, ttl: float | None) -> Any | None:
    if not path.is_file():
        return None
    if ttl is not None and time.time() - path.stat().st_mtime > ttl:
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, separators=(",", ":")), encoding="utf-8")
    tmp.replace(path)


def fetch_address_page_in_bbox(
    client: httpx.Client,
    bbox: tuple[float, float, float, float],
    offset: int,
    *,
    layer_url: str = VICMAP_ADDRESS_FEATURE_LAYER,
    out_fields: str = DEFAULT_ADDRESS_OUT_FIELDS,
    page_size: int = 2000,
    cache_dir: Path | None = None,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> list[dict[str, Any]]:
    """
    Single page of GeoJSON features from Vicmap Address layer 0 intersecting bbox envelope.
    Each item is ``{"type":"Feature","geometry":...,"properties":...}``.
    """
    if cache_dir is not None and disk_cache:
        cpath = _page_cache_path(cache_dir, bbox, out_fields, offset, page_size)
        if not refresh:
            hit = _read_json(cpath, cache_ttl_seconds)
            if hit is not None:
                return list(hit.get("features") or [])

    min_lon, min_lat, max_lon, max_lat = bbox
    geom = f"{min_lon},{min_lat},{max_lon},{max_lat}"
    base = layer_url.rstrip("/") + "/query"
    params = {
        "f": "geojson",
        "where": "1=1",
        "geometry": geom,
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "outSR": "4326",
        "outFields": out_fields,
        "returnGeometry": "true",
        "resultOffset": str(offset),
        "resultRecordCount": str(page_size),
    }
    r = client.get(
        base,
        params=params,
        headers={"User-Agent": "dapr-portal-cli/0.1 (+Vicmap Address bulk)"},
    )
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"Vicmap Address query error: {data.get('error')}")
    feats = list(data.get("features") or [])

    if cache_dir is not None and disk_cache:
        _write_json(
            _page_cache_path(cache_dir, bbox, out_fields, offset, page_size),
            {"features": feats},
        )
    return feats


def iter_address_features_in_bbox(
    client: httpx.Client,
    bbox: tuple[float, float, float, float],
    *,
    layer_url: str = VICMAP_ADDRESS_FEATURE_LAYER,
    out_fields: str = DEFAULT_ADDRESS_OUT_FIELDS,
    page_size: int = 2000,
    start_offset: int = 0,
    max_features: int | None = None,
    cache_dir: Path | None = None,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> Iterator[dict[str, Any]]:
    """
    Yield GeoJSON features until the service returns a short page or ``max_features`` reached.
    """
    offset = start_offset
    yielded = 0
    while True:
        feats = fetch_address_page_in_bbox(
            client,
            bbox,
            offset,
            layer_url=layer_url,
            out_fields=out_fields,
            page_size=page_size,
            cache_dir=cache_dir,
            cache_ttl_seconds=cache_ttl_seconds,
            refresh=refresh,
            disk_cache=disk_cache,
        )
        for f in feats:
            yield f
            yielded += 1
            if max_features is not None and yielded >= max_features:
                return
        if len(feats) < page_size:
            return
        offset += page_size


def feature_point_lonlat(feature: dict[str, Any]) -> tuple[float, float] | None:
    """Extract WGS84 lon, lat from GeoJSON Point feature; return None if missing/invalid."""
    geom = feature.get("geometry")
    if not geom or geom.get("type") != "Point":
        return None
    coords = geom.get("coordinates")
    if not coords or len(coords) < 2:
        return None
    try:
        return float(coords[0]), float(coords[1])
    except (TypeError, ValueError):
        return None


def feature_properties_dict(feature: dict[str, Any]) -> dict[str, Any]:
    return dict(feature.get("properties") or {})
