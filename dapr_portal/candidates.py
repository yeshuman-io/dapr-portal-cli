"""Build candidate site rows from Victorian planning zones and UDP industrial WFS."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx

from dapr_portal.vic_spatial import (
    DEFAULT_INDUSTRIAL_MAP_LAYER_IDS,
    DEFAULT_METRO_MELBOURNE_BBOX,
    DEFAULT_VIC_CACHE_DIR,
    PLANNING_SCHEME_ZONES_MAPSERVER,
    UDP_INDUSTRIAL_TYPENAME,
    feature_collection_to_site_rows,
    query_mapserver_layer_geojson,
    rows_to_csv_text,
    wfs_getfeature_geojson,
    zone_codes_to_layer_ids,
)


def collect_industrial_zone_rows(
    client: httpx.Client,
    bbox: tuple[float, float, float, float],
    *,
    zone_codes: list[str] | None,
    layer_ids: tuple[int, ...] | None,
    max_features: int | None,
    cache_dir: Path,
    cache_ttl: float | None,
    refresh: bool,
    disk_cache: bool = True,
) -> list[dict[str, Any]]:
    lids = layer_ids
    if lids is None:
        if zone_codes:
            lids = zone_codes_to_layer_ids(zone_codes)
        else:
            lids = DEFAULT_INDUSTRIAL_MAP_LAYER_IDS
    seen: set[tuple[float, float, str]] = set()
    out: list[dict[str, Any]] = []
    for lid in lids:
        where = "1=1"
        if zone_codes:
            esc = ",".join(f"'{z.strip().upper()}'" for z in zone_codes)
            where = f"ZONE_CODE IN ({esc})"
        fc = query_mapserver_layer_geojson(
            client,
            PLANNING_SCHEME_ZONES_MAPSERVER,
            lid,
            bbox,
            where=where,
            max_features=None,
            cache_dir=cache_dir,
            cache_ttl_seconds=cache_ttl,
            refresh=refresh,
            disk_cache=disk_cache,
        )
        rows = feature_collection_to_site_rows(
            fc.get("features", []),
            name_prefix="zone",
            id_prop_keys=("OBJECTID", "ZONE_CODE", "LGA"),
        )
        for r in rows:
            key = (round(r["lat"], 6), round(r["lon"], 6), r["name"])
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
            if max_features is not None and len(out) >= max_features:
                return out
    return out


def collect_udp_industrial_rows(
    client: httpx.Client,
    bbox: tuple[float, float, float, float],
    *,
    cql_filter: str | None,
    max_features: int | None,
    cache_dir: Path,
    cache_ttl: float | None,
    refresh: bool,
    disk_cache: bool = True,
) -> list[dict[str, Any]]:
    fc = wfs_getfeature_geojson(
        client,
        UDP_INDUSTRIAL_TYPENAME,
        bbox,
        cql_filter=cql_filter,
        max_features=max_features,
        cache_dir=cache_dir,
        cache_ttl_seconds=cache_ttl,
        refresh=refresh,
        disk_cache=disk_cache,
    )
    return feature_collection_to_site_rows(
        fc.get("features", []),
        name_prefix="udp",
        id_prop_keys=("zone_num", "lga_name", "zone_code", "status_desc_2022"),
    )


__all__ = [
    "collect_industrial_zone_rows",
    "collect_udp_industrial_rows",
    "rows_to_csv_text",
    "DEFAULT_METRO_MELBOURNE_BBOX",
    "DEFAULT_VIC_CACHE_DIR",
]
