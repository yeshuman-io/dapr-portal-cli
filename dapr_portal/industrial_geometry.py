"""Industrial zone + UDP polygon fetch by bbox and point-in-polygon classification (Victoria)."""

from __future__ import annotations

from typing import Any

import httpx
from shapely.geometry import Point, shape
from shapely.prepared import prep
from shapely.strtree import STRtree

from dapr_portal.vic_spatial import (
    DEFAULT_INDUSTRIAL_MAP_LAYER_IDS,
    PLANNING_SCHEME_ZONES_MAPSERVER,
    UDP_INDUSTRIAL_TYPENAME,
    WFS_BASE_URL,
    query_mapserver_layer_geojson,
    wfs_getfeature_geojson,
    zone_codes_to_layer_ids,
)


def _geometries_from_geojson_features(
    features: list[dict[str, Any]],
) -> list[Any]:
    out: list[Any] = []
    for feat in features:
        geom = feat.get("geometry")
        if not geom:
            continue
        try:
            g = shape(geom)
        except Exception:
            continue
        if g.is_empty:
            continue
        out.append(g)
    return out


def collect_industrial_zone_polygons(
    client: httpx.Client,
    bbox: tuple[float, float, float, float],
    *,
    zone_codes: list[str] | None = None,
    layer_ids: tuple[int, ...] | None = None,
    cache_dir,
    cache_ttl: float | None,
    refresh: bool,
    disk_cache: bool = True,
    max_features: int | None = None,
) -> list[Any]:
    """Shapely geometries (Polygon/MultiPolygon) for industrial planning layers in bbox."""
    lids = layer_ids
    if lids is None:
        lids = (
            zone_codes_to_layer_ids(zone_codes)
            if zone_codes
            else DEFAULT_INDUSTRIAL_MAP_LAYER_IDS
        )
    all_geoms: list[Any] = []
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
            out_fields="OBJECTID,ZONE_CODE,LGA,SCHEME_CODE,ZONE_DESCRIPTION",
            max_features=max_features,
            cache_dir=cache_dir,
            cache_ttl_seconds=cache_ttl,
            refresh=refresh,
            disk_cache=disk_cache,
        )
        all_geoms.extend(_geometries_from_geojson_features(fc.get("features") or []))
    return all_geoms


def collect_udp_industrial_polygons(
    client: httpx.Client,
    bbox: tuple[float, float, float, float],
    *,
    cql_filter: str | None = None,
    cache_dir,
    cache_ttl: float | None,
    refresh: bool,
    disk_cache: bool = True,
    max_features: int | None = None,
) -> list[Any]:
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
    return _geometries_from_geojson_features(fc.get("features") or [])


class IndustrialTagIndex:
    """Fast point tests against zone and/or UDP industrial polygons."""

    def __init__(
        self,
        zone_geoms: list[Any],
        udp_geoms: list[Any],
    ) -> None:
        self._zone_prep = [prep(g) for g in zone_geoms]
        self._udp_prep = [prep(g) for g in udp_geoms]
        self._zone_tree = STRtree(zone_geoms) if len(zone_geoms) > 0 else None
        self._udp_tree = STRtree(udp_geoms) if len(udp_geoms) > 0 else None

    def classify(self, lon: float, lat: float) -> tuple[bool, str]:
        """
        Returns (in_industrial, industrial_sources).
        industrial_sources: none | zones | udp | both
        """
        pt = Point(lon, lat)
        in_z = self._any_covers(self._zone_prep, self._zone_tree, pt)
        in_u = self._any_covers(self._udp_prep, self._udp_tree, pt)
        if in_z and in_u:
            return True, "both"
        if in_z:
            return True, "zones"
        if in_u:
            return True, "udp"
        return False, "none"

    @staticmethod
    def _any_covers(
        preps: list,
        tree: STRtree | None,
        pt: Point,
    ) -> bool:
        if not preps:
            return False
        if tree is not None:
            for idx in tree.query(pt):
                if preps[int(idx)].covers(pt):
                    return True
            return False
        return any(p.covers(pt) for p in preps)


def build_industrial_tag_index(
    client: httpx.Client,
    bbox: tuple[float, float, float, float],
    *,
    land_source: str,
    zone_codes: list[str] | None = None,
    max_polygon_features: int | None = None,
    cache_dir,
    cache_ttl: float | None,
    refresh: bool,
    disk_cache: bool,
) -> IndustrialTagIndex:
    """land_source: zones | udp | both"""
    zone_g: list[Any] = []
    udp_g: list[Any] = []
    if land_source in ("zones", "both"):
        zone_g = collect_industrial_zone_polygons(
            client,
            bbox,
            zone_codes=zone_codes,
            cache_dir=cache_dir,
            cache_ttl=cache_ttl,
            refresh=refresh,
            disk_cache=disk_cache,
            max_features=max_polygon_features,
        )
    if land_source in ("udp", "both"):
        udp_g = collect_udp_industrial_polygons(
            client,
            bbox,
            cache_dir=cache_dir,
            cache_ttl=cache_ttl,
            refresh=refresh,
            disk_cache=disk_cache,
            max_features=max_polygon_features,
        )
    return IndustrialTagIndex(zone_g, udp_g)
