"""Build ranked site reports from screen/enrich JSON + DAPR tables + planning overlays."""

from __future__ import annotations

from typing import Any

import httpx

from dapr_portal.dapr_tables import (
    DEFAULT_LGA_SUMMARY_PATH,
    fetch_dapr_csv_cached,
    index_lga_summary_by_type,
    join_lga_rows,
    load_portal_session,
)
from dapr_portal.portal import DEFAULT_BASE_URL
from dapr_portal.vic_admin import (
    lga_name_for_code,
    lga_name_from_planning_candidate,
    normalize_lga_key,
)
from dapr_portal.vic_planning_overlays import query_bushfire_hits, query_floodway_hit
from dapr_portal.vicmap_address import address_summary_fields


REPORT_DISCLAIMER = (
    "This report combines open data for screening only. DAPR tables and planning overlays "
    "are general information, not legal flood/bushfire advice, permits, or connection offers. "
    "Urban Floodway Zone (UFZ) is used as a flood-related planning signal, not hydrology."
)


def _vicmap_parcels_list(entry: dict[str, Any]) -> list[dict[str, Any]]:
    """Screen JSON uses vicmap_parcels; enrich-parcels JSON uses parcels."""
    p = entry.get("vicmap_parcels")
    if p is not None:
        return list(p)
    p2 = entry.get("parcels")
    if p2 is not None:
        return list(p2)
    return []


def _nearest_distance_m(entry: dict[str, Any]) -> float:
    nearest = entry.get("nearest") or []
    if not nearest:
        return float("inf")
    try:
        return float(nearest[0].get("distance_m", float("inf")))
    except (TypeError, ValueError):
        return float("inf")


def _report_sort_key(r: dict[str, Any]) -> tuple[float, int, str]:
    d = _nearest_distance_m(r)
    friction = int(r.get("planning_overlay_friction", 0))
    name = str(r.get("site_name") or "")
    return (d, friction, name)


def sort_and_rank_entries(entries: list[dict[str, Any]]) -> None:
    """Sort by nearest line distance, then fewer overlay hits; assign rank 1..n."""
    entries.sort(key=_report_sort_key)
    for i, row in enumerate(entries, start=1):
        row["rank"] = i


def _resolve_lga_name(entry: dict[str, Any]) -> tuple[str | None, str | None]:
    """
    Returns (lga_name, source) where source is parcel_lga_code | planning_zone | None.
    """
    parcels = _vicmap_parcels_list(entry)
    if parcels:
        code = parcels[0].get("parcel_lga_code")
        name = lga_name_for_code(code)
        if name:
            return name, "parcel_lga_code"
        raw = parcels[0].get("parcel_lga_code")
        if raw is not None:
            return str(raw), "parcel_lga_code_unmapped"
    attrs = entry.get("candidate_attributes") or {}
    lga = attrs.get("LGA")
    if lga:
        return lga_name_from_planning_candidate(str(lga)), "planning_zone"
    return None, None


def enrich_one_entry(
    entry: dict[str, Any],
    client: httpx.Client,
    lga_index: dict[str, list[dict[str, Any]]],
    *,
    vic_cache_dir,
    vic_cache_ttl: float | None,
    vic_refresh: bool,
    vic_disk_cache: bool,
) -> dict[str, Any]:
    lat = float(entry["query"]["lat"])
    lon = float(entry["query"]["lon"])
    out = dict(entry)

    parcels = _vicmap_parcels_list(entry)
    out["parcel_match_count"] = len(parcels)
    out["parcel_ambiguous"] = len(parcels) > 1
    if parcels:
        out["parcel_primary"] = parcels[0]

    lga_name, lga_src = _resolve_lga_name(entry)
    out["resolved_lga_name"] = lga_name
    out["resolved_lga_source"] = lga_src
    out["resolved_lga_key"] = normalize_lga_key(lga_name)

    rows, matched = join_lga_rows(lga_index, lga_name)
    out["dapr_lga_summary_rows"] = rows
    out["dapr_table_match"] = matched

    bf, bmo, bpa = query_bushfire_hits(
        client,
        lon,
        lat,
        cache_dir=vic_cache_dir,
        cache_ttl_seconds=vic_cache_ttl,
        refresh=vic_refresh,
        disk_cache=vic_disk_cache,
    )
    flood = query_floodway_hit(
        client,
        lon,
        lat,
        cache_dir=vic_cache_dir,
        cache_ttl_seconds=vic_cache_ttl,
        refresh=vic_refresh,
        disk_cache=vic_disk_cache,
    )
    out["bushfire_overlay_hit"] = bf
    out["bushfire_bmo_hit"] = bmo
    out["bushfire_bpa_hit"] = bpa
    out["flood_overlay_hit"] = flood
    out["planning_overlay_friction"] = int(bf) + int(flood)
    return out


def build_report(
    entries: list[dict[str, Any]],
    client: httpx.Client,
    *,
    base_url: str = DEFAULT_BASE_URL,
    lga_summary_path: str = DEFAULT_LGA_SUMMARY_PATH,
    vic_cache_dir,
    vic_cache_ttl: float | None,
    vic_refresh: bool,
    vic_no_cache: bool,
    dapr_csv_refresh: bool,
) -> list[dict[str, Any]]:
    disk = not vic_no_cache
    session = load_portal_session(client, base_url)
    csv_text = fetch_dapr_csv_cached(
        client,
        session,
        lga_summary_path,
        cache_dir=vic_cache_dir,
        cache_ttl_seconds=vic_cache_ttl,
        refresh=dapr_csv_refresh or vic_refresh,
        disk_cache=disk,
    )
    lga_index = index_lga_summary_by_type(csv_text)

    enriched = [
        enrich_one_entry(
            e,
            client,
            lga_index,
            vic_cache_dir=vic_cache_dir,
            vic_cache_ttl=vic_cache_ttl,
            vic_refresh=vic_refresh,
            vic_disk_cache=disk,
        )
        for e in entries
    ]

    sort_and_rank_entries(enriched)
    return enriched


def report_to_csv_rows(entries: list[dict[str, Any]]) -> tuple[list[str], list[dict[str, Any]]]:
    """Flatten for CSV: core columns + stringified extras."""
    base_cols = [
        "rank",
        "site_name",
        "lat",
        "lon",
        "nearest_distance_m",
        "nearest_circuit",
        "parcel_spi",
        "parcel_match_count",
        "parcel_ambiguous",
        "resolved_lga_name",
        "resolved_lga_source",
        "dapr_table_match",
        "bushfire_overlay_hit",
        "bushfire_bmo_hit",
        "bushfire_bpa_hit",
        "flood_overlay_hit",
        "planning_overlay_friction",
        "ezi_address",
        "address_match_count",
        "address_ambiguous",
        "address_match_source",
    ]
    rows_out: list[dict[str, Any]] = []
    for e in entries:
        nearest = (e.get("nearest") or [{}])[0]
        pp = e.get("parcel_primary") or {}
        addr = address_summary_fields(e.get("vicmap_addresses") or [])
        rows_out.append(
            {
                "rank": e.get("rank"),
                "site_name": e.get("site_name"),
                "lat": e.get("query", {}).get("lat"),
                "lon": e.get("query", {}).get("lon"),
                "nearest_distance_m": nearest.get("distance_m"),
                "nearest_circuit": nearest.get("circuit"),
                "parcel_spi": pp.get("parcel_spi"),
                "parcel_match_count": e.get("parcel_match_count"),
                "parcel_ambiguous": e.get("parcel_ambiguous"),
                "resolved_lga_name": e.get("resolved_lga_name"),
                "resolved_lga_source": e.get("resolved_lga_source"),
                "dapr_table_match": e.get("dapr_table_match"),
                "bushfire_overlay_hit": e.get("bushfire_overlay_hit"),
                "bushfire_bmo_hit": e.get("bushfire_bmo_hit"),
                "bushfire_bpa_hit": e.get("bushfire_bpa_hit"),
                "flood_overlay_hit": e.get("flood_overlay_hit"),
                "planning_overlay_friction": e.get("planning_overlay_friction"),
                "ezi_address": addr["ezi_address"],
                "address_match_count": addr["address_match_count"],
                "address_ambiguous": addr["address_ambiguous"],
                "address_match_source": e.get("vicmap_address_match_source") or "",
            }
        )
    return base_cols, rows_out
