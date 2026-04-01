"""Batch datacenter-oriented screening: tiled industrial seeds + DAPR proximity + overlays + optional enrich."""

from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import httpx

from dapr_portal.candidates import (
    collect_industrial_zone_rows,
    collect_udp_industrial_rows,
)
from dapr_portal.dapr_tables import join_lga_rows
from dapr_portal.scout import (
    LineRecord,
    build_line_index,
    lines_within,
    nearest_lines,
    records_to_jsonable,
)
from dapr_portal.vic_admin import (
    lga_name_for_code,
    lga_name_from_planning_candidate,
)
from dapr_portal.vic_planning_overlays import query_bushfire_hits, query_floodway_hit
from dapr_portal.vic_tiling import VIC_STATE_BBOX, candidate_dedupe_key, iter_bbox_tiles
from dapr_portal.vicmap_address import address_summary_fields, query_addresses_for_site
from dapr_portal.vicmap_property import query_parcels_at_point

DC_SCREEN_DISCLAIMER = (
    "Datacenter screening uses open DAPR map lines and planning data only. "
    "It does not prove available MW, substation headroom, or connection feasibility. "
    "Engage the DNSP for capacity. Not legal planning or bushfire/flood determination."
)


@dataclass
class DcScreenConfig:
    out_dir: Path
    run_id: str
    tile_step_lon: float
    tile_step_lat: float
    outer_bbox: tuple[float, float, float, float]
    land_source: str  # zones | udp | both
    zone_codes: list[str] | None
    max_features_per_tile: int | None
    layers: tuple[str, ...]
    top_k: int
    within_m: float | None
    within_limit: int
    shortlist_max_m: float | None
    with_parcels: bool
    address_mode: str  # none | all | shortlist
    overlay_delay_s: float
    parcel_layer_url: str
    parcel_out_fields: str
    max_parcel_matches: int
    address_layer_url: str
    address_out_fields: str
    max_address_matches: int
    max_address_point_matches: int
    address_point_buffer_m: float
    no_address_point_fallback: bool
    vic_cache_dir: Path
    vic_cache_ttl: float | None
    vic_refresh: bool
    vic_no_cache: bool
    scout_cache_dir: Path
    scout_refresh: bool
    base_url: str
    lga_summary_path: str
    dapr_csv_refresh: bool


def merge_seed_rows(
    zone_rows: list[dict[str, Any]],
    udp_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Dedupe by rounded lat/lon; track land_sources zones / udp / zones+udp."""
    merged: dict[tuple[float, float], dict[str, Any]] = {}
    for src, rows in (("zones", zone_rows), ("udp", udp_rows)):
        for r in rows:
            k = candidate_dedupe_key(r["lat"], r["lon"])
            props = dict(r.get("properties") or {})
            if k not in merged:
                merged[k] = {
                    "name": r["name"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "_sources": {src},
                    "candidate_attributes": props,
                }
            else:
                merged[k]["_sources"].add(src)
                if props:
                    merged[k]["candidate_attributes"].update(props)
    out: list[dict[str, Any]] = []
    for m in merged.values():
        srcs = "+".join(sorted(m.pop("_sources")))
        m["land_sources"] = srcs
        out.append(m)
    return out


def _resolve_lga_name(
    parcels: list[dict[str, Any]], candidate_attributes: dict[str, Any]
) -> tuple[str | None, str | None]:
    if parcels:
        code = parcels[0].get("parcel_lga_code")
        name = lga_name_for_code(code)
        if name:
            return name, "parcel_lga_code"
        raw = parcels[0].get("parcel_lga_code")
        if raw is not None:
            return str(raw), "parcel_lga_code_unmapped"
    lga = candidate_attributes.get("LGA") or candidate_attributes.get("lga_name")
    if lga:
        return lga_name_from_planning_candidate(str(lga)), "planning_or_udp"
    return None, None


def _should_fetch_addresses(
    cfg: DcScreenConfig,
    nearest_distance_m: float,
) -> bool:
    if cfg.address_mode == "none":
        return False
    if cfg.address_mode == "all":
        return True
    if cfg.address_mode == "shortlist":
        if cfg.shortlist_max_m is None:
            return False
        return nearest_distance_m <= cfg.shortlist_max_m
    return False


def score_candidate(
    client: httpx.Client,
    cfg: DcScreenConfig,
    lga_index: dict[str, list[dict[str, Any]]],
    tree,
    attrs: list[dict],
    geoms: list,
    *,
    tile_id: str,
    name: str,
    lat: float,
    lon: float,
    land_sources: str,
    candidate_attributes: dict[str, Any],
) -> dict[str, Any]:
    near: list[LineRecord] = nearest_lines(tree, attrs, geoms, lat, lon, k=cfg.top_k)
    n0 = near[0] if near else None
    nearest_m = float(n0.distance_m) if n0 else float("inf")
    within_list = None
    within_count = ""
    if cfg.within_m is not None:
        within_list = lines_within(
            tree, attrs, geoms, lat, lon, cfg.within_m, limit=cfg.within_limit
        )
        within_count = len(within_list)

    parcels: list[dict[str, Any]] = []
    if cfg.with_parcels:
        parcels = query_parcels_at_point(
            client,
            lon,
            lat,
            layer_url=cfg.parcel_layer_url,
            out_fields=cfg.parcel_out_fields,
            max_records=cfg.max_parcel_matches,
            cache_dir=cfg.vic_cache_dir,
            cache_ttl_seconds=cfg.vic_cache_ttl,
            refresh=cfg.vic_refresh,
            disk_cache=not cfg.vic_no_cache,
        )

    lga_name, lga_src = _resolve_lga_name(parcels, candidate_attributes)
    _, dapr_match = join_lga_rows(lga_index, lga_name)

    disk = not cfg.vic_no_cache
    bf, bmo, bpa = query_bushfire_hits(
        client,
        lon,
        lat,
        cache_dir=cfg.vic_cache_dir,
        cache_ttl_seconds=cfg.vic_cache_ttl,
        refresh=cfg.vic_refresh,
        disk_cache=disk,
    )
    flood = query_floodway_hit(
        client,
        lon,
        lat,
        cache_dir=cfg.vic_cache_dir,
        cache_ttl_seconds=cfg.vic_cache_ttl,
        refresh=cfg.vic_refresh,
        disk_cache=disk,
    )
    if cfg.overlay_delay_s > 0:
        time.sleep(cfg.overlay_delay_s)

    pfi = parcels[0].get("parcel_pfi") if parcels else None
    addrs: list[dict[str, Any]] = []
    addr_src = ""
    if _should_fetch_addresses(cfg, nearest_m):
        addrs, addr_src = query_addresses_for_site(
            client,
            lon,
            lat,
            pfi,
            layer_url=cfg.address_layer_url,
            out_fields=cfg.address_out_fields,
            max_pfi_matches=cfg.max_address_matches,
            point_fallback=not cfg.no_address_point_fallback,
            max_point_matches=cfg.max_address_point_matches,
            point_buffer_meters=cfg.address_point_buffer_m,
            cache_dir=cfg.vic_cache_dir,
            cache_ttl_seconds=cfg.vic_cache_ttl,
            refresh=cfg.vic_refresh,
            disk_cache=disk,
        )
    addr_fields = address_summary_fields(addrs)

    shortlist = (
        cfg.shortlist_max_m is not None and nearest_m <= cfg.shortlist_max_m
        if nearest_m != float("inf")
        else False
    )

    pp = parcels[0] if parcels else {}
    row: dict[str, Any] = {
        "tile_id": tile_id,
        "land_sources": land_sources,
        "site_name": name,
        "lat": lat,
        "lon": lon,
        "nearest_distance_m": "" if n0 is None else round(nearest_m, 2),
        "nearest_circuit": n0.circuit if n0 else "",
        "nearest_layer": n0.layer if n0 else "",
        "nearest_line_type": n0.line_type if n0 else "",
        "nearest_top_json": json.dumps(records_to_jsonable(near), separators=(",", ":")),
        "within_count": within_count,
        "shortlist": shortlist,
        "bushfire_overlay_hit": bf,
        "bushfire_bmo_hit": bmo,
        "bushfire_bpa_hit": bpa,
        "flood_overlay_hit": flood,
        "planning_overlay_friction": int(bf) + int(flood),
        "parcel_match_count": len(parcels),
        "parcel_spi": pp.get("parcel_spi", ""),
        "parcel_pfi": pp.get("parcel_pfi", ""),
        "resolved_lga_name": lga_name or "",
        "resolved_lga_source": lga_src or "",
        "dapr_table_match": dapr_match,
        "ezi_address": addr_fields["ezi_address"],
        "address_match_count": addr_fields["address_match_count"],
        "address_ambiguous": addr_fields["address_ambiguous"],
        "address_match_source": addr_src,
    }
    return row


CSV_FIELDNAMES = [
    "tile_id",
    "land_sources",
    "site_name",
    "lat",
    "lon",
    "nearest_distance_m",
    "nearest_circuit",
    "nearest_layer",
    "nearest_line_type",
    "nearest_top_json",
    "within_count",
    "shortlist",
    "bushfire_overlay_hit",
    "bushfire_bmo_hit",
    "bushfire_bpa_hit",
    "flood_overlay_hit",
    "planning_overlay_friction",
    "parcel_match_count",
    "parcel_spi",
    "parcel_pfi",
    "resolved_lga_name",
    "resolved_lga_source",
    "dapr_table_match",
    "ezi_address",
    "address_match_count",
    "address_ambiguous",
    "address_match_source",
]


def _checkpoint_path(out_dir: Path, run_id: str, tile_id: str) -> Path:
    return out_dir / run_id / "checkpoints" / f"{tile_id}.done"


def _shard_path(out_dir: Path, run_id: str, tile_id: str) -> Path:
    return out_dir / run_id / "shards" / f"{tile_id}.csv"


def _manifest_path(out_dir: Path, run_id: str) -> Path:
    return out_dir / run_id / "manifest.json"


def run_dc_screen(
    client: httpx.Client,
    cfg: DcScreenConfig,
    *,
    load_dapr_index: Callable[[], dict[str, list[dict[str, Any]]]],
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """
    Iterate tiles, write shard CSVs + checkpoints, update manifest.
    ``load_dapr_index`` loads LGA summary index once (portal session + CSV inside closure).
    """
    log = log or (lambda _m: None)
    run_root = cfg.out_dir / cfg.run_id
    (run_root / "shards").mkdir(parents=True, exist_ok=True)
    (run_root / "checkpoints").mkdir(parents=True, exist_ok=True)

    tiles = iter_bbox_tiles(cfg.outer_bbox, cfg.tile_step_lon, cfg.tile_step_lat)
    log(f"dc-screen: {len(tiles)} tiles, run_id={cfg.run_id}")

    lga_index = load_dapr_index()
    tree, attrs, geoms = build_line_index(
        client,
        cfg.layers,
        cache_dir=cfg.scout_cache_dir,
        refresh=cfg.scout_refresh,
    )

    vic_ttl = None if cfg.vic_no_cache else cfg.vic_cache_ttl
    disk = not cfg.vic_no_cache

    manifest: dict[str, Any] = {
        "run_id": cfg.run_id,
        "outer_bbox": list(cfg.outer_bbox),
        "tile_step_lon": cfg.tile_step_lon,
        "tile_step_lat": cfg.tile_step_lat,
        "land_source": cfg.land_source,
        "tiles_total": len(tiles),
        "tiles_completed": [],
        "candidates_scored": 0,
        "disclaimer": DC_SCREEN_DISCLAIMER,
        "csv_columns": CSV_FIELDNAMES,
    }
    mpath = _manifest_path(cfg.out_dir, cfg.run_id)
    if mpath.is_file():
        try:
            old = json.loads(mpath.read_text())
            manifest["tiles_completed"] = list(old.get("tiles_completed") or [])
            manifest["candidates_scored"] = int(old.get("candidates_scored") or 0)
        except (json.JSONDecodeError, OSError):
            pass

    chk_dir = run_root / "checkpoints"
    completed: set[str] = set(manifest["tiles_completed"])
    if chk_dir.is_dir():
        for p in chk_dir.glob("*.done"):
            completed.add(p.stem)
        manifest["tiles_completed"] = sorted(completed)

    for tile_id, bbox in tiles:
        cpath = _checkpoint_path(cfg.out_dir, cfg.run_id, tile_id)
        if cpath.is_file():
            log(f"skip tile {tile_id} (checkpoint)")
            continue

        zone_rows: list[dict[str, Any]] = []
        udp_rows: list[dict[str, Any]] = []
        if cfg.land_source in ("zones", "both"):
            zone_rows = collect_industrial_zone_rows(
                client,
                bbox,
                zone_codes=cfg.zone_codes,
                layer_ids=None,
                max_features=cfg.max_features_per_tile,
                cache_dir=cfg.vic_cache_dir,
                cache_ttl=vic_ttl,
                refresh=cfg.vic_refresh,
                disk_cache=disk,
            )
        if cfg.land_source in ("udp", "both"):
            udp_rows = collect_udp_industrial_rows(
                client,
                bbox,
                cql_filter=None,
                max_features=cfg.max_features_per_tile,
                cache_dir=cfg.vic_cache_dir,
                cache_ttl=vic_ttl,
                refresh=cfg.vic_refresh,
                disk_cache=disk,
            )

        seeds = (
            merge_seed_rows(zone_rows, udp_rows)
            if cfg.land_source == "both"
            else [
                {
                    "name": r["name"],
                    "lat": r["lat"],
                    "lon": r["lon"],
                    "land_sources": "zones" if cfg.land_source == "zones" else "udp",
                    "candidate_attributes": dict(r.get("properties") or {}),
                }
                for r in (zone_rows if cfg.land_source == "zones" else udp_rows)
            ]
        )

        spath = _shard_path(cfg.out_dir, cfg.run_id, tile_id)
        with spath.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
            w.writeheader()
            for s in seeds:
                row = score_candidate(
                    client,
                    cfg,
                    lga_index,
                    tree,
                    attrs,
                    geoms,
                    tile_id=tile_id,
                    name=s["name"],
                    lat=s["lat"],
                    lon=s["lon"],
                    land_sources=s["land_sources"],
                    candidate_attributes=s["candidate_attributes"],
                )
                w.writerow(row)
                manifest["candidates_scored"] += 1

        cpath.write_text("ok\n", encoding="utf-8")
        completed.add(tile_id)
        manifest["tiles_completed"] = sorted(completed)
        _manifest_path(cfg.out_dir, cfg.run_id).write_text(
            json.dumps(manifest, indent=2), encoding="utf-8"
        )
        log(f"tile {tile_id}: {len(seeds)} candidates -> {spath.name}")

    return manifest
