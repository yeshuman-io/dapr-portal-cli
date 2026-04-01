"""Resumable Victoria address scan: all Vicmap Address points per tile + industrial tag + DAPR + overlays."""

from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

import httpx

from dapr_portal.dapr_tables import join_lga_rows
from dapr_portal.industrial_geometry import IndustrialTagIndex, build_industrial_tag_index
from dapr_portal.scout import (
    LineRecord,
    build_line_index,
    lines_within,
    nearest_lines,
    records_to_jsonable,
)
from dapr_portal.vic_admin import lga_name_for_code
from dapr_portal.vic_planning_overlays import query_bushfire_hits, query_floodway_hit
from dapr_portal.vic_tiling import VIC_STATE_BBOX, iter_bbox_tiles
from dapr_portal.vicmap_address import DEFAULT_ADDRESS_OUT_FIELDS, VICMAP_ADDRESS_FEATURE_LAYER
from dapr_portal.vicmap_address_bulk import (
    feature_point_lonlat,
    feature_properties_dict,
    fetch_address_page_in_bbox,
)

ADDRESS_SCAN_DISCLAIMER = (
    "Vicmap Address scan (CC BY): every point in tile bbox with industrial tagging from open planning/UDP "
    "polygons. DAPR distances are indicative only. No MW proof or connection feasibility. "
    "See Data Vic Vicmap Address and DAPR disclaimers."
)

CSV_FIELDNAMES = [
    "tile_id",
    "ezi_address",
    "property_pfi",
    "lat",
    "lon",
    "in_industrial",
    "industrial_sources",
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
    "address_lga_code",
    "resolved_lga_name",
    "dapr_table_match",
]


@dataclass
class AddressScanConfig:
    out_dir: Path
    run_id: str
    tile_step_lon: float
    tile_step_lat: float
    outer_bbox: tuple[float, float, float, float]
    land_source: str
    zone_codes: list[str] | None
    max_polygon_features: int | None
    page_size: int
    max_addresses_per_tile: int | None
    address_layer_url: str
    address_out_fields: str
    layers: tuple[str, ...]
    top_k: int
    within_m: float | None
    within_limit: int
    shortlist_max_m: float | None
    overlay_delay_s: float
    skip_dapr: bool
    skip_overlays: bool
    vic_cache_dir: Path
    vic_cache_ttl: float | None
    vic_refresh: bool
    vic_no_cache: bool
    scout_cache_dir: Path
    scout_refresh: bool


def _checkpoint_path(out_dir: Path, run_id: str, tile_id: str) -> Path:
    return out_dir / run_id / "checkpoints" / f"{tile_id}.done"


def _progress_path(out_dir: Path, run_id: str, tile_id: str) -> Path:
    return out_dir / run_id / "progress" / f"{tile_id}.json"


def _shard_path(out_dir: Path, run_id: str, tile_id: str) -> Path:
    return out_dir / run_id / "shards" / f"{tile_id}.csv"


def _manifest_path(out_dir: Path, run_id: str) -> Path:
    return out_dir / run_id / "manifest.json"


def _load_progress(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"next_offset": 0, "skip_in_page": 0}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if "skip_in_page" not in data:
            data["skip_in_page"] = 0
        return data
    except (json.JSONDecodeError, OSError):
        return {"next_offset": 0, "skip_in_page": 0}


def _save_progress(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)


def build_address_row(
    *,
    tile_id: str,
    lon: float,
    lat: float,
    props: dict[str, Any],
    tag_index: IndustrialTagIndex,
    tree,
    attrs: list[dict],
    geoms: list,
    cfg: AddressScanConfig,
    lga_index: dict[str, list[dict[str, Any]]],
    client: httpx.Client,
) -> dict[str, Any]:
    in_ind, ind_src = tag_index.classify(lon, lat)
    ezi = (props.get("ezi_address") or "").strip()
    pfi = props.get("property_pfi") or ""
    lga_code_raw = props.get("lga_code")
    lga_name = lga_name_for_code(lga_code_raw) if lga_code_raw is not None else None
    if lga_name is None and lga_code_raw is not None:
        lga_name = str(lga_code_raw).strip() or None
    _, dapr_match = join_lga_rows(lga_index, lga_name)

    disk = not cfg.vic_no_cache
    near: list[LineRecord] = []
    n0 = None
    within_count: int | str = ""
    if not cfg.skip_dapr:
        near = nearest_lines(tree, attrs, geoms, lat, lon, k=cfg.top_k)
        n0 = near[0] if near else None
        if cfg.within_m is not None:
            wl = lines_within(
                tree, attrs, geoms, lat, lon, cfg.within_m, limit=cfg.within_limit
            )
            within_count = len(wl)

    nearest_m = float(n0.distance_m) if n0 else float("inf")
    shortlist = (
        cfg.shortlist_max_m is not None
        and nearest_m <= cfg.shortlist_max_m
        and nearest_m != float("inf")
    )

    bf = bmo = bpa = flood = False
    if not cfg.skip_overlays:
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

    return {
        "tile_id": tile_id,
        "ezi_address": ezi,
        "property_pfi": pfi,
        "lat": lat,
        "lon": lon,
        "in_industrial": in_ind,
        "industrial_sources": ind_src,
        "nearest_distance_m": "" if n0 is None else round(nearest_m, 2),
        "nearest_circuit": n0.circuit if n0 else "",
        "nearest_layer": n0.layer if n0 else "",
        "nearest_line_type": n0.line_type if n0 else "",
        "nearest_top_json": json.dumps(records_to_jsonable(near), separators=(",", ":"))
        if near
        else "[]",
        "within_count": within_count,
        "shortlist": shortlist,
        "bushfire_overlay_hit": bf,
        "bushfire_bmo_hit": bmo,
        "bushfire_bpa_hit": bpa,
        "flood_overlay_hit": flood,
        "planning_overlay_friction": int(bf) + int(flood),
        "address_lga_code": lga_code_raw if lga_code_raw is not None else "",
        "resolved_lga_name": lga_name or "",
        "dapr_table_match": dapr_match,
    }


def run_address_scan(
    client: httpx.Client,
    cfg: AddressScanConfig,
    *,
    lga_index: dict[str, list[dict[str, Any]]],
    log: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    log = log or (lambda _m: None)
    run_root = cfg.out_dir / cfg.run_id
    (run_root / "shards").mkdir(parents=True, exist_ok=True)
    (run_root / "checkpoints").mkdir(parents=True, exist_ok=True)
    (run_root / "progress").mkdir(parents=True, exist_ok=True)

    tiles = iter_bbox_tiles(cfg.outer_bbox, cfg.tile_step_lon, cfg.tile_step_lat)
    log(f"vic address-scan: {len(tiles)} tiles, run_id={cfg.run_id}")

    tree = attrs = geoms = None
    if not cfg.skip_dapr:
        tree, attrs, geoms = build_line_index(
            client,
            cfg.layers,
            cache_dir=cfg.scout_cache_dir,
            refresh=cfg.scout_refresh,
        )

    manifest: dict[str, Any] = {
        "run_id": cfg.run_id,
        "outer_bbox": list(cfg.outer_bbox),
        "tile_step_lon": cfg.tile_step_lon,
        "tile_step_lat": cfg.tile_step_lat,
        "land_source": cfg.land_source,
        "tiles_total": len(tiles),
        "tiles_completed": [],
        "addresses_written": 0,
        "in_industrial_count": 0,
        "disclaimer": ADDRESS_SCAN_DISCLAIMER,
        "csv_columns": CSV_FIELDNAMES,
    }
    mp = _manifest_path(cfg.out_dir, cfg.run_id)
    if mp.is_file():
        try:
            old = json.loads(mp.read_text(encoding="utf-8"))
            manifest["tiles_completed"] = list(old.get("tiles_completed") or [])
            manifest["addresses_written"] = int(old.get("addresses_written") or 0)
            manifest["in_industrial_count"] = int(old.get("in_industrial_count") or 0)
        except (json.JSONDecodeError, OSError):
            pass

    chk_dir = run_root / "checkpoints"
    completed = set(manifest["tiles_completed"])
    if chk_dir.is_dir():
        for p in chk_dir.glob("*.done"):
            completed.add(p.stem)
        manifest["tiles_completed"] = sorted(completed)

    vic_ttl = None if cfg.vic_no_cache else cfg.vic_cache_ttl
    disk = not cfg.vic_no_cache

    for tile_id, bbox in tiles:
        cpath = _checkpoint_path(cfg.out_dir, cfg.run_id, tile_id)
        if cpath.is_file():
            log(f"skip tile {tile_id} (checkpoint)")
            continue

        spath = _shard_path(cfg.out_dir, cfg.run_id, tile_id)
        total_tile_rows = 0
        if spath.is_file() and spath.stat().st_size > 0:
            with spath.open(encoding="utf-8") as sf:
                total_tile_rows = max(0, sum(1 for _ in sf) - 1)
        if (
            cfg.max_addresses_per_tile is not None
            and total_tile_rows >= cfg.max_addresses_per_tile
        ):
            log(
                f"tile {tile_id}: shard already has {total_tile_rows} rows "
                f"(>= max_addresses_per_tile); marking checkpoint"
            )
            cpath.write_text("ok\n", encoding="utf-8")
            completed.add(tile_id)
            manifest["tiles_completed"] = sorted(completed)
            mp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            continue

        tag_index = build_industrial_tag_index(
            client,
            bbox,
            land_source=cfg.land_source,
            zone_codes=cfg.zone_codes,
            max_polygon_features=cfg.max_polygon_features,
            cache_dir=cfg.vic_cache_dir,
            cache_ttl=vic_ttl,
            refresh=cfg.vic_refresh,
            disk_cache=disk,
        )

        prog = _load_progress(_progress_path(cfg.out_dir, cfg.run_id, tile_id))
        offset = int(prog.get("next_offset") or 0)
        skip = int(prog.get("skip_in_page") or 0)

        total_industrial = 0
        hit_cap = False

        while True:
            feats = fetch_address_page_in_bbox(
                client,
                bbox,
                offset,
                layer_url=cfg.address_layer_url,
                out_fields=cfg.address_out_fields,
                page_size=cfg.page_size,
                cache_dir=cfg.vic_cache_dir,
                cache_ttl_seconds=vic_ttl,
                refresh=cfg.vic_refresh,
                disk_cache=disk,
            )
            if len(feats) == 0:
                break

            if skip >= len(feats):
                offset += len(feats)
                skip = 0
                _save_progress(
                    _progress_path(cfg.out_dir, cfg.run_id, tile_id),
                    {"next_offset": offset, "skip_in_page": 0},
                )
                mp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
                if len(feats) < cfg.page_size:
                    break
                continue

            append_shard = spath.is_file() and spath.stat().st_size > 0
            slice_feats = feats[skip:]
            with spath.open("a" if append_shard else "w", newline="", encoding="utf-8") as sf:
                w = csv.DictWriter(sf, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
                if not append_shard:
                    w.writeheader()
                for i, feat in enumerate(slice_feats):
                    if cfg.max_addresses_per_tile is not None and total_tile_rows >= cfg.max_addresses_per_tile:
                        _save_progress(
                            _progress_path(cfg.out_dir, cfg.run_id, tile_id),
                            {"next_offset": offset, "skip_in_page": skip + i},
                        )
                        mp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
                        hit_cap = True
                        break
                    ll = feature_point_lonlat(feat)
                    if ll is None:
                        continue
                    lon, lat = ll
                    props = feature_properties_dict(feat)
                    row = build_address_row(
                        tile_id=tile_id,
                        lon=lon,
                        lat=lat,
                        props=props,
                        tag_index=tag_index,
                        tree=tree,
                        attrs=attrs or [],
                        geoms=geoms or [],
                        cfg=cfg,
                        lga_index=lga_index,
                        client=client,
                    )
                    w.writerow(row)
                    total_tile_rows += 1
                    manifest["addresses_written"] += 1
                    if row["in_industrial"]:
                        manifest["in_industrial_count"] += 1
                        total_industrial += 1
                    if cfg.max_addresses_per_tile is not None and total_tile_rows >= cfg.max_addresses_per_tile:
                        _save_progress(
                            _progress_path(cfg.out_dir, cfg.run_id, tile_id),
                            {"next_offset": offset, "skip_in_page": skip + i + 1},
                        )
                        mp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
                        hit_cap = True
                        break

            if hit_cap:
                break

            _save_progress(
                _progress_path(cfg.out_dir, cfg.run_id, tile_id),
                {"next_offset": offset + len(feats), "skip_in_page": 0},
            )
            offset += len(feats)
            skip = 0
            mp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            if len(feats) < cfg.page_size:
                break

        if not hit_cap:
            cpath.write_text("ok\n", encoding="utf-8")
            completed.add(tile_id)
            manifest["tiles_completed"] = sorted(completed)
            mp.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        log(
            f"tile {tile_id}: addresses in shard ~{total_tile_rows} (industrial hits ~{total_industrial})"
            + (" [capped; no checkpoint — resume to continue]" if hit_cap else "")
        )

    return manifest
