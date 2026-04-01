# Datacenter-oriented screening (`dapr dc-screen`)

## Purpose

`dapr dc-screen` is a **resumable batch** job for **Victoria** that:

1. Tiles an outer bounding box (default: mainland Victoria in [`vic_tiling.VIC_STATE_BBOX`](../vic_tiling.py)).
2. Collects **industrial-suitable seeds** from planning **IN zones** (MapServer) and/or **UDP industrial** WFS, with **deduplication** by rounded lat/lon when using `both`.
3. Builds **one** DAPR Rosetta **line index** per run and scores each candidate with **nearest** segments (`nearest_distance_m`, `nearest_top_json` for top-k).
4. Optionally counts lines **`within_m`** (`within_count`).
5. Queries **bushfire (BMO/BPA)** and **UFZ** overlays per candidate, with configurable **`--overlay-delay`** between candidates.
6. Loads the **DAPR LGA summary** CSV **once** and joins by resolved LGA (`dapr_table_match`).
7. Optionally queries **Vicmap Property** (`--with-parcels`) and **Vicmap Address** (`--addresses none|all|shortlist`).

## What this is not

- **No proof of available MW**, substation headroom, or connection feasibility — engage the DNSP.
- **Not** legal planning, bushfire, or flood liability advice.
- **Not** a full scan of every Victorian address; seeds are **industrial land** representations (polygon representative points), with addresses optional.

## Output layout

```
OUT_DIR/RUN_ID/
  manifest.json       # run metadata, tiles_completed, candidates_scored, csv_columns
  shards/r0c0.csv     # one CSV per tile
  checkpoints/r0c0.done
```

Resume by reusing the same `--out-dir` and `--run-id`; completed tiles are skipped when the `.done` checkpoint exists.

## Key columns (shards)

| Column | Meaning |
|--------|---------|
| `tile_id` | Tile identifier (`r{row}c{col}`) |
| `land_sources` | `zones`, `udp`, or `zones+udp` |
| `nearest_distance_m` | Distance to closest DAPR line segment (metres) |
| `nearest_top_json` | JSON array of top-k nearest segments (audit) |
| `shortlist` | `True` if `nearest_distance_m <= --shortlist-max-m` (when set) |
| `planning_overlay_friction` | Bushfire hit + UFZ hit (0–2) |
| `dapr_table_match` | LGA row found in portal LGA summary CSV |
| `ezi_address` / `address_match_source` | Vicmap Address when `--addresses` requests it |

## Example commands

```bash
# Dry small area (one degree tile over Melbourne-ish bbox), zones only, cap features
dapr dc-screen --out-dir ./runs --bbox 144.5,-38.2,145.7,-37.5 --tile-step 2 \
  --land-source zones --max-features-per-tile 50

# Full state preset (many tiles); addresses only on shortlist within 500 m of 22 kV
dapr dc-screen --out-dir ./runs --tile-step 1 --land-source both \
  --shortlist-max-m 500 --addresses shortlist

# Resume
dapr dc-screen --out-dir ./runs --run-id 20260331-120000
```

## Operational notes

- **Runtime** scales with tiles × candidates × (overlays + optional parcel/address). Use **`--overlay-delay`** to reduce rate-limit risk.
- **Caches**: reuse `~/.cache/dapr-portal-cli/vicmap` and Rosetta layer cache like other `dapr` commands (`--vic-refresh`, `--scout-refresh`).
