# Victoria address-candidate scan (`dapr vic address-scan`)

VIC-only tool that **emits every Vicmap Address point** returned inside each WGS84 tile (paginated FeatureServer query), adds **`in_industrial`** and **`industrial_sources`** from open planning-zone and UDP industrial polygons (point-in-polygon), then optionally scores **DAPR line proximity**, **bushfire / floodway overlays**, and joins the **DAPR LGA summary** CSV by resolved LGA from address attributes.

This is **not** a proof of available capacity, connection feasibility, or planning approval. Distances and overlays are indicative; respect Data Vic, spatial.vic.gov.au, and DAPR portal disclaimers.

## Scale and runtime

A full-state run is **many tiles × many address pages × (line query + two overlay calls)** per address when DAPR and overlays are enabled. Mitigate with:

- Smaller **`--bbox`** or coarser **`--tile-step`** for experiments
- **`--max-addresses-per-tile`** to cap work per tile (see resume below)
- **`--overlay-delay`** to reduce overlay rate limiting
- **`--skip-dapr`** / **`--skip-overlays`** for dry runs
- Vic and scout **disk caches** (`--vic-cache-dir`, `--scout-cache-dir`; disable with `--vic-no-cache`)

## CLI

```bash
dapr vic address-scan --out-dir ./runs \
  --tile-step 1.0 \
  --land-source both \
  --overlay-delay 0.05
```

Common flags align with `dapr dc-screen` where applicable: `--bbox`, `--run-id`, `--zone-codes`, `--shortlist-max-m`, `--within-m`, Rosetta `--layers`, portal **`--lga-summary-path`**, **`--dapr-csv-refresh`**.

## Outputs

Under `OUT_DIR/RUN_ID/`:

| Path | Purpose |
|------|---------|
| `shards/{tile_id}.csv` | One CSV per tile |
| `progress/{tile_id}.json` | `next_offset` and `skip_in_page` for Vicmap pagination resume |
| `checkpoints/{tile_id}.done` | Written only when the tile is **fully** paginated (not when stopped by `--max-addresses-per-tile`) |
| `manifest.json` | Run metadata, tile list, `addresses_written`, `in_industrial_count`, disclaimer |

## Resume semantics

- Reuse the same **`--out-dir`** and **`--run-id`**.
- Tiles with a **checkpoint** (`.done`) are skipped.
- **Mid-tile**: `progress/*.json` stores the next `resultOffset` and, if you stopped on a page boundary cap, **`skip_in_page`** so the same page is not skipped incorrectly.
- **`--max-addresses-per-tile`**: when the cap is hit, **no** checkpoint is written; re-run with the same run id to append more rows until you remove the cap or finish the tile manually.
- Manifest counters are loaded from disk and incremented for **new** rows only in that session (same run id).

## CSV columns (minimum set)

| Column | Description |
|--------|-------------|
| `tile_id` | Tile key from the tiling iterator |
| `ezi_address`, `property_pfi` | Vicmap Address fields |
| `lat`, `lon` | Point (WGS84) |
| `in_industrial` | Any industrial hit from enabled sources |
| `industrial_sources` | `none`, `zones`, `udp`, or `both` |
| `nearest_distance_m`, `nearest_circuit`, `nearest_layer`, `nearest_line_type`, `nearest_top_json`, `within_count` | From Rosetta line index (empty / `[]` if `--skip-dapr`) |
| `shortlist` | `True` if `--shortlist-max-m` set and nearest within threshold |
| `bushfire_*`, `flood_overlay_hit`, `planning_overlay_friction` | Overlay flags (defaults false if `--skip-overlays`) |
| `address_lga_code`, `resolved_lga_name` | From Vicmap + `lga_name_for_code` |
| `dapr_table_match` | Whether LGA matched rows in the decrypted portal LGA summary |

## Non-goals (v1)

- Multi-state / jurisdiction plugins (future `jurisdictions/` registry).
- Parquet output.
- Proving MW, hosting suitability, or network connection.

## Gist-style summary

After a run, emit a short Markdown report (for a GitHub Gist or ticket):

```bash
dapr vic gist-report --out-dir ./runs --run-id 20260115-120000 -o scan-gist.md
```

Create the gist on GitHub with the [GitHub CLI](https://cli.github.com/) (after `gh auth login`):

```bash
dapr vic gist-report --out-dir ./runs --run-id 20260115-120000 --gh-create
```

That prints the gist URL to stdout. Combine with `-o scan-gist.md` to keep a local copy. Optional: `--gh-public`, `--gh-desc "…"`, `--gh-filename name.md`, `--gh-web`.

Add `--aggregate-shards` to scan every shard CSV for exact row counts, shortlist totals, and top LGAs (can be slow on very large runs). Without it, the report uses **`manifest.json` only** (fast).

## Related

- `docs/dc_screening.md` — tiled industrial **seed** screening (centroids / candidates).
- `dapr dc-screen` — shard CSVs from zone/UDP **seeds**, not full address enumeration.
