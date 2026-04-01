---
name: dapr-portal
description: >-
  Runs DAPR portal CLI workflows and Victorian site screening using dapr scout, screen, report,
  enrich-parcels, dc-screen, and vic address-scan. Use when the user asks about DAPR CSVs,
  22kV proximity, Vicmap parcels or addresses, industrial candidates, planning overlays,
  LGA joins, or gist-style summaries of address-scan runs.
---

# dapr-portal workspace skill

## Context

This workspace is **dapr-portal-cli**: command `dapr` ([README.md](../../README.md)). Data is **screening-only** — not grid commitments, title, or permits.

## Essential commands

```bash
dapr --help
dapr list-csv
dapr scout --lat LAT --lon LON -o out.json
dapr scout --sites examples/vic_candidates.csv -o scout.json
dapr report --from-json scout.json --format json -o report.json
dapr enrich-parcels --sites examples/vic_candidates.csv --with-addresses -o enrich.json
dapr vic address-scan --out-dir ./runs   # large; see docs
dapr vic gist-report --out-dir ./runs --run-id RUN_ID -o gist.md
```

`gh` is **optional** — only for `dapr vic gist-report --gh-create`.

## Full report with Vicmap addresses

`dapr report` uses `vicmap_addresses` when present. **`dapr scout` alone does not fetch addresses.** Chain:

1. `dapr scout --sites SITES.csv -o scout.json`
2. `dapr enrich-parcels --sites SITES.csv --format json --with-addresses -o enrich.json`
3. Merge JSON: for each scout entry, attach `parcels`, `vicmap_addresses`, and `vicmap_address_match_source` from the enrich row with the same `query.lat` / `query.lon` (and `site_name` if needed). Example:

```python
import json
from pathlib import Path

scout = json.loads(Path("scout.json").read_text())
enrich = json.loads(Path("enrich.json").read_text())

def key(e):
    q = e["query"]
    return (round(float(q["lat"]), 5), round(float(q["lon"]), 5))

em = {key(e): e for e in enrich}
for s in scout:
    e = em.get(key(s))
    if not e:
        continue
    s["parcels"] = e.get("parcels") or []
    s["vicmap_parcels"] = e.get("parcels") or []
    s["vicmap_addresses"] = e.get("vicmap_addresses") or []
    if e.get("vicmap_address_match_source"):
        s["vicmap_address_match_source"] = e["vicmap_address_match_source"]

Path("merged.json").write_text(json.dumps(scout, indent=2))
```

4. `dapr report --from-json merged.json --format csv -o report.csv`

## Docs in repo

- [dapr_portal/docs/dc_screening.md](../../dapr_portal/docs/dc_screening.md)
- [dapr_portal/docs/vic_address_scan.md](../../dapr_portal/docs/vic_address_scan.md)

## Tests

```bash
uv run pytest
```

Run before claiming CLI behavior changed.
