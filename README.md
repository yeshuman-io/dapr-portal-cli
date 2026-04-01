# dapr-portal-cli

Scriptable access to the **CitiPower / Powercor DAPR** portal (tabular CSVs, static files, map layers) plus **Victorian spatial screening** (Vicmap, planning overlays, industrial seeds) via the **`dapr`** CLI.

- **Python:** 3.10+
- **Entry point:** `dapr` → [`dapr_portal.cli:main`](dapr_portal/cli.py)

## Install

From the repo root:

```bash
uv sync --group dev    # recommended: venv + runtime + pytest
# or
uv pip install -e .
uv pip install pytest
```

Then:

```bash
uv run dapr --help
uv run pytest
```

## Documentation

| Doc | Topic |
|-----|--------|
| [dc_screening.md](dapr_portal/docs/dc_screening.md) | Tiled industrial seed screening (`dc-screen`) |
| [vic_address_scan.md](dapr_portal/docs/vic_address_scan.md) | Full Vicmap address tile scan, gist report, `gh` (optional) |

## Disclaimers

- **DAPR** map and CSV data are general information only — not as-built, not a connection offer. See [dapr.powercor.com.au](https://dapr.powercor.com.au/).
- **Data Vic / Vicmap / planning** layers are subject to their licences and disclaimers; this tool is for **screening**, not legal, planning approval, or guaranteed grid capacity.

## Optional: GitHub CLI

`gh` is **not** required. It is only used if you pass **`--gh-create`** to `dapr vic gist-report` to create a GitHub Gist from the Markdown report.

## Example sites CSV

See [examples/vic_candidates.csv](examples/vic_candidates.csv) (`name`, `lat`, `lon`) for a small VIC industrial-style point list usable with `dapr scout --sites …`.

## OpenClaw colleague checklist

1. Clone this repo and set your **OpenClaw workspace** to the **repo root** (so `./skills` and `BOOT.md` apply).
2. Install dependencies; run `dapr --help` and `pytest` (see above).
3. On gateway start, OpenClaw may run **`BOOT.md`** if the [boot-md hook](https://docs.openclaw.ai/hooks#boot-md) is enabled — read it for orientation.
4. Workspace skills live under **`skills/`** (e.g. `skills/dapr-portal/SKILL.md`).
5. Optional network smoke: `dapr scout --sites examples/vic_candidates.csv -o /tmp/scout.json` (downloads Rosetta line layers on first run).

## Licence

See [LICENSE](LICENSE). Third-party data and API terms (DAPR, Data Vic, etc.) are separate from this software.
