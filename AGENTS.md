# Agent orientation — dapr-portal-cli

## What this repo is

A **Python package** (`dapr_portal/`) exposing the **`dapr`** command-line tool for DAPR portal assets and Victorian geospatial screening. It is **not** a Medusa / e-commerce app; ignore Medusa-specific workspace rules when working here.

## Layout

| Path | Purpose |
|------|--------|
| `dapr_portal/` | Library + CLI implementation |
| `dapr_portal/docs/` | Long-form docs (dc screening, Vic address scan, transmission data pointers) |
| `dapr_portal/data/` | Packaged data (e.g. `vic_lga_codes.csv`) |
| `tests/` | `pytest` suite (mocked HTTP; no network required) |
| `skills/` | **OpenClaw workspace skills** (AgentSkills-style `SKILL.md`) |
| `examples/` | Sample inputs (e.g. `vic_candidates.csv`) |

## Before changing behavior

1. Run **`dapr <subcommand> --help`** for the area you touch.
2. Run **`uv run pytest`** (or `pytest` from an env with the package installed).
3. Prefer small, focused diffs; match existing style in [`dapr_portal/cli.py`](dapr_portal/cli.py) and modules.

## Playbooks

- **`dapr list-layers`** / **`dapr config`** — discover Rosetta `.txt` line layers embedded in the portal (for **`--layers`**); JSON includes `rosetta_map_layer_hints` for map-only ids.
- **`skills/dapr-portal/SKILL.md`** — screening workflows, CLI chains, merge step for scout + enrich + report.
- **[README.md](README.md)** — install, disclaimers, OpenClaw checklist.
- **[BOOT.md](BOOT.md)** — short bootstrap for OpenClaw gateway startup.

## Cursor vs OpenClaw skills

- **OpenClaw** loads **`./skills/<name>/SKILL.md`** when this repo is the workspace.
- **Cursor** project skills normally live under **`.cursor/skills/`**. To avoid duplicating content, symlink:

  ```bash
  mkdir -p .cursor/skills
  ln -s ../../skills/dapr-portal .cursor/skills/dapr-portal
  ```

  On systems where symlinks are awkward, copy the folder and accept drift risk, or see **`.cursor/skills/README.md`**.

## Tests

```bash
uv sync --group dev
uv run pytest
```

All tests should pass offline; network calls are mocked.
