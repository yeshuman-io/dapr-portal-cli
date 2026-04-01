# OpenClaw workspace boot — dapr-portal-cli

Short checks when this repository is the OpenClaw **workspace** (see [OpenClaw boot-md hook](https://docs.openclaw.ai/hooks#boot-md)).

1. **`dapr` on PATH** — Install the package (`uv sync --group dev` or `pip install -e .`) and activate the venv if you use one. Verify: `dapr --help`.
2. **Read [AGENTS.md](AGENTS.md)** — Repo layout, tests, where skills live.
3. **Vic / DAPR workflows** — See [skills/dapr-portal/SKILL.md](skills/dapr-portal/SKILL.md).

Optional **offline** smoke: `pytest` from repo root (no network).

Optional **network** smoke (downloads/cache Rosetta layers): `dapr list-csv` or `dapr scout --lat -37.8 --lon 144.9` — skip on slow or air-gapped gateway startup if your policy prefers.
