"""Short Markdown summary of a `vic address-scan` run (e.g. paste into a GitHub Gist)."""

from __future__ import annotations

import csv
import json
import shutil
import subprocess
from collections import Counter
from pathlib import Path
from typing import Any


def load_address_scan_manifest(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(f"manifest not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _truthy_cell(val: str) -> bool:
    s = (val or "").strip().lower()
    return s in ("true", "1", "yes", "y")


def aggregate_shard_csvs(shards_dir: Path) -> dict[str, Any]:
    """
    Stream all *.csv under shards_dir; return counts (for modest runs; can be slow on huge shards).
    """
    rows = 0
    industrial = 0
    shortlist = 0
    lga_counts: Counter[str] = Counter()
    if not shards_dir.is_dir():
        return {
            "rows": 0,
            "industrial": 0,
            "shortlist": 0,
            "lga_top": [],
            "shards_read": 0,
        }
    n_shards = 0
    for csv_path in sorted(shards_dir.glob("*.csv")):
        n_shards += 1
        with csv_path.open(newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows += 1
                if _truthy_cell(str(row.get("in_industrial", ""))):
                    industrial += 1
                if _truthy_cell(str(row.get("shortlist", ""))):
                    shortlist += 1
                lga = (row.get("resolved_lga_name") or "").strip()
                if lga:
                    lga_counts[lga] += 1
    top = lga_counts.most_common(15)
    return {
        "rows": rows,
        "industrial": industrial,
        "shortlist": shortlist,
        "lga_top": top,
        "shards_read": n_shards,
    }


def build_address_scan_gist_markdown(
    manifest: dict[str, Any],
    *,
    run_root: Path,
    aggregate: dict[str, Any] | None = None,
) -> str:
    rid = manifest.get("run_id", run_root.name)
    disc = (manifest.get("disclaimer") or "").strip()
    tiles_done = manifest.get("tiles_completed") or []
    tiles_total = manifest.get("tiles_total")
    addr_w = manifest.get("addresses_written")
    ind_m = manifest.get("in_industrial_count")
    bbox = manifest.get("outer_bbox")
    tlon = manifest.get("tile_step_lon")
    tlat = manifest.get("tile_step_lat")
    land = manifest.get("land_source")

    lines: list[str] = [
        "# Victoria address scan — gist report",
        "",
        f"**Run ID:** `{rid}`",
        f"**Run path:** `{run_root.resolve()}`",
        "",
    ]
    if disc:
        lines.extend(["## Disclaimer", "", disc, ""])

    lines.append("## Totals (manifest)")
    lines.append("")
    if tiles_total is not None:
        lines.append(f"- **Tiles completed:** {len(tiles_done)} / {tiles_total}")
    else:
        lines.append(f"- **Tiles completed:** {len(tiles_done)}")
    if addr_w is not None:
        lines.append(f"- **Addresses written:** {addr_w}")
    if ind_m is not None and addr_w is not None and addr_w > 0:
        pct = 100.0 * float(ind_m) / float(addr_w)
        lines.append(
            f"- **In industrial (tagged):** {ind_m} ({pct:.1f}% of rows in manifest)"
        )
    elif ind_m is not None:
        lines.append(f"- **In industrial (tagged):** {ind_m}")
    lines.append("")

    lines.append("## Configuration")
    lines.append("")
    if bbox is not None:
        lines.append(f"- **Outer bbox (WGS84):** `{bbox}`")
    if tlon is not None and tlat is not None:
        lines.append(f"- **Tile step (lon, lat deg):** `{tlon}`, `{tlat}`")
    if land is not None:
        lines.append(f"- **Land source:** `{land}`")
    lines.append("")

    if aggregate is not None:
        lines.extend(
            [
                "## Totals (from shard CSVs)",
                "",
                f"- **Shards read:** {aggregate['shards_read']}",
                f"- **Row count:** {aggregate['rows']}",
                f"- **In industrial:** {aggregate['industrial']}",
                f"- **Shortlist:** {aggregate['shortlist']}",
                "",
            ]
        )
        top = aggregate.get("lga_top") or []
        if top:
            lines.append("### Top LGAs by row count")
            lines.append("")
            lines.append("| LGA | Rows |")
            lines.append("|-----|------|")
            for name, c in top:
                lines.append(f"| {name} | {c} |")
            lines.append("")

    lines.extend(
        [
            "## Completed tile IDs",
            "",
            "```",
            *(tiles_done if tiles_done else ["(none yet)"]),
            "```",
            "",
            "---",
            "",
            "*Generated for paste into a GitHub Gist or ticket; not a planning or grid decision.*",
            "",
        ]
    )
    return "\n".join(lines)


def write_gist_for_run(
    out_dir: Path,
    run_id: str,
    *,
    aggregate_shards: bool = False,
) -> str:
    run_root = out_dir / run_id
    mp = run_root / "manifest.json"
    manifest = load_address_scan_manifest(mp)
    agg = None
    if aggregate_shards:
        agg = aggregate_shard_csvs(run_root / "shards")
    return build_address_scan_gist_markdown(manifest, run_root=run_root, aggregate=agg)


def create_gist_with_gh(
    body: str,
    *,
    filename: str = "vic-address-scan-gist.md",
    description: str | None = None,
    public: bool = False,
    open_web: bool = False,
) -> str:
    """
    Create a GitHub Gist from markdown via ``gh gist create`` (stdin).

    Requires the GitHub CLI (``gh``) and a logged-in account (``gh auth login``).

    Returns the gist URL printed by ``gh`` (single line).
    """
    if not shutil.which("gh"):
        raise RuntimeError(
            "GitHub CLI not found (expected `gh` on PATH). Install: https://cli.github.com/"
        )
    cmd: list[str] = ["gh", "gist", "create", "-", "-f", filename]
    if description:
        cmd.extend(["-d", description])
    if public:
        cmd.append("--public")
    if open_web:
        cmd.append("--web")
    proc = subprocess.run(
        cmd,
        input=body,
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"gh gist create failed ({proc.returncode}): {err}")
    url = proc.stdout.strip()
    if not url:
        raise RuntimeError("gh gist create produced no URL (empty stdout)")
    return url
