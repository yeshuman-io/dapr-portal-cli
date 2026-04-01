"""Gist-style Markdown report from address-scan manifest."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dapr_portal.address_gist_report import (
    aggregate_shard_csvs,
    build_address_scan_gist_markdown,
    create_gist_with_gh,
    load_address_scan_manifest,
    write_gist_for_run,
)


def test_load_and_build_gist(tmp_path: Path) -> None:
    run = tmp_path / "run1"
    (run / "shards").mkdir(parents=True)
    manifest = {
        "run_id": "run1",
        "disclaimer": "Test disclaimer.",
        "tiles_completed": ["0_0", "0_1"],
        "tiles_total": 10,
        "addresses_written": 100,
        "in_industrial_count": 25,
        "outer_bbox": [140.0, -39.0, 150.0, -34.0],
        "tile_step_lon": 1.0,
        "tile_step_lat": 1.0,
        "land_source": "both",
    }
    (run / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    m = load_address_scan_manifest(run / "manifest.json")
    md = build_address_scan_gist_markdown(m, run_root=run, aggregate=None)
    assert "Victoria address scan" in md
    assert "run1" in md
    assert "Test disclaimer." in md
    assert "100" in md
    assert "25" in md
    assert "0_0" in md


def test_aggregate_shards(tmp_path: Path) -> None:
    shards = tmp_path / "shards"
    shards.mkdir()
    (shards / "a.csv").write_text(
        "in_industrial,shortlist,resolved_lga_name\n"
        "True,False,Melbourne\n"
        "False,True,Geelong\n"
        "True,True,Melbourne\n",
        encoding="utf-8",
    )
    agg = aggregate_shard_csvs(shards)
    assert agg["rows"] == 3
    assert agg["industrial"] == 2
    assert agg["shortlist"] == 2
    assert agg["lga_top"][0][0] == "Melbourne"


def test_write_gist_for_run_requires_manifest(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        write_gist_for_run(tmp_path, "missing")


def test_create_gist_with_gh_invokes_cli() -> None:
    captured: dict = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured["input"] = kwargs.get("input")
        class R:
            returncode = 0
            stdout = "https://gist.github.com/user/abc123\n"
            stderr = ""

        return R()

    with patch("dapr_portal.address_gist_report.shutil.which", return_value="/bin/gh"):
        with patch("dapr_portal.address_gist_report.subprocess.run", side_effect=fake_run):
            url = create_gist_with_gh(
                "# hi",
                filename="r.md",
                description="d",
                public=True,
                open_web=False,
            )
    assert url.startswith("https://gist.github.com")
    assert captured["input"] == "# hi"
    assert "-f" in captured["cmd"]
    assert "r.md" in captured["cmd"]
    assert "--public" in captured["cmd"]
    assert "-d" in captured["cmd"]
    assert captured["cmd"].index("-d") + 1 < len(captured["cmd"])
    assert captured["cmd"][captured["cmd"].index("-d") + 1] == "d"
    assert captured["cmd"][:4] == ["gh", "gist", "create", "-"]


def test_create_gist_with_gh_requires_gh_binary() -> None:
    with patch("dapr_portal.address_gist_report.shutil.which", return_value=None):
        with pytest.raises(RuntimeError, match="GitHub CLI not found"):
            create_gist_with_gh("x")
