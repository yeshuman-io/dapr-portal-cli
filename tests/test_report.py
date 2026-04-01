"""Report ranking and enrichment (mocked overlays; synthetic DAPR CSV index)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx

from dapr_portal.report import enrich_one_entry, sort_and_rank_entries
from dapr_portal.vic_spatial import PLANNING_SCHEME_ZONES_MAPSERVER
from dapr_portal.vic_planning_overlays import BUSHFIRE_PRONE_MAPSERVER, FLOOD_UFZ_LAYER_ID

FIXTURES = Path(__file__).resolve().parent / "fixtures"


def _transport_zero_counts() -> httpx.MockTransport:
    def send(request: httpx.Request) -> httpx.Response:
        u = str(request.url)
        if "returnCountOnly" not in u:
            return httpx.Response(404, text=u)
        if BUSHFIRE_PRONE_MAPSERVER in u and "/MapServer/0/query" in u:
            return httpx.Response(200, json={"count": 0})
        if BUSHFIRE_PRONE_MAPSERVER in u and "/MapServer/1/query" in u:
            return httpx.Response(200, json={"count": 0})
        if PLANNING_SCHEME_ZONES_MAPSERVER in u and f"/MapServer/{FLOOD_UFZ_LAYER_ID}/query" in u:
            return httpx.Response(200, json={"count": 0})
        return httpx.Response(404, text=u)

    return httpx.MockTransport(send)


def test_sort_and_rank_by_distance_then_friction() -> None:
    rows = [
        {
            "site_name": "a",
            "nearest": [{"distance_m": 200.0}],
            "planning_overlay_friction": 1,
        },
        {
            "site_name": "b",
            "nearest": [{"distance_m": 100.0}],
            "planning_overlay_friction": 2,
        },
        {
            "site_name": "c",
            "nearest": [{"distance_m": 100.0}],
            "planning_overlay_friction": 0,
        },
    ]
    sort_and_rank_entries(rows)
    assert [r["site_name"] for r in rows] == ["c", "b", "a"]
    assert [r["rank"] for r in rows] == [1, 2, 3]


def test_enrich_one_entry_lga_join_and_parcels_alias(tmp_path) -> None:
    csv_text = "LGA,Type\nCASEY,X\n"
    from dapr_portal.dapr_tables import index_lga_summary_by_type

    idx = index_lga_summary_by_type(csv_text)
    entry = {
        "site_name": "s",
        "query": {"lat": -37.0, "lon": 145.0},
        "nearest": [{"distance_m": 1.0, "circuit": "Z"}],
        "parcels": [{"parcel_spi": "P1", "parcel_lga_code": "312"}],
    }
    transport = _transport_zero_counts()
    with httpx.Client(transport=transport) as client:
        out = enrich_one_entry(
            entry,
            client,
            idx,
            vic_cache_dir=tmp_path,
            vic_cache_ttl=None,
            vic_refresh=True,
            vic_disk_cache=False,
        )
    assert out["resolved_lga_name"] == "CASEY"
    assert out["dapr_table_match"] is True
    assert out["parcel_match_count"] == 1
    assert out["parcel_ambiguous"] is False
    assert out["parcel_primary"]["parcel_spi"] == "P1"


def test_fixture_json_loads() -> None:
    data = json.loads((FIXTURES / "report_screen_like.json").read_text(encoding="utf-8"))
    assert len(data) == 3
    assert data[0]["vicmap_parcels"][0]["parcel_lga_code"] == "312"
