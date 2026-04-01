"""Tests for dc_screen seed merge (no network)."""

from __future__ import annotations

from dapr_portal.dc_screen import merge_seed_rows


def test_merge_seed_rows_dedupes_and_tags_sources() -> None:
    zones = [
        {
            "name": "z1",
            "lat": -37.0,
            "lon": 145.0,
            "properties": {"LGA": "X", "ZONE_CODE": "IN1Z"},
        },
    ]
    udp = [
        {
            "name": "u1",
            "lat": -37.0,
            "lon": 145.0,
            "properties": {"lga_name": "Y"},
        },
    ]
    out = merge_seed_rows(zones, udp)
    assert len(out) == 1
    assert out[0]["land_sources"] == "udp+zones"
    assert "LGA" in out[0]["candidate_attributes"]
    assert "lga_name" in out[0]["candidate_attributes"]


def test_merge_seed_rows_distinct_points() -> None:
    z = [{"name": "a", "lat": -37.0, "lon": 145.0, "properties": {}}]
    u = [{"name": "b", "lat": -37.1, "lon": 145.1, "properties": {}}]
    out = merge_seed_rows(z, u)
    assert len(out) == 2
