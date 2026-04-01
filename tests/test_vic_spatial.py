"""Offline tests for Vic spatial helpers."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dapr_portal.vic_spatial import (
    feature_collection_to_site_rows,
    parse_bbox,
    rows_to_csv_text,
    zone_codes_to_layer_ids,
)

_FIXTURES = Path(__file__).resolve().parent / "fixtures"


def test_parse_bbox_ok() -> None:
    assert parse_bbox("144,-38,145,-37") == (144.0, -38.0, 145.0, -37.0)


def test_parse_bbox_invalid() -> None:
    with pytest.raises(ValueError):
        parse_bbox("1,2,3")
    with pytest.raises(ValueError):
        parse_bbox("10,10,5,20")
    with pytest.raises(ValueError):
        parse_bbox("200,0,0,0")


def test_zone_codes_to_layer_ids() -> None:
    assert zone_codes_to_layer_ids(["IN1Z", "IN3Z"]) == (11, 13)
    with pytest.raises(ValueError):
        zone_codes_to_layer_ids(["FOO"])


def test_feature_collection_to_site_rows() -> None:
    fc = json.loads((_FIXTURES / "sample_feature_collection.json").read_text())
    rows = feature_collection_to_site_rows(
        fc["features"],
        name_prefix="zone",
        id_prop_keys=("OBJECTID", "ZONE_CODE", "LGA"),
    )
    assert len(rows) == 1
    r = rows[0]
    assert "TEST_LGA" in r["name"]
    assert r["properties"]["ZONE_CODE"] == "IN1Z"
    assert -37.85 <= r["lat"] <= -37.84
    assert 144.9 <= r["lon"] <= 144.91


def test_rows_to_csv_text() -> None:
    csv = rows_to_csv_text(
        [
            {"name": "a|b", "lat": -37.0, "lon": 145.0, "properties": {}},
        ]
    )
    assert "name,lat,lon" in csv
    assert "-37.0" in csv
    assert "145.0" in csv
