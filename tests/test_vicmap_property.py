"""Offline tests for Vicmap parcel helpers."""

from __future__ import annotations

import httpx

from dapr_portal.vicmap_property import parcels_to_flat_row, query_parcels_at_point


def test_parcels_to_flat_row_empty() -> None:
    r = parcels_to_flat_row("s", -37.0, 145.0, [])
    assert r["parcel_match_count"] == 0
    assert r["site_name"] == "s"
    assert "parcel_spi" not in r


def test_parcels_to_flat_row_first() -> None:
    r = parcels_to_flat_row(
        "x",
        -37.0,
        145.0,
        [{"parcel_spi": "SPI1", "parcel_pfi": "99"}],
    )
    assert r["parcel_match_count"] == 1
    assert r["parcel_spi"] == "SPI1"
    assert r["parcel_pfi"] == "99"


def test_query_parcels_at_point_parses_geojson() -> None:
    mock_json = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"parcel_spi": "A", "OBJECTID": 1},
            }
        ],
    }
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json=mock_json)
    )
    with httpx.Client(transport=transport) as client:
        out = query_parcels_at_point(
            client,
            145.0,
            -37.0,
            disk_cache=False,
            cache_dir=None,
        )
    assert len(out) == 1
    assert out[0]["parcel_spi"] == "A"


def test_query_parcels_uses_cache(tmp_path) -> None:
    calls = {"n": 0}
    mock_json = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"parcel_spi": "CACHED"}},
        ],
    }

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=mock_json)

    transport = httpx.MockTransport(handler)
    with httpx.Client(transport=transport) as client:
        query_parcels_at_point(
            client,
            144.1,
            -37.2,
            disk_cache=True,
            cache_dir=tmp_path,
            refresh=False,
        )
        query_parcels_at_point(
            client,
            144.1,
            -37.2,
            disk_cache=True,
            cache_dir=tmp_path,
            refresh=False,
        )
    assert calls["n"] == 1
