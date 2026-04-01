"""Tests for Vicmap Address queries (mocked HTTP)."""

from __future__ import annotations

import httpx

from dapr_portal.vicmap_address import (
    VICMAP_ADDRESS_FEATURE_LAYER,
    address_summary_fields,
    pick_preferred_address_row,
    query_addresses_for_property_pfi,
    query_addresses_at_point,
    query_addresses_for_site,
)


def test_query_addresses_empty_pfi() -> None:
    transport = httpx.MockTransport(lambda r: httpx.Response(500))
    with httpx.Client(transport=transport) as client:
        assert query_addresses_for_property_pfi(client, None, disk_cache=False) == []
        assert query_addresses_for_property_pfi(client, "", disk_cache=False) == []
        assert query_addresses_for_property_pfi(client, "   ", disk_cache=False) == []


def test_query_addresses_parses_geojson() -> None:
    mock_json = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "ezi_address": "1 TEST ST EXAMPLE 3000",
                    "property_pfi": "123",
                    "is_primary": "N",
                },
            },
            {
                "type": "Feature",
                "properties": {
                    "ezi_address": "2 TEST ST EXAMPLE 3000",
                    "property_pfi": "123",
                    "is_primary": "Y",
                },
            },
        ],
    }

    def send(request: httpx.Request) -> httpx.Response:
        assert VICMAP_ADDRESS_FEATURE_LAYER in str(request.url)
        assert request.url.params.get("where") == "property_pfi = '999'"
        assert request.url.params.get("f") == "geojson"
        return httpx.Response(200, json=mock_json)

    transport = httpx.MockTransport(send)
    with httpx.Client(transport=transport) as client:
        rows = query_addresses_for_property_pfi(
            client, "999", disk_cache=False, cache_dir=None
        )
    assert len(rows) == 2
    assert pick_preferred_address_row(rows)["ezi_address"] == "2 TEST ST EXAMPLE 3000"
    summ = address_summary_fields(rows)
    assert summ["address_match_count"] == 2
    assert summ["address_ambiguous"] is True
    assert "2 TEST ST" in summ["ezi_address"]


def test_where_escapes_single_quote() -> None:
    captured: dict[str, str] = {}

    def send(request: httpx.Request) -> httpx.Response:
        captured["where"] = request.url.params.get("where") or ""
        return httpx.Response(200, json={"type": "FeatureCollection", "features": []})

    transport = httpx.MockTransport(send)
    with httpx.Client(transport=transport) as client:
        query_addresses_for_property_pfi(client, "O'NEIL", disk_cache=False)
    assert captured["where"] == "property_pfi = 'O''NEIL'"


def test_query_addresses_uses_cache(tmp_path) -> None:
    calls = {"n": 0}
    mock_json = {
        "type": "FeatureCollection",
        "features": [
            {"type": "Feature", "properties": {"ezi_address": "X", "property_pfi": "1"}},
        ],
    }

    def send(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=mock_json)

    transport = httpx.MockTransport(send)
    with httpx.Client(transport=transport) as client:
        query_addresses_for_property_pfi(
            client,
            "555",
            disk_cache=True,
            cache_dir=tmp_path,
            out_fields="ezi_address,property_pfi",
        )
        query_addresses_for_property_pfi(
            client,
            "555",
            disk_cache=True,
            cache_dir=tmp_path,
            out_fields="ezi_address,property_pfi",
        )
    assert calls["n"] == 1


def test_query_addresses_at_point_uses_geometry_and_buffer() -> None:
    captured: dict[str, str] = {}

    def send(request: httpx.Request) -> httpx.Response:
        captured.update(dict(request.url.params))
        return httpx.Response(
            200,
            json={
                "type": "FeatureCollection",
                "features": [
                    {
                        "type": "Feature",
                        "properties": {"ezi_address": "NEARBY", "property_pfi": "9"},
                    }
                ],
            },
        )

    transport = httpx.MockTransport(send)
    with httpx.Client(transport=transport) as client:
        rows = query_addresses_at_point(
            client,
            145.0,
            -37.0,
            disk_cache=False,
            buffer_meters=100.0,
        )
    assert rows[0]["ezi_address"] == "NEARBY"
    assert captured.get("geometry") == "145.0,-37.0"
    assert captured.get("distance") == "100.0"
    assert captured.get("units") == "esriSRUnit_Meter"
    assert captured.get("geometryType") == "esriGeometryPoint"


def test_query_addresses_for_site_pfi_before_point() -> None:
    """Empty where-query triggers second request with geometry (fallback)."""

    def send(request: httpx.Request) -> httpx.Response:
        p = dict(request.url.params)
        if p.get("where"):
            return httpx.Response(200, json={"type": "FeatureCollection", "features": []})
        if p.get("geometry"):
            return httpx.Response(
                200,
                json={
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "properties": {"ezi_address": "FALLBACK ST"},
                        }
                    ],
                },
            )
        return httpx.Response(400, text="unexpected")

    transport = httpx.MockTransport(send)
    with httpx.Client(transport=transport) as client:
        rows, src = query_addresses_for_site(
            client,
            144.0,
            -37.2,
            "12345",
            point_fallback=True,
            disk_cache=False,
        )
    assert src == "point_intersect"
    assert rows[0]["ezi_address"] == "FALLBACK ST"


def test_query_addresses_for_site_skips_point_when_disabled() -> None:
    calls = {"n": 0}

    def send(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"type": "FeatureCollection", "features": []})

    transport = httpx.MockTransport(send)
    with httpx.Client(transport=transport) as client:
        rows, src = query_addresses_for_site(
            client,
            144.0,
            -37.2,
            "x",
            point_fallback=False,
            disk_cache=False,
        )
    assert rows == []
    assert src is None
    assert calls["n"] == 1
