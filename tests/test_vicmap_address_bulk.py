"""Tests for Vicmap Address bbox pagination (mocked HTTP)."""

from __future__ import annotations

import httpx

from dapr_portal.vicmap_address_bulk import fetch_address_page_in_bbox


def test_fetch_address_page_pagination_two_pages() -> None:
    bbox = (144.9, -37.92, 145.02, -37.88)
    page_size = 2

    def feat(i: int) -> dict:
        return {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [144.91 + i * 0.01, -37.9]},
            "properties": {"ezi_address": f"{i} ST", "OBJECTID": str(i)},
        }

    def send(request: httpx.Request) -> httpx.Response:
        assert "/query" in str(request.url)
        assert request.url.params.get("f") == "geojson"
        assert request.url.params.get("geometryType") == "esriGeometryEnvelope"
        assert request.url.params.get("inSR") == "4326"
        off = int(request.url.params.get("resultOffset") or 0)
        if off == 0:
            return httpx.Response(
                200,
                json={"type": "FeatureCollection", "features": [feat(0), feat(1)]},
            )
        if off == 2:
            return httpx.Response(
                200,
                json={"type": "FeatureCollection", "features": [feat(2)]},
            )
        return httpx.Response(200, json={"type": "FeatureCollection", "features": []})

    transport = httpx.MockTransport(send)
    with httpx.Client(transport=transport) as client:
        p0 = fetch_address_page_in_bbox(
            client,
            bbox,
            0,
            layer_url="https://example/arcgis/rest/services/X/FeatureServer/0",
            out_fields="ezi_address,OBJECTID",
            page_size=page_size,
            disk_cache=False,
            cache_dir=None,
        )
        p1 = fetch_address_page_in_bbox(
            client,
            bbox,
            2,
            layer_url="https://example/arcgis/rest/services/X/FeatureServer/0",
            out_fields="ezi_address,OBJECTID",
            page_size=page_size,
            disk_cache=False,
            cache_dir=None,
        )
    assert len(p0) == 2
    assert len(p1) == 1
    assert p0[0]["properties"]["ezi_address"] == "0 ST"
