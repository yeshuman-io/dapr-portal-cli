"""Tests for planning overlay point queries (mocked HTTP)."""

from __future__ import annotations

import httpx

from dapr_portal.vic_spatial import PLANNING_SCHEME_ZONES_MAPSERVER
from dapr_portal.vic_planning_overlays import (
    BUSHFIRE_PRONE_MAPSERVER,
    FLOOD_UFZ_LAYER_ID,
    query_bushfire_hits,
    query_floodway_hit,
)


def _overlay_handler(
    bmo: int, bpa: int, ufz: int
) -> httpx.MockTransport:
    def send(request: httpx.Request) -> httpx.Response:
        u = str(request.url)
        if BUSHFIRE_PRONE_MAPSERVER in u and "/MapServer/0/query" in u:
            return httpx.Response(200, json={"count": bmo})
        if BUSHFIRE_PRONE_MAPSERVER in u and "/MapServer/1/query" in u:
            return httpx.Response(200, json={"count": bpa})
        if PLANNING_SCHEME_ZONES_MAPSERVER in u and f"/MapServer/{FLOOD_UFZ_LAYER_ID}/query" in u:
            return httpx.Response(200, json={"count": ufz})
        return httpx.Response(404, text=f"unexpected url {u}")

    return httpx.MockTransport(send)


def test_query_bushfire_hits_bpa_only() -> None:
    transport = _overlay_handler(bmo=0, bpa=1, ufz=0)
    with httpx.Client(transport=transport) as client:
        bf, bmo_hit, bpa_hit = query_bushfire_hits(
            client, 145.0, -37.0, disk_cache=False, cache_dir=None
        )
    assert bf is True
    assert bmo_hit is False
    assert bpa_hit is True


def test_query_floodway_hit() -> None:
    transport = _overlay_handler(bmo=0, bpa=0, ufz=1)
    with httpx.Client(transport=transport) as client:
        hit = query_floodway_hit(
            client, 145.0, -37.0, disk_cache=False, cache_dir=None
        )
    assert hit is True
