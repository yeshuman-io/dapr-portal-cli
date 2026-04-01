"""Tests for Victoria bbox tiling."""

from __future__ import annotations

from dapr_portal.vic_tiling import (
    VIC_STATE_BBOX,
    candidate_dedupe_key,
    iter_bbox_tiles,
)


def test_iter_bbox_tiles_single_cell() -> None:
    # 1x1 degree box with 1 degree step => one tile
    tiles = iter_bbox_tiles((144.0, -38.0, 145.0, -37.0), 1.0, 1.0)
    assert len(tiles) == 1
    assert tiles[0][0] == "r0c0"
    assert tiles[0][1] == (144.0, -38.0, 145.0, -37.0)


def test_iter_bbox_tiles_grid() -> None:
    tiles = iter_bbox_tiles((0.0, 0.0, 2.0, 2.0), 1.0, 1.0)
    assert len(tiles) == 4
    ids = [t[0] for t in tiles]
    assert "r0c0" in ids and "r1c1" in ids


def test_vic_state_bbox_ordered() -> None:
    min_lon, min_lat, max_lon, max_lat = VIC_STATE_BBOX
    assert min_lon < max_lon and min_lat < max_lat


def test_candidate_dedupe_key() -> None:
    assert candidate_dedupe_key(-37.1234567, 144.9876543) == (-37.123457, 144.987654)
