"""Point-in-polygon industrial tagging (no network)."""

from __future__ import annotations

from shapely.geometry import box

from dapr_portal.industrial_geometry import IndustrialTagIndex


def test_classify_zones_only() -> None:
    z = box(144.9, -37.92, 145.0, -37.88)
    idx = IndustrialTagIndex([z], [])
    assert idx.classify(144.95, -37.9) == (True, "zones")
    assert idx.classify(144.0, -37.9) == (False, "none")


def test_classify_udp_only() -> None:
    u = box(145.0, -37.85, 145.1, -37.8)
    idx = IndustrialTagIndex([], [u])
    assert idx.classify(145.05, -37.82) == (True, "udp")


def test_classify_both_sources() -> None:
    g = box(144.95, -37.91, 145.05, -37.89)
    idx = IndustrialTagIndex([g], [g])
    assert idx.classify(145.0, -37.9) == (True, "both")
