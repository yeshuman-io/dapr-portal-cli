"""Victoria mainland WGS84 bbox tiling for batch spatial jobs."""

from __future__ import annotations

# Approximate extent: mainland Victoria + Bass Strait islands margin (WGS84).
# Refine with official bounds if needed; excludes NSW/SA overlap conservatively.
VIC_STATE_BBOX: tuple[float, float, float, float] = (
    140.95,  # minLon
    -39.25,  # minLat
    149.98,  # maxLon
    -33.98,  # maxLat
)


def iter_bbox_tiles(
    outer: tuple[float, float, float, float],
    step_lon: float,
    step_lat: float,
) -> list[tuple[str, tuple[float, float, float, float]]]:
    """
    Partition ``outer`` (minLon, minLat, maxLon, maxLat) into non-overlapping tiles.
    Returns ``(tile_id, (minLon, minLat, maxLon, maxLat))`` with stable ids ``r{row}c{col}``.
    """
    min_lon, min_lat, max_lon, max_lat = outer
    if step_lon <= 0 or step_lat <= 0:
        raise ValueError("step_lon and step_lat must be positive")

    tiles: list[tuple[str, tuple[float, float, float, float]]] = []
    row = 0
    y = min_lat
    while y < max_lat - 1e-9:
        y1 = min(y + step_lat, max_lat)
        col = 0
        x = min_lon
        while x < max_lon - 1e-9:
            x1 = min(x + step_lon, max_lon)
            tid = f"r{row}c{col}"
            tiles.append((tid, (x, y, x1, y1)))
            col += 1
            x = x1
        row += 1
        y = y1
    return tiles


def candidate_dedupe_key(lat: float, lon: float, ndigits: int = 6) -> tuple[float, float]:
    """Stable key for merging zone + UDP seeds at the same nominal point."""
    return (round(lat, ndigits), round(lon, ndigits))
