"""Site scouting: proximity to published HV line segments from the DAPR map layers."""

from __future__ import annotations

import csv
import io
import json
import os
import pickle
import time
import zlib
from dataclasses import asdict, dataclass
from pathlib import Path

import httpx
import polyline
from pyproj import Transformer
from shapely.geometry import LineString, Point
from shapely.strtree import STRtree

from dapr_portal.portal import fetch_rosetta_layer

# GDA2020 Australian Albers — metres, full CitiPower/Powercor footprint
_PROJECTED_CRS = "EPSG:7855"
_WGS84 = "EPSG:4326"

DEFAULT_LINE_LAYERS = (
    "22kV_Powercor_Lines.txt",
    "22kV_CitiPower_Lines.txt",
)

DEFAULT_CACHE_DIR = Path(
    os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache"))
) / "dapr-portal-cli" / "layers"

_TO_METRES = Transformer.from_crs(_WGS84, _PROJECTED_CRS, always_xy=True)


@dataclass
class LineRecord:
    layer: str
    asset_id: str
    circuit: str
    line_type: str
    distance_m: float


def _cache_path(cache_dir: Path, filename: str) -> Path:
    return cache_dir / filename.replace("/", "_")


def _read_cached(path: Path, max_age_seconds: float | None) -> bytes | None:
    if not path.is_file():
        return None
    if max_age_seconds is not None:
        age = time.time() - path.stat().st_mtime
        if age > max_age_seconds:
            return None
    return path.read_bytes()


def fetch_layer_text(
    client: httpx.Client,
    filename: str,
    *,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    cache_ttl_seconds: float | None = 86400.0,
    refresh: bool = False,
) -> str:
    path = _cache_path(cache_dir, filename)
    if not refresh:
        raw = _read_cached(path, cache_ttl_seconds)
        if raw is not None:
            return raw.decode("utf-8")
    cache_dir.mkdir(parents=True, exist_ok=True)
    data = fetch_rosetta_layer(client, filename, query=None)
    path.write_bytes(data)
    return data.decode("utf-8")


def _is_numeric_token(s: str) -> bool:
    s = s.strip()
    if not s:
        return True
    try:
        float(s)
        return True
    except ValueError:
        return False


def _decode_polyline_parts(parts: list[str]) -> list[tuple[float, float]]:
    """Decode alternating polyline / GIS-id rows from Rosetta export."""
    latlng: list[tuple[float, float]] = []
    for p in parts:
        if _is_numeric_token(p):
            continue
        try:
            latlng.extend(polyline.decode(p, precision=5))
        except Exception:
            continue
    return latlng


def parse_layer_lines(layer_name: str, text: str) -> tuple[list[LineString], list[dict]]:
    """Return projected LineStrings and parallel attribute dicts."""
    geoms: list[LineString] = []
    attrs: list[dict] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        row = next(csv.reader(io.StringIO(line)))
        if len(row) < 3:
            continue
        line_type = row[-1]
        if line_type not in ("Overhead", "Underground"):
            continue
        if len(line_type) > 16:
            continue
        if len(row) == 3:
            asset_id = row[0]
            circuit = row[0] if row[0] != "None" else ""
            middle = [row[1]]
        else:
            asset_id = row[0]
            circuit = row[-2]
            if len(circuit) > 48:
                continue
            middle = row[1:-2]
        latlng = _decode_polyline_parts(middle)
        if len(latlng) < 2:
            continue
        xy = [(lon, lat) for lat, lon in latlng]
        try:
            projected = LineString([_TO_METRES.transform(x, y) for x, y in xy])
        except Exception:
            continue
        if projected.is_empty or projected.length <= 0:
            continue
        geoms.append(projected)
        attrs.append(
            {
                "layer": layer_name,
                "asset_id": asset_id,
                "circuit": circuit,
                "line_type": line_type,
            }
        )
    return geoms, attrs


def build_line_index(
    client: httpx.Client,
    layers: tuple[str, ...] = DEFAULT_LINE_LAYERS,
    *,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    cache_ttl_seconds: float | None = 86400.0,
    refresh: bool = False,
    index_cache_ttl_seconds: float | None = 86400.0,
) -> tuple[STRtree, list[dict], list[LineString]]:
    if not refresh:
        ic = _try_load_index_cache(
            _index_cache_path(cache_dir, layers),
            layers,
            cache_dir,
            index_cache_ttl_seconds,
        )
        if ic is not None:
            return ic

    all_geoms: list[LineString] = []
    all_attrs: list[dict] = []
    for name in layers:
        body = fetch_layer_text(
            client,
            name,
            cache_dir=cache_dir,
            cache_ttl_seconds=cache_ttl_seconds,
            refresh=refresh,
        )
        geoms, attrs = parse_layer_lines(name, body)
        all_geoms.extend(geoms)
        all_attrs.extend(attrs)
    if not all_geoms:
        raise RuntimeError("no line segments loaded; check layer names and network")
    tree = STRtree(all_geoms)
    _save_index_cache(_index_cache_path(cache_dir, layers), layers, all_geoms, all_attrs)
    return tree, all_attrs, all_geoms


def _point_projected(lat: float, lon: float) -> Point:
    x, y = _TO_METRES.transform(lon, lat)
    return Point(x, y)


def _index_cache_path(cache_dir: Path, layers: tuple[str, ...]) -> Path:
    key = "|".join(sorted(layers))
    h = hex(zlib.adler32(key.encode()) & 0xFFFFFFFF)
    return cache_dir / f"line_index_{h}.pkl"


def _try_load_index_cache(
    path: Path, layers: tuple[str, ...], cache_dir: Path, max_age: float | None
) -> tuple[STRtree, list[dict], list[LineString]] | None:
    if not path.is_file():
        return None
    if max_age is not None and time.time() - path.stat().st_mtime > max_age:
        return None
    layer_paths = [_cache_path(cache_dir, n) for n in layers]
    if not all(p.is_file() for p in layer_paths):
        return None
    index_mtime = path.stat().st_mtime
    if any(p.stat().st_mtime > index_mtime for p in layer_paths):
        return None
    try:
        with path.open("rb") as f:
            payload = pickle.load(f)
    except Exception:
        return None
    if payload.get("layers") != list(layers):
        return None
    geoms = payload["geoms"]
    attrs = payload["attrs"]
    return STRtree(geoms), attrs, geoms


def _save_index_cache(
    path: Path, layers: tuple[str, ...], geoms: list[LineString], attrs: list[dict]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("wb") as f:
        pickle.dump({"layers": list(layers), "geoms": geoms, "attrs": attrs}, f, protocol=4)
    tmp.replace(path)


def nearest_lines(
    tree: STRtree,
    attrs: list[dict],
    geoms: list[LineString],
    lat: float,
    lon: float,
    *,
    k: int = 5,
) -> list[LineRecord]:
    pt = _point_projected(lat, lon)
    dists: list[tuple[float, int]] = []
    for i, g in enumerate(geoms):
        d = float(pt.distance(g))
        dists.append((d, i))
    dists.sort(key=lambda t: t[0])
    out: list[LineRecord] = []
    for d, i in dists[:k]:
        a = attrs[i]
        out.append(
            LineRecord(
                layer=a["layer"],
                asset_id=a["asset_id"],
                circuit=a["circuit"],
                line_type=a["line_type"],
                distance_m=round(d, 1),
            )
        )
    return out


def lines_within(
    tree: STRtree,
    attrs: list[dict],
    geoms: list[LineString],
    lat: float,
    lon: float,
    radius_m: float,
    *,
    limit: int = 100,
) -> list[LineRecord]:
    pt = _point_projected(lat, lon)
    idx = tree.query(pt, predicate="dwithin", distance=radius_m)
    dists: list[tuple[float, int]] = []
    for i in idx:
        d = float(pt.distance(geoms[int(i)]))
        dists.append((d, int(i)))
    dists.sort(key=lambda t: t[0])
    out: list[LineRecord] = []
    for d, i in dists[:limit]:
        a = attrs[i]
        out.append(
            LineRecord(
                layer=a["layer"],
                asset_id=a["asset_id"],
                circuit=a["circuit"],
                line_type=a["line_type"],
                distance_m=round(d, 1),
            )
        )
    return out


def records_to_jsonable(records: list[LineRecord]) -> list[dict]:
    return [asdict(r) for r in records]


def scout_payload(
    *,
    lat: float,
    lon: float,
    nearest: list[LineRecord],
    within: list[LineRecord] | None,
    layers: tuple[str, ...],
) -> dict:
    return {
        "query": {"lat": lat, "lon": lon},
        "layers": list(layers),
        "projected_crs": _PROJECTED_CRS,
        "nearest": records_to_jsonable(nearest),
        "within_radius_m": records_to_jsonable(within) if within is not None else None,
        "disclaimer": (
            "DAPR map data are general information only, may be incomplete, and must not "
            "be used for excavation or as-built design. Validate with the network operator "
            "and Before You Dig. See https://dapr.powercor.com.au/"
        ),
    }


def dump_json(data: dict) -> str:
    return json.dumps(data, indent=2)
