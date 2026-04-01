"""Victorian admin lookups (LGA codes) — bundled open data."""

from __future__ import annotations

import csv
from functools import lru_cache
from importlib import resources
from pathlib import Path


def _csv_rows() -> list[tuple[str, str]]:
    """Load (lga_code, lga_name) from bundled CSV."""
    path = Path(__file__).resolve().parent / "data" / "vic_lga_codes.csv"
    if path.is_file():
        text = path.read_text(encoding="utf-8")
    else:
        pkg = resources.files("dapr_portal.data")
        text = (pkg / "vic_lga_codes.csv").read_text(encoding="utf-8")
    rows: list[tuple[str, str]] = []
    for line in text.splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        if line.startswith("lga_code,"):
            continue
        r = next(csv.reader([line]))
        if len(r) >= 2 and r[0].strip() and not r[0].startswith("#"):
            rows.append((r[0].strip(), r[1].strip()))
    return rows


@lru_cache(maxsize=1)
def _lga_code_to_name() -> dict[str, str]:
    return {code: name for code, name in _csv_rows()}


def lga_name_for_code(code: str | int | None) -> str | None:
    """Resolve Vicmap-style `parcel_lga_code` to uppercase LGA name."""
    if code is None:
        return None
    s = str(code).strip()
    if not s:
        return None
    return _lga_code_to_name().get(s)


def normalize_lga_key(name: str | None) -> str | None:
    """Normalize LGA string for joining (uppercase, collapse spaces)."""
    if not name:
        return None
    return " ".join(name.upper().split())


def lga_name_from_planning_candidate(lga_field: str | None) -> str | None:
    """Planning zone `LGA` is already typically uppercase."""
    return normalize_lga_key(lga_field)
