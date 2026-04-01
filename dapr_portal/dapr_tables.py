"""Fetch and index decrypted DAPR CSVs (LGA-level summaries) for report joins."""

from __future__ import annotations

import csv
import hashlib
import io
import time
from pathlib import Path
from typing import Any

import httpx

from dapr_portal.portal import (
    DEFAULT_BASE_URL,
    PortalSession,
    fetch_decrypted_csv,
    fetch_portal_html,
)
from dapr_portal.vic_admin import normalize_lga_key


def _dapr_csv_cache_path(cache_dir: Path, serve_ts: str, portal_path: str) -> Path:
    h = hashlib.sha256(f"{serve_ts}|{portal_path}".encode()).hexdigest()[:24]
    return cache_dir / "dapr_csv" / f"{h}.csv"


def _read_text_cache(path: Path, ttl: float | None) -> str | None:
    if not path.is_file():
        return None
    if ttl is not None and time.time() - path.stat().st_mtime > ttl:
        return None
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def _write_text_cache(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def fetch_dapr_csv_cached(
    client: httpx.Client,
    session: PortalSession,
    csv_portal_path: str,
    *,
    cache_dir: Path,
    cache_ttl_seconds: float | None = 3600.0,
    refresh: bool = False,
    disk_cache: bool = True,
) -> str:
    """Download and decrypt a portal CSV; cache plaintext by serve timestamp + path."""
    cpath = _dapr_csv_cache_path(cache_dir, session.serve_timestamp, csv_portal_path)
    if disk_cache and not refresh:
        hit = _read_text_cache(cpath, cache_ttl_seconds)
        if hit is not None:
            return hit
    text = fetch_decrypted_csv(client, session, csv_portal_path)
    if disk_cache:
        _write_text_cache(cpath, text)
    return text


def index_lga_summary_by_type(csv_text: str, lga_column: str = "LGA") -> dict[str, list[dict[str, Any]]]:
    """
    Group rows by normalized LGA name.
    DAPR LGA Summary has multiple rows per LGA (Type = Commercial, Residential, ...).
    """
    # Portal CSVs may start with UTF-8 BOM, which otherwise becomes the column name "\\ufeffLGA".
    csv_text = csv_text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(csv_text))
    if not reader.fieldnames or lga_column not in reader.fieldnames:
        raise ValueError(f"CSV missing {lga_column!r} column; got {reader.fieldnames}")
    out: dict[str, list[dict[str, Any]]] = {}
    for row in reader:
        raw = (row.get(lga_column) or "").strip()
        key = normalize_lga_key(raw)
        if not key:
            continue
        out.setdefault(key, []).append(dict(row))
    return out


def join_lga_rows(
    index: dict[str, list[dict[str, Any]]],
    lga_name: str | None,
) -> tuple[list[dict[str, Any]] | None, bool]:
    """Return rows for LGA and whether any match."""
    if not lga_name:
        return None, False
    key = normalize_lga_key(lga_name)
    if not key:
        return None, False
    rows = index.get(key)
    if not rows:
        return None, False
    return rows, True


def load_portal_session(client: httpx.Client, base_url: str) -> PortalSession:
    html = fetch_portal_html(client, base_url)
    return PortalSession.from_html(html, base_url)


DEFAULT_LGA_SUMMARY_PATH = (
    "./powercor_data/Citipower_Powercor_LGA_Summary.csv?timestamp=12345"
)
