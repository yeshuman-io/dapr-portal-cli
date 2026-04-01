"""Command-line entry for agents: list and fetch DAPR CSVs, static files, and map layers."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import httpx

from dapr_portal.portal import (
    DEFAULT_BASE_URL,
    ROSETTA_LAYER_BASE,
    PortalSession,
    fetch_decrypted_csv,
    fetch_portal_html,
    fetch_rosetta_layer,
    fetch_static,
    iter_static_powercor_links,
)
from dapr_portal.candidates import (
    DEFAULT_METRO_MELBOURNE_BBOX,
    DEFAULT_VIC_CACHE_DIR,
    collect_industrial_zone_rows,
    collect_udp_industrial_rows,
    rows_to_csv_text,
)
from dapr_portal.scout import (
    DEFAULT_CACHE_DIR,
    DEFAULT_LINE_LAYERS,
    build_line_index,
    dump_json,
    lines_within,
    nearest_lines,
    scout_payload,
)
from dapr_portal.vic_spatial import parse_bbox
from dapr_portal.vicmap_property import (
    DEFAULT_PARCEL_OUT_FIELDS,
    VICMAP_PARCEL_FEATURE_LAYER,
    parcels_to_flat_row,
    query_parcels_at_point,
)
from dapr_portal.vicmap_address import (
    DEFAULT_ADDRESS_OUT_FIELDS,
    DEFAULT_ADDRESS_POINT_BUFFER_M,
    VICMAP_ADDRESS_FEATURE_LAYER,
    address_summary_fields,
    query_addresses_for_site,
)
from dapr_portal.dapr_tables import (
    DEFAULT_LGA_SUMMARY_PATH,
    fetch_dapr_csv_cached,
    index_lga_summary_by_type,
    load_portal_session,
)
from dapr_portal.address_gist_report import create_gist_with_gh, write_gist_for_run
from dapr_portal.address_scan import ADDRESS_SCAN_DISCLAIMER, AddressScanConfig, run_address_scan
from dapr_portal.dc_screen import DC_SCREEN_DISCLAIMER, DcScreenConfig, run_dc_screen
from dapr_portal.report import REPORT_DISCLAIMER, build_report, report_to_csv_rows
from dapr_portal.vic_tiling import VIC_STATE_BBOX


def _client(timeout: float) -> httpx.Client:
    return httpx.Client(timeout=timeout, follow_redirects=True)


def cmd_config(args: argparse.Namespace) -> int:
    with _client(args.timeout) as client:
        html = fetch_portal_html(client, args.base_url)
        session = PortalSession.from_html(html, args.base_url)
    payload = {
        "base_url": session.base_url,
        "serve_timestamp": session.serve_timestamp,
        "aes_key_hex_preview": session.key_hex[:16] + "…",
        "csv_paths": session.csv_paths(),
        "static_hrefs": iter_static_powercor_links(html),
    }
    print(json.dumps(payload, indent=2))
    return 0


def cmd_list_csv(args: argparse.Namespace) -> int:
    with _client(args.timeout) as client:
        html = fetch_portal_html(client, args.base_url)
        session = PortalSession.from_html(html, args.base_url)
    for p in session.csv_paths():
        print(p)
    return 0


def cmd_list_static(args: argparse.Namespace) -> int:
    with _client(args.timeout) as client:
        html = fetch_portal_html(client, args.base_url)
    for p in iter_static_powercor_links(html):
        print(p)
    return 0


def cmd_get_csv(args: argparse.Namespace) -> int:
    path = args.path.strip()
    if not path.startswith("./"):
        path = "./powercor_data/" + path.lstrip("/")
    with _client(args.timeout) as client:
        html = fetch_portal_html(client, args.base_url)
        session = PortalSession.from_html(html, args.base_url)
        text = fetch_decrypted_csv(client, session, path)
    if args.output == "-":
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")
    else:
        args.output.write_text(text, encoding="utf-8")
    return 0


def cmd_get_static(args: argparse.Namespace) -> int:
    path = args.path.strip()
    with _client(args.timeout) as client:
        html = fetch_portal_html(client, args.base_url)
        session = PortalSession.from_html(html, args.base_url)
        data = fetch_static(client, session, path)
    if args.output == "-":
        sys.stdout.buffer.write(data)
    else:
        args.output.write_bytes(data)
    return 0


def cmd_get_layer(args: argparse.Namespace) -> int:
    with _client(args.timeout) as client:
        data = fetch_rosetta_layer(client, args.filename, args.query)
    if args.output == "-":
        sys.stdout.buffer.write(data)
    else:
        args.output.write_bytes(data)
    return 0


def _parse_layers_arg(raw: list[str] | None) -> tuple[str, ...]:
    if not raw:
        return DEFAULT_LINE_LAYERS
    out: list[str] = []
    for part in raw:
        for name in part.split(","):
            name = name.strip()
            if name:
                out.append(name)
    return tuple(out)


def _read_sites_csv(path: Path) -> list[tuple[float, float, str | None]]:
    import csv as csv_mod

    rows: list[tuple[float, float, str | None]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv_mod.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("sites CSV has no header row")
        fields = {h.lower().strip(): h for h in reader.fieldnames if h}
        lat_h = fields.get("lat") or fields.get("latitude")
        lon_h = fields.get("lon") or fields.get("lng") or fields.get("longitude")
        if not lat_h or not lon_h:
            raise ValueError("sites CSV needs lat/latitude and lon/longitude columns")
        name_h = fields.get("name") or fields.get("id") or fields.get("site")
        for row in reader:
            lat_s = row.get(lat_h, "").strip()
            lon_s = row.get(lon_h, "").strip()
            if not lat_s or not lon_s:
                continue
            label = (row.get(name_h, "").strip() if name_h else None) or None
            rows.append((float(lat_s), float(lon_s), label))
    return rows


def cmd_scout(args: argparse.Namespace) -> int:
    layers = _parse_layers_arg(args.layers)
    cache_dir = Path(args.cache_dir) if args.cache_dir else DEFAULT_CACHE_DIR
    with _client(args.timeout) as client:
        tree, attrs, geoms = build_line_index(
            client,
            layers,
            cache_dir=cache_dir,
            refresh=args.refresh,
        )

    results: list[dict] = []
    if args.sites:
        sites = _read_sites_csv(args.sites)
        for lat, lon, name in sites:
            near = nearest_lines(tree, attrs, geoms, lat, lon, k=args.top_k)
            within = None
            if args.within_m is not None:
                within = lines_within(
                    tree, attrs, geoms, lat, lon, args.within_m, limit=args.within_limit
                )
            payload = scout_payload(
                lat=lat, lon=lon, nearest=near, within=within, layers=layers
            )
            if name is not None:
                payload["site_name"] = name
            results.append(payload)
        out_obj: dict | list = results
    else:
        if args.lat is None or args.lon is None:
            raise SystemExit("scout: provide --lat and --lon, or --sites CSV")
        near = nearest_lines(tree, attrs, geoms, args.lat, args.lon, k=args.top_k)
        within = None
        if args.within_m is not None:
            within = lines_within(
                tree, attrs, geoms, args.lat, args.lon, args.within_m, limit=args.within_limit
            )
        out_obj = scout_payload(
            lat=args.lat, lon=args.lon, nearest=near, within=within, layers=layers
        )

    text = dump_json(out_obj)
    if args.output == "-":
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")
    else:
        args.output.write_text(text, encoding="utf-8")
    return 0


def _vic_cache_dir(args: argparse.Namespace) -> Path:
    return Path(args.vic_cache_dir) if args.vic_cache_dir else DEFAULT_VIC_CACHE_DIR


def cmd_candidates_from_zones(args: argparse.Namespace) -> int:
    bbox = parse_bbox(args.bbox)
    zone_codes = (
        [z.strip() for z in args.zone_codes.split(",") if z.strip()]
        if args.zone_codes
        else None
    )
    layer_ids = None
    if args.mapserver_layers:
        layer_ids = tuple(
            int(x.strip())
            for x in args.mapserver_layers.split(",")
            if x.strip()
        )
    with _client(args.timeout) as client:
        rows = collect_industrial_zone_rows(
            client,
            bbox,
            zone_codes=zone_codes,
            layer_ids=layer_ids,
            max_features=args.limit,
            cache_dir=_vic_cache_dir(args),
            cache_ttl=None if args.vic_no_cache else args.vic_cache_ttl,
            refresh=args.vic_refresh,
            disk_cache=not args.vic_no_cache,
        )
    text = rows_to_csv_text(rows)
    if args.output == "-":
        sys.stdout.write(text)
    else:
        args.output.write_text(text, encoding="utf-8")
    return 0


def cmd_candidates_from_udp(args: argparse.Namespace) -> int:
    bbox = (
        parse_bbox(args.bbox)
        if args.bbox
        else DEFAULT_METRO_MELBOURNE_BBOX
    )
    with _client(args.timeout) as client:
        rows = collect_udp_industrial_rows(
            client,
            bbox,
            cql_filter=args.cql_filter,
            max_features=args.limit,
            cache_dir=_vic_cache_dir(args),
            cache_ttl=None if args.vic_no_cache else args.vic_cache_ttl,
            refresh=args.vic_refresh,
            disk_cache=not args.vic_no_cache,
        )
    text = rows_to_csv_text(rows)
    if args.output == "-":
        sys.stdout.write(text)
    else:
        args.output.write_text(text, encoding="utf-8")
    return 0


def cmd_screen(args: argparse.Namespace) -> int:
    if args.source == "zones" and not args.bbox:
        raise ValueError("screen --source zones requires --bbox minLon,minLat,maxLon,maxLat")
    if getattr(args, "with_addresses", False) and not getattr(args, "enrich_parcels", False):
        raise ValueError("screen: --with-addresses requires --enrich-parcels")
    bbox = parse_bbox(args.bbox) if args.bbox else DEFAULT_METRO_MELBOURNE_BBOX
    zone_codes = (
        [z.strip() for z in args.zone_codes.split(",") if z.strip()]
        if getattr(args, "zone_codes", None)
        else None
    )
    layers = _parse_layers_arg(args.layers)
    scout_cache = Path(args.cache_dir) if args.cache_dir else DEFAULT_CACHE_DIR
    vic_cache = _vic_cache_dir(args)

    with _client(args.timeout) as client:
        if args.source == "zones":
            rows = collect_industrial_zone_rows(
                client,
                bbox,
                zone_codes=zone_codes,
                layer_ids=None,
                max_features=args.limit,
                cache_dir=vic_cache,
                cache_ttl=None if args.vic_no_cache else args.vic_cache_ttl,
                refresh=args.vic_refresh,
                disk_cache=not args.vic_no_cache,
            )
        else:
            rows = collect_udp_industrial_rows(
                client,
                bbox,
                cql_filter=args.cql_filter,
                max_features=args.limit,
                cache_dir=vic_cache,
                cache_ttl=None if args.vic_no_cache else args.vic_cache_ttl,
                refresh=args.vic_refresh,
                disk_cache=not args.vic_no_cache,
            )
        tree, attrs, geoms = build_line_index(
            client,
            layers,
            cache_dir=scout_cache,
            refresh=args.refresh,
        )

    results: list[dict] = []
    for r in rows:
        lat, lon = r["lat"], r["lon"]
        near = nearest_lines(tree, attrs, geoms, lat, lon, k=args.top_k)
        within = None
        if args.within_m is not None:
            within = lines_within(
                tree, attrs, geoms, lat, lon, args.within_m, limit=args.within_limit
            )
        payload = scout_payload(
            lat=lat, lon=lon, nearest=near, within=within, layers=layers
        )
        payload["site_name"] = r["name"]
        props = r.get("properties")
        if props:
            payload["candidate_attributes"] = props
        results.append(payload)

    if getattr(args, "enrich_parcels", False):
        vic_cache = _vic_cache_dir(args)
        with _client(args.timeout) as pc:
            for payload in results:
                lat = payload["query"]["lat"]
                lon = payload["query"]["lon"]
                parcels = query_parcels_at_point(
                    pc,
                    lon,
                    lat,
                    layer_url=args.parcel_layer_url,
                    out_fields=args.parcel_out_fields,
                    max_records=args.max_parcel_matches,
                    cache_dir=vic_cache,
                    cache_ttl_seconds=None if args.vic_no_cache else args.vic_cache_ttl,
                    refresh=args.vic_refresh,
                    disk_cache=not args.vic_no_cache,
                )
                payload["vicmap_parcels"] = parcels
                payload["vicmap_parcel_note"] = _VICMAP_PARCEL_DISCLAIMER
                if getattr(args, "with_addresses", False):
                    pfi = parcels[0].get("parcel_pfi") if parcels else None
                    addrs, src = query_addresses_for_site(
                        pc,
                        lon,
                        lat,
                        pfi,
                        layer_url=args.address_layer_url,
                        out_fields=args.address_out_fields,
                        max_pfi_matches=args.max_address_matches,
                        point_fallback=not getattr(
                            args, "no_address_point_fallback", False
                        ),
                        max_point_matches=args.max_address_point_matches,
                        point_buffer_meters=args.address_point_buffer_m,
                        cache_dir=vic_cache,
                        cache_ttl_seconds=None if args.vic_no_cache else args.vic_cache_ttl,
                        refresh=args.vic_refresh,
                        disk_cache=not args.vic_no_cache,
                    )
                    payload["vicmap_addresses"] = addrs
                    if src:
                        payload["vicmap_address_match_source"] = src
                    payload["vicmap_address_note"] = _VICMAP_ADDRESS_DISCLAIMER

    text = dump_json(results)
    if args.output == "-":
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")
    else:
        args.output.write_text(text, encoding="utf-8")
    return 0


_VICMAP_PARCEL_DISCLAIMER = (
    "Vicmap Property parcel polygons (CC BY). Not a certificate of title; "
    "multiple polygons can intersect one point (e.g. boundaries). "
    "See https://discover.data.vic.gov.au/dataset/vicmap-property-rest-api"
)

_VICMAP_ADDRESS_DISCLAIMER = (
    "Vicmap Address (CC BY). Primary match uses parcel property_pfi; if none, nearby address "
    "points within a buffer of the site (not necessarily on the same title). "
    "Not a verified mailing or service address. "
    "See https://discover.data.vic.gov.au/dataset/vicmap-address-rest-api"
)


def _add_parcel_address_flags(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--with-addresses",
        action="store_true",
        help=(
            "Vicmap Address: match by parcel_pfi, then if empty a buffered point search "
            "(see --address-point-buffer-m; disable buffer fallback with --no-address-point-fallback)"
        ),
    )
    p.add_argument(
        "--address-layer-url",
        default=VICMAP_ADDRESS_FEATURE_LAYER,
        metavar="URL",
        help="Esri FeatureLayer URL (default: Vicmap_Address/0)",
    )
    p.add_argument(
        "--address-out-fields",
        default=DEFAULT_ADDRESS_OUT_FIELDS,
        help="Comma-separated address attribute fields to request",
    )
    p.add_argument(
        "--max-address-matches",
        type=int,
        default=50,
        metavar="N",
        help="Max address points per property_pfi (default: 50)",
    )
    p.add_argument(
        "--no-address-point-fallback",
        action="store_true",
        help="Do not search address points near lat/lon when property_pfi returns no rows",
    )
    p.add_argument(
        "--max-address-point-matches",
        type=int,
        default=15,
        metavar="N",
        help="Max address points for buffered point fallback (default: 15)",
    )
    p.add_argument(
        "--address-point-buffer-m",
        type=float,
        default=DEFAULT_ADDRESS_POINT_BUFFER_M,
        metavar="M",
        help=(
            "Buffer radius in metres for point fallback (default: "
            f"{DEFAULT_ADDRESS_POINT_BUFFER_M}; use 0 for strict point intersect only)"
        ),
    )


def cmd_enrich_parcels(args: argparse.Namespace) -> int:
    sites = _read_sites_csv(args.sites)
    if args.limit is not None:
        sites = sites[: args.limit]
    vic_cache = _vic_cache_dir(args)
    out: list[dict] = []
    with _client(args.timeout) as client:
        for lat, lon, name in sites:
            parcels = query_parcels_at_point(
                client,
                lon,
                lat,
                layer_url=args.parcel_layer_url,
                out_fields=args.parcel_out_fields,
                max_records=args.max_parcel_matches,
                cache_dir=vic_cache,
                cache_ttl_seconds=None if args.vic_no_cache else args.vic_cache_ttl,
                refresh=args.vic_refresh,
                disk_cache=not args.vic_no_cache,
            )
            item = {
                "site_name": name,
                "query": {"lat": lat, "lon": lon},
                "parcels": parcels,
                "parcel_match_count": len(parcels),
                "disclaimer": _VICMAP_PARCEL_DISCLAIMER,
            }
            if getattr(args, "with_addresses", False):
                pfi = parcels[0].get("parcel_pfi") if parcels else None
                addrs, src = query_addresses_for_site(
                    client,
                    lon,
                    lat,
                    pfi,
                    layer_url=args.address_layer_url,
                    out_fields=args.address_out_fields,
                    max_pfi_matches=args.max_address_matches,
                    point_fallback=not getattr(args, "no_address_point_fallback", False),
                    max_point_matches=args.max_address_point_matches,
                    point_buffer_meters=args.address_point_buffer_m,
                    cache_dir=vic_cache,
                    cache_ttl_seconds=None if args.vic_no_cache else args.vic_cache_ttl,
                    refresh=args.vic_refresh,
                    disk_cache=not args.vic_no_cache,
                )
                item["vicmap_addresses"] = addrs
                if src:
                    item["vicmap_address_match_source"] = src
                item["vicmap_address_note"] = _VICMAP_ADDRESS_DISCLAIMER
            out.append(item)

    if args.format == "json":
        text = dump_json(out)
        if args.output == "-":
            sys.stdout.write(text)
            if not text.endswith("\n"):
                sys.stdout.write("\n")
        else:
            args.output.write_text(text, encoding="utf-8")
        return 0

    import csv
    import io

    flat: list[dict] = []
    for (lat, lon, name), item in zip(sites, out):
        row = parcels_to_flat_row(name, lat, lon, item["parcels"])
        if getattr(args, "with_addresses", False):
            row.update(
                address_summary_fields(item.get("vicmap_addresses") or [])
            )
            row["address_match_source"] = item.get("vicmap_address_match_source") or ""
        flat.append(row)
    keys: set[str] = set()
    for row in flat:
        keys.update(row.keys())
    base = ["site_name", "lat", "lon", "parcel_match_count"]
    fieldnames = base + sorted(k for k in keys if k not in base)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    w.writeheader()
    for row in flat:
        w.writerow({k: row.get(k, "") for k in fieldnames})
    csv_text = buf.getvalue()
    if args.output == "-":
        sys.stdout.write(csv_text)
    else:
        args.output.write_text(csv_text, encoding="utf-8")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Fetch tabular and file assets from the CitiPower/Powercor DAPR portal "
            "(https://dapr.powercor.com.au/), and run simple proximity checks against "
            "published map line layers (e.g. 22 kV). "
            "CSV payloads use the same AES parameters as the public web app. "
            "Respect the portal disclaimer and terms of use; map data are not as-built."
        ),
        epilog=(
            "Victorian land seeds (CC BY — Data Vic / spatial.planning.vic.gov.au): "
            "`dapr candidates from-zones --bbox MINLON,MINLAT,MAXLON,MAXLAT -o sites.csv` "
            "then `dapr scout --sites sites.csv -o out.json`, or `dapr screen` for one step. "
            "Phase 2 — Vicmap parcels: `dapr enrich-parcels --sites sites.csv` "
            "or `dapr screen ... --enrich-parcels`. "
            "Phase 3 — ranked report: `dapr report --from-json screen.json`. "
            "Optional Vicmap Address: `dapr enrich-parcels --sites ... --with-addresses` "
            "(or `dapr screen ... --enrich-parcels --with-addresses`). "
            "Batch DC screen: `dapr dc-screen --out-dir ./runs` (see docs/dc_screening.md)."
        ),
    )
    p.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Portal origin (default: {DEFAULT_BASE_URL})",
    )
    p.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout in seconds (default: 120)",
    )
    sub = p.add_subparsers(dest="command", required=True)

    sc = sub.add_parser(
        "config",
        help="Print resolved CSV paths, static links, and serve timestamp (JSON)",
    )
    sc.set_defaults(func=cmd_config)

    sc = sub.add_parser("list-csv", help="List CSV paths referenced by the portal")
    sc.set_defaults(func=cmd_list_csv)

    sc = sub.add_parser(
        "list-static", help="List ./powercor_data/ document links from the portal HTML"
    )
    sc.set_defaults(func=cmd_list_static)

    sc = sub.add_parser(
        "get-csv",
        help="Download and decrypt a CSV (path as shown by list-csv, or basename under powercor_data/)",
    )
    sc.add_argument(
        "path",
        help='e.g. ./powercor_data/Citipower_Powercor_Constraints_Table.csv?timestamp=12345',
    )
    sc.add_argument(
        "-o",
        "--output",
        default="-",
        help='Output file, or "-" for stdout (default: -)',
    )
    sc.set_defaults(func=cmd_get_csv)

    sc = sub.add_parser(
        "get-static",
        help="Download a binary/static file (PDF, XLSX, etc.) from the portal",
    )
    sc.add_argument(
        "path",
        help="e.g. powercor_data/DAPR_2023_Powercor_Distribution Annual Planning Report.pdf",
    )
    sc.add_argument(
        "-o",
        "--output",
        default="-",
        help='Output file, or "-" for stdout (default: -)',
    )
    sc.set_defaults(func=cmd_get_static)

    sc = sub.add_parser(
        "get-layer",
        help=f"Download a map layer file from {ROSETTA_LAYER_BASE}",
    )
    sc.add_argument(
        "filename",
        help="e.g. 22kV_Powercor_Lines.txt",
    )
    sc.add_argument(
        "-q",
        "--query",
        default=None,
        help="Optional raw query string after ?, e.g. 112345678 (portal uses varying cache-busters)",
    )
    sc.add_argument(
        "-o",
        "--output",
        default="-",
        help='Output file, or "-" for stdout (default: -)',
    )
    sc.set_defaults(func=cmd_get_layer)

    sc = sub.add_parser(
        "scout",
        help=(
            "Find nearest 22 kV (or other) line segments to coordinates — "
            "high-level site screening only"
        ),
    )
    sc.add_argument("--lat", type=float, default=None, help="Latitude (WGS84)")
    sc.add_argument("--lon", type=float, default=None, help="Longitude (WGS84)")
    sc.add_argument(
        "--sites",
        type=Path,
        default=None,
        help="CSV with lat,lon columns (optional name/id) for batch queries",
    )
    sc.add_argument(
        "--layers",
        nargs="*",
        default=None,
        help=(
            "Rosetta layer .txt filenames (default: 22 kV Powercor + CitiPower). "
            "Examples: 11kV_CitiPower_Powercor_Lines.txt 6.6kV_CitiPower_Powercor_Lines.txt"
        ),
    )
    sc.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of nearest segments to report (default: 5)",
    )
    sc.add_argument(
        "--within-m",
        type=float,
        default=None,
        metavar="METERS",
        help="Also list segments within this horizontal distance (metres, projected)",
    )
    sc.add_argument(
        "--within-limit",
        type=int,
        default=100,
        help="Max rows for --within-m (default: 100)",
    )
    sc.add_argument(
        "--cache-dir",
        default=None,
        help=f"Override layer/index cache directory (default: {DEFAULT_CACHE_DIR})",
    )
    sc.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download layer files and rebuild spatial index cache",
    )
    sc.add_argument(
        "-o",
        "--output",
        default="-",
        help='JSON output path, or "-" for stdout (default: -)',
    )
    sc.set_defaults(func=cmd_scout)

    def _add_vic_cache_flags(sub: argparse.ArgumentParser) -> None:
        sub.add_argument(
            "--vic-cache-dir",
            default=None,
            help=f"Vic spatial JSON cache directory (default: {DEFAULT_VIC_CACHE_DIR})",
        )
        sub.add_argument(
            "--vic-cache-ttl",
            type=float,
            default=3600.0,
            metavar="SEC",
            help="Vic spatial cache TTL in seconds (default: 3600; use 0 with --vic-no-cache)",
        )
        sub.add_argument(
            "--vic-refresh",
            action="store_true",
            help="Ignore Vic spatial disk cache for this run",
        )
        sub.add_argument(
            "--vic-no-cache",
            action="store_true",
            help="Do not read or write Vic spatial disk cache",
        )

    sc_cand = sub.add_parser(
        "candidates",
        help=(
            "Emit candidate site CSV (name,lat,lon) from Victorian industrial planning "
            "zones or UDP industrial land (Data Vic / spatial.planning.vic.gov.au). "
            "CC BY — not planning approval. Combine with: dapr scout --sites FILE.csv"
        ),
    )
    cand_sub = sc_cand.add_subparsers(dest="_cand_cmd", required=True)

    c_zones = cand_sub.add_parser(
        "from-zones",
        help=(
            "Industrial 1/2/3 zone polygons from planning_scheme_zones MapServer "
            "(layers 11–13; see vic_spatial.PLANNING_SCHEME_ZONES_MAPSERVER)"
        ),
    )
    _add_vic_cache_flags(c_zones)
    c_zones.add_argument(
        "--bbox",
        required=True,
        metavar="MINLON,MINLAT,MAXLON,MAXLAT",
        help="WGS84 bounding box",
    )
    c_zones.add_argument(
        "--zone-codes",
        default=None,
        help="Optional comma list: IN1Z,IN2Z,IN3Z (limits which MapServer layers / attributes are queried)",
    )
    c_zones.add_argument(
        "--mapserver-layers",
        default=None,
        help="Override MapServer layer ids (comma-separated), e.g. 11,12,13",
    )
    c_zones.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Max total sites after deduplication",
    )
    c_zones.add_argument(
        "-o",
        "--output",
        default="-",
        help='Output CSV path, or "-" for stdout (default: -)',
    )
    c_zones.set_defaults(func=cmd_candidates_from_zones)

    c_udp = cand_sub.add_parser(
        "from-udp",
        help=(
            "UDP industrial land 2022 parcels via GeoServer WFS "
            "(open-data-platform:ind2022; metro default bbox)"
        ),
    )
    _add_vic_cache_flags(c_udp)
    c_udp.add_argument(
        "--bbox",
        default=None,
        metavar="MINLON,MINLAT,MAXLON,MAXLAT",
        help=(
            "WGS84 bbox; default is metro Melbourne "
            "(see dapr_portal.vic_spatial.DEFAULT_METRO_MELBOURNE_BBOX)"
        ),
    )
    c_udp.add_argument(
        "--cql-filter",
        default=None,
        help="Optional GeoServer CQL_FILTER (e.g. \"status_desc_2022='Zoned vacant'\")",
    )
    c_udp.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Max features",
    )
    c_udp.add_argument(
        "-o",
        "--output",
        default="-",
        help='Output CSV path, or "-" for stdout (default: -)',
    )
    c_udp.set_defaults(func=cmd_candidates_from_udp)

    sc_screen = sub.add_parser(
        "screen",
        help=(
            "One-shot: candidate points from zones or UDP, then DAPR line proximity "
            "(JSON array). --source zones requires --bbox."
        ),
    )
    _add_vic_cache_flags(sc_screen)
    sc_screen.add_argument(
        "--source",
        choices=("zones", "udp"),
        required=True,
        help="Land seed: planning industrial zones (bbox required) or UDP industrial WFS",
    )
    sc_screen.add_argument(
        "--bbox",
        default=None,
        metavar="MINLON,MINLAT,MAXLON,MAXLAT",
        help="Required for --source zones; optional for udp (metro default)",
    )
    sc_screen.add_argument(
        "--zone-codes",
        default=None,
        help="For --source zones: optional IN1Z,IN2Z,IN3Z filter",
    )
    sc_screen.add_argument(
        "--cql-filter",
        default=None,
        help="For --source udp: optional CQL_FILTER",
    )
    sc_screen.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Max candidate sites",
    )
    sc_screen.add_argument(
        "--layers",
        nargs="*",
        default=None,
        help="Rosetta line layers for scout (default: 22 kV + 11 kV as in dapr scout)",
    )
    sc_screen.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Nearest segments per site (default: 5)",
    )
    sc_screen.add_argument(
        "--within-m",
        type=float,
        default=None,
        metavar="METERS",
        help="Also list segments within this distance (metres)",
    )
    sc_screen.add_argument(
        "--within-limit",
        type=int,
        default=100,
        help="Max rows for --within-m (default: 100)",
    )
    sc_screen.add_argument(
        "--cache-dir",
        default=None,
        help=f"DAPR Rosetta layer/index cache (default: {DEFAULT_CACHE_DIR})",
    )
    sc_screen.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download DAPR line layers / rebuild scout index",
    )
    sc_screen.add_argument(
        "--enrich-parcels",
        action="store_true",
        help="Attach Vicmap Property parcel intersects (FeatureServer) per site",
    )
    sc_screen.add_argument(
        "--parcel-layer-url",
        default=VICMAP_PARCEL_FEATURE_LAYER,
        metavar="URL",
        help="Esri FeatureLayer URL for parcel polygons (default: Vicmap_Parcel/0)",
    )
    sc_screen.add_argument(
        "--parcel-out-fields",
        default=DEFAULT_PARCEL_OUT_FIELDS,
        help="Comma-separated field list for parcel query",
    )
    sc_screen.add_argument(
        "--max-parcel-matches",
        type=int,
        default=20,
        metavar="N",
        help="Max parcel features returned per point (default: 20)",
    )
    _add_parcel_address_flags(sc_screen)
    sc_screen.add_argument(
        "-o",
        "--output",
        default="-",
        help='JSON output path, or "-" for stdout (default: -)',
    )
    sc_screen.set_defaults(func=cmd_screen)

    sc_enrich = sub.add_parser(
        "enrich-parcels",
        help=(
            "Look up Vicmap Property parcel polygon attributes at each site (WGS84 point "
            "intersect). Feed a CSV from candidates/scout. CC BY — not title."
        ),
    )
    _add_vic_cache_flags(sc_enrich)
    sc_enrich.add_argument(
        "--sites",
        type=Path,
        required=True,
        help="CSV with lat,lon (optional name) — same shape as dapr scout --sites",
    )
    sc_enrich.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (default: json)",
    )
    sc_enrich.add_argument(
        "--parcel-layer-url",
        default=VICMAP_PARCEL_FEATURE_LAYER,
        metavar="URL",
        help="Esri FeatureLayer URL (default: Vicmap_Parcel/0)",
    )
    sc_enrich.add_argument(
        "--parcel-out-fields",
        default=DEFAULT_PARCEL_OUT_FIELDS,
        help="Comma-separated parcel attribute fields to request",
    )
    sc_enrich.add_argument(
        "--max-parcel-matches",
        type=int,
        default=20,
        metavar="N",
        help="Max parcel polygons per point (default: 20)",
    )
    sc_enrich.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N rows from the CSV",
    )
    _add_parcel_address_flags(sc_enrich)
    sc_enrich.add_argument(
        "-o",
        "--output",
        default="-",
        help='Output path, or "-" for stdout (default: -)',
    )
    sc_enrich.set_defaults(func=cmd_enrich_parcels)

    sc_report = sub.add_parser(
        "report",
        help=(
            "Rank sites from screen/enrich JSON: LGA decode, DAPR LGA summary join, "
            "bushfire (BMO/BPA) + floodway (UFZ) point checks. Not legal or permit advice."
        ),
    )
    _add_vic_cache_flags(sc_report)
    sc_report.add_argument(
        "--from-json",
        type=Path,
        required=True,
        metavar="PATH",
        help=(
            "JSON array from dapr screen ... --enrich-parcels (or compatible); "
            "may include vicmap_addresses from --with-addresses"
        ),
    )
    sc_report.add_argument(
        "--format",
        choices=("json", "csv"),
        default="json",
        help="Output format (default: json)",
    )
    sc_report.add_argument(
        "--lga-summary-path",
        default=DEFAULT_LGA_SUMMARY_PATH,
        metavar="PORTAL_PATH",
        help=(
            "Portal CSV path for LGA summary (as from dapr list-csv; "
            f"default: {DEFAULT_LGA_SUMMARY_PATH!r})"
        ),
    )
    sc_report.add_argument(
        "--dapr-csv-refresh",
        action="store_true",
        help="Re-fetch decrypted LGA summary CSV ignoring disk cache",
    )
    sc_report.add_argument(
        "-o",
        "--output",
        default="-",
        help='Output path, or "-" for stdout (default: -)',
    )
    sc_report.set_defaults(func=cmd_report)

    sc_dc = sub.add_parser(
        "dc-screen",
        help=(
            "Batch datacenter-oriented screen: tiled industrial seeds (zones/UDP), "
            "DAPR line proximity, overlays, DAPR LGA CSV join; shard CSVs + checkpoints."
        ),
    )
    _add_vic_cache_flags(sc_dc)
    sc_dc.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Output directory; writes OUT_DIR/RUN_ID/shards/*.csv and manifest.json",
    )
    sc_dc.add_argument(
        "--run-id",
        default=None,
        metavar="ID",
        help="Run folder name (default: timestamp). Reuse to resume checkpoints.",
    )
    sc_dc.add_argument(
        "--bbox",
        default=None,
        metavar="MINLON,MINLAT,MAXLON,MAXLAT",
        help="Outer WGS84 bbox to tile (default: Victoria mainland preset from vic_tiling)",
    )
    sc_dc.add_argument(
        "--tile-step",
        default="1.0",
        metavar="DEG[,DEG]",
        help="Tile size in degrees: one value (square) or lon,lat (default: 1.0)",
    )
    sc_dc.add_argument(
        "--land-source",
        choices=("zones", "udp", "both"),
        default="both",
        help="Industrial seed: planning zones, UDP WFS, or both (deduped)",
    )
    sc_dc.add_argument(
        "--zone-codes",
        default=None,
        help="Optional IN1Z,IN2Z,IN3Z filter for zones source",
    )
    sc_dc.add_argument(
        "--max-features-per-tile",
        type=int,
        default=None,
        metavar="N",
        help="Cap zone/UDP features collected per tile (default: no cap)",
    )
    sc_dc.add_argument(
        "--layers",
        nargs="*",
        default=None,
        help="Rosetta line layers (default: 22 kV Powercor + CitiPower)",
    )
    sc_dc.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Nearest segments to store in nearest_top_json (default: 5)",
    )
    sc_dc.add_argument(
        "--within-m",
        type=float,
        default=None,
        metavar="METERS",
        help="Also count segments within this distance (within_count column)",
    )
    sc_dc.add_argument(
        "--within-limit",
        type=int,
        default=100,
        help="Max segments for --within-m (default: 100)",
    )
    sc_dc.add_argument(
        "--shortlist-max-m",
        type=float,
        default=None,
        metavar="METERS",
        help="Mark shortlist=true when nearest_distance_m <= this value",
    )
    sc_dc.add_argument(
        "--addresses",
        choices=("none", "all", "shortlist"),
        default="none",
        help="Vicmap Address: none | all candidates | shortlist only (needs --shortlist-max-m)",
    )
    sc_dc.add_argument(
        "--with-parcels",
        action="store_true",
        help="Vicmap Property parcel query per candidate (slower)",
    )
    sc_dc.add_argument(
        "--overlay-delay",
        type=float,
        default=0.05,
        metavar="SEC",
        help="Sleep after each candidate overlay pair (rate limit; default: 0.05)",
    )
    sc_dc.add_argument(
        "--scout-cache-dir",
        default=None,
        help=f"Rosetta layer cache (default: {DEFAULT_CACHE_DIR})",
    )
    sc_dc.add_argument(
        "--scout-refresh",
        action="store_true",
        help="Re-download Rosetta layers / rebuild line index",
    )
    sc_dc.add_argument(
        "--parcel-layer-url",
        default=VICMAP_PARCEL_FEATURE_LAYER,
        metavar="URL",
        help="Parcel FeatureLayer URL when --with-parcels",
    )
    sc_dc.add_argument(
        "--parcel-out-fields",
        default=DEFAULT_PARCEL_OUT_FIELDS,
        help="Parcel outFields when --with-parcels",
    )
    sc_dc.add_argument(
        "--max-parcel-matches",
        type=int,
        default=5,
        metavar="N",
        help="Max parcel features per point when --with-parcels (default: 5)",
    )
    sc_dc.add_argument(
        "--address-layer-url",
        default=VICMAP_ADDRESS_FEATURE_LAYER,
        metavar="URL",
    )
    sc_dc.add_argument(
        "--address-out-fields",
        default=DEFAULT_ADDRESS_OUT_FIELDS,
        help="Address outFields",
    )
    sc_dc.add_argument(
        "--max-address-matches",
        type=int,
        default=50,
        metavar="N",
    )
    sc_dc.add_argument(
        "--max-address-point-matches",
        type=int,
        default=15,
        metavar="N",
    )
    sc_dc.add_argument(
        "--address-point-buffer-m",
        type=float,
        default=DEFAULT_ADDRESS_POINT_BUFFER_M,
        metavar="M",
    )
    sc_dc.add_argument(
        "--no-address-point-fallback",
        action="store_true",
        help="PFI-only for addresses (no buffered point fallback)",
    )
    sc_dc.add_argument(
        "--lga-summary-path",
        default=DEFAULT_LGA_SUMMARY_PATH,
        metavar="PORTAL_PATH",
    )
    sc_dc.add_argument(
        "--dapr-csv-refresh",
        action="store_true",
    )
    sc_dc.set_defaults(func=cmd_dc_screen)

    sc_vic = sub.add_parser(
        "vic",
        help="Victoria-only bulk scans (Vicmap Address + industrial tagging, …).",
    )
    vic_sub = sc_vic.add_subparsers(dest="_vic_cmd", required=True)

    sc_vas = vic_sub.add_parser(
        "address-scan",
        help=(
            "All Vicmap Address points per bbox tile (paginated), with in_industrial from "
            "zones/UDP polygons, DAPR line proximity, planning overlays, and LGA table join."
        ),
    )
    _add_vic_cache_flags(sc_vas)
    sc_vas.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Output directory; writes OUT_DIR/RUN_ID/shards/*.csv, progress/, checkpoints/, manifest.json",
    )
    sc_vas.add_argument(
        "--run-id",
        default=None,
        metavar="ID",
        help="Run folder name (default: timestamp). Reuse to resume progress/checkpoints.",
    )
    sc_vas.add_argument(
        "--bbox",
        default=None,
        metavar="MINLON,MINLAT,MAXLON,MAXLAT",
        help="Outer WGS84 bbox to tile (default: Victoria preset from vic_tiling)",
    )
    sc_vas.add_argument(
        "--tile-step",
        default="1.0",
        metavar="DEG[,DEG]",
        help="Tile size in degrees: one value (square) or lon,lat (default: 1.0)",
    )
    sc_vas.add_argument(
        "--land-source",
        choices=("zones", "udp", "both"),
        default="both",
        help="Which industrial polygon sets tag in_industrial (default: both)",
    )
    sc_vas.add_argument(
        "--zone-codes",
        default=None,
        help="Optional IN1Z,IN2Z,IN3Z filter when land-source includes zones",
    )
    sc_vas.add_argument(
        "--max-polygon-features",
        type=int,
        default=None,
        metavar="N",
        help="Cap industrial polygon features fetched per tile (default: no cap)",
    )
    sc_vas.add_argument(
        "--page-size",
        type=int,
        default=2000,
        metavar="N",
        help="Vicmap Address query page size (resultRecordCount, default: 2000)",
    )
    sc_vas.add_argument(
        "--max-addresses-per-tile",
        type=int,
        default=None,
        metavar="N",
        help="Stop after N address rows per tile (no .done checkpoint; resume continues)",
    )
    sc_vas.add_argument(
        "--address-layer-url",
        default=VICMAP_ADDRESS_FEATURE_LAYER,
        metavar="URL",
        help="Vicmap Address FeatureLayer URL (default: public layer 0)",
    )
    sc_vas.add_argument(
        "--address-out-fields",
        default=DEFAULT_ADDRESS_OUT_FIELDS,
        help="Comma-separated address outFields",
    )
    sc_vas.add_argument(
        "--layers",
        nargs="*",
        default=None,
        help="Rosetta line layers for DAPR proximity (default: 22 kV + 11 kV)",
    )
    sc_vas.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Nearest segments stored in nearest_top_json (default: 5)",
    )
    sc_vas.add_argument(
        "--within-m",
        type=float,
        default=None,
        metavar="METERS",
        help="Also set within_count for segments within this distance",
    )
    sc_vas.add_argument(
        "--within-limit",
        type=int,
        default=100,
        help="Max segments for --within-m (default: 100)",
    )
    sc_vas.add_argument(
        "--shortlist-max-m",
        type=float,
        default=None,
        metavar="METERS",
        help="shortlist=true when nearest_distance_m <= this value",
    )
    sc_vas.add_argument(
        "--overlay-delay",
        type=float,
        default=0.05,
        metavar="SEC",
        help="Sleep after each address overlay pair (default: 0.05)",
    )
    sc_vas.add_argument(
        "--skip-dapr",
        action="store_true",
        help="Do not build line index or nearest-line columns",
    )
    sc_vas.add_argument(
        "--skip-overlays",
        action="store_true",
        help="Skip bushfire / floodway overlay HTTP calls",
    )
    sc_vas.add_argument(
        "--scout-cache-dir",
        default=None,
        help=f"Rosetta layer cache (default: {DEFAULT_CACHE_DIR})",
    )
    sc_vas.add_argument(
        "--scout-refresh",
        action="store_true",
        help="Re-download Rosetta layers / rebuild line index",
    )
    sc_vas.add_argument(
        "--lga-summary-path",
        default=DEFAULT_LGA_SUMMARY_PATH,
        metavar="PORTAL_PATH",
    )
    sc_vas.add_argument(
        "--dapr-csv-refresh",
        action="store_true",
    )
    sc_vas.set_defaults(func=cmd_vic_address_scan)

    sc_vgr = vic_sub.add_parser(
        "gist-report",
        help=(
            "Emit a short Markdown summary of a vic address-scan run (manifest + optional shard stats); "
            "suitable to paste into a GitHub Gist or ticket."
        ),
    )
    sc_vgr.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Parent output directory (same as address-scan --out-dir)",
    )
    sc_vgr.add_argument(
        "--run-id",
        required=True,
        metavar="ID",
        help="Run folder name (same as address-scan --run-id)",
    )
    sc_vgr.add_argument(
        "--aggregate-shards",
        action="store_true",
        help="Scan every shard CSV for row counts, industrial/shortlist totals, top LGAs (slower)",
    )
    sc_vgr.add_argument(
        "-o",
        "--output",
        default="-",
        help='Markdown path, or "-" for stdout (default: -)',
    )
    sc_vgr.add_argument(
        "--gh-create",
        action="store_true",
        help="Create a GitHub Gist via `gh gist create` (requires `gh` and `gh auth login`)",
    )
    sc_vgr.add_argument(
        "--gh-filename",
        default="vic-address-scan-gist.md",
        metavar="NAME",
        help="Filename inside the gist when using --gh-create (default: vic-address-scan-gist.md)",
    )
    sc_vgr.add_argument(
        "--gh-desc",
        default=None,
        metavar="TEXT",
        help="Gist description passed to `gh gist create -d`",
    )
    sc_vgr.add_argument(
        "--gh-public",
        action="store_true",
        help="Create a public gist (default: secret gist)",
    )
    sc_vgr.add_argument(
        "--gh-web",
        action="store_true",
        help="Open the new gist in the browser (`gh gist create -w`)",
    )
    sc_vgr.set_defaults(func=cmd_vic_gist_report)

    return p


def cmd_vic_gist_report(args: argparse.Namespace) -> int:
    text = write_gist_for_run(
        args.out_dir,
        args.run_id,
        aggregate_shards=args.aggregate_shards,
    )
    gh_create = getattr(args, "gh_create", False)
    if args.output == "-" and not gh_create:
        sys.stdout.write(text)
        if not text.endswith("\n"):
            sys.stdout.write("\n")
    elif args.output != "-":
        args.output.write_text(text, encoding="utf-8")

    if gh_create:
        url = create_gist_with_gh(
            text,
            filename=args.gh_filename,
            description=args.gh_desc,
            public=args.gh_public,
            open_web=args.gh_web,
        )
        print(url)
    return 0


def cmd_vic_address_scan(args: argparse.Namespace) -> int:
    from datetime import datetime

    outer = parse_bbox(args.bbox) if args.bbox else VIC_STATE_BBOX
    ts_parts = args.tile_step.split(",")
    if len(ts_parts) == 1:
        tslon = tslat = float(ts_parts[0].strip())
    elif len(ts_parts) == 2:
        tslon = float(ts_parts[0].strip())
        tslat = float(ts_parts[1].strip())
    else:
        raise ValueError("vic address-scan: --tile-step must be DEG or DEG,DEG")
    run_id = args.run_id or datetime.now().strftime("%Y%m%d-%H%M%S")
    vic_cache = _vic_cache_dir(args)
    scout_cache = Path(args.scout_cache_dir) if args.scout_cache_dir else DEFAULT_CACHE_DIR
    layers = _parse_layers_arg(args.layers)
    zone_codes = (
        [z.strip() for z in args.zone_codes.split(",") if z.strip()]
        if getattr(args, "zone_codes", None)
        else None
    )
    cfg = AddressScanConfig(
        out_dir=args.out_dir,
        run_id=run_id,
        tile_step_lon=tslon,
        tile_step_lat=tslat,
        outer_bbox=outer,
        land_source=args.land_source,
        zone_codes=zone_codes,
        max_polygon_features=args.max_polygon_features,
        page_size=args.page_size,
        max_addresses_per_tile=args.max_addresses_per_tile,
        address_layer_url=args.address_layer_url,
        address_out_fields=args.address_out_fields,
        layers=layers,
        top_k=args.top_k,
        within_m=args.within_m,
        within_limit=args.within_limit,
        shortlist_max_m=args.shortlist_max_m,
        overlay_delay_s=args.overlay_delay,
        skip_dapr=args.skip_dapr,
        skip_overlays=args.skip_overlays,
        vic_cache_dir=vic_cache,
        vic_cache_ttl=None if args.vic_no_cache else args.vic_cache_ttl,
        vic_refresh=args.vic_refresh,
        vic_no_cache=args.vic_no_cache,
        scout_cache_dir=scout_cache,
        scout_refresh=args.scout_refresh,
    )

    with _client(args.timeout) as client:
        session = load_portal_session(client, args.base_url)
        text = fetch_dapr_csv_cached(
            client,
            session,
            args.lga_summary_path.strip(),
            cache_dir=vic_cache,
            cache_ttl_seconds=None if args.vic_no_cache else args.vic_cache_ttl,
            refresh=args.dapr_csv_refresh or args.vic_refresh,
            disk_cache=not args.vic_no_cache,
        )
        lga_index = index_lga_summary_by_type(text)
        manifest = run_address_scan(
            client,
            cfg,
            lga_index=lga_index,
            log=lambda msg: print(msg, file=sys.stderr),
        )

    summary = {
        "run_id": run_id,
        "manifest_path": str((args.out_dir / run_id / "manifest.json").resolve()),
        "disclaimer": ADDRESS_SCAN_DISCLAIMER,
        "tiles_completed": len(manifest.get("tiles_completed") or []),
        "tiles_total": manifest.get("tiles_total"),
        "addresses_written": manifest.get("addresses_written"),
        "in_industrial_count": manifest.get("in_industrial_count"),
    }
    print(json.dumps(summary, indent=2))
    return 0


def cmd_dc_screen(args: argparse.Namespace) -> int:
    from datetime import datetime

    if args.addresses == "shortlist" and args.shortlist_max_m is None:
        raise ValueError("dc-screen: --addresses shortlist requires --shortlist-max-m")
    outer = parse_bbox(args.bbox) if args.bbox else VIC_STATE_BBOX
    ts_parts = args.tile_step.split(",")
    if len(ts_parts) == 1:
        tslon = tslat = float(ts_parts[0].strip())
    elif len(ts_parts) == 2:
        tslon = float(ts_parts[0].strip())
        tslat = float(ts_parts[1].strip())
    else:
        raise ValueError("dc-screen: --tile-step must be DEG or DEG,DEG")
    run_id = args.run_id or datetime.now().strftime("%Y%m%d-%H%M%S")
    vic_cache = _vic_cache_dir(args)
    scout_cache = Path(args.scout_cache_dir) if args.scout_cache_dir else DEFAULT_CACHE_DIR
    layers = _parse_layers_arg(args.layers)
    zone_codes = (
        [z.strip() for z in args.zone_codes.split(",") if z.strip()]
        if getattr(args, "zone_codes", None)
        else None
    )
    cfg = DcScreenConfig(
        out_dir=args.out_dir,
        run_id=run_id,
        tile_step_lon=tslon,
        tile_step_lat=tslat,
        outer_bbox=outer,
        land_source=args.land_source,
        zone_codes=zone_codes,
        max_features_per_tile=args.max_features_per_tile,
        layers=layers,
        top_k=args.top_k,
        within_m=args.within_m,
        within_limit=args.within_limit,
        shortlist_max_m=args.shortlist_max_m,
        with_parcels=args.with_parcels,
        address_mode=args.addresses,
        overlay_delay_s=args.overlay_delay,
        parcel_layer_url=args.parcel_layer_url,
        parcel_out_fields=args.parcel_out_fields,
        max_parcel_matches=args.max_parcel_matches,
        address_layer_url=args.address_layer_url,
        address_out_fields=args.address_out_fields,
        max_address_matches=args.max_address_matches,
        max_address_point_matches=args.max_address_point_matches,
        address_point_buffer_m=args.address_point_buffer_m,
        no_address_point_fallback=args.no_address_point_fallback,
        vic_cache_dir=vic_cache,
        vic_cache_ttl=None if args.vic_no_cache else args.vic_cache_ttl,
        vic_refresh=args.vic_refresh,
        vic_no_cache=args.vic_no_cache,
        scout_cache_dir=scout_cache,
        scout_refresh=args.scout_refresh,
        base_url=args.base_url,
        lga_summary_path=args.lga_summary_path.strip(),
        dapr_csv_refresh=args.dapr_csv_refresh,
    )

    with _client(args.timeout) as client:

        def load_dapr_index():
            session = load_portal_session(client, args.base_url)
            text = fetch_dapr_csv_cached(
                client,
                session,
                cfg.lga_summary_path,
                cache_dir=vic_cache,
                cache_ttl_seconds=None if args.vic_no_cache else args.vic_cache_ttl,
                refresh=cfg.dapr_csv_refresh or args.vic_refresh,
                disk_cache=not args.vic_no_cache,
            )
            return index_lga_summary_by_type(text)

        manifest = run_dc_screen(
            client,
            cfg,
            load_dapr_index=load_dapr_index,
            log=lambda msg: print(msg, file=sys.stderr),
        )

    summary = {
        "run_id": run_id,
        "manifest_path": str((args.out_dir / run_id / "manifest.json").resolve()),
        "disclaimer": DC_SCREEN_DISCLAIMER,
        "tiles_completed": len(manifest.get("tiles_completed") or []),
        "tiles_total": manifest.get("tiles_total"),
        "candidates_scored": manifest.get("candidates_scored"),
    }
    print(json.dumps(summary, indent=2))
    return 0


def _load_report_json_entries(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("report --from-json expects a JSON array of site objects")
    for i, item in enumerate(data):
        if not isinstance(item, dict):
            raise ValueError(f"report entry {i} must be a JSON object")
        q = item.get("query")
        if not isinstance(q, dict):
            raise ValueError(f"report entry {i} must have a query object")
        if "lat" not in q or "lon" not in q:
            raise ValueError(f"report entry {i} query must include lat and lon")
        float(q["lat"])
        float(q["lon"])
    return data


def cmd_report(args: argparse.Namespace) -> int:
    entries = _load_report_json_entries(args.from_json)
    vic_cache = _vic_cache_dir(args)
    with _client(args.timeout) as client:
        out = build_report(
            entries,
            client,
            base_url=args.base_url,
            lga_summary_path=args.lga_summary_path.strip(),
            vic_cache_dir=vic_cache,
            vic_cache_ttl=None if args.vic_no_cache else args.vic_cache_ttl,
            vic_refresh=args.vic_refresh,
            vic_no_cache=args.vic_no_cache,
            dapr_csv_refresh=args.dapr_csv_refresh,
        )

    if args.format == "json":
        payload = {"entries": out, "disclaimer": REPORT_DISCLAIMER}
        text = json.dumps(payload, indent=2)
        if args.output == "-":
            sys.stdout.write(text)
            if not text.endswith("\n"):
                sys.stdout.write("\n")
        else:
            args.output.write_text(text, encoding="utf-8")
        return 0

    import csv
    import io

    cols, rows = report_to_csv_rows(out)
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=cols, extrasaction="ignore")
    w.writeheader()
    for row in rows:
        w.writerow({k: row.get(k, "") for k in cols})
    csv_text = buf.getvalue()
    if args.output == "-":
        sys.stdout.write("# ")
        sys.stdout.write(REPORT_DISCLAIMER.replace("\n", " ").strip())
        sys.stdout.write("\n")
        sys.stdout.write(csv_text)
    else:
        args.output.write_text(
            "# " + REPORT_DISCLAIMER.replace("\n", " ").strip() + "\n" + csv_text,
            encoding="utf-8",
        )
    return 0


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if hasattr(args, "output") and args.output != "-":
        args.output = Path(args.output)
    if getattr(args, "sites", None) is not None:
        args.sites = Path(args.sites)
    if getattr(args, "from_json", None) is not None:
        args.from_json = Path(args.from_json)
    if getattr(args, "out_dir", None) is not None:
        args.out_dir = Path(args.out_dir)
    if getattr(args, "enrich_parcels", None) is None:
        args.enrich_parcels = False

    try:
        rc = args.func(args)
    except (RuntimeError, ValueError) as e:
        print(f"dapr: {e}", file=sys.stderr)
        raise SystemExit(1) from e
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
