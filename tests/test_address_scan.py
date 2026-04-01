"""Address scan row shape (no network)."""

from __future__ import annotations

from pathlib import Path

import httpx

from dapr_portal.address_scan import CSV_FIELDNAMES, AddressScanConfig, build_address_row
from dapr_portal.industrial_geometry import IndustrialTagIndex


def _minimal_cfg() -> AddressScanConfig:
    return AddressScanConfig(
        out_dir=Path("."),
        run_id="test",
        tile_step_lon=1.0,
        tile_step_lat=1.0,
        outer_bbox=(0.0, 0.0, 1.0, 1.0),
        land_source="both",
        zone_codes=None,
        max_polygon_features=None,
        page_size=100,
        max_addresses_per_tile=None,
        address_layer_url="https://example/layer/0",
        address_out_fields="ezi_address,property_pfi,lga_code",
        layers=(),
        top_k=3,
        within_m=None,
        within_limit=100,
        shortlist_max_m=None,
        overlay_delay_s=0.0,
        skip_dapr=True,
        skip_overlays=True,
        vic_cache_dir=Path("."),
        vic_cache_ttl=None,
        vic_refresh=False,
        vic_no_cache=True,
        scout_cache_dir=Path("."),
        scout_refresh=False,
    )


def test_build_address_row_has_expected_columns() -> None:
    transport = httpx.MockTransport(lambda r: httpx.Response(500))
    with httpx.Client(transport=transport) as client:
        row = build_address_row(
            tile_id="0_0",
            lon=145.0,
            lat=-37.0,
            props={"ezi_address": "1 TEST ST", "property_pfi": "42", "lga_code": None},
            tag_index=IndustrialTagIndex([], []),
            tree=None,
            attrs=[],
            geoms=[],
            cfg=_minimal_cfg(),
            lga_index={},
            client=client,
        )
    for col in CSV_FIELDNAMES:
        assert col in row
