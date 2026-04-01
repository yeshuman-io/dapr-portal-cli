# Transmission and sub-transmission data (beyond DAPR Rosetta)

The **`dapr scout --layers`** path uses **CitiPower/Powercor Rosetta** polyline `.txt` files. Those exports cover **distribution** voltages the portal embeds as download URLs (see **`dapr list-layers`**). **66 kV+** and **NEM transmission** layers may appear on the DAPR **map** as UI hints but are **not** served as the same Rosetta `.txt` assets on the CDN this CLI uses.

This page lists **other public or semi-public sources** useful for **higher-voltage** or **statewide transmission** context. Licensing and refresh cadence differ from DAPR; verify terms before production use.

## Victoria — government / spatial portals

| Source | What it is | Access notes |
|--------|------------|--------------|
| **DEECA MapShare — Electricity infrastructure** | Vic planning-related GIS; includes an **Electricity Transmission Lines** layer (metadata often cites ~6,500 km in Victoria, voltage/feature attributes). | ArcGIS REST: `plan-gis.mapshare.vic.gov.au` — service **Radius/Electricity_infrastructure** (layer id **8** for transmission lines in common configs). Query with `geometry` / `where` like other Esri layers. **Check current service URL and fields in the REST directory** — services move. |
| **Vicmap Property — Easement Line** | Easement **lines** (approved/proposed); subset of easements captured. | [Data Vic — Vicmap Property Easement Line](https://discover.data.vic.gov.au/dataset/vicmap-property-easement-line): SHP, GDB, WMS/WFS, etc. **CC BY** (verify dataset page). Easements approximate corridors, not always coincident with every conductor. |
| **VicGrid / REZ infrastructure** | Transmission-related infrastructure for renewable energy zone planning. | Esri FeatureServer published under VicGrid programs (search Data Vic / ArcGIS Online for current endpoint). Good for **planned** assets; not a full DNSP as-built. |

## Australia — national coverage

| Source | What it is | Access notes |
|--------|------------|--------------|
| **Geoscience Australia — National electricity infrastructure** | National **transmission** polylines (historically aligned to AEMO diagrams; revised over time). | ArcGIS REST e.g. `services.ga.gov.au/gis/rest/services/National_Electricity_Infrastructure/MapServer` — see GA catalog for **Electricity Transmission Lines** layer id. Also packaged via **AURIN** / research data portals (**CC BY** typical). Resolution is **national strategic**, not DNSP construction drawings. |
| **Open Net Zero / aggregators** | Metadata and links to GA and related layers. | Useful for discovery; always follow through to the **canonical publisher** for licence and updates. |

## Market / DNSP — maps vs downloadable GIS

| Source | What it is | Access notes |
|--------|------------|--------------|
| **AEMO** | **Interactive maps**, network data policies, outage and rating tables. | [AEMO map](https://aemo.com.au/aemo/apps/visualisations/map.html), [Network data](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem/network-data). **Bulk GIS line geometry** for the whole NEM is **not** the same as Rosetta `.txt`; participant / policy channels may apply for detailed feeds. |
| **AusNet Services** | **GridView** map portal; **AusNet DAPR** (Rosetta-style) for their network. | [GridView](https://ausnetservices.com.au/electricity/network-information/gridview-portal), [dapr.ausnetservices.com.au](https://dapr.ausnetservices.com.au/). Similar **web map** model to Powercor DAPR; **shapefile bulk download** is not assumed public—contact AusNet if you need GIS exports. |
| **Other TNSPs / DNSPs** | Transmission in other states or shared interconnectors. | Each publishes **viewer** or **regulatory** datasets; integrate **per publisher** if you leave Victoria. |

## How this relates to `dapr-portal-cli`

- **No code path today** ingests MapShare, GA, or Vicmap easements into `build_line_index` (that pipeline expects **Rosetta-encoded polylines** in [`parse_layer_lines`](../scout.py)).
- **Feasible extensions** (separate work):
  1. **New module** (e.g. `transmission_query.py`) — Esri `query` / WFS bbox → GeoJSON → reproject to **`EPSG:7855`** (or reuse metres CRS) → Shapely `LineString` → same **STRtree + nearest** pattern as scout.
  2. **Offline** — user downloads SHP/GeoJSON from Data Vic / GA and a small **`dapr transmission-nearest --geojson …`** style command loads geometries locally.
- **Attribution**: cite **DEECA**, **Data Vic**, **Geoscience Australia**, **AEMO**, and **DNSPs** per their terms when you publish maps or reports.

## Quick verification commands (manual)

```bash
# What Rosetta .txt the Powercor DAPR portal actually embeds
dapr list-layers --json

# Probe a hint name (often 404 for transmission-only hints)
dapr get-layer 66kV_CitiPower_Powercor_Lines.txt -o /dev/null
```

For MapShare / GA layers, use **browser** or **`curl`** against the layer’s `…/query?f=geojson&where=1=1&returnGeometry=true` (respect `maxRecordCount`, paginate).

---

*This document is indicative only—not legal, planning, or connection advice. Service URLs and layer ids change; confirm on the publisher’s site.*
