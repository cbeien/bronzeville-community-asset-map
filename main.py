"""
main.py — Orchestration script for the Bronzeville Community Asset Map.

Run:
    python main.py

Prerequisites:
    pip install -r requirements.txt

What it does
------------
Step 1 — Fetch transportation data from the City of Chicago Data Portal
         (CTA Rail, CTA Bus, Metra — 6 SODA datasets) and the community
         area boundary polygons.

Step 2 — Fetch community assets from two public sources:
         • City of Chicago Data Portal (landmarks, business licenses,
           public schools, parks, libraries)
         • OpenStreetMap Overpass API (restaurants, worship, historic
           landmarks, private schools, healthcare, cultural institutions,
           social services)
         Merge and deduplicate, tagging each record with its source.

Step 3 — Apply address geocoding (Census Bureau batch → Nominatim fallback)
         to any records that lack coordinates (rare — most SODA and all
         OSM data already includes lat/lon).

Step 4 — Build the Folium interactive map with all layers.

Step 5 — Export four deliverables:
         • bronzeville_map.html    — self-contained Folium map
         • bronzeville_assets.xlsx — multi-sheet Excel workbook
         • index.html              — standalone Leaflet.js website
         • data.js                 — JavaScript data file
         • bronzeville_summary.html — formatted HTML summary tables

Environment variables (all optional)
-------------------------------------
SODA_APP_TOKEN   — Socrata app token (raises rate limit; no key required).
                   Register at https://data.cityofchicago.org/profile/app_tokens
"""

from __future__ import annotations

import sys
import logging
import pandas as pd

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bronzeville_map.log", mode="w", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# Local modules
from config         import COMMUNITY_AREAS
from fetch_data     import fetch_all_transportation, fetch_community_boundaries
from fetch_assets   import fetch_all_assets
from fetch_parcels  import fetch_building_footprints, fetch_chicago_building_footprints, match_assets_to_footprints
from fetch_zoning   import fetch_zoning_districts, classify_pd_zones, clip_zoning_to_boundary
from fetch_vacant   import fetch_vacant_lots
from geocode        import geocode_dataframe
from build_map      import build_full_map
from export         import (
    export_html_map,
    export_excel,
    export_index_html,
    export_summary_html,
)


def _clip_to_boundary(assets: dict[str, pd.DataFrame],
                       community_features: list[dict]) -> dict[str, pd.DataFrame]:
    """
    Drop any asset rows whose coordinates fall outside the Bronzeville
    community area polygons (Douglas, Grand Boulevard, Kenwood, Washington Park).
    Uses shapely for point-in-polygon testing.
    """
    if not community_features:
        logger.warning("No community boundary available — skipping clip.")
        return assets

    try:
        from shapely.geometry import shape, Point
        from shapely.ops import unary_union

        # Merge the 4 community area polygons into one unified boundary
        boundary = unary_union([
            shape(f["geometry"])
            for f in community_features
            if f.get("geometry")
        ])
    except Exception as exc:
        logger.warning(f"Could not build boundary for clipping: {exc}")
        return assets

    clipped = {}
    for key, df in assets.items():
        if df.empty:
            clipped[key] = df
            continue

        def _inside(row):
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                return boundary.contains(Point(lon, lat))
            except Exception:
                return False

        mask = df.apply(_inside, axis=1)
        before = len(df)
        df = df[mask].reset_index(drop=True)
        logger.info(f"Clip {key}: {before} → {len(df)} records inside boundary")
        clipped[key] = df

    return clipped


def _apply_geocoding(assets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Run geocoding on any asset DataFrame rows that are missing coordinates.
    Most records already have lat/lon from SODA or OSM, so this is a safety net.
    """
    geocoded = {}
    for key, df in assets.items():
        if df.empty:
            geocoded[key] = df
            continue

        needs_geocoding = (
            df.get("latitude", pd.Series()).isna() |
            df.get("longitude", pd.Series()).isna()
        ).any()

        if needs_geocoding:
            n_missing = (df.get("latitude", pd.Series()).isna()).sum()
            print(f"    Geocoding {n_missing} {key} records without coordinates …")
            df = geocode_dataframe(df, address_col="Address")

        geocoded[key] = df
    return geocoded


def _print_zone_summary(zone_df: pd.DataFrame) -> None:
    """Pretty-print the zone summary table to the console."""
    print("\n" + "=" * 70)
    print("  Zone Asset Count Summary")
    print("=" * 70)
    print(zone_df.to_string(index=False))
    print("=" * 70)


def main() -> None:
    print("\n" + "=" * 70)
    print("  Bronzeville Community Asset Map")
    print("=" * 70)

    # ── Step 1: Transportation data ───────────────────────────────────────────
    print("\n[STEP 1] Fetching transportation data …")
    transport = fetch_all_transportation()

    print("\n[STEP 1b] Fetching community area boundaries …")
    community_features = fetch_community_boundaries(tuple(COMMUNITY_AREAS.keys()))
    print(f"  {len(community_features)} community area polygons")

    # ── Step 2: Community assets ──────────────────────────────────────────────
    print("\n[STEP 2] Fetching community assets (SODA + OpenStreetMap) …")
    assets = fetch_all_assets()

    # ── Step 3: Geocoding fallback ────────────────────────────────────────────
    print("\n[STEP 3] Applying geocoding fallback where needed …")
    assets = _apply_geocoding(assets)

    # ── Step 3b: Clip to Bronzeville boundary ─────────────────────────────────
    print("\n[STEP 3b] Clipping assets to Bronzeville community boundary …")
    assets = _clip_to_boundary(assets, community_features)
    total = sum(len(v) for v in assets.values())
    print(f"  {total} assets inside boundary")

    # Also clip transport point layers (bus stops, rail stations, metra stops)
    # Leave line layers (rail lines, bus routes) unclipped so they render fully.
    POINT_TRANSPORT_KEYS = {"cta_rail_stations", "cta_bus_stops", "metra_stations", "divvy_stations", "bike_racks"}
    transport_points = {k: v for k, v in transport.items() if k in POINT_TRANSPORT_KEYS}
    transport_lines  = {k: v for k, v in transport.items() if k not in POINT_TRANSPORT_KEYS}
    transport_points = _clip_to_boundary(transport_points, community_features)
    transport = {**transport_lines, **transport_points}

    # ── Step 4: Match assets to parcel lots and building footprints ───────────
    print("\nStep 4 — Matching assets to parcel lots and building footprints …")
    parcel_footprints = fetch_building_footprints()
    print(f"  Fetched {len(parcel_footprints)} Cook County parcel polygons")
    building_footprints = fetch_chicago_building_footprints()
    print(f"  Fetched {len(building_footprints)} Chicago building footprints")
    assets = match_assets_to_footprints(assets, parcel_footprints, building_footprints)
    parcel_matched = sum(
        df["geometry"].notna().sum()
        for df in assets.values()
        if "geometry" in df.columns
    )
    building_matched = sum(
        df["building_geom"].notna().sum()
        for df in assets.values()
        if "building_geom" in df.columns
    )
    print(f"  Matched {parcel_matched} assets to parcel lots")
    print(f"  Matched {building_matched} assets to building footprints")

    # ── Step 4b: Fetch zoning districts ──────────────────────────────────────
    print("\n[STEP 4b] Fetching Chicago zoning districts …")
    zoning_districts = fetch_zoning_districts()
    print(f"  {len(zoning_districts)} zoning polygons fetched")
    print("  Classifying Planned Development zones by spatial context …")
    zoning_districts = classify_pd_zones(zoning_districts)
    print("  Clipping zoning to Bronzeville community boundary …")
    zoning_districts = clip_zoning_to_boundary(zoning_districts, community_features)

    # ── Step 4c: Fetch vacant lots ────────────────────────────────────────────
    print("\n[STEP 4c] Fetching vacant lots & storefronts …")
    vacant_lots = fetch_vacant_lots(community_features)
    print(f"  {len(vacant_lots)} vacant lots fetched")

    # ── Step 5: Build Folium map ──────────────────────────────────────────────
    print("\n[STEP 5] Building Folium interactive map …")
    folium_map = build_full_map(
        transport=transport,
        assets=assets,
        community_features=community_features,
    )

    # ── Step 6: Export all deliverables ───────────────────────────────────────
    print("\n[STEP 6] Exporting deliverables …")

    export_html_map(folium_map)

    zone_df = export_excel(assets, transport)

    export_index_html(assets, transport, community_features, zoning=zoning_districts, vacant_lots=vacant_lots)

    export_summary_html(assets)

    # ── Summary ───────────────────────────────────────────────────────────────
    _print_zone_summary(zone_df)

    print("\n" + "=" * 70)
    print("  Done! Deliverables written to the current directory:")
    print("    bronzeville_map.html    — Folium interactive map")
    print("    index.html              — Leaflet.js website (self-contained)")
    print("    data.js                 — JavaScript data file")
    print("    bronzeville_assets.xlsx — Multi-sheet Excel workbook")
    print("    bronzeville_summary.html— HTML summary tables")
    print("    bronzeville_map.log     — Full run log")
    print()
    print("  To view index.html locally without a server:")
    print("    Firefox — open the file directly (File → Open File)")
    print("    Chrome  — run: python -m http.server  then open http://localhost:8000")
    print("=" * 70)


if __name__ == "__main__":
    main()
