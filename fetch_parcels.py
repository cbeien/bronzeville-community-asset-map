"""
fetch_parcels.py — Fetch Cook County parcel lot boundary polygons and Chicago
building footprints, and match community assets to them via point-in-polygon.

Dataset 1: 77tz-riq7 — "ccgisdata - Parcel 2021" (Cook County Data Portal)
  Each record has a the_geom MultiPolygon/Polygon field representing the full
  property lot boundary (including yard space, not just the building footprint).

Dataset 2: syp8-uezg — "Building Footprints (current)" (City of Chicago SODA)
  Each record has a the_geom Polygon field representing the actual building
  footprint (roof outline), much tighter than parcel lot boundaries.
"""

from __future__ import annotations

import json
import logging
import requests
import pandas as pd
from shapely.geometry import shape, Point

from cache import load_cache, save_cache
from config import BRONZEVILLE_BBOX

logger = logging.getLogger(__name__)

SODA_BASE       = "https://datacatalog.cookcountyil.gov/resource"
CHICAGO_SODA    = "https://data.cityofchicago.org/resource"
PARCEL_LOTS_ID  = "77tz-riq7"
BLDG_FOOTPRINTS_ID = "syp8-uezg"

_HEADERS = {"User-Agent": "BronzevilleCommunityAssetMap/1.0 (university-research)"}


def fetch_building_footprints() -> list[dict]:
    """
    Fetch Cook County parcel lot boundary polygons for the Bronzeville bbox.

    Uses dataset 77tz-riq7 ("ccgisdata - Parcel 2021") which contains full
    property lot polygons (parcel boundaries including yard space).

    Returns a list of dicts, each with keys:
      - 'geometry': GeoJSON geometry dict (Polygon or MultiPolygon)
      - 'address': str (PIN10 identifier, since street addresses are not in this dataset)
    """
    cached = load_cache("parcel_lots")
    if cached is not None:
        logger.info(f"Parcel Lots: {len(cached)} polygons loaded from cache")
        return cached

    bb = BRONZEVILLE_BBOX

    # Build a WKT MULTIPOLYGON bounding box for the SODA intersects() filter
    wkt = (
        f"MULTIPOLYGON ((("
        f"{bb['lon_min']} {bb['lat_min']}, "
        f"{bb['lon_max']} {bb['lat_min']}, "
        f"{bb['lon_max']} {bb['lat_max']}, "
        f"{bb['lon_min']} {bb['lat_max']}, "
        f"{bb['lon_min']} {bb['lat_min']}"
        f")))"
    )

    url = f"{SODA_BASE}/{PARCEL_LOTS_ID}.json"
    where = f"intersects(the_geom, '{wkt}')"

    # Paginate through all parcels — the bbox may contain 10k+ records
    PAGE = 5000
    all_rows: list[dict] = []
    offset = 0
    while True:
        params = {"$limit": PAGE, "$offset": offset, "$where": where}
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=90)
            resp.raise_for_status()
            page = resp.json()
        except Exception as exc:
            logger.warning(f"Parcel Lots SODA error (offset {offset}): {exc}")
            break
        all_rows.extend(page)
        logger.info(f"  Parcels page offset={offset}: {len(page)} rows (total so far: {len(all_rows)})")
        if len(page) < PAGE:
            break  # last page
        offset += PAGE

    rows = all_rows
    results = []
    for row in rows:
        geom = row.get("the_geom")
        if not geom:
            continue

        # the_geom may come back as a string in some SODA responses
        if isinstance(geom, str):
            try:
                geom = json.loads(geom)
            except Exception:
                continue

        if not isinstance(geom, dict) or "type" not in geom:
            continue

        # Use PIN10 as the identifier (street addresses are not in this dataset)
        pin10       = row.get("pin10", "").strip()
        municipality = row.get("municipality", "").strip()
        parts       = [p for p in [pin10, municipality] if p]
        address     = " ".join(parts)

        results.append({
            "geometry": geom,
            "address":  address,
        })

    logger.info(f"Parcel Lots: {len(results)} polygons fetched for Bronzeville bbox")
    save_cache("parcel_lots", results)
    return results


def fetch_chicago_building_footprints() -> list[dict]:
    """
    Fetch Chicago building footprint polygons (actual roof outlines) for the
    Bronzeville bbox from the City of Chicago SODA API.

    Dataset: syp8-uezg — "Building Footprints (current)"
    Each record has a the_geom Polygon/MultiPolygon representing the building
    roof footprint — significantly tighter than Cook County parcel lot polygons.

    Returns a list of dicts, each with keys:
      - 'geometry': GeoJSON geometry dict (Polygon or MultiPolygon)
      - 'address': str (street address when available)
    """
    cached = load_cache("building_footprints")
    if cached is not None:
        logger.info(f"Building Footprints: {len(cached)} polygons loaded from cache")
        return cached

    bb = BRONZEVILLE_BBOX

    wkt = (
        f"MULTIPOLYGON ((("
        f"{bb['lon_min']} {bb['lat_min']}, "
        f"{bb['lon_max']} {bb['lat_min']}, "
        f"{bb['lon_max']} {bb['lat_max']}, "
        f"{bb['lon_min']} {bb['lat_max']}, "
        f"{bb['lon_min']} {bb['lat_min']}"
        f")))"
    )

    url = f"{CHICAGO_SODA}/{BLDG_FOOTPRINTS_ID}.json"
    where = f"intersects(the_geom, '{wkt}')"

    PAGE = 5000
    all_rows: list[dict] = []
    offset = 0
    while True:
        params = {"$limit": PAGE, "$offset": offset, "$where": where}
        try:
            resp = requests.get(url, params=params, headers=_HEADERS, timeout=90)
            resp.raise_for_status()
            page = resp.json()
        except Exception as exc:
            logger.warning(f"Building Footprints SODA error (offset {offset}): {exc}")
            break
        all_rows.extend(page)
        logger.info(
            f"  Building footprints page offset={offset}: "
            f"{len(page)} rows (total so far: {len(all_rows)})"
        )
        if len(page) < PAGE:
            break
        offset += PAGE

    results = []
    for row in all_rows:
        geom = row.get("the_geom")
        if not geom:
            continue

        if isinstance(geom, str):
            try:
                geom = json.loads(geom)
            except Exception:
                continue

        if not isinstance(geom, dict) or "type" not in geom:
            continue

        address = str(row.get("address", "")).strip()
        results.append({
            "geometry": geom,
            "address":  address,
        })

    logger.info(f"Building Footprints: {len(results)} polygons fetched for Bronzeville bbox")
    save_cache("building_footprints", results)
    return results


def _build_shapes(footprints: list[dict]) -> list[tuple]:
    """Pre-build shapely shapes from a list of footprint dicts."""
    shapes: list[tuple] = []
    for fp in footprints:
        try:
            s = shape(fp["geometry"])
            shapes.append((s, fp["geometry"]))
        except Exception as exc:
            logger.debug(f"Could not parse footprint geometry: {exc}")
    return shapes


def _match_point(pt, shapes: list[tuple], nearby_deg: float):
    """
    Return the best-matching GeoJSON geometry dict for a point, or None.

    Pass 1: find all shapes containing the point; return the largest by area.
    Pass 2: find all shapes within nearby_deg degrees; return the largest.
    """
    containing = [(s.area, geom_dict) for (s, geom_dict) in shapes if s.contains(pt)]
    if containing:
        return max(containing, key=lambda x: x[0])[1]

    nearby = [(s.area, geom_dict) for (s, geom_dict) in shapes
              if s.distance(pt) < nearby_deg]
    if nearby:
        return max(nearby, key=lambda x: x[0])[1]

    return None


def match_assets_to_footprints(
    assets: dict[str, pd.DataFrame],
    parcel_footprints: list[dict],
    building_footprints: list[dict] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    For each asset point (lat/lon), spatially match it to:
      - a Cook County parcel lot polygon   → stored in 'geometry' column
      - a Chicago building footprint       → stored in 'building_geom' column

    Assets not matched to any polygon get the respective column set to None.

    Uses shapely for point-in-polygon. Pre-builds shapely shapes once per
    dataset for performance — O(n*m) but fast for ~10k polygons and ~5k assets.
    """
    # Degrees to metres conversion (approx at Chicago latitude ~41.8°N)
    # 1° lat ≈ 111,000 m; 1° lon ≈ 82,000 m  →  use 95,000 as average
    NEARBY_DEG = 30 / 95_000  # ~30 metres expressed in degrees

    if not parcel_footprints:
        logger.warning("No parcel footprints available — skipping geometry match.")
        for key in assets:
            df = assets[key].copy()
            df["geometry"]      = None
            df["building_geom"] = None
            assets[key] = df
        return assets

    parcel_shapes   = _build_shapes(parcel_footprints)
    building_shapes = _build_shapes(building_footprints) if building_footprints else []
    logger.info(
        f"Pre-built {len(parcel_shapes)} parcel shapes "
        f"and {len(building_shapes)} building shapes for matching"
    )

    result = {}
    for key, df in assets.items():
        if df.empty:
            df = df.copy()
            df["geometry"]      = None
            df["building_geom"] = None
            result[key] = df
            continue

        geoms:         list = []
        building_geoms: list = []

        for _, row in df.iterrows():
            parcel_match   = None
            building_match = None
            try:
                # Support both 'lat'/'lon' and 'latitude'/'longitude' column names
                lat = float(row.get("lat") or row.get("latitude"))
                lon = float(row.get("lon") or row.get("longitude"))
                pt  = Point(lon, lat)

                parcel_match   = _match_point(pt, parcel_shapes,   NEARBY_DEG)
                building_match = _match_point(pt, building_shapes, NEARBY_DEG) if building_shapes else None

            except (TypeError, ValueError):
                pass

            geoms.append(parcel_match)
            building_geoms.append(building_match)

        df = df.copy()
        df["geometry"]      = geoms
        df["building_geom"] = building_geoms
        result[key] = df

        parcel_matched   = sum(1 for g in geoms if g is not None)
        building_matched = sum(1 for g in building_geoms if g is not None)
        logger.info(
            f"  {key}: {parcel_matched}/{len(df)} parcel matches, "
            f"{building_matched}/{len(df)} building footprint matches"
        )

    return result
