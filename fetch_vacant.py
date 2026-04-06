"""
fetch_vacant.py — Fetch vacant / abandoned properties in Bronzeville from
two public sources:

  1. Cook County Assessor (tx2p-k2g9)
       class 1xx  — legally vacant land (no improvements)

  2. Hardcoded manually-verified vacant storefronts
       Small curated list of confirmed vacancies from field observation.
       Add entries to _HARDCODED_STOREFRONTS as verified.

All points are clipped to the Bronzeville community area boundary (Douglas,
Grand Boulevard, Kenwood, Washington Park).

Uses urllib.request throughout (requests has DNS failures for external
domains on this machine).
"""
from __future__ import annotations

import logging
import urllib.request
import urllib.parse
import json

from cache import load_cache, save_cache

logger = logging.getLogger(__name__)

_COOK_URL = "https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.json"

# Cook County: community area names in UPPERCASE as stored in the dataset
_COOK_AREAS = ("DOUGLAS", "GRAND BOULEVARD", "KENWOOD", "WASHINGTON PARK")

# Manually verified vacant storefronts — add new entries as confirmed in the field
_HARDCODED_STOREFRONTS = [
    {"address": "4200 S King Dr, Chicago, IL", "lat": 41.8186, "lon": -87.6160},
    # Add more here as verified
]


def _urllib_get(url: str, params: dict, timeout: int = 30) -> list[dict]:
    qs  = urllib.parse.urlencode(params)
    req = urllib.request.Request(
        f"{url}?{qs}",
        headers={"User-Agent": "BronzevilleAssetMap/1.0 (university-research)"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _fetch_cook_county() -> list[dict]:
    """Fetch class 1xx (vacant land) parcels from Cook County Assessor."""
    area_list = ",".join(f"'{a}'" for a in _COOK_AREAS)
    rows = _urllib_get(_COOK_URL, {
        "$where":  f"year = '2022' AND chicago_community_area_name in ({area_list})",
        "$select": "prop_address_full,class,lat,lon",
        "$limit":  10000,
    })
    logger.info(f"Cook County: {len(rows)} total parcels fetched")

    lots = []
    for row in rows:
        cls = (row.get("class") or "").strip()
        if not cls or not cls.startswith("1"):
            continue
        try:
            lat = float(row["lat"])
            lon = float(row["lon"])
        except (KeyError, TypeError, ValueError):
            continue

        lots.append({
            "address":    (row.get("prop_address_full") or "").strip().title(),
            "class_code": cls,
            "type":       "vacant_land",
            "status":     "Vacant Land",
            "lat":        lat,
            "lon":        lon,
        })

    logger.info(f"Cook County filtered: {len(lots)} vacant land parcels")
    return lots


def _clip_to_boundary(lots: list[dict], community_features: list[dict]) -> list[dict]:
    """
    Drop any vacant lot rows whose coordinates fall outside the Bronzeville
    community area polygons (Douglas, Grand Boulevard, Kenwood, Washington Park).
    Uses shapely for point-in-polygon testing.
    """
    if not community_features or not lots:
        return lots

    try:
        from shapely.geometry import shape, Point
        from shapely.ops import unary_union

        # Merge the 4 community area polygons into one unified boundary
        boundary = unary_union([
            shape(f["geometry"])
            for f in community_features
            if f.get("geometry")
        ])

        clipped = []
        for lot in lots:
            try:
                pt = Point(lot["lon"], lot["lat"])
                if boundary.contains(pt):
                    clipped.append(lot)
            except:
                pass

        logger.info(f"Clipped vacant lots: {len(lots)} → {len(clipped)} inside boundary")
        return clipped
    except Exception as exc:
        logger.warning(f"Could not clip to boundary: {exc}")
        return lots


def _hardcoded_storefronts() -> list[dict]:
    """Return manually verified vacant storefronts from the curated list."""
    return [
        {
            "address":    h["address"],
            "class_code": "",
            "type":       "vacant_storefront",
            "status":     "Vacant Storefront (Verified)",
            "lat":        h["lat"],
            "lon":        h["lon"],
        }
        for h in _HARDCODED_STOREFRONTS
    ]


def fetch_vacant_lots(community_features: list[dict] = None) -> list[dict]:
    """
    Return all vacant properties in Bronzeville as a flat list.

    Each record has: address, class_code, type, status, lat, lon
      type ∈ {"vacant_land", "vacant_storefront"}

    Sources:
      - Cook County class 1xx: bare vacant land parcels (2022 tax year)
      - Hardcoded curated list: manually verified vacant storefronts

    All points are clipped to the Bronzeville community area boundary.
    """
    cached = load_cache("vacant_lots_v4")
    if cached is not None:
        logger.info(f"Vacant lots: {len(cached)} records loaded from cache")
        return cached

    lots: list[dict] = []

    try:
        lots.extend(_fetch_cook_county())
    except Exception as exc:
        logger.warning(f"Cook County vacant fetch error: {exc}")

    lots.extend(_hardcoded_storefronts())

    # Clip to Bronzeville boundary if features provided
    if community_features:
        lots = _clip_to_boundary(lots, community_features)

    by_type = {t: sum(1 for l in lots if l["type"] == t)
               for t in ("vacant_land", "vacant_storefront")}
    logger.info(
        f"Vacant lots total: {len(lots)} records — "
        + ", ".join(f"{t}: {n}" for t, n in by_type.items())
    )

    save_cache("vacant_lots_v4", lots)
    return lots
