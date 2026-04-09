"""
fetch_vacant.py — Fetch vacant / abandoned properties in Bronzeville from
three public sources:

  1. Cook County Assessor (tx2p-k2g9)
       class 1xx  — legally vacant land (no improvements)

  2. Hardcoded manually-verified vacant storefronts
       Small curated list of confirmed vacancies from field observation.
       Add entries to _HARDCODED_STOREFRONTS as verified.

  3. City of Chicago Vacant Building Registry (PDF)
       The "gold standard" list of registered vacant buildings from the city.
       PDF is parsed with PyPDF2 and addresses are geocoded via Census batch.

All points are clipped to the Bronzeville community area boundary (Douglas,
Grand Boulevard, Kenwood, Washington Park).

Uses urllib.request throughout (requests has DNS failures for external
domains on this machine).
"""
from __future__ import annotations

import logging
import re
import urllib.request
import urllib.parse
import json
from pathlib import Path

import pandas as pd

from cache import load_cache, save_cache
from config import VACANT_PDF_PATH

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


# ─── Category → human-readable status mapping ────────────────────────────────
_CATEGORY_STATUS = {
    "GOVERNMENT ENTITY":              "Govt-Owned Vacant",
    "CITY INITIATED PRIVATE OWNER":   "City-Initiated (Private)",
    "MORTGAGEE REGISTRATION":         "Mortgagee Registered",
    "VOLUNTARY PRIVATE OWN REGISTER": "Voluntary Private",
    "VOLUNTARY REO REGISTRATION":     "Voluntary REO",
    "EXEMPT MORTGAGEE REGISTRATION":  "Exempt Mortgagee",
    "CITY INITIATED REO REGISTERED":  "City-Initiated REO",
    "VACANT STORE VOLUNTARY PRV REG": "Vacant Storefront (Voluntary)",
    "EXEMPT PER MOU REGISTRATION":    "Exempt (MOU)",
}


def _fetch_pdf_registry() -> list[dict]:
    """
    Parse the City of Chicago Vacant Building Registry PDF and geocode
    each address via the Census batch geocoder.

    Returns a list of dicts with keys:
        address, category, ward, type, status, lat, lon, class_code
    """
    pdf_path = Path(VACANT_PDF_PATH)
    if not pdf_path.is_absolute():
        pdf_path = Path(__file__).parent / pdf_path

    if not pdf_path.exists():
        logger.warning(f"Vacant Building Registry PDF not found: {pdf_path}")
        return []

    # ── Extract text from all pages ──────────────────────────────────────
    try:
        from PyPDF2 import PdfReader
    except ImportError:
        logger.warning("PyPDF2 not installed — skipping PDF registry")
        return []

    reader = PdfReader(str(pdf_path))
    all_lines: list[str] = []
    for page in reader.pages:
        text = page.extract_text() or ""
        all_lines.extend(text.splitlines())

    logger.info(f"PDF registry: {len(reader.pages)} pages, {len(all_lines)} raw lines")

    # ── Parse rows (Category, Address, Ward) ─────────────────────────────
    # Known category prefixes (used to detect the start of a real data row)
    _CATEGORIES = set(_CATEGORY_STATUS.keys())

    # Build a regex that matches any known category at the start of a line
    # followed by an address and a ward number.
    # Example line: "GOVERNMENT ENTITY 123 S STATE ST 3"
    # The ward is the last token (1-2 digit number).
    rows: list[dict] = []
    i = 0
    while i < len(all_lines):
        line = all_lines[i].strip()
        i += 1

        # Skip blank lines and header/footer lines
        if not line or line.startswith("Category Type") or line.startswith("Page "):
            continue

        # Check if the line starts with a known category
        matched_cat = None
        remainder = ""
        for cat in _CATEGORIES:
            if line.upper().startswith(cat):
                matched_cat = cat
                remainder = line[len(cat):].strip()
                break

        if not matched_cat:
            continue

        # Handle MLK Dr line-wrap: if the next line starts with "DR ##"
        # (the ward number part that wrapped), join it to the remainder.
        while i < len(all_lines):
            next_line = all_lines[i].strip()
            # Pattern: "DR" followed by a space and 1-2 digit number (ward)
            # This catches the wrapped "DR ##" continuation of MLK addresses
            if re.match(r'^DR\s+\d{1,2}\s*$', next_line, re.IGNORECASE):
                remainder = remainder + " " + next_line
                i += 1
                break
            else:
                break

        # Extract ward (last 1-2 digit number at end of remainder)
        ward_match = re.search(r'\s(\d{1,2})\s*$', remainder)
        ward = ""
        address_part = remainder
        if ward_match:
            ward = ward_match.group(1)
            address_part = remainder[:ward_match.start()].strip()

        if not address_part:
            continue

        # Append city/state for geocoding
        full_address = f"{address_part}, Chicago, IL"

        rows.append({
            "address":  full_address,
            "category": matched_cat,
            "ward":     ward,
        })

    logger.info(f"PDF registry: {len(rows)} rows parsed from PDF")

    if not rows:
        return []

    # ── Geocode via Census batch ─────────────────────────────────────────
    from geocode import geocode_batch_census, geocode_fallback_nominatim

    df = pd.DataFrame(rows)
    df = geocode_batch_census(df, address_col="address", city="Chicago", state="IL")
    df = geocode_fallback_nominatim(df, address_col="address")

    matched = df["latitude"].notna().sum()
    logger.info(f"PDF registry geocoded: {matched}/{len(df)} addresses matched")

    # ── Convert to list of dicts ─────────────────────────────────────────
    results: list[dict] = []
    for _, row in df.iterrows():
        lat = row.get("latitude")
        lon = row.get("longitude")
        if pd.isna(lat) or pd.isna(lon):
            continue

        cat = row["category"]
        results.append({
            "address":    row["address"],
            "category":   cat,
            "ward":       row["ward"],
            "class_code": "",
            "type":       "vacant_registered",
            "status":     _CATEGORY_STATUS.get(cat, "Registered Vacant"),
            "lat":        float(lat),
            "lon":        float(lon),
        })

    logger.info(f"PDF registry: {len(results)} geocoded vacant buildings")
    return results


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


def _deduplicate_by_proximity(lots: list[dict], threshold_deg: float = 0.0003) -> list[dict]:
    """
    Remove near-duplicate records by address proximity.

    If two records are within *threshold_deg* (~30 m) of each other,
    keep the one with the more specific type (prefer vacant_registered
    over vacant_land since it has richer metadata).
    """
    TYPE_PRIORITY = {"vacant_registered": 3, "vacant_storefront": 2, "vacant_land": 1}

    # Sort so higher-priority types come first — they survive dedup
    lots_sorted = sorted(lots, key=lambda r: TYPE_PRIORITY.get(r.get("type", ""), 0), reverse=True)
    kept: list[dict] = []

    for lot in lots_sorted:
        is_dup = False
        for existing in kept:
            if (abs(lot["lat"] - existing["lat"]) < threshold_deg
                    and abs(lot["lon"] - existing["lon"]) < threshold_deg):
                is_dup = True
                break
        if not is_dup:
            kept.append(lot)

    if len(lots) != len(kept):
        logger.info(f"Dedup by proximity: {len(lots)} → {len(kept)} records")
    return kept


def fetch_vacant_lots(community_features: list[dict] = None) -> list[dict]:
    """
    Return all vacant properties in Bronzeville as a flat list.

    Each record has: address, class_code, type, status, lat, lon
      type ∈ {"vacant_land", "vacant_storefront", "vacant_registered"}

    Sources:
      - Cook County class 1xx: bare vacant land parcels (2022 tax year)
      - Hardcoded curated list: manually verified vacant storefronts
      - City of Chicago Vacant Building Registry (PDF)

    All points are clipped to the Bronzeville community area boundary.
    """
    cached = load_cache("vacant_lots_v5")
    if cached is not None:
        logger.info(f"Vacant lots: {len(cached)} records loaded from cache")
        return cached

    lots: list[dict] = []

    try:
        lots.extend(_fetch_cook_county())
    except Exception as exc:
        logger.warning(f"Cook County vacant fetch error: {exc}")

    lots.extend(_hardcoded_storefronts())

    try:
        pdf_lots = _fetch_pdf_registry()
        lots.extend(pdf_lots)
    except Exception as exc:
        logger.warning(f"PDF registry fetch error: {exc}")

    # Clip to Bronzeville boundary if features provided
    if community_features:
        lots = _clip_to_boundary(lots, community_features)

    # Deduplicate records that are very close together
    lots = _deduplicate_by_proximity(lots)

    by_type = {t: sum(1 for l in lots if l["type"] == t)
               for t in ("vacant_land", "vacant_storefront", "vacant_registered")}
    logger.info(
        f"Vacant lots total: {len(lots)} records — "
        + ", ".join(f"{t}: {n}" for t, n in by_type.items())
    )

    save_cache("vacant_lots_v5", lots)
    return lots
