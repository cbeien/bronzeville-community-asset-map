"""
fetch_zoning.py — Fetch and classify Chicago zoning district polygons for Bronzeville.

Source: Chicago Data Portal dataset dj47-wfun
        "Boundaries - Zoning Districts (current)"
"""
from __future__ import annotations

import logging
import requests

from cache import load_cache, save_cache
from config import BRONZEVILLE_BBOX

logger = logging.getLogger(__name__)

_DATASET = "dj47-wfun"
_BASE_URL = "https://data.cityofchicago.org/resource"

# Map zone class prefix → land use category
def _zone_category(zone_class: str) -> str:
    """Return a broad land-use category string for a zone class prefix."""
    c = zone_class.upper()
    if c.startswith("RS") or c.startswith("RT") or c.startswith("RM") \
            or c.startswith("RB") or c.startswith("DR") or c.startswith("RA"):
        return "residential"
    if c.startswith("B"):
        return "mixed"          # B-class = commercial ground + residential above
    if c.startswith("C"):
        return "commercial"
    if c.startswith("M") or c.startswith("PMD"):
        return "industrial"
    if c.startswith("POS") or c.startswith("POC"):
        return "park"
    if c.startswith("T"):
        return "transportation"
    return "unknown"


def fetch_zoning_districts() -> list[dict]:
    """
    Fetch Chicago zoning district polygons that overlap the Bronzeville bbox.

    Returns a list of dicts, each with:
        zone_class  — e.g. "B3-1", "RS-3", "M1-1", "PD 1234"
        zone_type   — numeric zone type code
        pd_use      — for PD zones: derived land-use category (see classify_pd_zones)
        geometry    — GeoJSON MultiPolygon dict (ready for Leaflet / JSON embed)
    """
    cached = load_cache("zoning_districts")
    if cached is not None:
        logger.info(f"Zoning: {len(cached)} polygons loaded from cache")
        return cached

    bb = BRONZEVILLE_BBOX
    # within_box() only works on Point columns; zoning uses MultiPolygon.
    # Use intersects() with a WKT bounding-box polygon instead.
    wkt_box = (
        f"POLYGON(("
        f"{bb['lon_min']} {bb['lat_min']},"
        f"{bb['lon_max']} {bb['lat_min']},"
        f"{bb['lon_max']} {bb['lat_max']},"
        f"{bb['lon_min']} {bb['lat_max']},"
        f"{bb['lon_min']} {bb['lat_min']}"
        f"))"
    )
    bbox_filter = f"intersects(the_geom, '{wkt_box}')"

    url    = f"{_BASE_URL}/{_DATASET}.json"
    limit  = 1000
    offset = 0
    records: list[dict] = []

    while True:
        params = {
            "$where":  bbox_filter,
            "$select": "zone_class,zone_type,the_geom",
            "$limit":  limit,
            "$offset": offset,
        }
        try:
            resp = requests.get(url, params=params, timeout=45)
            resp.raise_for_status()
            batch = resp.json()
        except Exception as exc:
            logger.warning(f"Zoning fetch error at offset {offset}: {exc}")
            break

        if not batch:
            break

        for row in batch:
            geom = row.get("the_geom")
            zc   = row.get("zone_class", "").strip()
            if geom and zc:
                records.append({
                    "zone_class": zc,
                    "zone_type":  row.get("zone_type", ""),
                    "pd_use":     "",   # filled in by classify_pd_zones()
                    "geometry":   geom,
                })

        if len(batch) < limit:
            break
        offset += limit

    logger.info(f"Fetched {len(records)} zoning district polygons")
    save_cache("zoning_districts", records)
    return records


def classify_pd_zones(zoning: list[dict]) -> list[dict]:
    """
    For each Planned Development (PD) polygon, determine its effective land-use
    category by examining the surrounding (non-PD) zone types.

    Algorithm:
      1. Build Shapely geometries for all non-PD zones.
      2. For each PD polygon, buffer it slightly (~75 m) and find all
         non-PD zones that intersect the buffer.
      3. Count weighted land-use categories (area of intersection).
      4. If one category dominates (>60% weight) → assign that category.
         Otherwise → "mixed".

    Modifies each PD record in-place, setting pd_use.
    Returns the full zoning list (PD + non-PD).
    """
    try:
        from shapely.geometry import shape
    except ImportError:
        logger.warning("shapely not available — PD zones will use fallback color")
        return zoning

    # ~75 m in degrees at Chicago latitude (~41.8°N)
    BUFFER_DEG = 75 / 95_000

    # Build shapely objects for non-PD zones (used as context)
    non_pd: list[tuple[str, object]] = []  # (category, shapely_geom)
    for z in zoning:
        if z["zone_class"].upper().startswith("PD"):
            continue
        cat = _zone_category(z["zone_class"])
        if cat == "unknown":
            continue
        try:
            non_pd.append((cat, shape(z["geometry"])))
        except Exception:
            pass

    pd_count  = 0
    for z in zoning:
        if not z["zone_class"].upper().startswith("PD"):
            continue
        pd_count += 1
        try:
            pd_geom = shape(z["geometry"])
            buffered = pd_geom.buffer(BUFFER_DEG)

            weights: dict[str, float] = {}
            for cat, ctx_geom in non_pd:
                try:
                    inter = buffered.intersection(ctx_geom)
                    if not inter.is_empty:
                        weights[cat] = weights.get(cat, 0.0) + inter.area
                except Exception:
                    pass

            if not weights:
                z["pd_use"] = "unknown"
                continue

            total = sum(weights.values())
            dominant = max(weights, key=weights.__getitem__)
            dom_pct  = weights[dominant] / total

            z["pd_use"] = dominant if dom_pct >= 0.50 else "mixed"

        except Exception:
            z["pd_use"] = "unknown"

    logger.info(f"Classified {pd_count} PD zones by spatial context")
    return zoning


def clip_zoning_to_boundary(zoning: list[dict],
                             community_features: list[dict]) -> list[dict]:
    """
    Clip each zoning polygon to the Bronzeville community area boundary
    (union of Douglas, Grand Boulevard, Kenwood, Washington Park).

    Polygons entirely outside the boundary are dropped.
    Polygons that cross the boundary edge are trimmed.
    """
    try:
        from shapely.geometry import shape
        from shapely.ops import unary_union
    except ImportError:
        logger.warning("shapely not available — zoning polygons will not be clipped")
        return zoning

    boundary_shapes = [
        shape(f["geometry"])
        for f in community_features
        if f.get("geometry")
    ]
    if not boundary_shapes:
        return zoning

    boundary = unary_union(boundary_shapes)

    clipped: list[dict] = []
    for z in zoning:
        try:
            geom = shape(z["geometry"])
            if not geom.intersects(boundary):
                continue
            trimmed = geom.intersection(boundary)
            if trimmed.is_empty:
                continue
            z = dict(z)
            z["geometry"] = trimmed.__geo_interface__
            clipped.append(z)
        except Exception:
            pass

    logger.info(
        f"Clipped zoning: {len(zoning)} → {len(clipped)} polygons inside boundary"
    )
    return clipped
