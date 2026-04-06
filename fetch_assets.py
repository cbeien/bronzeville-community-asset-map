"""
fetch_assets.py — Pull all nine community asset categories from two public
sources and return clean, consistently-schemed DataFrames.

Asset Categories
----------------
  landmarks_df    — designated historic landmarks (Chicago Data Portal)
  restaurants_df  — food-service businesses (Chicago Data Portal + OSM)
  businesses_df   — other licensed businesses (Chicago Data Portal)
  schools_df      — public schools (Chicago Data Portal) + private (OSM)
  worship_df      — places of worship (OSM)
  parks_df        — parks (Chicago Data Portal)
  healthcare_df   — hospitals, clinics, pharmacies (OSM)
  cultural_df     — museums, galleries, theatres, libraries (OSM + CDP)
  social_df       — social-service facilities (OSM)

Sources
-------
1. City of Chicago Data Portal (SODA — no key required)
   Landmarks       tdab-kixi   filter: community areas DOUGLAS, GRAND BOULEVARD,
                               KENWOOD, WASHINGTON PARK
   Business Licens uupf-x98q   filter: ZIP 60615 / 60616 / 60653
   Schools         nu7z-2fbt   filter: same community areas
   Parks           eix4-gf83   filter: same community areas
   Libraries       x8fc-8rcq   filter: community area name

2. Overpass API / OpenStreetMap (no key required, 1 req/sec courtesy limit)
   bbox: south=41.79, west=-87.64, north=41.87, east=-87.60

Output Schema (all DataFrames)
-------------------------------
    Name, Address, Type, category, latitude, longitude, source
"""

from __future__ import annotations

import re
import logging
import requests
import pandas as pd

from config import (
    SODA_BASE_URL, DATASET_IDS, SODA_LIMIT,
    BRONZEVILLE_ZIPS, RESTAURANT_KEYWORDS,
    OVERPASS_URL, OVERPASS_BBOX,
    COMMUNITY_AREAS,
)

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "BronzevilleCommunityAssetMap/1.0 (university-research)"}

# ─── Shared Column Schema ─────────────────────────────────────────────────────
OUTPUT_COLS = ["Name", "Address", "Type", "category", "latitude", "longitude", "source"]


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=OUTPUT_COLS)


# ─── SODA Helpers ─────────────────────────────────────────────────────────────

def _soda_get(dataset_id: str, params: dict) -> list[dict]:
    """Issue a SODA GET request and return a list of row-dicts."""
    url = f"{SODA_BASE_URL}/{dataset_id}.json"
    params.setdefault("$limit", SODA_LIMIT)
    resp = requests.get(url, params=params, headers=_HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()


def _coords(row: dict) -> tuple[float | None, float | None]:
    """
    Extract (lat, lon) from a SODA row.
    Handles both top-level fields and nested 'location' objects.
    """
    lat = row.get("latitude") or row.get("lat")
    lon = row.get("longitude") or row.get("lon") or row.get("long")

    if (lat is None or lon is None) and isinstance(row.get("location"), dict):
        loc = row["location"]
        lat = lat or loc.get("latitude")
        lon = lon or loc.get("longitude")

    try:
        return float(lat), float(lon)
    except (TypeError, ValueError):
        return None, None


def _community_where(field: str = "community_area_name",
                     title_case: bool = False) -> str:
    """
    Build a SODA $where clause filtering to Bronzeville community areas.
    Use title_case=True for datasets that store area names as e.g. 'Grand Boulevard'
    rather than the all-caps form 'GRAND BOULEVARD'.
    """
    if title_case:
        names = ", ".join(f"'{n.title()}'" for n in COMMUNITY_AREAS.keys())
    else:
        names = ", ".join(f"'{n}'" for n in COMMUNITY_AREAS.keys())
    return f"{field} in ({names})"


# ─── 1. Landmarks (SODA: tdab-kixi) ──────────────────────────────────────────

def fetch_chicago_landmarks() -> pd.DataFrame:
    """
    Designated Chicago landmarks in Douglas, Grand Boulevard, Kenwood,
    and Washington Park community areas.

    Dataset: tdab-kixi — "Chicago Landmarks"
    """
    try:
        # The Landmarks dataset has lat/lon columns but no community_area_name.
        # Use a numeric bbox filter instead.
        from config import BRONZEVILLE_BBOX as BB
        bbox_where = (
            f"latitude  >= {BB['lat_min']} AND latitude  <= {BB['lat_max']} AND "
            f"longitude >= {BB['lon_min']} AND longitude <= {BB['lon_max']}"
        )
        rows = _soda_get(DATASET_IDS["landmarks"], {"$where": bbox_where})
    except Exception as exc:
        logger.warning(f"Landmarks SODA error: {exc}")
        return _empty_df()

    records = []
    for row in rows:
        lat, lon = _coords(row)
        records.append({
            "Name":      row.get("landmark_name", row.get("name", "")),
            "Address":   row.get("street_address", row.get("address", "")),
            "Type":      row.get("landmark_type", "Historic Landmark"),
            "category":  "landmark",
            "latitude":  lat,
            "longitude": lon,
            "source":    "Chicago Data Portal",
        })

    df = pd.DataFrame(records).dropna(subset=["Name"])
    df["Name"] = df["Name"].str.strip()
    logger.info(f"Landmarks (SODA): {len(df)} records")
    return df


# ─── 2. Business Licenses (SODA: uupf-x98q) ──────────────────────────────────

def fetch_business_licenses() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Active business licenses in Bronzeville ZIP codes (60615, 60616, 60653).
    Splits into restaurants (RETAIL FOOD / CONSUMPTION ON PREMISES) and all others.

    Dataset: uupf-x98q — "Business Licenses - Current Active"

    Returns: (restaurants_df, businesses_df)
    """
    zip_list = ", ".join(f"'{z}'" for z in BRONZEVILLE_ZIPS)
    try:
        rows = _soda_get(
            DATASET_IDS["business_licenses"],
            {"$where": f"zip_code in ({zip_list})"},
        )
    except Exception as exc:
        logger.warning(f"Business Licenses SODA error: {exc}")
        return _empty_df(), _empty_df()

    restaurants, businesses = [], []

    for row in rows:
        lat, lon = _coords(row)

        name = (
            row.get("doing_business_as_name")
            or row.get("dba_name")
            or row.get("legal_name")
            or ""
        ).strip()

        street  = row.get("address", row.get("street_address", ""))
        city    = row.get("city", "Chicago")
        state   = row.get("state", "IL")
        zip_cd  = row.get("zip_code", "")
        address = f"{street}, {city}, {state} {zip_cd}".strip(", ")

        biz_type = (
            row.get("license_description")
            or row.get("business_activity")
            or row.get("license_code_description")
            or ""
        ).strip()

        # Exclude non-consumer-facing license types
        NON_CONSUMER = {
            "limited business license",
            "regulated business license",
            "peddler license",
            "shared kitchen user (long term)",
            "shared housing unit operator",
            "manufacturing establishments",
            "food - shared kitchen - supplemental",
            "raffles",
        }
        if biz_type.lower() in NON_CONSUMER:
            continue

        is_restaurant  = any(kw in biz_type.upper() for kw in RESTAURANT_KEYWORDS)
        is_childcare   = biz_type.lower() == "children's services facility license"

        category = "restaurant" if is_restaurant else ("school" if is_childcare else "business")

        record = {
            "Name":      name,
            "Address":   address,
            "Type":      biz_type if not is_childcare else "Childcare / Early Learning",
            "category":  category,
            "latitude":  lat,
            "longitude": lon,
            "source":    "Chicago Data Portal",
        }

        (restaurants if is_restaurant else businesses).append(record)

    rest_df = pd.DataFrame(restaurants).dropna(subset=["Name"])
    biz_df  = pd.DataFrame(businesses).dropna(subset=["Name"])
    logger.info(f"Business Licenses (SODA): {len(rest_df)} restaurants, {len(biz_df)} businesses "
                f"({len(biz_df[biz_df['category']=='school']) if not biz_df.empty else 0} childcare→school)")
    return rest_df, biz_df


# ─── 3. Public Schools (SODA: nu7z-2fbt) ─────────────────────────────────────

def fetch_public_schools() -> pd.DataFrame:
    """
    Chicago Public Schools in Bronzeville community areas.

    Dataset: c7jj-qjvh — "CPS Schools 2013-2014 Academic Year"
    Has latitude, longitude, community_area (name), and school_type columns.
    Filter uses the 'community_area' field (e.g. 'GRAND BOULEVARD').
    """
    try:
        rows = _soda_get(
            DATASET_IDS["schools"],
            {"$where": _community_where("community_area", title_case=True)},
        )
    except Exception as exc:
        logger.warning(f"Public Schools SODA error: {exc}")
        return _empty_df()

    records = []
    for row in rows:
        lat, lon = _coords(row)

        # Build a normalised street address from component fields
        num  = row.get("street_number", "")
        dirn = row.get("street_direction", "")
        st   = row.get("street_name", "")
        city = row.get("city", "Chicago")
        state = row.get("state", "IL")
        zip_  = row.get("zip", "")
        address = " ".join(p for p in [num, dirn, st] if p)
        address = f"{address}, {city}, {state} {zip_}".strip(", ")

        # Sanity-check coordinates against the Bronzeville bbox.
        # Some records (e.g. Wendell Phillips HS) have bad lat/lon in the
        # source dataset despite having the correct community_area tag.
        # Null them out so the geocoding step can re-resolve by address.
        from config import BRONZEVILLE_BBOX as _BB
        bb = _BB
        if lat is not None and lon is not None:
            if not (bb["lat_min"] <= lat <= bb["lat_max"] and
                    bb["lon_min"] <= lon <= bb["lon_max"]):
                logger.warning(
                    f"School '{row.get('schoolname')}' has out-of-bbox coords "
                    f"({lat:.4f}, {lon:.4f}) — will re-geocode by address."
                )
                lat, lon = None, None

        records.append({
            "Name":      row.get("schoolname", row.get("fullname", "")),
            "Address":   address,
            "Type":      row.get("school_type", row.get("governance", "Public School")),
            "category":  "school",
            "latitude":  lat,
            "longitude": lon,
            "source":    "Chicago Data Portal",
        })

    if not records:
        logger.warning("Public Schools (SODA): no records returned")
        return _empty_df()
    df = pd.DataFrame(records).dropna(subset=["Name"])
    logger.info(f"Public Schools (SODA): {len(df)} records")
    return df


# ─── 4. Parks (SODA: eix4-gf83) ──────────────────────────────────────────────

def fetch_parks() -> pd.DataFrame:
    """
    Chicago Park District parks in Bronzeville.

    Dataset: eix4-gf83 — "Parks - Chicago Park District Park Boundaries (current)"
    This dataset stores geometry in a 'the_geom' Polygon column with no separate
    lat/lon or community_area_name fields. We fetch all parks and filter in Python
    by computing each polygon's centroid and checking against the Bronzeville bbox.
    """
    from config import BRONZEVILLE_BBOX as BB
    try:
        rows = _soda_get(DATASET_IDS["parks"], {})
    except Exception as exc:
        logger.warning(f"Parks SODA error: {exc}")
        return _empty_df()

    records = []
    for row in rows:
        # Compute centroid from the polygon's outer ring (GeoJSON coords are [lon, lat])
        lat, lon = None, None
        geom = row.get("the_geom", {})
        if isinstance(geom, str):
            import json as _json
            try:
                geom = _json.loads(geom)
            except Exception:
                geom = {}
        if isinstance(geom, dict):
            coords_raw = geom.get("coordinates", [])
            try:
                outer_ring = coords_raw[0] if coords_raw else []
                if outer_ring:
                    lon = sum(c[0] for c in outer_ring) / len(outer_ring)
                    lat = sum(c[1] for c in outer_ring) / len(outer_ring)
            except Exception:
                pass

        # Skip parks whose centroid falls outside Bronzeville
        if lat is None or not (BB["lat_min"] <= lat <= BB["lat_max"]
                               and BB["lon_min"] <= lon <= BB["lon_max"]):
            continue

        records.append({
            "Name":      row.get("park", row.get("facility_n", row.get("label", ""))),
            "Address":   "",   # parks dataset has no address field
            "Type":      row.get("facility_t", row.get("park_class", "Park")),
            "category":  "park",
            "latitude":  lat,
            "longitude": lon,
            "source":    "Chicago Data Portal",
        })

    if not records:
        logger.warning("Parks (SODA): no records in Bronzeville bbox")
        return _empty_df()
    df = pd.DataFrame(records)
    df = df[df["Name"].notna() & df["Name"].str.strip().ne("")]
    logger.info(f"Parks (SODA): {len(df)} records in Bronzeville bbox")
    return df


# ─── 5. Libraries (SODA: x8fc-8rcq) ──────────────────────────────────────────

def fetch_libraries() -> pd.DataFrame:
    """
    Chicago Public Libraries in Bronzeville community areas.

    Dataset: x8fc-8rcq — "Libraries - Locations, Hours and Contact Information"
    """
    try:
        from config import BRONZEVILLE_BBOX as bb
        rows = _soda_get(
            DATASET_IDS["libraries"],
            {"$where": (
                f"within_box(location, {bb['lat_max']}, {bb['lon_min']}, "
                f"{bb['lat_min']}, {bb['lon_max']})"
            )},
        )
    except Exception as exc:
        logger.warning(f"Libraries SODA error: {exc}")
        return _empty_df()

    records = []
    for row in rows:
        lat, lon = _coords(row)
        records.append({
            "Name":      row.get("name_", row.get("name", "")),
            "Address":   row.get("address", ""),
            "Type":      "Public Library",
            "category":  "cultural",
            "latitude":  lat,
            "longitude": lon,
            "source":    "Chicago Data Portal",
        })

    df = pd.DataFrame(records).dropna(subset=["Name"])
    logger.info(f"Libraries (SODA): {len(df)} records")
    return df


# ─── Overpass API (OpenStreetMap) ─────────────────────────────────────────────

def _overpass_query(tag_filters: list[tuple[str, str]], timeout: int = 60,
                    cache_name: str | None = None) -> list[dict]:
    """
    Run an Overpass QL query for a set of tag filters within OVERPASS_BBOX.

    tag_filters: list of (key, value) pairs; use '*' for any value.
    cache_name:  human-readable cache key (e.g. 'osm_landmarks').  If omitted
                 a hash of the tag filters is used.
    Returns a list of OSM element dicts (with 'tags', 'lat'/'lon' or 'center').
    """
    import hashlib
    from cache import load_cache, save_cache

    if cache_name:
        cache_key = cache_name
    else:
        key_str = "|".join(f"{k}={v}" for k, v in sorted(tag_filters))
        cache_key = "osm_" + hashlib.md5(key_str.encode()).hexdigest()[:12]

    cached = load_cache(cache_key)
    if cached is not None:
        return cached

    south, west, north, east = OVERPASS_BBOX
    bbox = f"{south},{west},{north},{east}"

    lines = []
    for key, value in tag_filters:
        tag_expr = f'["{key}"]' if value == "*" else f'["{key}"="{value}"]'
        lines.append(f'  node{tag_expr}({bbox});')
        lines.append(f'  way{tag_expr}({bbox});')

    query = f"[out:json][timeout:{timeout}];\n(\n" + "\n".join(lines) + "\n);\nout center;\n"

    from config import OVERPASS_URLS
    last_exc: Exception = RuntimeError("No Overpass mirrors configured")
    for url in OVERPASS_URLS:
        try:
            resp = requests.post(url, data={"data": query}, headers=_HEADERS,
                                 timeout=timeout + 30)
            resp.raise_for_status()
            elements = resp.json().get("elements", [])
            if elements:
                save_cache(cache_key, elements)
            return elements
        except Exception as exc:
            logger.debug(f"Overpass mirror {url} failed: {exc}")
            last_exc = exc
    raise last_exc


def _osm_coords(el: dict) -> tuple[float | None, float | None]:
    """Extract (lat, lon) from a node or a way with 'out center;' data."""
    if el.get("type") == "node":
        return el.get("lat"), el.get("lon")
    center = el.get("center", {})
    return center.get("lat"), center.get("lon")


def _osm_address(tags: dict) -> str:
    """Reconstruct a street address from OSM addr:* tags."""
    num    = tags.get("addr:housenumber", "")
    street = tags.get("addr:street", "")
    city   = tags.get("addr:city", "Chicago")
    state  = tags.get("addr:state", "IL")

    parts = []
    if num and street:
        parts.append(f"{num} {street}")
    elif street:
        parts.append(street)
    parts += [city, state]
    return ", ".join(p for p in parts if p)


def _osm_records(elements: list[dict],
                 category: str,
                 type_fn) -> pd.DataFrame:
    """
    Convert a list of Overpass elements into a standard DataFrame.
    type_fn(tags) should return a human-readable 'Type' string.
    """
    records = []
    for el in elements:
        tags = el.get("tags", {})
        lat, lon = _osm_coords(el)
        if lat is None or lon is None:
            continue

        name = tags.get("name", tags.get("name:en", ""))
        if not name:
            continue

        records.append({
            "Name":      name,
            "Address":   _osm_address(tags),
            "Type":      type_fn(tags),
            "category":  category,
            "latitude":  float(lat),
            "longitude": float(lon),
            "source":    "OpenStreetMap",
        })
    return pd.DataFrame(records) if records else _empty_df()


# ─── 6. OSM Restaurants ───────────────────────────────────────────────────────

def fetch_osm_restaurants() -> pd.DataFrame:
    """OSM amenity=restaurant within Bronzeville bbox."""
    try:
        elements = _overpass_query([("amenity", "restaurant")], cache_name="osm_restaurants")
        df = _osm_records(elements, "restaurant",
                          lambda t: t.get("cuisine", "Restaurant").replace(";", ", ").title())
        logger.info(f"OSM Restaurants: {len(df)} records")
        return df
    except Exception as exc:
        logger.warning(f"OSM Restaurants Overpass error: {exc}")
        return _empty_df()


# ─── 7. OSM Places of Worship ─────────────────────────────────────────────────

def fetch_osm_worship() -> pd.DataFrame:
    """OSM amenity=place_of_worship — includes religion and denomination."""
    try:
        elements = _overpass_query([("amenity", "place_of_worship")], cache_name="osm_worship")

        def worship_type(tags: dict) -> str:
            rel  = tags.get("religion", "").replace("_", " ").title()
            denom = tags.get("denomination", "").replace("_", " ").title()
            parts = [p for p in [rel, denom] if p]
            return " — ".join(parts) if parts else "Place of Worship"

        df = _osm_records(elements, "worship", worship_type)
        logger.info(f"OSM Worship: {len(df)} records")
        return df
    except Exception as exc:
        logger.warning(f"OSM Worship Overpass error: {exc}")
        return _empty_df()


# ─── 8. OSM Historic Landmarks ────────────────────────────────────────────────

def fetch_osm_landmarks() -> pd.DataFrame:
    """
    OSM historic=*, tourism=museum, amenity=police, amenity=fire_station,
    and office=government within Bronzeville bbox.
    Civic buildings (police HQ, fire stations, government offices) are folded
    into the Landmarks category as significant public institutions.
    """
    try:
        elements = _overpass_query([
            ("historic",  "*"),
            ("tourism",   "museum"),
            ("amenity",   "police"),
            ("amenity",   "fire_station"),
            ("office",    "government"),
        ], cache_name="osm_landmarks")

        def lm_type(tags: dict) -> str:
            a = tags.get("amenity", "")
            o = tags.get("office", "")
            h = tags.get("historic", "")
            t = tags.get("tourism", "")
            if a == "police":
                return "Police Facility"
            if a == "fire_station":
                return "Fire Station"
            if o == "government":
                return "Government Office"
            if h:
                return f"Historic {h.replace('_', ' ').title()}"
            if t:
                return t.replace("_", " ").title()
            return "Landmark"

        df = _osm_records(elements, "landmark", lm_type)
        logger.info(f"OSM Landmarks: {len(df)} records")
        return df
    except Exception as exc:
        logger.warning(f"OSM Landmarks Overpass error: {exc}")
        return _empty_df()


# ─── 9. OSM Private Schools ───────────────────────────────────────────────────

def fetch_osm_private_schools(public_school_names: set[str]) -> pd.DataFrame:
    """
    OSM amenity=school, cross-referenced against CPS school names to tag
    schools not already in the public dataset as private.
    """
    try:
        elements = _overpass_query([("amenity", "school")], cache_name="osm_schools")

        def school_type(tags: dict) -> str:
            # If the school name appears in the CPS dataset it's public;
            # otherwise label it as private.
            name = tags.get("name", "")
            if any(pub.lower() in name.lower() for pub in public_school_names if pub):
                return "Public School (CPS)"
            return tags.get("school:type", "Private School")

        df = _osm_records(elements, "school", school_type)
        logger.info(f"OSM Schools: {len(df)} records")
        return df
    except Exception as exc:
        logger.warning(f"OSM Schools Overpass error: {exc}")
        return _empty_df()


# ─── 10. OSM Parks ────────────────────────────────────────────────────────────

def fetch_osm_parks() -> pd.DataFrame:
    """
    Fetch parks and green spaces from OpenStreetMap via Overpass.
    Includes formal parks, community gardens, and urban farms —
    all of which are active green-space assets even when the underlying
    parcel carries a Cook County 'vacant' class code.
    """
    try:
        elements = _overpass_query([
            ("leisure", "park"),
            ("leisure", "garden"),
            ("leisure", "nature_reserve"),
            ("landuse", "allotments"),  # community gardens / urban farms
        ], cache_name="osm_parks")
        records = []
        for el in elements:
            tags = el.get("tags", {})
            name = tags.get("name", "")
            if not name:
                continue
            lat, lon = _osm_coords(el)
            if lat is None or lon is None:
                continue
            # Determine a human-readable type
            if tags.get("landuse") == "allotments":
                park_type = "Community Garden"
            elif tags.get("leisure") == "garden":
                park_type = "Garden"
            elif tags.get("leisure") == "nature_reserve":
                park_type = "Nature Reserve"
            else:
                park_type = "Park"
            records.append({
                "Name":      name,
                "Address":   _osm_address(tags),
                "Type":      park_type,
                "category":  "park",
                "latitude":  float(lat),
                "longitude": float(lon),
                "source":    "OpenStreetMap",
            })
        df = pd.DataFrame(records) if records else _empty_df()
        logger.info(f"OSM Parks: {len(df)} records")
        return df
    except Exception as exc:
        logger.warning(f"OSM Parks Overpass error: {exc}")
        return _empty_df()


# ─── 11. OSM Healthcare ───────────────────────────────────────────────────────

def fetch_osm_healthcare() -> pd.DataFrame:
    """OSM hospital, clinic, pharmacy, doctors within Bronzeville bbox."""
    try:
        elements = _overpass_query([
            ("amenity", "hospital"),
            ("amenity", "clinic"),
            ("amenity", "pharmacy"),
            ("amenity", "doctors"),
        ], cache_name="osm_healthcare")

        def health_type(tags: dict) -> str:
            return tags.get("amenity", "healthcare").replace("_", " ").title()

        df = _osm_records(elements, "healthcare", health_type)
        logger.info(f"OSM Healthcare: {len(df)} records")
        return df
    except Exception as exc:
        logger.warning(f"OSM Healthcare Overpass error: {exc}")
        return _empty_df()


# ─── 11. OSM Cultural Institutions ───────────────────────────────────────────

def fetch_osm_cultural() -> pd.DataFrame:
    """OSM museums, galleries, theatres, arts centres, libraries."""
    try:
        elements = _overpass_query([
            ("tourism",  "museum"),
            ("tourism",  "gallery"),
            ("amenity",  "theatre"),
            ("amenity",  "arts_centre"),
            ("amenity",  "library"),
        ], cache_name="osm_cultural")

        def cultural_type(tags: dict) -> str:
            for key in ("amenity", "tourism"):
                val = tags.get(key, "")
                if val:
                    return val.replace("_", " ").title()
            return "Cultural Institution"

        df = _osm_records(elements, "cultural", cultural_type)
        logger.info(f"OSM Cultural: {len(df)} records")
        return df
    except Exception as exc:
        logger.warning(f"OSM Cultural Overpass error: {exc}")
        return _empty_df()


# ─── 12. OSM Social Services ──────────────────────────────────────────────────

def fetch_osm_social() -> pd.DataFrame:
    """OSM amenity=social_facility within Bronzeville bbox."""
    try:
        elements = _overpass_query([("amenity", "social_facility")], cache_name="osm_social")

        def social_type(tags: dict) -> str:
            stype = tags.get("social_facility", tags.get("social_facility:for", ""))
            return stype.replace("_", " ").title() if stype else "Social Service"

        df = _osm_records(elements, "social", social_type)
        logger.info(f"OSM Social Services: {len(df)} records")
        return df
    except Exception as exc:
        logger.warning(f"OSM Social Services Overpass error: {exc}")
        return _empty_df()


# ─── Deduplication ────────────────────────────────────────────────────────────

def _norm_key(name: str, address: str) -> str:
    """
    Six-token fingerprint for deduplication (case-insensitive,
    punctuation-stripped). SODA records placed first will survive
    when keys collide with OSM records.
    """
    raw    = f"{name} {address}".lower()
    tokens = re.sub(r"[^a-z0-9 ]", " ", raw).split()
    return " ".join(tokens[:6])


def _dedup(soda_df: pd.DataFrame, osm_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge two DataFrames and drop duplicates, preferring SODA rows.
    Also drops rows with blank names or missing coordinates.
    """
    combined = pd.concat([soda_df, osm_df], ignore_index=True)
    combined["_key"] = combined.apply(
        lambda r: _norm_key(str(r.get("Name", "")), str(r.get("Address", ""))),
        axis=1,
    )
    combined = combined.drop_duplicates(subset="_key", keep="first")
    combined = combined.drop(columns=["_key"])
    combined = combined[combined["Name"].str.strip().ne("")]
    return combined.reset_index(drop=True)


# ─── Public Entry Point ───────────────────────────────────────────────────────

def fetch_all_assets() -> dict[str, pd.DataFrame]:
    """
    Fetch, merge, and deduplicate all community asset categories.

    Returns a dict keyed by category name:
        landmarks, restaurants, businesses, education, worship, parks, health

    Results are cached to _cache/all_assets.json for 7 days.
    OSM queries are individually cached in _cache/osm_*.json.
    Delete _cache/ to force a full refresh.
    """
    from cache import load_cache, save_cache

    cached = load_cache("all_assets_v2")
    if cached is not None:
        print("  [cache] Loading all assets from local cache …", end=" ", flush=True)
        result = {k: pd.DataFrame(v) for k, v in cached.items()}
        print("done")
        totals = {k: len(v) for k, v in result.items()}
        print("  Totals — " + ", ".join(f"{k}: {v}" for k, v in totals.items()))
        return result

    # ── SODA pulls ──────────────────────────────────────────────────────────
    print("  [a] Chicago Landmarks (SODA) …", end=" ", flush=True)
    soda_landmarks = fetch_chicago_landmarks()
    print(len(soda_landmarks))

    print("  [b] Business Licenses (SODA) …", end=" ", flush=True)
    soda_rest, soda_biz = fetch_business_licenses()
    print(f"{len(soda_rest)} restaurants / {len(soda_biz)} businesses")

    print("  [c] Public Schools (SODA) …", end=" ", flush=True)
    soda_schools = fetch_public_schools()
    print(len(soda_schools))

    print("  [d] Parks (OSM) …", end=" ", flush=True)
    soda_parks = fetch_osm_parks()
    print(len(soda_parks))

    print("  [e] Libraries (SODA) …", end=" ", flush=True)
    soda_libraries = fetch_libraries()
    print(len(soda_libraries))

    # ── Overpass / OSM pulls ─────────────────────────────────────────────────
    print("  [f] OSM Restaurants …", end=" ", flush=True)
    osm_rest = fetch_osm_restaurants()
    print(len(osm_rest))

    print("  [g] OSM Landmarks (historic/museum) …", end=" ", flush=True)
    osm_landmarks = fetch_osm_landmarks()
    print(len(osm_landmarks))

    print("  [h] OSM Places of Worship …", end=" ", flush=True)
    worship_df = fetch_osm_worship()
    print(len(worship_df))

    # Pass CPS names so OSM schools can be tagged as private when unlisted
    cps_names = set(soda_schools["Name"].str.lower().tolist())
    print("  [i] OSM Schools (private cross-ref) …", end=" ", flush=True)
    osm_schools = fetch_osm_private_schools(cps_names)
    print(len(osm_schools))

    print("  [j] OSM Healthcare …", end=" ", flush=True)
    healthcare_df = fetch_osm_healthcare()
    print(len(healthcare_df))

    print("  [k] OSM Cultural Institutions …", end=" ", flush=True)
    osm_cultural = fetch_osm_cultural()
    print(len(osm_cultural))

    print("  [l] OSM Social Services …", end=" ", flush=True)
    social_df = fetch_osm_social()
    print(len(social_df))

    print("  [m] IRS Nonprofits (EO BMF) …", end=" ", flush=True)
    from fetch_irs import fetch_irs_nonprofits
    irs = fetch_irs_nonprofits()
    print(sum(len(v) for v in irs.values()))

    # ── Merge & deduplicate ──────────────────────────────────────────────────
    print("  [n] Merging & deduplicating …", end=" ", flush=True)

    landmarks_df   = _dedup(soda_landmarks,  osm_landmarks)
    restaurants_df = _dedup(soda_rest,       osm_rest)
    # Split childcare (category="school") out of business licenses into schools
    if not soda_biz.empty and "category" in soda_biz.columns:
        childcare_df  = soda_biz[soda_biz["category"] == "school"].reset_index(drop=True)
        businesses_df = soda_biz[soda_biz["category"] != "school"].reset_index(drop=True)
    else:
        childcare_df  = _empty_df()
        businesses_df = soda_biz.reset_index(drop=True)
    schools_df = _dedup(_dedup(soda_schools, osm_schools), childcare_df)
    parks_df       = soda_parks.reset_index(drop=True)         # CDP is authoritative
    cultural_df    = _dedup(soda_libraries,  osm_cultural)

    # worship: OSM base + IRS fills gaps (IRS placed second so OSM wins on dedup)
    if "category" not in worship_df.columns:
        worship_df["category"] = "worship"
    irs_worship = irs.get("worship", _empty_df())
    if not irs_worship.empty:
        irs_worship["category"] = "worship"
    worship_df = _dedup(worship_df, irs_worship)

    # ── Manual additions ────────────────────────────────────────────────────
    # City Bureau — community media nonprofit at 3619 S State St.
    # Not captured by business licenses (no city license) or OSM.
    city_bureau = pd.DataFrame([{
        "Name":      "City Bureau",
        "Address":   "3619 S State St, Chicago, IL 60609",
        "Type":      "Community Media",
        "category":  "education",
        "latitude":  41.8288517,
        "longitude": -87.6264011,
        "source":    "Manual",
    }])

    # Merge schools + cultural + manual → "education", then IRS fills gaps
    for df in [schools_df, cultural_df, city_bureau]:
        if not df.empty:
            df["category"] = "education"
    education_df = _dedup(_dedup(schools_df, cultural_df), city_bureau)
    irs_education = irs.get("education", _empty_df())
    if not irs_education.empty:
        irs_education["category"] = "education"
    education_df = _dedup(education_df, irs_education)
    education_df["category"] = "education"

    # Merge healthcare + social → "health", then IRS fills gaps
    for df in [healthcare_df, social_df]:
        if not df.empty:
            df["category"] = "health"
    health_df = _dedup(healthcare_df, social_df)
    irs_health = irs.get("health", _empty_df())
    if not irs_health.empty:
        irs_health["category"] = "health"
    health_df = _dedup(health_df, irs_health)
    health_df["category"] = "health"

    print("done")

    result = {
        "landmarks":   landmarks_df,
        "restaurants": restaurants_df,
        "businesses":  businesses_df,
        "education":   education_df,
        "worship":     worship_df,
        "parks":       parks_df,
        "health":      health_df,
    }

    # Summary
    totals = {k: len(v) for k, v in result.items()}
    print(
        "  Totals — "
        + ", ".join(f"{k}: {v}" for k, v in totals.items())
    )

    # Save to cache (convert DataFrames → list-of-dicts for JSON serialisation)
    save_cache("all_assets_v2", {k: v.to_dict(orient="records") for k, v in result.items()})

    return result
