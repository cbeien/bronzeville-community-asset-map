"""
fetch_data.py — Fetch transportation layers from the City of Chicago
Socrata Open Data API (SODA).

Covers six datasets:
  1. CTA Rail Stations   (8pix-ypme)
  2. CTA Rail Lines      (sgbp-kdxi)
  3. CTA Bus Stops       (d5bx-dr8z)
  4. CTA Bus Routes      (hvnx-qtky)
  5. Metra Stations      (enz5-3bkg)
  6. Metra Lines         (q8wx-dznq)

Also fetches Community Area boundaries (igwz-8jzy) for map display.

No API key is required for read-only access. If you have a Socrata
app token, set the environment variable SODA_APP_TOKEN to raise the
rate limit from ~1 000 to 50 000 requests/day.

SODA docs: https://dev.socrata.com/docs/queries/
"""

from __future__ import annotations

import os
import json
import logging
import requests
import pandas as pd

from config import (
    SODA_BASE_URL, DATASET_IDS, SODA_LIMIT,
    BRONZEVILLE_BBOX, CTA_LINE_COLORS, OVERPASS_URLS,
)

logger = logging.getLogger(__name__)

# Optional Socrata app token — reduces throttling risk for high-volume work.
_APP_TOKEN = os.getenv("SODA_APP_TOKEN", "")
_HEADERS   = {"X-App-Token": _APP_TOKEN} if _APP_TOKEN else {}


# ─── Core HTTP Helper ─────────────────────────────────────────────────────────

def _soda_get(dataset_id: str, params: dict) -> list[dict]:
    """
    Generic SODA GET request. Returns a list of row-dicts.
    Raises requests.HTTPError on non-2xx responses.
    Adds the dataset $limit automatically if not already set.
    """
    url = f"{SODA_BASE_URL}/{dataset_id}.json"
    params.setdefault("$limit", SODA_LIMIT)

    logger.debug(f"GET {url}  params={params}")
    resp = requests.get(url, params=params, headers=_HEADERS, timeout=45)
    resp.raise_for_status()
    return resp.json()


# ─── Bounding Box Filter ──────────────────────────────────────────────────────

def _bbox_where(lat_col: str = "latitude",
                lon_col: str = "longitude") -> str:
    """
    Build a SODA $where clause for datasets with separate numeric
    latitude/longitude columns (e.g. Landmarks).
    """
    bb = BRONZEVILLE_BBOX
    return (
        f"{lat_col} >= {bb['lat_min']} AND {lat_col} <= {bb['lat_max']} "
        f"AND {lon_col} >= {bb['lon_min']} AND {lon_col} <= {bb['lon_max']}"
    )


def _bbox_where_point(col: str = "location") -> str:
    """
    Build a SODA $where clause using within_box() for datasets that store
    coordinates in a single Socrata point column (e.g. CTA L Stops use
    a 'location' column rather than separate lat/lon fields).

    within_box(point_col, nw_lat, nw_lon, se_lat, se_lon)
    NW = (max_lat, min_lon)   SE = (min_lat, max_lon)
    """
    bb = BRONZEVILLE_BBOX
    return (
        f"within_box({col}, "
        f"{bb['lat_max']}, {bb['lon_min']}, "
        f"{bb['lat_min']}, {bb['lon_max']})"
    )


# ─── Column Normalisation ─────────────────────────────────────────────────────

def _normalise_coords(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure the DataFrame has float 'latitude' and 'longitude' columns.
    Handles three cases:
      1. Separate top-level latitude/longitude columns (most datasets)
      2. Dot-notation columns like 'location.latitude' (json_normalize output)
      3. A 'location' column containing a dict with 'latitude'/'longitude' keys
         (CTA L Stops, for example)
    """
    # Case 2: json_normalize already flattened into dot-notation columns
    if "latitude" not in df.columns and "location.latitude" in df.columns:
        df = df.rename(columns={
            "location.latitude":  "latitude",
            "location.longitude": "longitude",
        })

    # Case 3: 'location' column is a dict — extract coords row-by-row
    if ("latitude" not in df.columns or df["latitude"].isna().all()) \
            and "location" in df.columns:
        def _from_loc(val):
            if isinstance(val, dict):
                try:
                    return float(val.get("latitude")), float(val.get("longitude"))
                except (TypeError, ValueError):
                    pass
            return None, None

        coords = df["location"].apply(_from_loc)
        df = df.copy()
        df["latitude"]  = coords.apply(lambda x: x[0])
        df["longitude"] = coords.apply(lambda x: x[1])

    df["latitude"]  = pd.to_numeric(df.get("latitude"),  errors="coerce")
    df["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")
    return df


# ─── CTA Rail ─────────────────────────────────────────────────────────────────

def fetch_cta_rail_stations() -> pd.DataFrame:
    """
    Fetch CTA 'L' station stops within the Bronzeville bounding box.

    Dataset: 8pix-ypme — "CTA - System Information - List of 'L' Stops"
    Key columns returned:
        stop_id, stop_name, station_name, latitude, longitude, ada,
        red, blue, g, brn, p, pexp, y, pnk, o   (boolean line flags)
    """
    rows = _soda_get(
        DATASET_IDS["cta_rail_stations"],
        {"$where": _bbox_where_point("location")},
    )
    if not rows:
        logger.warning("CTA rail stations: empty response — check bbox / dataset ID.")
        return pd.DataFrame()

    df = pd.json_normalize(rows)
    df = _normalise_coords(df)
    logger.info(f"CTA rail stations: {len(df)} stops in Bronzeville bbox")
    return df


def fetch_cta_rail_lines() -> pd.DataFrame:
    """
    Fetch CTA rail line geometries (entire system — filter happens on map).

    Dataset: sgbp-kdxi — verify at https://data.cityofchicago.org
    The geometry is expected in a 'the_geom' column (GeoJSON-style dict).
    Falls back to an empty DataFrame with a warning if unavailable.
    """
    try:
        rows = _soda_get(DATASET_IDS["cta_rail_lines"], {})
        if not rows:
            raise ValueError("Empty response from CTA rail lines dataset.")
        df = pd.json_normalize(rows)
        logger.info(f"CTA rail lines: {len(df)} features")
        return df
    except Exception as exc:
        logger.warning(
            f"Could not fetch CTA rail lines (ID: {DATASET_IDS['cta_rail_lines']}): {exc}\n"
            "  → Update DATASET_IDS['cta_rail_lines'] in config.py if the ID is wrong.\n"
            "  → Search 'CTA L Lines' at https://data.cityofchicago.org to find the correct ID."
        )
        return pd.DataFrame()


# ─── CTA Bus ──────────────────────────────────────────────────────────────────

def fetch_cta_bus_stops() -> pd.DataFrame:
    """
    Fetch CTA bus stops within the Bronzeville bounding box.

    Dataset: d5bx-dr8z — verify ID at https://data.cityofchicago.org
    (search "CTA Bus Stops" or "CTA - Bus - Stops")
    """
    try:
        # hvnx-qtky doesn't support within_box; fetch all and filter in Python.
        rows = _soda_get(DATASET_IDS["cta_bus_stops"], {})
        if not rows:
            raise ValueError("Empty response.")
        df = pd.json_normalize(rows)
        # Drop completely empty rows (dataset sometimes returns schema-only rows)
        df = df.dropna(how="all")
        if df.empty:
            raise ValueError("All rows were empty after normalisation.")
        df = _normalise_coords(df)
        # Clip to Bronzeville bbox
        bb = BRONZEVILLE_BBOX
        df = df[
            df["latitude"].between(bb["lat_min"], bb["lat_max"]) &
            df["longitude"].between(bb["lon_min"], bb["lon_max"])
        ]
        logger.info(f"CTA bus stops: {len(df)} stops in bbox")
        return df
    except Exception as exc:
        logger.warning(f"Could not fetch CTA bus stops: {exc}")
        return pd.DataFrame()


def fetch_cta_bus_routes() -> pd.DataFrame:
    """
    Fetch CTA bus route geometries (full system — spatial clip on map).

    Dataset: hvnx-qtky — verify ID at https://data.cityofchicago.org
    (search "CTA Bus Routes" or "CTA - Bus - Routes")
    """
    try:
        rows = _soda_get(DATASET_IDS["cta_bus_routes"], {})
        if not rows:
            raise ValueError("Empty response.")
        df = pd.json_normalize(rows)
        logger.info(f"CTA bus routes: {len(df)} features")
        return df
    except Exception as exc:
        logger.warning(f"Could not fetch CTA bus routes: {exc}")
        return pd.DataFrame()


# ─── OSM / Overpass helper ────────────────────────────────────────────────────

_OSM_HEADERS = {"User-Agent": "BronzevilleCommunityAssetMap/1.0 (university-research)"}

def _overpass_get(query: str, timeout: int = 30) -> list[dict]:
    """POST an Overpass QL query; tries each mirror in OVERPASS_URLS.
    Results are cached for 7 days to survive transient Overpass outages.
    """
    import hashlib
    from cache import load_cache, save_cache

    cache_key = "osm_transport_" + hashlib.md5(query.encode()).hexdigest()[:12]
    cached = load_cache(cache_key)
    if cached is not None:
        return cached

    for url in OVERPASS_URLS:
        try:
            r = requests.post(url, data={"data": query},
                              headers=_OSM_HEADERS, timeout=timeout + 10)
            r.raise_for_status()
            elements = r.json().get("elements", [])
            if elements:
                save_cache(cache_key, elements)
            return elements
        except Exception as exc:
            logger.debug(f"Overpass mirror {url} failed: {exc}")
    return []


def fetch_osm_bus_stops() -> pd.DataFrame:
    """
    Fetch CTA bus stops from OpenStreetMap via Overpass.
    The official Chicago SODA bus-stop dataset (hvnx-qtky) is a non-tabular
    shapefile with no JSON API, so OSM is used instead.
    """
    bb = BRONZEVILLE_BBOX
    bbox = f"{bb['lat_min']},{bb['lon_min']},{bb['lat_max']},{bb['lon_max']}"
    query = f"[out:json][timeout:30];node[\"highway\"=\"bus_stop\"]({bbox});out body;"
    elements = _overpass_get(query)
    if not elements:
        logger.warning("OSM bus stops: no results returned")
        return pd.DataFrame()
    records = []
    for el in elements:
        tags = el.get("tags", {})
        lat, lon = el.get("lat"), el.get("lon")
        if lat is None or lon is None:
            continue
        records.append({
            "name":      tags.get("name", tags.get("ref", "CTA Bus Stop")),
            "label":     "CTA Bus Stop",
            "latitude":  float(lat),
            "longitude": float(lon),
        })
    df = pd.DataFrame(records)
    logger.info(f"OSM CTA bus stops: {len(df)} stops in Bronzeville bbox")
    return df


def fetch_osm_metra_stations() -> pd.DataFrame:
    """
    Fetch Metra commuter-rail stations from OpenStreetMap via Overpass.
    The Metra SODA datasets require auth (403), so OSM is used instead.
    Uses a slightly expanded bbox so lakefront Metra Electric stations aren't clipped.
    """
    bb = BRONZEVILLE_BBOX
    # Expand slightly eastward to catch Metra Electric lakefront stations
    bbox = f"{bb['lat_min']},{bb['lon_min']},{bb['lat_max']},-87.58"
    query = (
        f"[out:json][timeout:30];\n"
        f"(\n"
        f"  node[\"railway\"=\"station\"][\"operator\"=\"Metra\"]({bbox});\n"
        f"  node[\"railway\"=\"halt\"][\"operator\"=\"Metra\"]({bbox});\n"
        f");\n"
        f"out body;\n"
    )
    elements = _overpass_get(query)
    if not elements:
        logger.warning("OSM Metra stations: no results returned")
        return pd.DataFrame()
    records = []
    for el in elements:
        tags = el.get("tags", {})
        lat, lon = el.get("lat"), el.get("lon")
        if lat is None or lon is None:
            continue
        line = tags.get("line", tags.get("network", "Metra"))
        records.append({
            "name":      tags.get("name", "Metra Station"),
            "label":     f"Metra — {line}",
            "latitude":  float(lat),
            "longitude": float(lon),
        })
    df = pd.DataFrame(records)
    logger.info(f"OSM Metra stations: {len(df)} stations in/near Bronzeville")
    return df


# ─── Metra (legacy SODA — kept for reference, replaced by OSM above) ──────────

def fetch_metra_stations() -> pd.DataFrame:
    """
    Fetch Metra commuter-rail station locations in/near Bronzeville.

    Dataset: enz5-3bkg — Metra is a regional agency; the dataset may also
    be found on the RTA or CMAP open data portals if this ID fails.
    """
    try:
        rows = _soda_get(
            DATASET_IDS["metra_stations"],
            {"$where": _bbox_where()},
        )
        if not rows:
            raise ValueError("Empty response.")
        df = pd.json_normalize(rows)
        df = _normalise_coords(df)
        logger.info(f"Metra stations: {len(df)} in bbox")
        return df
    except Exception as exc:
        logger.warning(f"Could not fetch Metra stations: {exc}")
        return pd.DataFrame()


def fetch_metra_lines() -> pd.DataFrame:
    """
    Fetch Metra rail line geometries.

    Tries SODA dataset q8wx-dznq first; falls back to OSM Overpass for
    railway=rail + operator~Metra within the Bronzeville bbox.
    """
    # Try SODA first
    try:
        rows = _soda_get(DATASET_IDS["metra_lines"], {})
        if not rows:
            raise ValueError("Empty response.")
        df = pd.json_normalize(rows)
        logger.info(f"Metra lines (SODA): {len(df)} features")
        return df
    except Exception:
        logger.info("SODA Metra lines unavailable — falling back to OSM Overpass")

    # OSM Overpass fallback — fetch Metra rail ways in a wider bbox
    try:
        bb = BRONZEVILLE_BBOX
        # Expand bbox slightly so lines extend beyond the visible area
        bbox = f"{bb['lat_min']-0.02},{bb['lon_min']-0.02},{bb['lat_max']+0.02},{bb['lon_max']+0.02}"
        query = (
            f'[out:json][timeout:30];'
            f'way["railway"="rail"]["operator"~"Metra",i]({bbox});'
            f'out body;>;out skel qt;'
        )
        elements = _overpass_get(query)
        if not elements:
            raise ValueError("Empty Overpass response for Metra lines.")

        # Build node lookup for resolving way coordinates
        nodes = {e["id"]: (e["lon"], e["lat"]) for e in elements if e["type"] == "node"}
        ways = [e for e in elements if e["type"] == "way"]

        features = []
        for way in ways:
            coords = []
            for nid in way.get("nodes", []):
                if nid in nodes:
                    coords.append(list(nodes[nid]))
            if len(coords) >= 2:
                name = way.get("tags", {}).get("name", "Metra Line")
                features.append({
                    "name": name,
                    "the_geom.type": "LineString",
                    "the_geom.coordinates": coords,
                })

        if not features:
            raise ValueError("No Metra line ways resolved.")

        df = pd.DataFrame(features)
        logger.info(f"Metra lines (OSM): {len(df)} way segments")
        return df
    except Exception as exc:
        logger.warning(f"Could not fetch Metra lines from OSM: {exc}")
        return pd.DataFrame()


# ─── Bicycle Infrastructure ──────────────────────────────────────────────────

def fetch_bike_routes() -> pd.DataFrame:
    """
    Fetch bike route/lane line geometries within the Bronzeville area.

    Dataset: hvv9-38ut — "Bike Routes"
    Returns all routes system-wide; spatial clipping happens on the map.
    """
    try:
        bb = BRONZEVILLE_BBOX
        where = (
            f"within_box(the_geom, "
            f"{bb['lat_max']}, {bb['lon_min']}, "
            f"{bb['lat_min']}, {bb['lon_max']})"
        )
        rows = _soda_get(DATASET_IDS["bike_routes"], {"$where": where})
        if not rows:
            # Fall back to fetching all and filtering later
            rows = _soda_get(DATASET_IDS["bike_routes"], {})
        if not rows:
            raise ValueError("Empty response.")
        df = pd.json_normalize(rows)
        logger.info(f"Bike routes: {len(df)} features")
        return df
    except Exception as exc:
        logger.warning(f"Could not fetch bike routes: {exc}")
        return pd.DataFrame()


def fetch_divvy_stations() -> pd.DataFrame:
    """
    Fetch Divvy bicycle-share stations within the Bronzeville bounding box.

    Dataset: bbyy-e7gq — "Divvy Bicycle Stations"
    """
    try:
        bb = BRONZEVILLE_BBOX
        where = (
            f"latitude >= {bb['lat_min']} AND latitude <= {bb['lat_max']} "
            f"AND longitude >= {bb['lon_min']} AND longitude <= {bb['lon_max']}"
        )
        rows = _soda_get(DATASET_IDS["divvy_stations"], {"$where": where})
        if not rows:
            raise ValueError("Empty response.")
        df = pd.json_normalize(rows)
        df = _normalise_coords(df)
        logger.info(f"Divvy stations: {len(df)} in bbox")
        return df
    except Exception as exc:
        logger.warning(f"Could not fetch Divvy stations: {exc}")
        return pd.DataFrame()


def fetch_bike_racks() -> pd.DataFrame:
    """
    Fetch bike rack locations within the Bronzeville bounding box
    from OpenStreetMap via Overpass API.

    The Chicago SODA dataset (4ywc-hr3a) is an empty map view,
    so we use OSM instead.
    """
    bb = BRONZEVILLE_BBOX
    bbox = f"{bb['lat_min']},{bb['lon_min']},{bb['lat_max']},{bb['lon_max']}"
    query = f'[out:json][timeout:30];node["amenity"="bicycle_parking"]({bbox});out body;'
    elements = _overpass_get(query)
    if not elements:
        logger.warning("OSM bike racks: no results returned")
        return pd.DataFrame()
    records = []
    for el in elements:
        tags = el.get("tags", {})
        lat, lon = el.get("lat"), el.get("lon")
        if lat is None or lon is None:
            continue
        records.append({
            "name":      tags.get("name", "Bike Rack"),
            "label":     "Bike Rack",
            "racktype":  tags.get("bicycle_parking", "rack"),
            "capacity":  tags.get("capacity", "N/A"),
            "latitude":  float(lat),
            "longitude": float(lon),
        })
    df = pd.DataFrame(records)
    logger.info(f"OSM bike racks: {len(df)} in Bronzeville bbox")
    return df


# ─── Community Area Boundaries ────────────────────────────────────────────────

def fetch_community_boundaries(area_names: tuple[str, ...]) -> list[dict]:
    """
    Fetch GeoJSON geometries for the specified community areas.

    Dataset: igwz-8jzy — "Boundaries - Community Areas (current)"
    Returns a list of GeoJSON Feature dicts, one per community area.
    Each feature has 'properties' (community name, number) and 'geometry'.
    """
    name_list = ", ".join(f"'{n}'" for n in area_names)
    try:
        rows = _soda_get(
            DATASET_IDS["community_areas"],
            {"$where": f"community in ({name_list})"},
        )
        if not rows:
            logger.warning("Community areas: no rows returned.")
            return []

        features = []
        for row in rows:
            geom = row.get("the_geom", row.get("geometry"))
            if geom is None:
                continue
            if isinstance(geom, str):
                geom = json.loads(geom)
            features.append({
                "type": "Feature",
                "properties": {
                    "community":  row.get("community", ""),
                    "area_num":   row.get("area_numbe", row.get("area_num_1", "")),
                },
                "geometry": geom,
            })

        logger.info(f"Community boundaries: {len(features)} area polygons fetched")
        return features

    except Exception as exc:
        logger.warning(f"Could not fetch community boundaries: {exc}")
        return []


# ─── Convenience Wrapper ──────────────────────────────────────────────────────

def fetch_all_transportation() -> dict[str, pd.DataFrame]:
    """
    Fetch all six transportation datasets and the community boundaries.

    Returns a dict with keys:
        cta_rail_stations, cta_rail_lines,
        cta_bus_stops,     cta_bus_routes,
        metra_stations,    metra_lines
    """
    datasets = [
        ("cta_rail_stations", fetch_cta_rail_stations),
        ("cta_rail_lines",    fetch_cta_rail_lines),
        ("cta_bus_stops",     fetch_osm_bus_stops),     # OSM — SODA dataset has no JSON API
        ("cta_bus_routes",    fetch_cta_bus_routes),
        ("metra_stations",    fetch_osm_metra_stations), # OSM — SODA requires auth
        ("metra_lines",       fetch_metra_lines),
        ("bike_routes",       fetch_bike_routes),
        ("divvy_stations",    fetch_divvy_stations),
        ("bike_racks",        fetch_bike_racks),
    ]

    result = {}
    for i, (name, fn) in enumerate(datasets, start=1):
        label = name.replace("_", " ").title()
        print(f"  [{i}/{len(datasets)}] {label} …", end=" ", flush=True)
        df = fn()
        result[name] = df
        print(f"{len(df)} records" if not df.empty else "unavailable")

    return result
