"""
geocode.py — Address geocoding utilities used as a fallback for any asset
records that arrive without coordinates (most SODA datasets already include
lat/lon, and OSM results always have coordinates, so this module is rarely
invoked in practice).

Primary:  US Census Bureau Geocoding Service (batch, free, no API key)
          https://geocoding.geo.census.gov/geocoder/

Fallback: Nominatim / OpenStreetMap (single address, free, 1 req/sec limit)
          https://nominatim.openstreetmap.org/
          Usage policy: https://operations.osmfoundation.org/policies/nominatim/
"""

import io
import time
import logging
import requests
import pandas as pd
from typing import Optional

logger = logging.getLogger(__name__)

CENSUS_BATCH_URL  = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch"
NOMINATIM_URL     = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "BronzevilleCommunityAssetMap/1.0 (university-research)"}
NOMINATIM_DELAY   = 1.1   # seconds between requests — Nominatim policy


# ─── Census Batch Geocoder ────────────────────────────────────────────────────

def _build_census_csv(df: pd.DataFrame,
                      address_col: str,
                      city: str = "Chicago",
                      state: str = "IL") -> str:
    """
    Build the CSV payload expected by the Census batch endpoint.

    CSV format (no header row):
        unique_id, street_address, city, state, zip
    The unique_id is the DataFrame index so results can be joined back.
    """
    lines = []
    for idx, row in df.iterrows():
        address = str(row[address_col]).strip()

        # If the address field already ends with a 5-digit ZIP, split it out.
        zip_code = ""
        parts = address.split()
        if parts and parts[-1].isdigit() and len(parts[-1]) == 5:
            zip_code = parts[-1]
            address  = " ".join(parts[:-1])

        lines.append(f'{idx},"{address}","{city}","{state}","{zip_code}"')

    return "\n".join(lines)


def geocode_batch_census(df: pd.DataFrame,
                         address_col: str = "Address",
                         city: str = "Chicago",
                         state: str = "IL") -> pd.DataFrame:
    """
    Batch-geocode every row in df[address_col] via the Census Bureau API.

    Adds 'latitude' and 'longitude' columns. Rows that don't match are
    left as NaN — geocode_fallback_nominatim() can fill those in later.

    The Census geocoder accepts up to 10 000 addresses per request.
    """
    if df.empty:
        return df

    csv_payload = _build_census_csv(df, address_col, city, state)
    logger.info(f"Census batch geocoder: {len(df)} addresses …")

    try:
        resp = requests.post(
            CENSUS_BATCH_URL,
            data={"benchmark": "Public_AR_Current"},
            files={"addressFile": ("addresses.csv",
                                   csv_payload.encode("utf-8"),
                                   "text/csv")},
            timeout=120,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(f"Census geocoder failed: {exc}")
        df = df.copy()
        df["latitude"]  = None
        df["longitude"] = None
        return df

    # Parse the returned CSV.
    # Response columns (no header):
    #   id | input_address | match | match_type | matched_address | coordinates | tiger_id | side
    # 'coordinates' is formatted as "lon,lat" (longitude first — note the swap).
    result = pd.read_csv(
        io.StringIO(resp.text),
        header=None,
        names=["id", "input_address", "match", "match_type",
               "matched_address", "coordinates", "tiger_id", "side"],
        dtype=str,
    )

    def _parse_coords(coord_str: str):
        try:
            lon_s, lat_s = coord_str.split(",")
            return float(lat_s.strip()), float(lon_s.strip())   # return (lat, lon)
        except Exception:
            return None, None

    coords_series = result["coordinates"].apply(_parse_coords)
    result["latitude"]  = coords_series.apply(lambda x: x[0])
    result["longitude"] = coords_series.apply(lambda x: x[1])

    result["id"] = pd.to_numeric(result["id"], errors="coerce")
    result = result.set_index("id")[["latitude", "longitude"]]

    df = df.copy()
    df["latitude"]  = result["latitude"]
    df["longitude"] = result["longitude"]

    matched = df["latitude"].notna().sum()
    logger.info(f"Census geocoder: {matched}/{len(df)} addresses matched")
    return df


# ─── Nominatim Fallback ───────────────────────────────────────────────────────

def _geocode_single_nominatim(address: str,
                               city: str = "Chicago, IL",
                               ) -> tuple[Optional[float], Optional[float]]:
    """
    Geocode a single address via Nominatim. Returns (lat, lon) or (None, None).
    Respects the 1-request-per-second usage policy with a built-in delay.
    """
    query  = f"{address}, {city}"
    params = {"q": query, "format": "json", "limit": 1, "countrycodes": "us"}

    try:
        resp = requests.get(
            NOMINATIM_URL, params=params,
            headers=NOMINATIM_HEADERS, timeout=15,
        )
        resp.raise_for_status()
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as exc:
        logger.debug(f"Nominatim: '{query}' → {exc}")

    return None, None


def geocode_fallback_nominatim(df: pd.DataFrame,
                                address_col: str = "Address") -> pd.DataFrame:
    """
    Fill any NaN latitude/longitude rows left by the Census geocoder
    using Nominatim, one address at a time.
    """
    mask = df["latitude"].isna() | df["longitude"].isna()
    n_missing = mask.sum()
    if n_missing == 0:
        return df

    logger.info(f"Nominatim fallback: {n_missing} unmatched addresses …")
    df = df.copy()

    for idx in df[mask].index:
        lat, lon = _geocode_single_nominatim(str(df.at[idx, address_col]))
        df.at[idx, "latitude"]  = lat
        df.at[idx, "longitude"] = lon
        time.sleep(NOMINATIM_DELAY)

    still_missing = df["latitude"].isna().sum()
    logger.info(
        f"Nominatim: resolved {n_missing - still_missing} additional addresses "
        f"({still_missing} still unmatched)"
    )
    return df


# ─── Public Entry Point ───────────────────────────────────────────────────────

def geocode_dataframe(df: pd.DataFrame,
                      address_col: str = "Address",
                      city: str = "Chicago",
                      state: str = "IL") -> pd.DataFrame:
    """
    Geocode all rows in df that lack coordinates.

    If 'latitude' and 'longitude' columns are already present and non-null,
    only missing values are filled (Census first, then Nominatim fallback).
    If the columns don't exist at all, all rows are geocoded.
    """
    if "latitude" not in df.columns:
        df["latitude"] = None
    if "longitude" not in df.columns:
        df["longitude"] = None

    # Only geocode rows that are actually missing coordinates
    needs_geocoding = df["latitude"].isna() | df["longitude"].isna()
    if not needs_geocoding.any():
        logger.info("geocode_dataframe: all rows already have coordinates — skipping")
        return df

    subset = df[needs_geocoding].copy()
    subset = geocode_batch_census(subset, address_col, city, state)
    subset = geocode_fallback_nominatim(subset, address_col)

    df = df.copy()
    df.loc[needs_geocoding, "latitude"]  = subset["latitude"].values
    df.loc[needs_geocoding, "longitude"] = subset["longitude"].values

    return df
