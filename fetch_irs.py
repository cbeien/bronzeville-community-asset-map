"""
fetch_irs.py — Fetch IRS Exempt Organizations (EO BMF) for Bronzeville nonprofits.

The IRS Exempt Organizations Business Master File lists every federally recognized
tax-exempt organization. All records have STATUS='01' (active) — orgs that fail to
file for 3 consecutive years are auto-revoked and removed from this file entirely.

This fills a key gap: OSM only tags *places* of worship it knows about, missing
churches that rent space or haven't been mapped. The IRS dataset captures all
registered religious nonprofits regardless of property ownership.

Stale-address risk is mitigated downstream by geocoding + boundary clipping:
addresses that don't resolve inside Bronzeville simply don't appear on the map.

Source: https://www.irs.gov/pub/irs-soi/eo_il.csv (Illinois, updated periodically)
"""
from __future__ import annotations

import logging
import re
import urllib.request
import pandas as pd

from cache import load_cache, save_cache

logger = logging.getLogger(__name__)

_IRS_IL_URL   = "https://www.irs.gov/pub/irs-soi/eo_il.csv"
_BRONZEVILLE_ZIPS = {"60615", "60616", "60653"}

# NTEE code prefix → our 7-category schema
_NTEE_CATEGORY: dict[str, tuple[str, str]] = {
    "X": ("worship",   "Religious Organization"),
    "A": ("education", "Arts & Cultural Organization"),
    "B": ("education", "Educational Organization"),
    "E": ("health",    "Health Organization"),
    "F": ("health",    "Mental Health Organization"),
    "P": ("health",    "Social Services"),
}

# Name keywords that indicate a place of worship even when NTEE is blank.
# Catches registered orgs like "Metropolitan Community Church" (blank NTEE code).
_WORSHIP_KEYWORDS = re.compile(
    r"\b(church|temple|mosque|synagogue|congregation|parish|chapel|"
    r"ministries|ministry|tabernacle|cathedral|baptist|methodist|"
    r"pentecostal|evangelical|islamic|buddhist|masjid|shul|"
    r"assembly of god|church of god|church of christ)\b",
    re.IGNORECASE,
)

# NTEE sub-code descriptions for common X* codes (used as Type label)
_NTEE_X_LABELS: dict[str, str] = {
    "X11":  "Religious Foundation",
    "X12":  "Religious Fund",
    "X19":  "Religious Support",
    "X20":  "Christian Organization",
    "X21":  "Protestant Church",
    "X22":  "Roman Catholic",
    "X30":  "Jewish Organization",
    "X40":  "Islamic Organization",
    "X50":  "Buddhist Organization",
    "X70":  "Hindu Organization",
    "X80":  "Mormon Organization",
    "X90":  "Interfaith / Religious Activities",
    "X99":  "Religious Organization",
    "X100": "Religious Organization",
}


def _worship_type_from_name(name: str) -> str:
    """Derive a readable worship type from org name keywords."""
    n = name.upper()
    if "BAPTIST"    in n: return "Baptist Church"
    if "METHODIST"  in n: return "Methodist Church"
    if "CATHOLIC"   in n: return "Roman Catholic"
    if "PENTECOSTAL" in n: return "Pentecostal Church"
    if "APOSTOLIC"  in n: return "Apostolic Church"
    if "LUTHERAN"   in n: return "Lutheran Church"
    if "PRESBYTERIAN" in n: return "Presbyterian Church"
    if "EPISCOPAL"  in n: return "Episcopal Church"
    if "MOSQUE"     in n or "ISLAMIC" in n or "MASJID" in n: return "Islamic"
    if "SYNAGOGUE"  in n or "JEWISH"  in n or "SHUL"   in n: return "Jewish"
    if "BUDDHIST"   in n: return "Buddhist"
    if "HINDU"      in n: return "Hindu"
    if "TEMPLE"     in n: return "Temple"
    return "Church"


def fetch_irs_nonprofits() -> dict[str, pd.DataFrame]:
    """
    Download the IRS EO BMF for Illinois, filter to Bronzeville ZIP codes,
    and return a dict of DataFrames keyed by category: worship, education, health.

    Records are tagged with source='IRS EO BMF' and have no lat/lon (geocoding
    is applied by the caller via geocode_dataframe()).
    """
    cached = load_cache("irs_nonprofits")
    if cached is not None:
        logger.info(f"IRS EO BMF: {sum(len(v) for v in cached.values())} records loaded from cache")
        return {k: pd.DataFrame(v) for k, v in cached.items()}

    logger.info("Fetching IRS EO BMF for Illinois …")
    try:
        req = urllib.request.Request(
            _IRS_IL_URL,
            headers={"User-Agent": "BronzevilleAssetMap/1.0 (university-research)"},
        )
        with urllib.request.urlopen(req, timeout=60) as r:
            content = r.read().decode("utf-8", errors="replace")
    except Exception as exc:
        logger.warning(f"IRS EO BMF fetch error: {exc}")
        return {"worship": pd.DataFrame(), "education": pd.DataFrame(), "health": pd.DataFrame()}

    lines   = content.strip().split("\n")
    header  = [h.strip().strip('"') for h in lines[0].split(",")]

    worship_rows, education_rows, health_rows = [], [], []

    for line in lines[1:]:
        cols = [c.strip().strip('"') for c in line.split(",")]
        if len(cols) < len(header):
            continue
        row = dict(zip(header, cols))

        # Filter to Bronzeville ZIPs
        if row.get("ZIP", "")[:5] not in _BRONZEVILLE_ZIPS:
            continue

        name   = row.get("NAME", "").strip().title()
        street = row.get("STREET", "").strip()
        zip5   = row.get("ZIP", "")[:5]

        if not name or not street:
            continue

        # Skip PO Boxes — no geocodable location
        su = street.upper()
        if su.startswith("PO ") or su.startswith("P.O.") or "PO BOX" in su:
            continue

        ntee = row.get("NTEE_CD", "").strip()

        # Determine category and type
        category = None
        type_label = None

        # 1. NTEE-based mapping
        for prefix, (cat, lbl) in _NTEE_CATEGORY.items():
            if ntee.startswith(prefix):
                category   = cat
                type_label = _NTEE_X_LABELS.get(ntee, lbl) if cat == "worship" else lbl
                break

        # 2. Blank-NTEE keyword catch for worship
        if category is None and _WORSHIP_KEYWORDS.search(name):
            category   = "worship"
            type_label = _worship_type_from_name(name)

        if category is None:
            continue

        address = f"{street.title()}, Chicago, IL {zip5}"
        record  = {
            "Name":      name,
            "Address":   address,
            "Type":      type_label,
            "category":  category,
            "latitude":  None,
            "longitude": None,
            "source":    "IRS EO BMF",
        }

        if   category == "worship":   worship_rows.append(record)
        elif category == "education": education_rows.append(record)
        elif category == "health":    health_rows.append(record)

    results = {
        "worship":   pd.DataFrame(worship_rows)   if worship_rows   else pd.DataFrame(),
        "education": pd.DataFrame(education_rows) if education_rows else pd.DataFrame(),
        "health":    pd.DataFrame(health_rows)    if health_rows    else pd.DataFrame(),
    }

    total = sum(len(v) for v in results.values())
    logger.info(
        f"IRS EO BMF: {total} records "
        f"(worship={len(worship_rows)}, education={len(education_rows)}, health={len(health_rows)})"
    )

    save_cache("irs_nonprofits", {k: v.to_dict(orient="records") for k, v in results.items()})
    return results
