"""
config.py — Central configuration for the Bronzeville Community Asset Map.

All geographic bounds, zone definitions, API endpoints, dataset IDs,
styling constants, and output paths live here so every other module
imports from one place.
"""

# ─── Bounding Boxes ───────────────────────────────────────────────────────────

# Display bbox — used for SODA $where filters and Folium map extent.
# Bronzeville: ~26th St (N) → ~63rd St (S), Dan Ryan (W) → Lake Michigan (E).
BRONZEVILLE_BBOX = {
    "lat_min": 41.780,   # ~63rd St
    "lat_max": 41.845,   # ~26th St
    "lon_min": -87.630,  # Dan Ryan Expressway (I-90/94)
    "lon_max": -87.525,  # Lake Michigan shoreline
}

# Overpass API bbox — as specified: (south, west, north, east)
OVERPASS_BBOX = (41.79, -87.64, 41.87, -87.60)

# Map center for Folium and Leaflet (roughly 43rd & King Dr)
BRONZEVILLE_CENTER = [41.812, -87.608]

# ─── Community Area Definitions ───────────────────────────────────────────────
# Chicago's official community area numbers (dataset igwz-8jzy).
# Douglas = 35, Grand Boulevard = 38 (not 37/38 as sometimes cited).
COMMUNITY_AREAS = {
    "DOUGLAS":         35,
    "GRAND BOULEVARD": 38,
}

# ─── Zone Definitions (5 Bronzeville Zones) ───────────────────────────────────
# Latitude boundaries from Chicago's address grid
# (~800 address numbers ≈ 1 mile ≈ 0.0145° latitude).
ZONES = {
    "Zone 1": {
        "label":   "Zone 1 — North (26th–31st St)",
        "lat_min": 41.838,
        "lat_max": 41.845,
        "color":   "#D6EAF8",
        "border":  "#2980B9",
    },
    "Zone 2": {
        "label":   "Zone 2 — 35th St Corridor (31st–37th St)",
        "lat_min": 41.826,
        "lat_max": 41.838,
        "color":   "#D5F5E3",
        "border":  "#1E8449",
    },
    "Zone 3": {
        "label":   "Zone 3 — 39th–43rd St",
        "lat_min": 41.811,
        "lat_max": 41.826,
        "color":   "#FEF9E7",
        "border":  "#D4AC0D",
    },
    "Zone 4": {
        "label":   "Zone 4 — 47th St Corridor (43rd–49th St)",
        "lat_min": 41.800,
        "lat_max": 41.811,
        "color":   "#FDEDEC",
        "border":  "#C0392B",
    },
    "Zone 5": {
        "label":   "Zone 5 — Hyde Park / 51st–63rd St",
        "lat_min": 41.780,
        "lat_max": 41.800,
        "color":   "#F5EEF8",
        "border":  "#7D3C98",
    },
}

# ─── City of Chicago Socrata Open Data API ────────────────────────────────────
SODA_BASE_URL = "https://data.cityofchicago.org/resource"

# Dataset IDs — verify at https://data.cityofchicago.org if a fetch fails.
DATASET_IDS = {
    # Transportation — CTA
    "cta_rail_stations": "8pix-ypme",  # CTA - System Information - List of 'L' Stops (location point field)
    "cta_rail_lines":    "xbyr-jnvx",  # CTA - 'L' (Rail) Lines (MultiLineString geometry)
    "cta_bus_stops":     "hvnx-qtky",  # CTA Bus Stops (location point field)
    "cta_bus_routes":    "hvnx-qtky",  # Same dataset; bus route geometries not separately published

    # Transportation — Metra (403 without app token; gracefully skipped)
    "metra_stations":    "nqm8-q2ym",  # Metra Stations
    "metra_lines":       "q8wx-dznq",  # Metra Lines

    # Bicycle Infrastructure
    "bike_routes":       "hvv9-38ut",  # Bike Routes (line geometries)
    "divvy_stations":    "bbyy-e7gq",  # Divvy Bicycle Stations
    "bike_racks":        "4ywc-hr3a",  # Bike Racks (point locations)

    # Community Assets — Chicago Data Portal
    "landmarks":         "tdab-kixi",  # Chicago Landmarks (has latitude/longitude columns)
    "business_licenses": "uupf-x98q",  # Active Business Licenses (Current)
    "schools":           "c7jj-qjvh",  # Chicago Public Schools - School Locations SY1314 (has coords)
    "parks":             "eix4-gf83",  # Chicago Park District - Park Boundaries (polygon; no community_area field)
    "libraries":         "x8fc-8rcq",  # Chicago Public Libraries

    # Boundaries
    "community_areas":   "igwz-8jzy",  # Community Area boundaries (GeoJSON)
}

# ZIP codes covering Bronzeville (used to filter business licenses)
BRONZEVILLE_ZIPS = ("60615", "60616", "60653")

# Business license description substrings → classifies a license as a restaurant
RESTAURANT_KEYWORDS = ("RETAIL FOOD", "CONSUMPTION ON PREMISES")

# Max records per SODA request (unauthenticated free tier)
SODA_LIMIT = 50_000

# ─── Overpass API (OpenStreetMap) ─────────────────────────────────────────────
OVERPASS_URL  = "https://overpass.kumi.systems/api/interpreter"   # primary mirror
OVERPASS_URLS = [                                                    # fallback list
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.openstreetmap.ru/api/interpreter",
]

# ─── CTA Line Colors (official brand palette) ─────────────────────────────────
CTA_LINE_COLORS = {
    "red":  "#c60c30",
    "blue": "#00a1de",
    "g":    "#009b3a",   # Green
    "brn":  "#62361b",   # Brown
    "p":    "#522398",   # Purple
    "pexp": "#522398",   # Purple Express
    "y":    "#f9e300",   # Yellow
    "pnk":  "#e27ea6",   # Pink
    "o":    "#f9461c",   # Orange
}

# ─── Marker Styles ────────────────────────────────────────────────────────────
# 'color' is the Folium named colour; 'hex' is used for Leaflet CSS/legend.
MARKER_STYLES = {
    # Community asset categories
    "landmark":    {"color": "blue",      "icon": "star",            "hex": "#1A5276", "label": "Landmarks"},
    "restaurant":  {"color": "red",       "icon": "cutlery",         "hex": "#C0392B", "label": "Restaurants"},
    "business":    {"color": "green",     "icon": "briefcase",       "hex": "#1E8449", "label": "Businesses"},
    "education":   {"color": "purple",    "icon": "graduation-cap",  "hex": "#6C3483", "label": "Education & Culture"},
    "worship":     {"color": "orange",    "icon": "church",          "hex": "#CA6F1E", "label": "Houses of Worship"},
    "park":        {"color": "darkgreen", "icon": "tree",            "hex": "#145A32", "label": "Parks"},
    "health":      {"color": "darkred",   "icon": "plus-square",     "hex": "#922B21", "label": "Health & Social Services"},

    # Transportation layers
    "cta_rail":    {"color": "gray",      "icon": "train",           "hex": "#455A64", "label": "CTA Rail Stations"},
    "cta_bus":     {"color": "gray",      "icon": "bus",             "hex": "#455A64", "label": "CTA Bus Stops"},
    "metra":       {"color": "gray",      "icon": "train",           "hex": "#455A64", "label": "Metra Stations"},

    # Rail line layers (polylines)
    "cta_rail_line": {"color": "gray",     "icon": "train",           "hex": "#c60c30", "label": "CTA Rail Lines"},
    "metra_line":    {"color": "darkblue", "icon": "train",           "hex": "#1A237E", "label": "Metra Lines"},

    # Bicycle infrastructure layers
    "bike_route":  {"color": "cadetblue", "icon": "bicycle",         "hex": "#00897B", "label": "Bike Lanes"},
    "divvy":       {"color": "blue",      "icon": "bicycle",         "hex": "#1565C0", "label": "Divvy Stations"},
    "bike_rack":   {"color": "lightblue", "icon": "lock",            "hex": "#4DD0E1", "label": "Bike Racks"},

    # Vacant property layers
    "vacant_registered": {"color": "brown", "icon": "ban", "hex": "#5D4037", "label": "Vacant (City Registry)"},
}

# Ordered list of all asset categories (used for filter panel and exports)
ASSET_CATEGORIES = [
    "landmark", "restaurant", "business",
    "education", "worship", "park", "health",
]

# ─── Output Paths ─────────────────────────────────────────────────────────────
VACANT_PDF_PATH   = "data/vacant_building_registry.pdf"  # City of Chicago Vacant Building Registry PDF

OUTPUT_MAP_HTML   = "bronzeville_map.html"      # Folium interactive map
OUTPUT_EXCEL      = "bronzeville_assets.xlsx"   # Multi-sheet workbook
OUTPUT_SUMMARY    = "bronzeville_summary.html"  # HTML summary tables
OUTPUT_DATA_JS    = "data.js"                   # JavaScript data for Leaflet site
OUTPUT_INDEX_HTML = "index.html"                # Standalone Leaflet website
