"""
build_map.py — Assemble the Folium interactive map for the Bronzeville
Community Asset Map.

Layer order (bottom → top):
  1.  Base tiles (CartoDB Light, OpenStreetMap selectable)
  2.  Community area boundary polygon
  3.  Zone rectangles (5 zones)
  4.  CTA Rail Lines
  5.  CTA Rail Stations
  6.  CTA Bus Routes
  7.  CTA Bus Stops (clustered)
  8.  Metra Lines
  9.  Metra Stations
  10–18. Nine asset category layers (landmarks … social services)
  19. Layer control panel (top-right)
"""

from __future__ import annotations

import json
import logging
import folium
from folium.plugins import MarkerCluster
import pandas as pd
from typing import Optional

from config import (
    BRONZEVILLE_CENTER,
    BRONZEVILLE_BBOX,
    ZONES,
    CTA_LINE_COLORS,
    MARKER_STYLES,
    ASSET_CATEGORIES,
)

logger = logging.getLogger(__name__)


# ─── Coordinate Helpers ───────────────────────────────────────────────────────

def _f(val) -> Optional[float]:
    """Safe float conversion; returns None on failure."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _geom_to_segments(geom) -> list[list[list[float]]]:
    """
    Convert a GeoJSON-style geometry dict (LineString or MultiLineString)
    to a list of [[lat, lon], ...] coordinate sequences.
    GeoJSON stores coordinates as [lon, lat]; Folium expects [lat, lon].
    """
    if not isinstance(geom, dict):
        return []

    gtype  = geom.get("type", "")
    coords = geom.get("coordinates", [])
    segments = []

    if gtype == "LineString":
        segments.append([[c[1], c[0]] for c in coords])
    elif gtype == "MultiLineString":
        for line in coords:
            segments.append([[c[1], c[0]] for c in line])

    return segments


def _parse_geom(raw) -> Optional[dict]:
    """Ensure a geometry value is a dict (parse JSON string if needed)."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            pass
    return None


# ─── Base Map ─────────────────────────────────────────────────────────────────

def create_base_map() -> folium.Map:
    """Create a Folium map centred on Bronzeville with two tile options."""
    m = folium.Map(
        location=BRONZEVILLE_CENTER,
        zoom_start=14,
        tiles=None,          # suppress default tiles; add named layers below
        control_scale=True,
    )
    folium.TileLayer("CartoDB positron",  name="CartoDB Light").add_to(m)
    folium.TileLayer("OpenStreetMap",     name="OpenStreetMap").add_to(m)
    return m


# ─── Community Boundary ───────────────────────────────────────────────────────

def add_community_boundary(m: folium.Map,
                            features: list[dict]) -> folium.Map:
    """
    Draw the Bronzeville community area boundaries as a GeoJSON overlay.
    features is the list returned by fetch_data.fetch_community_boundaries().
    """
    if not features:
        logger.info("Community boundary: no features available.")
        return m

    geojson = {"type": "FeatureCollection", "features": features}

    folium.GeoJson(
        geojson,
        name="Community Area Boundary",
        style_function=lambda _: {
            "fillColor":   "#000000",
            "fillOpacity": 0.05,
            "color":       "#333333",
            "weight":      3,
            "dashArray":   "6 4",
        },
        tooltip=folium.GeoJsonTooltip(fields=["community"],
                                      aliases=["Community Area:"]),
    ).add_to(m)

    return m


# ─── Zone Rectangles ─────────────────────────────────────────────────────────

def add_zone_boundaries(m: folium.Map) -> folium.Map:
    """
    Draw a semi-transparent coloured rectangle for each of the 5 zones.
    Each rectangle spans the full east-west extent of the bounding box.
    """
    lon_min = BRONZEVILLE_BBOX["lon_min"]
    lon_max = BRONZEVILLE_BBOX["lon_max"]

    layer = folium.FeatureGroup(name="Zone Boundaries", show=True)

    for zone_key, zone in ZONES.items():
        folium.Rectangle(
            bounds=[[zone["lat_min"], lon_min],
                    [zone["lat_max"], lon_max]],
            color=zone["border"],
            fill=True,
            fill_color=zone["color"],
            fill_opacity=0.20,
            weight=2,
            tooltip=zone["label"],
            popup=folium.Popup(
                f"<b>{zone['label']}</b>", parse_html=True, max_width=250
            ),
        ).add_to(layer)

    layer.add_to(m)
    return m


# ─── Transportation Layers ────────────────────────────────────────────────────

def add_cta_rail_lines(m: folium.Map, df: pd.DataFrame) -> folium.Map:
    """
    Draw CTA 'L' rail lines as coloured polylines.
    The 'the_geom' column is expected to hold a GeoJSON geometry dict or string.
    Line colour is derived from a 'lines' or 'name' field matched against the
    CTA brand palette in config.py.
    """
    if df.empty:
        return m

    layer = folium.FeatureGroup(name="CTA Rail Lines", show=True)

    for _, row in df.iterrows():
        geom = _parse_geom(row.get("the_geom", row.get("geometry")))
        segments = _geom_to_segments(geom)
        if not segments:
            continue

        line_str = str(row.get("lines", row.get("name", ""))).lower()
        color = next(
            (CTA_LINE_COLORS[k] for k in CTA_LINE_COLORS if k in line_str),
            "#888888",
        )
        label = row.get("lines", row.get("name", "CTA L Line"))

        for seg in segments:
            folium.PolyLine(
                locations=seg, color=color,
                weight=5, opacity=0.85, tooltip=label,
            ).add_to(layer)

    layer.add_to(m)
    return m


def add_cta_rail_stations(m: folium.Map, df: pd.DataFrame) -> folium.Map:
    """Plot CTA 'L' station markers within the Bronzeville bbox."""
    if df.empty:
        return m

    style = MARKER_STYLES["cta_rail"]
    layer = folium.FeatureGroup(name="CTA Rail Stations", show=True)

    for _, row in df.iterrows():
        lat, lon = _f(row.get("latitude")), _f(row.get("longitude"))
        if lat is None or lon is None:
            continue

        name  = row.get("station_name", row.get("stop_name", "CTA Station"))
        lines = [k.upper() for k in CTA_LINE_COLORS
                 if str(row.get(k, "false")).lower() == "true"]

        popup_html = (
            f"<b>{name}</b><br>"
            f"<i>CTA 'L' Station</i><br>"
            f"Lines: {', '.join(lines) or 'N/A'}<br>"
            f"ADA: {row.get('ada', 'N/A')}"
        )
        folium.Marker(
            location=[lat, lon],
            tooltip=name,
            popup=folium.Popup(popup_html, max_width=240),
            icon=folium.Icon(color=style["color"], icon=style["icon"], prefix="fa"),
        ).add_to(layer)

    layer.add_to(m)
    return m


def add_cta_bus_routes(m: folium.Map, df: pd.DataFrame) -> folium.Map:
    """Draw CTA bus route lines (hidden by default — visually noisy)."""
    if df.empty:
        return m

    style = MARKER_STYLES["cta_bus"]
    layer = folium.FeatureGroup(name="CTA Bus Routes", show=False)

    for _, row in df.iterrows():
        geom = _parse_geom(row.get("the_geom", row.get("geometry")))
        for seg in _geom_to_segments(geom):
            folium.PolyLine(
                locations=seg,
                color=style["hex"], weight=2,
                opacity=0.55, dash_array="5 5",
                tooltip=row.get("route", row.get("name", "Bus Route")),
            ).add_to(layer)

    layer.add_to(m)
    return m


def add_cta_bus_stops(m: folium.Map, df: pd.DataFrame) -> folium.Map:
    """CTA bus stops as small circle markers, grouped in a cluster."""
    if df.empty:
        return m

    layer   = folium.FeatureGroup(name="CTA Bus Stops", show=False)
    cluster = MarkerCluster().add_to(layer)

    for _, row in df.iterrows():
        lat, lon = _f(row.get("latitude")), _f(row.get("longitude"))
        if lat is None or lon is None:
            continue

        stop_name = row.get("stop_name", row.get("name", "Bus Stop"))
        folium.CircleMarker(
            location=[lat, lon], radius=4,
            color=MARKER_STYLES["cta_bus"]["hex"],
            fill=True, fill_opacity=0.7,
            tooltip=stop_name,
            popup=folium.Popup(
                f"<b>{stop_name}</b><br><i>CTA Bus Stop</i>", max_width=200
            ),
        ).add_to(cluster)

    layer.add_to(m)
    return m


def add_metra_lines(m: folium.Map, df: pd.DataFrame) -> folium.Map:
    """Draw Metra commuter-rail line geometries."""
    if df.empty:
        return m

    layer = folium.FeatureGroup(name="Metra Lines", show=True)

    for _, row in df.iterrows():
        geom = _parse_geom(row.get("the_geom", row.get("geometry")))
        label = row.get("lines", row.get("line_name", row.get("name", "Metra")))

        for seg in _geom_to_segments(geom):
            folium.PolyLine(
                locations=seg,
                color=MARKER_STYLES["metra"]["hex"],
                weight=5, opacity=0.85, tooltip=label,
            ).add_to(layer)

    layer.add_to(m)
    return m


def add_metra_stations(m: folium.Map, df: pd.DataFrame) -> folium.Map:
    """Plot Metra station markers."""
    if df.empty:
        return m

    style = MARKER_STYLES["metra"]
    layer = folium.FeatureGroup(name="Metra Stations", show=True)

    for _, row in df.iterrows():
        lat, lon = _f(row.get("latitude")), _f(row.get("longitude"))
        if lat is None or lon is None:
            continue

        name = row.get("stop_name", row.get("station_name", row.get("name", "Metra")))
        line = row.get("lines", row.get("line_name", "N/A"))

        folium.Marker(
            location=[lat, lon],
            tooltip=name,
            popup=folium.Popup(
                f"<b>{name}</b><br><i>Metra Station</i><br>Line: {line}",
                max_width=240,
            ),
            icon=folium.Icon(color=style["color"], icon=style["icon"], prefix="fa"),
        ).add_to(layer)

    layer.add_to(m)
    return m


# ─── Asset Marker Layers ──────────────────────────────────────────────────────

def _popup_html(name: str, address: str, asset_type: str,
                category: str, source: str,
                lat: float, lon: float) -> str:
    """Render a styled HTML popup for an asset marker."""
    cat_label = category.replace("_", " ").title()
    return f"""
    <div style="font-family:Arial,sans-serif;min-width:190px;max-width:260px;">
      <b style="font-size:1.05em;">{name}</b><br>
      <span style="color:#555;font-size:.9em;">{address or 'Address unavailable'}</span>
      <hr style="margin:5px 0;border-color:#ddd;">
      Category: <b>{cat_label}</b><br>
      Type: <span style="color:#333">{asset_type or '—'}</span><br>
      Source: <i style="color:#888;font-size:.85em;">{source}</i><br>
      <small style="color:#aaa;">{lat:.5f}, {lon:.5f}</small>
    </div>
    """


def add_asset_layer(m: folium.Map,
                    df: pd.DataFrame,
                    category: str,
                    layer_name: str,
                    show: bool = True) -> folium.Map:
    """
    Generic asset marker layer. Reads MARKER_STYLES[category] for icon/colour.
    Skips rows with missing coordinates without raising an error.
    """
    if df.empty:
        return m

    style = MARKER_STYLES.get(category, MARKER_STYLES["landmark"])
    layer = folium.FeatureGroup(name=layer_name, show=show)

    for _, row in df.iterrows():
        lat = _f(row.get("latitude"))
        lon = _f(row.get("longitude"))
        if lat is None or lon is None:
            continue

        name     = str(row.get("Name", ""))
        address  = str(row.get("Address", ""))
        atype    = str(row.get("Type", ""))
        cat      = str(row.get("category", category))
        source   = str(row.get("source", ""))

        folium.Marker(
            location=[lat, lon],
            tooltip=name or layer_name,
            popup=folium.Popup(
                _popup_html(name, address, atype, cat, source, lat, lon),
                max_width=280,
            ),
            icon=folium.Icon(
                color=style["color"], icon=style["icon"], prefix="fa"
            ),
        ).add_to(layer)

    layer.add_to(m)
    return m


# ─── Full Map Assembly ────────────────────────────────────────────────────────

def build_full_map(transport: dict[str, pd.DataFrame],
                   assets: dict[str, pd.DataFrame],
                   community_features: list[dict]) -> folium.Map:
    """
    Assemble the complete Folium map in layer order.

    Parameters
    ----------
    transport          : dict from fetch_data.fetch_all_transportation()
    assets             : dict from fetch_assets.fetch_all_assets()
    community_features : list from fetch_data.fetch_community_boundaries()
    """
    print("\nBuilding Folium map …")
    m = create_base_map()

    # Base geography — community boundary outline only (no zone rectangles)
    m = add_community_boundary(m, community_features)

    # Transportation
    m = add_cta_rail_lines(m,    transport.get("cta_rail_lines",    pd.DataFrame()))
    m = add_cta_rail_stations(m, transport.get("cta_rail_stations", pd.DataFrame()))
    m = add_cta_bus_routes(m,    transport.get("cta_bus_routes",    pd.DataFrame()))
    m = add_cta_bus_stops(m,     transport.get("cta_bus_stops",     pd.DataFrame()))
    m = add_metra_lines(m,       transport.get("metra_lines",       pd.DataFrame()))
    m = add_metra_stations(m,    transport.get("metra_stations",    pd.DataFrame()))

    # Asset categories — each is a separately toggleable layer
    asset_layer_map = {
        "landmarks":   ("Landmarks",             True),
        "restaurants": ("Restaurants",           True),
        "businesses":  ("Businesses",            False),
        "education":   ("Education & Culture",    True),
        "worship":     ("Houses of Worship",     True),
        "parks":       ("Parks",                 True),
        "health":      ("Health & Social Svcs",  True),
    }

    for key, (layer_name, show) in asset_layer_map.items():
        df = assets.get(key, pd.DataFrame())
        m  = add_asset_layer(m, df, key, layer_name, show=show)
        print(f"  Added {layer_name}: {len(df)} features")

    # Layer control panel (top-right, expanded on load)
    folium.LayerControl(collapsed=False).add_to(m)

    print("  Map assembled.")
    return m
