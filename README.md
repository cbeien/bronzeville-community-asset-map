# Bronzeville Community Asset Map

A GIS project mapping community assets in Bronzeville, Chicago using Python, Folium, and Leaflet.

## Overview

This project aggregates data from multiple sources to create an interactive map of:
- **9 asset categories**: landmarks, restaurants, businesses, schools, worship, parks, healthcare, cultural, social services
- **6 transport layers**: CTA Rail/Bus (stations, lines, routes) and Metra (stations, lines)

## Data Sources

- **City of Chicago SODA API** — transport layers and community boundaries
- **OpenStreetMap Overpass API** — additional asset data
- **US Census Geocoder** — address geocoding
- **Nominatim** — fallback geocoding

No API keys required (though `SODA_APP_TOKEN` env var can raise rate limits).

## Project Structure

```
├── config.py          # All constants (bbox, zones, dataset IDs, styles)
├── fetch_data.py      # SODA API: transport + boundaries
├── fetch_assets.py    # Asset categories from SODA + OSM
├── geocode.py         # Census geocoder + Nominatim fallback
├── build_map.py       # Folium map assembly
├── export.py          # Excel, HTML, data.js output
├── main.py            # Orchestration (run this)
├── requirements.txt   # Python dependencies
└── deploy/            # Static files for deployment
```

## Usage

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Run the pipeline**
   ```bash
   python main.py
   ```

3. **View the map**
   - Folium: `bronzeville_map.html`
   - Leaflet: `index.html` (requires `python -m http.server`)
   - Excel data: `bronzeville_assets.xlsx`

## Notes

- Some SODA dataset IDs in `config.py` are marked `VERIFY` and may need confirmation at [data.cityofchicago.org](https://data.cityofchicago.org)
- Bronzeville community areas: Douglas (35), Grand Boulevard (38), Kenwood (39), Washington Park (40)
- Map bounds: lat 41.780–41.845, lon -87.630–-87.525

## License

Academic project for university GIS course.
