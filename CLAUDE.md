# GeoInzicht App - Claude Code Instructies

## Project
Standalone kaartapplicatie voor CBS buurtdata en BAG adressen met Leaflet.
Locatie: `E:\scripts\webscraper\CBSbuurt\geoinzicht-app\`

## Starten
```bash
python serve.py  # http://localhost:8091
```

## Branch-strategie
- **main** = stabiel/productie
- **develop** = dagelijkse ontwikkeling (standaard werkbranch)
- **feature/*** = nieuwe features, maak aan vanuit develop

Werk op `develop`. Alleen mergen naar `main` als het stabiel is.

## Architectuur
- `index.html` - Volledige frontend (Leaflet + UI), alles in 1 bestand
- `serve.py` - Simpele Python HTTP server
- `analytics_api.py` - Flask analytics API
- `enrich_*.py` - Scripts die GeoJSON verrijken met externe data
- `build_geojson.py` - Bouwt GeoJSON vanuit BAG downloads
- Werkt ALLEEN met API calls en lokale GeoJSON - nooit direct met databases

## Waar gebleven (maart 2026)
- Onverzekerden & Uitkeringen proxy-analyse toevoegen
- Analytics API uitgebreid met nieuwe endpoints
- BAG bulk download verbeterd

## Let op
- BAG GeoJSON bestanden staan in .gitignore (te groot) - lokaal genereren
- Cache bestanden (*_cache.json) niet committen
- App moet werken ZONDER backend server (alleen statische bestanden + API calls)
