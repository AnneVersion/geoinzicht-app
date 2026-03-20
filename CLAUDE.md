# GeoInzicht App - Claude Code Instructies

## Project
Standalone kaartapplicatie voor CBS buurtdata en BAG adressen met Leaflet.
Locatie: `E:\scripts\webscraper\CBSbuurt\geoinzicht-app\`

## Starten
```bash
python serve.py  # http://localhost:8091 (standaard poort)
python serve.py --port 8091
```

## Branch-strategie
- **main** = stabiel/productie
- **develop** = dagelijkse ontwikkeling (standaard werkbranch)
- **feature/*** = nieuwe features, maak aan vanuit develop

Werk op `develop`. Alleen mergen naar `main` als het stabiel is.

## Architectuur
- `index.html` - Volledige frontend (Leaflet + UI), alles in 1 bestand (~10500 regels)
- `serve.py` - Python HTTP server met live data-refresh API (poort 8091)
- `analytics_api.py` - Flask analytics API (bezoekers, events, uploads)
- `enrich_*.py` - Scripts die GeoJSON verrijken met externe data
- `build_geojson.py` - Bouwt GeoJSON vanuit PDOK WFS CBS data
- Werkt ALLEEN met API calls en lokale GeoJSON - nooit direct met databases

## Features (maart 2026)
- **Auto-detectie nieuwste jaar**: Start automatisch met het nieuwste beschikbare CBS jaar
- **21 domeinen**: Bevolking, Inkomen, Wonen, Energie, Economie, Zorgkosten, Criminaliteit, Flora/Fauna, etc.
- **100+ indicatoren**: Automatisch gefilterd op beschikbaarheid per jaar
- **Contextuele vergelijking**: Bij klik op gebied: tabel met buurt vs wijk vs gemeente vs NL
- **Vergelijken tab**: Side-by-side vergelijking van 2+ geselecteerde gebieden
- **Inline vergelijking**: Groen/rood kleurcodering voor beste/slechtste waarden
- **Uitgebreid rapport**: Vergelijkingsrapport in nieuw venster met alle indicatoren
- **Tooltips**: Professionele tooltips met geformatteerde waarden en percentiel-positie
- **NL Dashboard**: Landelijk overzicht met KPI's en distributies per indicator
- **Indicator info**: Per-indicator popup met databron, analysemethode en landelijke statistieken
- **BAG adressen**: Vector tiles, WMS, lokale GeoJSON met filters
- **Zoeken**: PDOK Locatieserver integratie
- **Upload**: Eigen CSV/Excel data op de kaart
- **Analytics**: Bezoekersregistratie met IndexedDB + SQL Server sync

## Databronnen
| Bron | Type | Frequentie |
|------|------|-----------|
| CBS Kerncijfers Wijken en Buurten | GeoJSON via PDOK WFS | Jaarlijks |
| CBS Bodemgebruik (BBG) | CBS Open Data API | Per peiljaar |
| CBS Landbouwtelling | CBS Open Data API | Jaarlijks |
| Kadaster BAG | PDOK WMS/WFS/MVT | Dagelijks |
| GBIF Flora & Fauna | GBIF REST API | Cumulatief |
| Vektis Zorgkosten | Open Data | Jaarlijks |
| Politie Criminaliteit | data.politie.nl | Jaarlijks |
| PDOK Locatieserver | REST API | Continu |

## Waar gebleven (maart 2026)
- Complete overhaul: auto-detectie nieuwste jaar, contextuele vergelijking, vergelijk-tab
- Verbeterde tooltips met geformatteerde waarden en percentiel-positie
- NL Dashboard met KPI's
- Indicator info popups met databronnen en datakwaliteit
- Onverzekerden & Uitkeringen proxy-analyse
- Analytics API uitgebreid met nieuwe endpoints
- BAG bulk download verbeterd

## Let op
- BAG GeoJSON bestanden staan in .gitignore (te groot) - lokaal genereren
- Cache bestanden (*_cache.json) niet committen
- App moet werken ZONDER backend server (alleen statische bestanden + API calls)
- serve.py standaard poort is 8091 (niet 8080)
- DWH API op localhost:8888 (apart project: geoinzicht-dwh)
