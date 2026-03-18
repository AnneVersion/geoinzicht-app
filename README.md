# GeoInzicht App

Standalone interactieve kaartapplicatie voor CBS buurtdata, BAG adressen en verrijkte geodata. Draait zonder webserver - open `index.html` in de browser, of gebruik `serve.py` voor lokale ontwikkeling.

## Wat doet het?

- Leaflet-kaart met CBS buurtstatistieken (choropleth)
- BAG adres-overlays per gemeente
- Verrijking met flora/fauna (GBIF), zorgkosten, onverzekerden
- Analytics API voor geavanceerde analyses

## Starten

```bash
python serve.py
# Opent op http://localhost:8091
```

Of gewoon `index.html` openen in de browser.

## Data voorbereiden

```bash
python download_bag_bulk.py    # Download BAG data per gemeente
python build_geojson.py        # Bouw GeoJSON bestanden
python enrich_batch.py         # Verrijk met externe datasets
```

## Belangrijke bestanden

| Bestand | Functie |
|---------|---------|
| `index.html` | Hoofdapplicatie (Leaflet kaart + UI) |
| `serve.py` | Lokale development server |
| `analytics_api.py` | Analytics REST API |
| `build_geojson.py` | GeoJSON builder vanuit BAG data |
| `enrich_*.py` | Verrijkingsscripts (flora, fauna, zorg, DWH) |
| `download_bag_bulk.py` | BAG data downloader per gemeente |

## Branch-strategie

| Branch | Doel |
|--------|------|
| `main` | Stabiele versie |
| `develop` | Actieve ontwikkeling |
| `feature/*` | Nieuwe functionaliteit (vanuit develop) |

Werk altijd op `develop` of een `feature/*` branch. Merge naar `main` als het stabiel is.

## Werkt samen met

- **geoinzicht-dwh** - Backend API en datawarehouse (PostgreSQL/GeoServer)
- Data wordt opgehaald via API calls en lokale GeoJSON bestanden
