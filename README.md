# GeoInzicht App

Standalone interactieve kaartapplicatie voor CBS buurtdata, BAG adressen en verrijkte geodata. Draait zonder webserver - open `index.html` in de browser, of gebruik `serve.py` voor lokale ontwikkeling.

## Wat doet het?

- Leaflet-kaart met CBS buurtstatistieken (choropleth) op gemeente/wijk/buurtniveau
- BAG adres-overlays per gemeente (vector tiles, WMS, lokale GeoJSON)
- 100+ indicatoren verdeeld over 21 domeinen (bevolking, inkomen, wonen, energie, zorg, criminaliteit, flora/fauna, etc.)
- Automatische detectie van het nieuwste beschikbare CBS datajaar
- Contextuele vergelijking: klik op een buurt en zie waarden vs wijk, gemeente en landelijk gemiddelde
- Side-by-side vergelijking van meerdere gebieden met kleurcodering (groen=beter, rood=lager)
- NL Dashboard met landelijke KPI's en statistieken
- Per-indicator databron, datakwaliteit en analysemethode
- Professionele tooltips met geformatteerde waarden, percentiel-positie en trend-pijlen
- Zoekfunctie via PDOK Locatieserver
- Eigen data uploaden (CSV/Excel) en op de kaart tonen
- Analytics API voor bezoekersregistratie

## Starten

```bash
python serve.py
# Opent op http://localhost:8091
```

Of gewoon `index.html` openen in de browser.

## Data voorbereiden

```bash
python download_bag_bulk.py    # Download BAG data per gemeente
python build_geojson.py        # Bouw GeoJSON bestanden (CBS via PDOK WFS)
python enrich_batch.py         # Verrijk met externe datasets (flora, fauna, zorg, etc.)
```

## Belangrijke bestanden

| Bestand | Functie |
|---------|---------|
| `index.html` | Hoofdapplicatie (Leaflet kaart + volledige UI, ~10500 regels) |
| `serve.py` | Lokale development server met data-refresh API (poort 8091) |
| `analytics_api.py` | Analytics REST API (bezoekers, events, uploads) |
| `build_geojson.py` | GeoJSON builder vanuit CBS PDOK WFS data |
| `enrich_*.py` | Verrijkingsscripts (flora/fauna, zorgkosten, criminaliteit, DWH) |
| `download_bag_bulk.py` | BAG data downloader per gemeente |

## Databronnen

| Bron | Data | Frequentie |
|------|------|-----------|
| CBS Kerncijfers | Bevolking, woningen, inkomen, energie, arbeid, voorzieningen | Jaarlijks |
| CBS Bodemgebruik | Bebouwd, agrarisch, natuur, recreatie | Per peiljaar |
| CBS Landbouw | Cultuurgrond, rundvee, varkens, kippen, bedrijven | Jaarlijks |
| Kadaster BAG | 9.9M verblijfsobjecten, panden met bouwjaar/oppervlakte | Dagelijks |
| GBIF | Flora & fauna soorten en waarnemingen | Cumulatief |
| Vektis | Zorgkosten per verzekerde per gemeente | Jaarlijks |
| Politie | Criminaliteitscijfers per gemeente | Jaarlijks |
| PDOK Locatieserver | Adres zoeken en geocodering | Continu |

## Branch-strategie

| Branch | Doel |
|--------|------|
| `main` | Stabiele versie |
| `develop` | Actieve ontwikkeling |
| `feature/*` | Nieuwe functionaliteit (vanuit develop) |

Werk altijd op `develop` of een `feature/*` branch. Merge naar `main` als het stabiel is.

## Werkt samen met

- **geoinzicht-dwh** - Backend API en datawarehouse (PostgreSQL/GeoServer) op localhost:8888
- Data wordt opgehaald via API calls en lokale GeoJSON bestanden
