# GeoInzicht App - Claude Code Instructies

## 1. Project Overzicht
Standalone kaartapplicatie voor CBS buurtdata en BAG adressen met Leaflet.
- **Locatie**: `E:\scripts\webscraper\CBSbuurt\geoinzicht-app\`
- **URL**: `http://localhost:8091`
- **GitHub**: AnneVersion/geoinzicht-app
- **Type**: Pure frontend app (HTML/JS/CSS) met Python server voor static hosting en data-refresh API's
- **Werkt ALLEEN met API calls en lokale GeoJSON** -- nooit direct met databases

## 2. Starten
```bash
python serve.py              # http://localhost:8091 (standaard poort)
python serve.py --port 8091  # expliciet poort opgeven
```
De server biedt statische bestanden + API endpoints voor data-refresh en status.

## 3. Branch-strategie
- **main** = stabiel/productie
- **develop** = dagelijkse ontwikkeling (standaard werkbranch)
- **feature/*** = nieuwe features, maak aan vanuit develop

Werk op `develop`. Alleen mergen naar `main` als het stabiel is.

## 4. Architectuur

### Bestanden
| Bestand | Beschrijving |
|---------|-------------|
| `index.html` | Volledige frontend (~11.000 regels): Leaflet kaart, UI, alle logica in 1 bestand |
| `serve.py` | Python HTTP server (ThreadingTCPServer) met data-refresh API, poort 8091 |
| `analytics_api.py` | Flask analytics API (bezoekers, events, uploads) |
| `build_geojson.py` | Bouwt GeoJSON vanuit PDOK WFS CBS data |
| `build_bag.py` | Bouwt BAG GeoJSON vanuit PDOK |
| `enrich_cbs_2024.py` | Verrijkt GeoJSON met CBS 2024 data (dataset 85618NED, 89 indicatoren) |
| `enrich_cbs_2025.py` | Verrijkt GeoJSON met CBS 2025 data (dataset 86165NED, 83 indicatoren) |
| `enrich_from_sql.py` | Verrijkt met Bodemgebruik + Landbouw (CBS API) |
| `enrich_from_dwh.py` | Verrijkt met Zorgkosten + Criminaliteit (DWH API) |
| `enrich_flora_fauna.py` | Verrijkt met GBIF Flora & Fauna data |
| `enrich_batch.py` | Batch-verrijking wrapper |
| `enrich_bag_counts.py` | BAG telling per buurt |
| `enrich_geojson.py` | Algemene GeoJSON verrijking |
| `download_bag.py` | Download BAG data van PDOK |
| `download_bag_bulk.py` | Bulk download BAG data |
| `manifest.json` | PWA manifest |
| `logo.svg` | App logo |

### API Endpoints (serve.py)
| Endpoint | Methode | Beschrijving |
|----------|---------|-------------|
| `/api/status` | GET | Data versheid, refresh status, laatste fout |
| `/api/refresh` | GET/POST | Start achtergrond data-refresh pipeline |
| `/api/refresh/log` | GET | Refresh voortgang en logs |

### Externe API's
| Bron | Type | Gebruik |
|------|------|---------|
| CBS Open Data (opendata.cbs.nl) | OData REST API | Kerncijfers wijken/buurten, Bodemgebruik, Landbouw |
| PDOK WFS/WMS/MVT | OGC services | CBS gebiedsgrenzen, BAG panden/adressen |
| PDOK Locatieserver | REST API | Zoeken op adressen/plaatsnamen |
| GBIF | REST API | Flora & Fauna waarnemingen |
| Vektis | Open Data | Zorgkosten per gemeente |
| data.politie.nl | Open Data | Criminaliteitscijfers |
| GeoInzicht DWH | localhost:8888 | Zorgkosten + Criminaliteit (apart project) |

### GeoJSON Bestanden (lokaal)
Bestanden per jaar en gebiedsniveau. Grote bestanden staan in `.gitignore`.
- `buurten_2024.geojson`, `buurten_2025.geojson` -- buurtgrenzen met CBS data
- `wijken_2024.geojson`, `wijken_2025.geojson` -- wijkgrenzen met CBS data
- `gemeenten_2024.geojson`, `gemeenten_2025.geojson` -- gemeentegrenzen met CBS data
- `bag_panden_*.geojson`, `bag_verblijfsobjecten_*.geojson` -- BAG gebouwdata
- Oudere jaren (2022, 2017, etc.) ook beschikbaar
- Cache bestanden (`*_cache.json`) niet committen

## 5. Key Features

### CBS Data (kern)
- **CBS 2024**: 89 indicatoren via dataset 85618NED, verrijkt via `enrich_cbs_2024.py`
- **CBS 2025**: 83 indicatoren via dataset 86165NED, verrijkt via `enrich_cbs_2025.py`
- **Nieuwe indicatoren 2025**: aardgasvrije woningen, zonnestroom, armoede, laadpalen
- **Auto-detectie**: Start automatisch met het nieuwste beschikbare CBS jaar
- **21 domeinen**: Bevolking, Inkomen, Wonen, Energie, Economie, Zorgkosten, Criminaliteit, Flora/Fauna, etc.

### Enrich Scripts
De `enrich_*.py` scripts halen data op via API's en voegen deze toe aan de lokale GeoJSON bestanden. Het refresh-pipeline in `serve.py` draait ze achter elkaar:
1. `build_geojson.py` -- CBS Kerncijfers (PDOK WFS)
2. `enrich_from_sql.py` -- Bodemgebruik + Landbouw (CBS API)
3. `enrich_flora_fauna.py` -- Flora & Fauna (GBIF API)
4. `enrich_from_dwh.py` -- Zorgkosten + Criminaliteit (DWH)

### Visualisatie
- **Viridis kleurenpalet**: colorblind-friendly legenda voor alle indicatoren
- **Tooltips**: Professionele tooltips met geformateerde waarden en percentiel-positie
- **Indicator info**: Per-indicator popup met databron, analysemethode en landelijke statistieken

### Analyse
- **Contextuele vergelijking (context tabel)**: Bij klik op een gebied: tabel met buurt vs wijk vs gemeente vs Nederland
- **Vergelijken tab**: Side-by-side vergelijking van 2+ geselecteerde gebieden (window.open in directe click context)
- **Uitgebreid rapport**: Vergelijkingsrapport in nieuw venster met alle indicatoren
- **NL Dashboard**: Landelijk overzicht met KPI's en distributies per indicator

### Overig
- **BAG adressen**: Vector tiles, WMS, lokale GeoJSON met filters
- **Zoeken**: PDOK Locatieserver integratie
- **Upload**: Eigen CSV/Excel data op de kaart tonen
- **Analytics**: Bezoekersregistratie met IndexedDB + SQL Server sync

## 6. Startup Checklist

### Server starten
1. Open terminal in `E:\scripts\webscraper\CBSbuurt\geoinzicht-app\`
2. Run `python serve.py`
3. Controleer output: "GeoInzicht Server (met live refresh)" + URL info

### Testen
- [ ] Open `http://localhost:8091` in browser
- [ ] Kaart laadt met buurtgrenzen en kleuren (Viridis)
- [ ] Klik op een buurt: context tabel verschijnt (buurt/wijk/gemeente/NL)
- [ ] Selecteer een indicator uit de dropdown: kaart kleurt opnieuw
- [ ] Tooltips verschijnen bij hover over gebieden
- [ ] Zoekfunctie werkt (typ een plaatsnaam)
- [ ] NL Dashboard opent en toont KPI's
- [ ] Vergelijken: selecteer 2+ gebieden, open vergelijking
- [ ] Check `http://localhost:8091/api/status` -- JSON response met freshness info
- [ ] (Optioneel) Trigger refresh via `http://localhost:8091/api/refresh`, check log via `/api/refresh/log`

### GeoJSON data vernieuwen (als bestanden ontbreken of verouderd zijn)
```bash
python build_geojson.py          # Download CBS gebiedsgrenzen van PDOK
python enrich_cbs_2025.py        # Verrijk met CBS 2025 data (83 indicatoren)
python enrich_cbs_2024.py        # Verrijk met CBS 2024 data (89 indicatoren)
```
Of via de API: `http://localhost:8091/api/refresh` (draait het volledige pipeline).

## 7. TODO
- GBIF Flora & Fauna data nog niet volledig geladen
- BAG bulk download optimalisatie
- Edak project koppeling (wacht op Excel input)
- Performance optimalisatie index.html (11.000+ regels in 1 bestand)

## 8. Belangrijke Regels
- **ALLEEN API's, nooit direct database**: De app werkt uitsluitend met REST API calls en lokale GeoJSON bestanden. Geen directe database connecties.
- **DWH is een apart project**: GeoInzicht DWH draait op localhost:8888 (project: geoinzicht-dwh). De app kan er data van ophalen via API, maar is er niet afhankelijk van.
- **Poort 8091**: Standaard poort, niet wijzigen (andere projecten verwijzen hiernaar).
- **GeoJSON in .gitignore**: Grote bestanden (BAG bulk, cache) niet committen. Lokaal genereren.
- **Veldnamen synchroon houden**: De FIELD_MAP in `enrich_cbs_*.py` moet overeenkomen met de INDICATORS array in `index.html`. Bij 2025 zijn de veldindices verschoven t.o.v. 2024.
