"""
Enrich static GeoJSON files with RIVM Gezondheidsmonitor data.
===============================================================
Fetches health indicator data per wijk/buurt from the RIVM/CBS OData API
(table 50150NED, published on statline.rivm.nl).

Data: 2012, 2016, 2020, 2022, 2024 (5 meetjaren), 57 indicatoren, per wijk/buurt/gemeente.
API: https://dataderden.cbs.nl/ODataFeed/OData/50150NED

Usage:
    python enrich_from_rivm.py                          # Alle files, laatste jaar (2024)
    python enrich_from_rivm.py --year 2020              # Specifiek meetjaar
    python enrich_from_rivm.py --type gemeenten          # Alleen gemeenten
    python enrich_from_rivm.py --all-years               # Alle meetjaren in aparte velden

No dependencies beyond Python stdlib (urllib, json).
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.request

# ============================================================
# CONFIG
# ============================================================

API_BASE = "https://dataderden.cbs.nl/ODataFeed/OData/50150NED"
PAGE_SIZE = 10000

# Mapping: API field key -> GeoJSON field name (gz_ prefix voor gezondheid)
# We nemen de 20 meest relevante indicatoren
HEALTH_MAP = {
    # Ervaren gezondheid
    "GoedErvarenGezondheid_4":              "gz_ervaren_gezondheid",
    # Lichamelijk
    "EenOfMeerLangdurigeAandoeningen_5":    "gz_langdurige_aandoeningen",
    "LangdurigBeperkt_6":                   "gz_langdurig_beperkt",
    # Mentale gezondheid
    "HoogRisicoAngststoornisOfDepressie_14":"gz_angst_depressie",
    "HeelVeelStressAfg4Weken_17":           "gz_stress",
    # Eenzaamheid
    "Eenzaam_25":                           "gz_eenzaam",
    "SterkEenzaam_26":                      "gz_sterk_eenzaam",
    "EmotioneelEenzaam_27":                 "gz_emotioneel_eenzaam",
    "SociaalEenzaam_28":                    "gz_sociaal_eenzaam",
    # Veerkracht & regie
    "LageVeerkracht_22":                    "gz_lage_veerkracht",
    "MatigVeelRegieOverHetEigenLeven_24":   "gz_regie_eigen_leven",
    # Sociaal
    "Mantelzorger_30":                      "gz_mantelzorger",
    "Vrijwilligerswerk_31":                 "gz_vrijwilligerswerk",
    "KrijgtSteunVanAnderen_29":             "gz_steun_van_anderen",
    # Armoede
    "MoeiteMetRondkomen_32":                "gz_moeite_rondkomen",
    # Overgewicht
    "Overgewicht_33":                       "gz_overgewicht",
    "ErnstigOvergewichtObesitas_34":        "gz_obesitas",
    # Leefstijl
    "RooktTabak_35":                        "gz_rookt",
    "OvermatigDrinken_37":                  "gz_overmatig_drinken",
    "VoldoetAanDeBeweegrichtlijn_40":       "gz_voldoet_beweegrichtlijn",
    # Wonen
    "TevredenMetDeWoning_56":               "gz_tevreden_woning",
    "TevredenMetDeWoonomgeving_57":         "gz_tevreden_woonomgeving",
}

# GeoJSON file patterns
GEOJSON_FILES = {
    "gemeenten": "gemeenten_{year}.geojson",
    "wijken":    "wijken_{year}.geojson",
    "buurten":   "buurten_{year}.geojson",
}

CODE_FIELD = {
    "gemeenten": "gemeentecode",
    "wijken":    "wijkcode",
    "buurten":   "buurtcode",
}

log = logging.getLogger("rivm")


# ============================================================
# API FUNCTIONS
# ============================================================

def safe_float(v):
    """Convert value to float (percentages), return None on failure."""
    if v is None:
        return None
    try:
        return round(float(v), 1)
    except (ValueError, TypeError):
        return None


def fetch_all_data():
    """Fetch all rows from RIVM OData TypedDataSet with pagination.
    We filter on Leeftijd=Totaal and Marges=Waarde to get the main estimates.
    """
    all_rows = []
    skip = 0

    # Filter: alleen Totaal leeftijd, alleen Waarde (geen betrouwbaarheidsintervallen)
    filter_str = "substringof('T001038',Leeftijd) and substringof('MW00000',Marges)"

    while True:
        url = f"{API_BASE}/TypedDataSet?$filter={filter_str}&$top={PAGE_SIZE}&$skip={skip}&$format=json"
        log.info("  Fetching rows %d to %d...", skip, skip + PAGE_SIZE)
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=180) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            log.error("  API error at skip=%d: %s", skip, e)
            # Retry once without filter (some RIVM endpoints don't support $filter)
            if skip == 0 and "$filter" in url:
                log.info("  Retrying without filter...")
                return fetch_all_data_unfiltered()
            break
        rows = data.get("value", [])
        all_rows.extend(rows)
        log.info("  Got %d rows (total: %d)", len(rows), len(all_rows))
        if len(rows) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        time.sleep(0.5)
    return all_rows


def fetch_all_data_unfiltered():
    """Fallback: fetch ALL data without filter, then filter in Python."""
    all_rows = []
    skip = 0
    while True:
        url = f"{API_BASE}/TypedDataSet?$top={PAGE_SIZE}&$skip={skip}&$format=json"
        log.info("  Fetching rows %d to %d (unfiltered)...", skip, skip + PAGE_SIZE)
        try:
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=180) as resp:
                data = json.loads(resp.read().decode())
        except Exception as e:
            log.error("  API error at skip=%d: %s", skip, e)
            break
        rows = data.get("value", [])
        all_rows.extend(rows)
        log.info("  Got %d rows (total: %d)", len(rows), len(all_rows))
        if len(rows) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        time.sleep(0.5)

    # Filter in Python: alleen "18 jaar of ouder" en "Waarde" (geen marges)
    log.info("  Filtering %d rows in Python...", len(all_rows))
    filtered = []
    for row in all_rows:
        leeftijd = str(row.get("Leeftijd", "")).strip()
        marges = str(row.get("Marges", "")).strip()
        # 20300 = "18 jaar of ouder" (totaal)
        # MW00000 = "Waarde" (de schatting, niet de betrouwbaarheidsmarge)
        if leeftijd == "20300" and marges == "MW00000":
            filtered.append(row)
    log.info("  Na filtering: %d rows", len(filtered))
    return filtered


def parse_health_data(raw_rows):
    """Parse raw API rows into structured dict.
    Returns: {(region_code, year): {gz_field: value, ...}}
    """
    result = {}

    for row in raw_rows:
        regio = str(row.get("WijkenEnBuurten", "")).strip()
        periode = str(row.get("Perioden", "")).strip()

        if not regio or not periode:
            continue

        # Parse year from period (e.g., "2024JJ00" -> 2024)
        try:
            year = int(periode[:4])
        except (ValueError, IndexError):
            continue

        key = (regio, year)
        if key not in result:
            result[key] = {}

        # Extract all health indicators
        for api_field, gz_field in HEALTH_MAP.items():
            val = safe_float(row.get(api_field))
            if val is not None:
                result[key][gz_field] = val

    # Stats
    years = sorted(set(y for (_, y) in result.keys()))
    regions = len(set(r for (r, _) in result.keys()))
    log.info("  Parsed: %d regio x jaar combinaties", len(result))
    log.info("  Jaren beschikbaar: %s", ", ".join(str(y) for y in years))
    log.info("  Unieke regio's: %d", regions)

    return result, years


def nearest_year(available, target, max_gap=4):
    """Find nearest year <= target within max_gap tolerance.
    RIVM data comes every 2-4 years, so we use a larger gap.
    """
    candidates = [y for y in available if y <= target]
    if candidates:
        best = max(candidates)
        return best if (target - best) <= max_gap else None
    future = [y for y in available if y > target and (y - target) <= 2]
    return min(future) if future else None


def enrich_file(feat_type, year, health_data, available_years, all_years_mode=False):
    """Enrich a single GeoJSON file with health data."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(base_dir, GEOJSON_FILES[feat_type].format(year=year))

    if not os.path.exists(filename):
        log.warning("Bestand niet gevonden: %s", filename)
        return False

    code_field = CODE_FIELD[feat_type]

    log.info("=" * 55)
    log.info("  ENRICH %s %d (RIVM Gezondheidsmonitor)", feat_type.upper(), year)
    log.info("=" * 55)

    # Load GeoJSON
    log.info("Laden: %s...", os.path.basename(filename))
    with open(filename, "r", encoding="utf-8") as fh:
        geojson = json.load(fh)
    features = geojson.get("features", [])
    log.info("  %d features", len(features))

    # Find best year for health data
    gz_year = nearest_year(available_years, year)
    if not gz_year:
        log.warning("  Geen gezondheidsdata beschikbaar voor jaar %d (beschikbaar: %s)",
                     year, ", ".join(str(y) for y in available_years))
        return False

    log.info("  Gezondheid: jaar %d (voor GeoJSON jaar %d)", gz_year, year)

    enriched = 0
    indicators_with_data = set()

    for f in features:
        props = f.get("properties", {})
        code = str(props.get(code_field, "")).strip()

        if not code:
            continue

        # Default year (latest/best match)
        gz_row = health_data.get((code, gz_year))
        if gz_row:
            for key, val in gz_row.items():
                if val is not None:
                    props[key] = val
                    indicators_with_data.add(key)
            props["gz_jaar"] = gz_year
            enriched += 1

        # All-years mode
        if all_years_mode:
            for hist_year in available_years:
                hist_row = health_data.get((code, hist_year))
                if hist_row:
                    for key, val in hist_row.items():
                        if val is not None:
                            props[f"{key}_{hist_year}"] = val

    log.info("  Verrijkt: %d/%d features", enriched, len(features))
    log.info("  Indicatoren met data: %s", sorted(indicators_with_data))

    # Update metadata
    meta = geojson.get("metadata", {})
    if not meta:
        meta = {}
        geojson["metadata"] = meta

    existing_indicators = set(meta.get("indicators", []))
    existing_indicators.update(indicators_with_data)
    meta["indicators"] = sorted(existing_indicators)
    meta["indicators_count"] = len(meta["indicators"])
    meta["gz_jaar"] = gz_year
    meta["gz_jaren_beschikbaar"] = available_years
    meta["gz_source"] = "RIVM Gezondheidsmonitor (GGD/CBS/RIVM) via CBS OData API"
    meta["gz_dataset"] = "50150NED"
    meta["enriched_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")

    # Save
    log.info("Opslaan: %s...", os.path.basename(filename))
    with open(filename, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(filename) / (1024 * 1024)
    log.info("  Opgeslagen: %.1f MB", size_mb)
    return True


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Enrich GeoJSON met RIVM Gezondheidsmonitor data (API)")
    parser.add_argument("--type", choices=["gemeenten", "wijken", "buurten"],
                        help="Alleen dit gebiedsniveau verrijken")
    parser.add_argument("--year", type=int, help="GeoJSON jaar (default: 2025)")
    parser.add_argument("--all-years", action="store_true",
                        help="Alle historische meetjaren opslaan als gz_{veld}_{jaar}")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    log.info("=" * 55)
    log.info("  RIVM GEZONDHEIDSMONITOR")
    log.info("  Bron: GGD/CBS/RIVM (OData 50150NED)")
    log.info("=" * 55)

    # Step 1: Fetch data
    log.info("")
    log.info("Stap 1: Data ophalen van RIVM API...")
    log.info("  URL: %s/TypedDataSet", API_BASE)
    raw_rows = fetch_all_data()
    if not raw_rows:
        log.error("Geen data ontvangen van API!")
        sys.exit(1)
    log.info("  Totaal ontvangen: %d rows", len(raw_rows))

    # Step 2: Parse
    log.info("")
    log.info("Stap 2: Data parsen...")
    health_data, available_years = parse_health_data(raw_rows)
    if not health_data:
        log.error("Geen bruikbare gezondheidsdata na parsing!")
        sys.exit(1)

    latest_year = max(available_years)
    log.info("  Laatste meetjaar: %d", latest_year)

    # Step 3: Enrich
    years_to_process = [args.year] if args.year else [2025, 2024]
    types_to_process = [args.type] if args.type else ["gemeenten", "wijken", "buurten"]

    log.info("")
    log.info("Stap 3: GeoJSON bestanden verrijken...")
    log.info("  Jaren: %s", years_to_process)
    log.info("  Types: %s", types_to_process)

    success_count = 0
    for year in years_to_process:
        for feat_type in types_to_process:
            log.info("")
            ok = enrich_file(feat_type, year, health_data, available_years,
                             all_years_mode=args.all_years)
            if ok:
                success_count += 1

    log.info("")
    log.info("=" * 55)
    log.info("  KLAAR: %d bestanden verrijkt", success_count)
    log.info("  Gezondheidsdata: %s (%d meetjaren)",
             ", ".join(str(y) for y in available_years), len(available_years))
    log.info("  Indicatoren: %d", len(HEALTH_MAP))
    log.info("=" * 55)


if __name__ == "__main__":
    main()
