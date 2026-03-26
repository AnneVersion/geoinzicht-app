"""
Enrich static GeoJSON files with criminaliteitsdata from Politie Open Data API.
================================================================================
Fetches registered crime data per wijk/buurt from the CBS OData API
(table 47018NED, published by data.politie.nl).

Data: 2012-2025 (14 jaren), 60 misdrijftypen, per wijk/buurt/gemeente.
API: https://dataderden.cbs.nl/ODataApi/OData/47018NED

Usage:
    python enrich_from_politie_api.py                          # Alle files, laatste jaar
    python enrich_from_politie_api.py --year 2023              # Specifiek jaar
    python enrich_from_politie_api.py --type gemeenten         # Alleen gemeenten
    python enrich_from_politie_api.py --all-years              # Alle jaren in aparte velden

No dependencies beyond Python stdlib (urllib, json).
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.parse
import urllib.request

# ============================================================
# CONFIG
# ============================================================

API_BASE = "https://dataderden.cbs.nl/ODataFeed/OData/47018NED"
PAGE_SIZE = 10000

# Mapping: API SoortMisdrijf Key -> GeoJSON field name
CRIME_MAP = {
    "0.0.0 ":  "cr_totaal_misdrijven",    # Totaal misdrijven  (let op trailing space)
    "1.1.1 ":  "cr_inbraak_woning",        # Diefstal/inbraak woning
    "1.2.2 ":  "cr_diefstal_motor",        # Diefstal motorvoertuigen
    "1.2.3 ":  "cr_diefstal_fietsen",      # Diefstal brom/snor/fietsen
    "1.4.4 ":  "cr_bedreiging",            # Bedreiging
    "1.4.5 ":  "cr_mishandeling",          # Mishandeling
    "1.4.6 ":  "cr_straatroof",            # Straatroof
    "2.1.1 ":  "cr_drugs_drank",           # Drugs/drankoverlast
    "2.2.1 ":  "cr_vernieling",            # Vernieling
    "2.5.1 ":  "cr_inbraak_bedrijven",     # Diefstal/inbraak bedrijven
    "2.5.2 ":  "cr_winkeldiefstal",        # Winkeldiefstal
    "3.7.4 ":  "cr_cybercrime",            # Cybercrime
    "3.9.1 ":  "cr_fraude",                # Horizontale fraude
}

# Also match without trailing space (API may vary)
CRIME_MAP_STRIPPED = {k.strip(): v for k, v in CRIME_MAP.items()}

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

log = logging.getLogger("politie_api")


# ============================================================
# API FUNCTIONS
# ============================================================

def safe_int(v):
    """Convert value to int, return None on failure."""
    if v is None:
        return None
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return None


def fetch_all_data():
    """Fetch rows from Politie OData TypedDataSet with pagination.
    Fetches per misdrijftype to avoid downloading 3M+ rows.
    Returns list of dicts with keys: SoortMisdrijf, WijkenEnBuurten, Perioden, GeregistreerdeMisdrijven_1
    """
    all_rows = []
    crime_keys = list(CRIME_MAP_STRIPPED.keys())

    for i, crime_key in enumerate(crime_keys):
        log.info("  [%d/%d] Ophalen: %s (%s)...",
                 i + 1, len(crime_keys), CRIME_MAP_STRIPPED[crime_key], crime_key)
        skip = 0
        type_rows = 0
        # Filter op specifiek misdrijftype
        filter_str = urllib.parse.quote(f"trim(SoortMisdrijf) eq '{crime_key}'")
        while True:
            url = f"{API_BASE}/TypedDataSet?$filter={filter_str}&$top={PAGE_SIZE}&$skip={skip}&$format=json"
            try:
                req = urllib.request.Request(url, headers={"Accept": "application/json"})
                with urllib.request.urlopen(req, timeout=180) as resp:
                    data = json.loads(resp.read().decode())
            except Exception as e:
                log.error("    API error: %s", e)
                break
            rows = data.get("value", [])
            all_rows.extend(rows)
            type_rows += len(rows)
            if len(rows) < PAGE_SIZE:
                break
            skip += PAGE_SIZE
            time.sleep(0.3)
        log.info("    %d rows voor %s (totaal: %d)", type_rows, crime_key, len(all_rows))

    return all_rows


def parse_crime_data(raw_rows):
    """Parse raw API rows into structured dict.
    Returns: {(region_code, year): {cr_field: value, ...}}
    """
    result = {}
    relevant_keys = set(CRIME_MAP.keys()) | set(CRIME_MAP_STRIPPED.keys())
    skipped = 0

    for row in raw_rows:
        misdrijf = row.get("SoortMisdrijf", "")

        # Check if this is a crime type we care about
        field = CRIME_MAP.get(misdrijf) or CRIME_MAP_STRIPPED.get(misdrijf.strip())
        if not field:
            continue

        regio = str(row.get("WijkenEnBuurten", "")).strip()
        periode = str(row.get("Perioden", "")).strip()
        aantal = safe_int(row.get("GeregistreerdeMisdrijven_1"))

        if not regio or not periode:
            skipped += 1
            continue

        # Parse year from period (e.g., "2025JJ00" -> 2025)
        try:
            year = int(periode[:4])
        except (ValueError, IndexError):
            skipped += 1
            continue

        key = (regio, year)
        if key not in result:
            result[key] = {}
        result[key][field] = aantal

    if skipped:
        log.info("  %d rows skipped (no regio/periode)", skipped)

    # Count stats
    years = sorted(set(y for (_, y) in result.keys()))
    regions = len(set(r for (r, _) in result.keys()))
    log.info("  Parsed: %d regio x jaar combinaties", len(result))
    log.info("  Jaren beschikbaar: %s", ", ".join(str(y) for y in years))
    log.info("  Unieke regio's: %d", regions)

    return result, years


def nearest_year(available, target, max_gap=2):
    """Find nearest year <= target within max_gap tolerance."""
    candidates = [y for y in available if y <= target]
    if candidates:
        best = max(candidates)
        return best if (target - best) <= max_gap else None
    # Fallback: check if year is just after target (max 1 year)
    future = [y for y in available if y > target and (y - target) <= 1]
    return min(future) if future else None


def extract_match_code(feat_type, code_value):
    """Extract the region code to match against API data.

    API data uses WijkenEnBuurten codes like:
    - GM0363 (gemeente Amsterdam)
    - WK036300 (wijk)
    - BU03630000 (buurt)
    """
    code = str(code_value).strip()
    if feat_type == "gemeenten":
        return code  # GM0363
    elif feat_type == "wijken":
        return code  # WK036300
    elif feat_type == "buurten":
        return code  # BU03630000
    return None


# ============================================================
# ENRICHMENT
# ============================================================

def enrich_file(feat_type, year, crime_data, available_years, all_years_mode=False):
    """Enrich a single GeoJSON file with crime data."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(base_dir, GEOJSON_FILES[feat_type].format(year=year))

    if not os.path.exists(filename):
        log.warning("Bestand niet gevonden: %s", filename)
        return False

    code_field = CODE_FIELD[feat_type]

    log.info("=" * 55)
    log.info("  ENRICH %s %d (Politie API)", feat_type.upper(), year)
    log.info("=" * 55)

    # Load GeoJSON
    log.info("Laden: %s...", os.path.basename(filename))
    with open(filename, "r", encoding="utf-8") as fh:
        geojson = json.load(fh)
    features = geojson.get("features", [])
    log.info("  %d features", len(features))

    # Find best year for crime data
    cr_year = nearest_year(available_years, year)
    if not cr_year:
        log.warning("  Geen criminaliteitsdata beschikbaar voor jaar %d (beschikbaar: %s)",
                     year, ", ".join(str(y) for y in available_years))
        return False

    log.info("  Criminaliteit: jaar %d (voor GeoJSON jaar %d)", cr_year, year)

    enriched = 0
    indicators_with_data = set()

    for f in features:
        props = f.get("properties", {})
        code = props.get(code_field, "")
        match_code = extract_match_code(feat_type, code)

        if not match_code:
            continue

        # Default year (latest/best match)
        cr_row = crime_data.get((match_code, cr_year))
        if cr_row:
            for key, val in cr_row.items():
                if val is not None:
                    props[key] = val
                    indicators_with_data.add(key)
            props["cr_jaar"] = cr_year
            enriched += 1

        # All-years mode: add historical data as cr_{field}_{year}
        if all_years_mode:
            for hist_year in available_years:
                hist_row = crime_data.get((match_code, hist_year))
                if hist_row:
                    for key, val in hist_row.items():
                        if val is not None:
                            year_key = f"{key}_{hist_year}"
                            props[year_key] = val

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
    meta["cr_jaar"] = cr_year
    meta["cr_jaren_beschikbaar"] = available_years
    meta["cr_source"] = "Politie Open Data (data.politie.nl) via CBS OData API"
    meta["cr_dataset"] = "47018NED"
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
    parser = argparse.ArgumentParser(description="Enrich GeoJSON met Politie criminaliteitsdata (API)")
    parser.add_argument("--type", choices=["gemeenten", "wijken", "buurten"],
                        help="Alleen dit gebiedsniveau verrijken")
    parser.add_argument("--year", type=int, help="GeoJSON jaar (default: 2025)")
    parser.add_argument("--all-years", action="store_true",
                        help="Alle historische jaren opslaan als cr_{veld}_{jaar}")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    log.info("=" * 55)
    log.info("  POLITIE OPEN DATA - Criminaliteitsdata")
    log.info("  Bron: data.politie.nl (CBS OData 47018NED)")
    log.info("=" * 55)

    # Step 1: Fetch all data from API
    log.info("")
    log.info("Stap 1: Data ophalen van Politie API...")
    log.info("  URL: %s/TypedDataSet", API_BASE)
    raw_rows = fetch_all_data()
    if not raw_rows:
        log.error("Geen data ontvangen van API!")
        sys.exit(1)
    log.info("  Totaal ontvangen: %d rows", len(raw_rows))

    # Step 2: Parse into structured data
    log.info("")
    log.info("Stap 2: Data parsen en groeperen...")
    crime_data, available_years = parse_crime_data(raw_rows)
    if not crime_data:
        log.error("Geen bruikbare criminaliteitsdata na parsing!")
        sys.exit(1)

    latest_year = max(available_years)
    log.info("  Laatste jaar beschikbaar: %d", latest_year)

    # Step 3: Determine which files to enrich
    years_to_process = [args.year] if args.year else [2025, 2024]
    types_to_process = [args.type] if args.type else ["gemeenten", "wijken", "buurten"]

    log.info("")
    log.info("Stap 3: GeoJSON bestanden verrijken...")
    log.info("  Jaren: %s", years_to_process)
    log.info("  Types: %s", types_to_process)
    log.info("  All-years mode: %s", args.all_years)

    success_count = 0
    for year in years_to_process:
        for feat_type in types_to_process:
            log.info("")
            ok = enrich_file(feat_type, year, crime_data, available_years,
                             all_years_mode=args.all_years)
            if ok:
                success_count += 1

    log.info("")
    log.info("=" * 55)
    log.info("  KLAAR: %d bestanden verrijkt", success_count)
    log.info("  Criminaliteitsdata: %d-%d (%d jaren)",
             min(available_years), max(available_years), len(available_years))
    log.info("=" * 55)


if __name__ == "__main__":
    main()
