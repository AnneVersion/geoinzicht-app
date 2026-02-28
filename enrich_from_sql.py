"""
Enrich static GeoJSON files with CBS domain data.
==================================================
Fetches bodemgebruik (land use) and landbouw (agriculture) data
directly from the CBS Open Data API (cbsodata) and merges them
into existing GeoJSON files.

CBS tabellen gebruiken gemeente-NAMEN (niet GM-codes), dus we
matchen op naam via het 'gemeentenaam' veld in de GeoJSON.

Flora & Fauna data requires GBIF/NDFF loading (separate process).

Usage:
    python enrich_from_sql.py                              # All GeoJSON files
    python enrich_from_sql.py --type gemeenten --year 2024 # Specific file
    python enrich_from_sql.py --domains bodemgebruik       # Only bodemgebruik

Dependencies:
    pip install cbsodata
"""

import argparse
import json
import logging
import os
import re
import sys
import time

import cbsodata

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("enrich")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CODE_FIELD = {
    "gemeenten": "gemeentecode",
    "buurten": "buurtcode",
    "wijken": "wijkcode",
}

NAME_FIELD = {
    "gemeenten": "gemeentenaam",
    "buurten": "gemeentenaam",
    "wijken": "gemeentenaam",
}

# CBS Bodemgebruik dataset: 70262ned (per gemeente, op naam)
CBS_BBG_TABLE = "70262ned"
# CBS Landbouw dataset: 80781ned (per gemeente, op naam)
CBS_LBT_TABLE = "80781ned"

# Peiljaren bodemgebruik beschikbaar bij CBS
BBG_PEILJAREN = [1996, 2000, 2003, 2006, 2008, 2010, 2012, 2015, 2017]


def normalize_naam(naam):
    """Normalize gemeente naam for matching.
    CBS uses forms like "'s-Gravenhage (gemeente)" or "Utrecht (gemeente)".
    GeoJSON uses "'s-Gravenhage" or "Utrecht".
    """
    if not naam:
        return ""
    naam = naam.strip()
    # Remove suffixes like " (gemeente)", " (SG)", " (GA)"
    naam = re.sub(r'\s*\(gemeente\)\s*$', '', naam)
    naam = re.sub(r'\s*\(SG\)\s*$', '', naam)
    naam = re.sub(r'\s*\(GA\)\s*$', '', naam)
    return naam.strip().lower()


def is_gemeente_regio(naam):
    """Check if a RegioS value is a gemeente (not a regio/province/landsdeel)."""
    if not naam:
        return False
    naam = naam.strip()
    # Skip regio-aggregates with suffixes like (LB), (CR), (LD), (PV)
    if re.search(r'\((LB|CR|LD|PV)\)\s*$', naam):
        return False
    if naam in ('Nederland',):
        return False
    return True


def fetch_bodemgebruik():
    """
    Fetch bodemgebruik data from CBS Open Data API.
    Returns dict: {(gemeente_naam_lower, peiljaar): {field: val, ...}}
    """
    log.info("CBS Bodemgebruik ophalen (tabel %s)...", CBS_BBG_TABLE)
    try:
        data = cbsodata.get_data(CBS_BBG_TABLE)
    except Exception as e:
        log.error("CBS API fout: %s", e)
        return {}

    log.info("  %d rijen opgehaald", len(data))

    result = {}
    for row in data:
        regio = str(row.get("RegioS", "")).strip()
        periode = str(row.get("Perioden", "")).strip()

        # Only gemeente-level
        if not is_gemeente_regio(regio):
            continue

        # Parse year
        jaar_match = re.match(r"(\d{4})", periode)
        if not jaar_match:
            continue
        peiljaar = int(jaar_match.group(1))

        totaal = row.get("TotaleOppervlakte_1") or 0
        if totaal <= 0:
            continue

        bebouwd = row.get("TotaalBebouwdTerrein_6") or 0
        agrarisch = row.get("TotaalAgrarischTerrein_25") or 0
        natuur = row.get("TotaalBosEnOpenNatuurlijkTerrein_28") or 0
        recreatie = row.get("TotaalRecreatieterrein_19") or 0
        woon = row.get("Woonterrein_7") or 0
        bedrijf = row.get("Bedrijventerrein_11") or 0

        naam_key = normalize_naam(regio)
        result[(naam_key, peiljaar)] = {
            "bbg_peiljaar": peiljaar,
            "pct_bebouwd": round(bebouwd / totaal * 100, 2) if totaal else None,
            "pct_agrarisch": round(agrarisch / totaal * 100, 2) if totaal else None,
            "pct_natuur": round(natuur / totaal * 100, 2) if totaal else None,
            "pct_recreatie": round(recreatie / totaal * 100, 2) if totaal else None,
            "opp_woonterrein_ha": round(woon, 1),
            "opp_bedrijventerrein_ha": round(bedrijf, 1),
        }

    log.info("  %d gemeente×peiljaar combinaties", len(result))
    return result


def fetch_landbouw():
    """
    Fetch landbouw data from CBS Open Data API (tabel 80781ned).
    Returns dict: {(gemeente_naam_lower, jaar): {field: val, ...}}
    """
    log.info("CBS Landbouw ophalen (tabel %s)...", CBS_LBT_TABLE)
    try:
        data = cbsodata.get_data(CBS_LBT_TABLE)
    except Exception as e:
        log.error("CBS API fout: %s", e)
        return {}

    log.info("  %d rijen opgehaald", len(data))

    result = {}
    for row in data:
        regio = str(row.get("RegioS", "")).strip()
        periode = str(row.get("Perioden", "")).strip()

        if not is_gemeente_regio(regio):
            continue

        jaar_match = re.match(r"(\d{4})", periode)
        if not jaar_match:
            continue
        jaar = int(jaar_match.group(1))

        cultuurgrond = row.get("Cultuurgrond_3")
        rundvee = row.get("RundveeTotaal_84")
        varkens = row.get("VarkensTotaal_121")
        kippen = row.get("KippenTotaal_125")
        bedrijven = row.get("AantalLandbouwbedrijvenTotaal_1")

        # Fallback: alternatieve kolomnamen
        if cultuurgrond is None:
            cultuurgrond = row.get("Cultuurgrond_6")
        if rundvee is None:
            rundvee = row.get("RundveeTotaal_103")
        if varkens is None:
            varkens = row.get("VarkensTotaal_136")
        if kippen is None:
            kippen = row.get("KippenTotaal_140")

        has_data = any(v is not None and v != 0 for v in [cultuurgrond, rundvee, varkens, kippen, bedrijven])
        if not has_data:
            continue

        def to_num(v):
            if v is None:
                return None
            try:
                n = float(v)
                return round(n, 1) if n != int(n) else int(n)
            except (ValueError, TypeError):
                return None

        naam_key = normalize_naam(regio)
        result[(naam_key, jaar)] = {
            "lbt_totaal_cultuurgrond_ha": to_num(cultuurgrond),
            "lbt_totaal_rundvee": to_num(rundvee),
            "lbt_totaal_varkens": to_num(varkens),
            "lbt_totaal_kippen": to_num(kippen),
            "lbt_aantal_bedrijven": to_num(bedrijven),
        }

    log.info("  %d gemeente×jaar combinaties", len(result))
    return result


def find_nearest_peiljaar(peiljaren, target_year):
    """Find nearest peiljaar <= target_year."""
    candidates = [p for p in peiljaren if p <= target_year]
    return max(candidates) if candidates else None


def enrich_file(filename, bbg_data, lbt_data, domains):
    """Enrich a single GeoJSON file."""
    base = os.path.splitext(os.path.basename(filename))[0]
    parts = base.split("_")
    if len(parts) != 2:
        log.error("Onverwachte bestandsnaam: %s", filename)
        return False

    feat_type = parts[0]
    try:
        year = int(parts[1])
    except ValueError:
        log.error("Kan jaar niet parsen: %s", filename)
        return False

    if feat_type not in CODE_FIELD:
        log.error("Onbekend type: %s", feat_type)
        return False

    name_field = NAME_FIELD[feat_type]

    log.info("=" * 55)
    log.info("  ENRICH %s %d", feat_type.upper(), year)
    log.info("=" * 55)

    # Load GeoJSON
    log.info("Laden: %s...", filename)
    with open(filename, "r", encoding="utf-8") as fh:
        geojson = json.load(fh)
    features = geojson.get("features", [])
    log.info("  %d features", len(features))

    # Find nearest BBG peiljaar
    available_bbg_years = sorted(set(pj for (_, pj) in bbg_data.keys())) if bbg_data else []
    nearest_pj = find_nearest_peiljaar(available_bbg_years, year)
    if nearest_pj:
        log.info("  Bodemgebruik: peiljaar %d (voor CBS jaar %d)", nearest_pj, year)

    # Merge
    enriched = 0
    indicators_with_data = set()

    for f in features:
        props = f.get("properties", {})
        gem_naam = props.get(name_field, "")
        if not gem_naam:
            continue

        naam_key = normalize_naam(gem_naam)
        any_added = False

        # Bodemgebruik
        if "bodemgebruik" in domains and nearest_pj and naam_key:
            bbg_row = bbg_data.get((naam_key, nearest_pj))
            if bbg_row:
                for key, val in bbg_row.items():
                    if val is not None:
                        props[key] = val
                        if key != "bbg_peiljaar":
                            indicators_with_data.add(key)
                        any_added = True

        # Landbouw
        if "landbouw" in domains and naam_key:
            lbt_row = lbt_data.get((naam_key, year))
            # Fallback: try adjacent years
            if not lbt_row:
                for offset in [-1, 1, -2, 2]:
                    lbt_row = lbt_data.get((naam_key, year + offset))
                    if lbt_row:
                        break
            if lbt_row:
                for key, val in lbt_row.items():
                    if val is not None:
                        props[key] = val
                        indicators_with_data.add(key)
                        any_added = True

        if any_added:
            enriched += 1

    log.info("  %d/%d features verrijkt", enriched, len(features))
    log.info("  %d indicatoren met data", len(indicators_with_data))

    if not indicators_with_data:
        log.warning("  Geen data gevonden voor dit bestand")
        return False

    # Update metadata
    meta = geojson.get("metadata", {})
    existing = set(meta.get("indicators", []))
    meta["indicators"] = sorted(existing | indicators_with_data)
    meta["indicators_count"] = len(meta["indicators"])
    meta["enriched_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    if nearest_pj and "bodemgebruik" in domains:
        meta["bbg_peiljaar"] = nearest_pj
    geojson["metadata"] = meta

    # Save
    log.info("Opslaan: %s...", filename)
    with open(filename, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(filename) / (1024 * 1024)
    log.info("  %.1f MB - KLAAR!", size_mb)
    log.info("  Indicatoren: %s", ', '.join(sorted(indicators_with_data)))
    return True


def find_geojson_files(feat_type=None, year=None):
    """Find matching GeoJSON files."""
    files = []
    for f in os.listdir("."):
        if not f.endswith(".geojson") or f.startswith("bag"):
            continue
        base = f.replace(".geojson", "")
        parts = base.split("_")
        if len(parts) != 2:
            continue
        ftype, fyear = parts[0], parts[1]
        if feat_type and ftype != feat_type:
            continue
        if year and fyear != str(year):
            continue
        if ftype in ("gemeenten", "buurten", "wijken"):
            files.append(f)
    return sorted(files)


def main():
    parser = argparse.ArgumentParser(
        description="Verrijk GeoJSON met CBS domeindata (bodemgebruik, landbouw)"
    )
    parser.add_argument("--type", choices=["gemeenten", "buurten", "wijken"],
                        help="Feature type (default: alle)")
    parser.add_argument("--year", type=int, help="CBS jaar (default: alle)")
    parser.add_argument("--domains", nargs="+",
                        choices=["bodemgebruik", "landbouw"],
                        default=["bodemgebruik", "landbouw"],
                        help="Welke domeinen (default: alle)")
    args = parser.parse_args()

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    files = find_geojson_files(args.type, args.year)
    if not files:
        log.error("Geen GeoJSON bestanden gevonden!")
        return 1

    log.info("Te verrijken: %d bestanden", len(files))
    log.info("Domeinen: %s\n", ', '.join(args.domains))

    # Fetch domain data from CBS API
    bbg_data = {}
    lbt_data = {}

    if "bodemgebruik" in args.domains:
        bbg_data = fetch_bodemgebruik()
        if not bbg_data:
            log.warning("Geen bodemgebruik data opgehaald!")

    if "landbouw" in args.domains:
        lbt_data = fetch_landbouw()
        if not lbt_data:
            log.warning("Geen landbouw data opgehaald!")

    if not bbg_data and not lbt_data:
        log.error("Geen data beschikbaar - kan niet verrijken")
        return 1

    # Enrich files
    success = 0
    for filename in files:
        try:
            if enrich_file(filename, bbg_data, lbt_data, args.domains):
                success += 1
        except Exception as e:
            log.error("FOUT bij %s: %s", filename, e)
            import traceback
            traceback.print_exc()

    log.info("\n%s", '=' * 55)
    log.info("  KLAAR: %d/%d bestanden verrijkt", success, len(files))
    log.info("%s", '=' * 55)
    return 0 if success > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
