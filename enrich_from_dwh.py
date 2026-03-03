"""
Enrich static GeoJSON files with DWH domain data.
==================================================
Reads zorgkosten (Vektis) and criminaliteit (Politie) data
from CBS_Buurtdata DWH and merges into existing GeoJSON files.

Domeinen:
  - Zorgkosten:    transform.zorgkosten_per_gemeente  -> match op gemeentenaam
  - Criminaliteit: transform.misdrijven_alle_jaren     -> match op gemeente_code

Usage:
    python enrich_from_dwh.py                              # All GeoJSON files
    python enrich_from_dwh.py --type gemeenten --year 2024 # Specific file
    python enrich_from_dwh.py --domains zorgkosten         # Only zorgkosten
    python enrich_from_dwh.py --server localhost            # Custom server

Dependencies:
    pip install pyodbc
"""

import argparse
import json
import logging
import os
import re
import sys
import time

import pyodbc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("enrich_dwh")

DATABASE = "CBS_Buurtdata"

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


def get_connection(server: str):
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={DATABASE};Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)


def normalize_naam(naam):
    """Normalize gemeente naam for matching."""
    if not naam:
        return ""
    naam = naam.strip()
    naam = re.sub(r'\s*\(gemeente\)\s*$', '', naam)
    naam = re.sub(r'\s*\(SG\)\s*$', '', naam)
    naam = re.sub(r'\s*\(GA\)\s*$', '', naam)
    return naam.strip().lower()


def fetch_zorgkosten(conn):
    """
    Haal zorgkosten per gemeente uit DWH.
    Returns dict: {(gemeente_naam_lower, jaar): {field: val, ...}}
    """
    log.info("Zorgkosten ophalen uit DWH...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT gemeentenaam, jaar,
               aantal_verzekerden,
               kosten_totaal_per_verzekerde,
               kosten_medspec_per_verzekerde,
               kosten_farmacie_per_verzekerde,
               kosten_ggz_per_verzekerde,
               kosten_huisarts_per_verzekerde,
               kosten_paramedisch_per_verzekerde,
               kosten_overig_per_verzekerde
        FROM transform.zorgkosten_per_gemeente
    """)

    result = {}
    count = 0
    for row in cursor.fetchall():
        gemeentenaam = str(row[0]).strip()
        jaar = int(row[1])
        naam_key = normalize_naam(gemeentenaam)

        def safe_round(v, decimals=0):
            if v is None:
                return None
            try:
                return round(float(v), decimals)
            except (ValueError, TypeError):
                return None

        result[(naam_key, jaar)] = {
            "zk_aantal_verzekerden": safe_round(row[2]),
            "zk_kosten_totaal": safe_round(row[3]),
            "zk_kosten_medspec": safe_round(row[4]),
            "zk_kosten_farmacie": safe_round(row[5]),
            "zk_kosten_ggz": safe_round(row[6]),
            "zk_kosten_huisarts": safe_round(row[7]),
            "zk_kosten_paramedisch": safe_round(row[8]),
            "zk_kosten_overig": safe_round(row[9]),
        }
        count += 1

    cursor.close()
    log.info("  %d gemeente x jaar combinaties", count)
    return result


def fetch_criminaliteit(conn):
    """
    Haal criminaliteit per gemeente uit DWH (geaggregeerd van buurt naar gemeente).
    Returns dict: {(gemeente_code, jaar): {field: val, ...}}
    """
    log.info("Criminaliteit ophalen uit DWH...")
    cursor = conn.cursor()
    cursor.execute("""
        SELECT gemeente_code, jaar,
               SUM(totaal_misdrijven) AS totaal_misdrijven,
               SUM(diefstal_inbraak_woning) AS inbraak_woning,
               SUM(diefstal_motorvoertuigen) AS diefstal_motor,
               SUM(diefstal_fietsen) AS diefstal_fietsen,
               SUM(bedreiging) AS bedreiging,
               SUM(mishandeling) AS mishandeling,
               SUM(straatroof) AS straatroof,
               SUM(vernieling) AS vernieling,
               SUM(drugs_drankoverlast) AS drugs_drank,
               SUM(diefstal_inbraak_bedrijven) AS inbraak_bedrijven,
               SUM(winkeldiefstal) AS winkeldiefstal,
               SUM(cybercrime) AS cybercrime,
               SUM(horizontale_fraude) AS fraude
        FROM transform.misdrijven_alle_jaren
        WHERE gemeente_code IS NOT NULL
        GROUP BY gemeente_code, jaar
    """)

    result = {}
    count = 0
    for row in cursor.fetchall():
        gm_code = str(row[0]).strip()
        jaar = int(row[1])

        def safe_int(v):
            if v is None:
                return None
            try:
                return int(float(v))
            except (ValueError, TypeError):
                return None

        result[(gm_code, jaar)] = {
            "cr_totaal_misdrijven": safe_int(row[2]),
            "cr_inbraak_woning": safe_int(row[3]),
            "cr_diefstal_motor": safe_int(row[4]),
            "cr_diefstal_fietsen": safe_int(row[5]),
            "cr_bedreiging": safe_int(row[6]),
            "cr_mishandeling": safe_int(row[7]),
            "cr_straatroof": safe_int(row[8]),
            "cr_vernieling": safe_int(row[9]),
            "cr_drugs_drank": safe_int(row[10]),
            "cr_inbraak_bedrijven": safe_int(row[11]),
            "cr_winkeldiefstal": safe_int(row[12]),
            "cr_cybercrime": safe_int(row[13]),
            "cr_fraude": safe_int(row[14]),
        }
        count += 1

    cursor.close()
    log.info("  %d gemeente x jaar combinaties", count)
    return result


def enrich_file(filename, zk_data, cr_data, domains):
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

    code_field = CODE_FIELD[feat_type]
    name_field = NAME_FIELD[feat_type]

    log.info("=" * 55)
    log.info("  ENRICH %s %d (DWH)", feat_type.upper(), year)
    log.info("=" * 55)

    # Load GeoJSON
    log.info("Laden: %s...", filename)
    with open(filename, "r", encoding="utf-8") as fh:
        geojson = json.load(fh)
    features = geojson.get("features", [])
    log.info("  %d features", len(features))

    # Find best year for data
    zk_years = sorted(set(j for (_, j) in zk_data.keys())) if zk_data else []
    cr_years = sorted(set(j for (_, j) in cr_data.keys())) if cr_data else []

    # Find nearest year <= target year (max 2 jaar verschil, anders geen data)
    def nearest_year(available, target, max_gap=2):
        candidates = [y for y in available if y <= target]
        if candidates:
            best = max(candidates)
            return best if (target - best) <= max_gap else None
        # Fallback: kijk of er een jaar is net NA target (max 1 jaar)
        future = [y for y in available if y > target and (y - target) <= 1]
        return min(future) if future else None

    zk_year = nearest_year(zk_years, year)
    cr_year = nearest_year(cr_years, year)

    if zk_year:
        log.info("  Zorgkosten: jaar %d (voor GeoJSON jaar %d)", zk_year, year)
    if cr_year:
        log.info("  Criminaliteit: jaar %d (voor GeoJSON jaar %d)", cr_year, year)

    enriched = 0
    indicators_with_data = set()

    for f in features:
        props = f.get("properties", {})
        gem_naam = props.get(name_field, "")
        gem_code = props.get(code_field, "")
        any_added = False

        # ── Zorgkosten (match op gemeentenaam) ──
        if "zorgkosten" in domains and zk_year and gem_naam:
            naam_key = normalize_naam(gem_naam)
            zk_row = zk_data.get((naam_key, zk_year))
            if zk_row:
                for key, val in zk_row.items():
                    if val is not None:
                        props[key] = val
                        indicators_with_data.add(key)
                        any_added = True
                props["zk_jaar"] = zk_year

        # ── Criminaliteit (match op gemeentecode) ──
        if "criminaliteit" in domains and cr_year:
            # For gemeenten: gemeentecode = GM0363
            # For buurten/wijken: we need to extract gemeente code
            match_code = None
            if feat_type == "gemeenten":
                match_code = gem_code  # GM0363
            elif feat_type == "buurten":
                # buurtcode = BU03630000 -> gemeente = GM0363
                bc = str(gem_code).strip()
                if bc.startswith("BU") and len(bc) >= 6:
                    match_code = "GM" + bc[2:6]
            elif feat_type == "wijken":
                # wijkcode = WK036300 -> gemeente = GM0363
                wc = str(gem_code).strip()
                if wc.startswith("WK") and len(wc) >= 6:
                    match_code = "GM" + wc[2:6]

            if match_code:
                cr_row = cr_data.get((match_code, cr_year))
                if cr_row:
                    for key, val in cr_row.items():
                        if val is not None:
                            props[key] = val
                            indicators_with_data.add(key)
                            any_added = True
                    props["cr_jaar"] = cr_year

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
    if zk_year and "zorgkosten" in domains:
        meta["zk_jaar"] = zk_year
    if cr_year and "criminaliteit" in domains:
        meta["cr_jaar"] = cr_year
    meta["dwh_source"] = "CBS_Buurtdata DWH"
    geojson["metadata"] = meta

    # Save
    log.info("Opslaan: %s...", filename)
    with open(filename, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(filename) / (1024 * 1024)
    log.info("  %.1f MB - KLAAR!", size_mb)
    log.info("  Indicatoren: %s", ", ".join(sorted(indicators_with_data)))
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
        description="Verrijk GeoJSON met DWH data (zorgkosten, criminaliteit)"
    )
    parser.add_argument("--type", choices=["gemeenten", "buurten", "wijken"],
                        help="Feature type (default: alle)")
    parser.add_argument("--year", type=int, help="CBS jaar (default: alle)")
    parser.add_argument("--domains", nargs="+",
                        choices=["zorgkosten", "criminaliteit"],
                        default=["zorgkosten", "criminaliteit"],
                        help="Welke domeinen (default: alle)")
    parser.add_argument("--server", default="localhost",
                        help="SQL Server instance (default: localhost)")
    args = parser.parse_args()

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    files = find_geojson_files(args.type, args.year)
    if not files:
        log.error("Geen GeoJSON bestanden gevonden!")
        return 1

    log.info("Te verrijken: %d bestanden", len(files))
    log.info("Domeinen: %s", ", ".join(args.domains))
    log.info("Server: %s\n", args.server)

    # Connect to DWH
    try:
        conn = get_connection(args.server)
        log.info("DWH connectie OK")
    except Exception as e:
        log.error("Kan niet verbinden met DWH: %s", e)
        return 1

    # Fetch domain data
    zk_data = {}
    cr_data = {}

    if "zorgkosten" in args.domains:
        zk_data = fetch_zorgkosten(conn)
        if not zk_data:
            log.warning("Geen zorgkosten data!")

    if "criminaliteit" in args.domains:
        cr_data = fetch_criminaliteit(conn)
        if not cr_data:
            log.warning("Geen criminaliteit data!")

    conn.close()

    if not zk_data and not cr_data:
        log.error("Geen data beschikbaar - kan niet verrijken")
        return 1

    # Enrich files
    success = 0
    for filename in files:
        try:
            if enrich_file(filename, zk_data, cr_data, args.domains):
                success += 1
        except Exception as e:
            log.error("FOUT bij %s: %s", filename, e)
            import traceback
            traceback.print_exc()

    log.info("\n%s", "=" * 55)
    log.info("  KLAAR: %d/%d bestanden verrijkt", success, len(files))
    log.info("%s", "=" * 55)
    return 0 if success > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
