"""
Enrich GeoJSON with Flora & Fauna data from GBIF API.
=====================================================
Fetches species occurrence data per Dutch gemeente from the
Global Biodiversity Information Facility (GBIF) API.

GBIF uses GADM administrative boundaries. Dutch gemeenten are at
GADM level 2 with codes like NLD.9.4_1 (Amsterdam).

Indicators:
  - ff_totaal_soorten:      unique species count
  - ff_totaal_waarnemingen: total observations
  - ff_soorten_vogels:      bird species (Aves, classKey=212)
  - ff_soorten_zoogdieren:  mammal species (Mammalia, classKey=359)
  - ff_soorten_planten:     plant species (Plantae, kingdomKey=6)

Usage:
    python enrich_flora_fauna.py                              # All GeoJSON
    python enrich_flora_fauna.py --type gemeenten --year 2022
    python enrich_flora_fauna.py --resume                     # Resume from cache

Dependencies:
    pip install requests
"""

import argparse
import json
import logging
import os
import re
import sys
import time

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("flora")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GBIF_API = "https://api.gbif.org/v1"
GADM_COUNTRY = "NLD"
GADM_LEVEL = 2

# Taxonomy keys
CLASS_AVES = 212       # Birds
CLASS_MAMMALIA = 359   # Mammals
KINGDOM_PLANTAE = 6    # Plants

# Rate limiting
REQUEST_DELAY = 0.6  # seconds between API calls (be nice to GBIF)
MAX_RETRIES = 3

# Cache file for GBIF results (so we can resume)
CACHE_FILE = "gbif_flora_cache.json"

NAME_FIELD = {
    "gemeenten": "gemeentenaam",
    "buurten": "gemeentenaam",
    "wijken": "gemeentenaam",
}


def normalize_naam(naam):
    """Normalize gemeente naam for matching."""
    if not naam:
        return ""
    naam = naam.strip()
    # Common normalizations
    naam = re.sub(r'\s*\(gemeente\)\s*$', '', naam)
    naam = re.sub(r'\s*\(L\.\)\s*$', '', naam)
    naam = re.sub(r'\s*\(O\.\)\s*$', '', naam)
    naam = re.sub(r'\s*\(GR\.\)\s*$', '', naam)
    naam = re.sub(r'\s*\(NH\.\)\s*$', '', naam)
    return naam.strip().lower()


def gbif_get(url, params=None):
    """Make a GET request to GBIF API with retry logic."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 503:
                log.warning("  GBIF 503 (overbelast), wacht 10s...")
                time.sleep(10)
            else:
                log.warning("  GBIF %d: %s", resp.status_code, resp.text[:200])
                time.sleep(2)
        except requests.exceptions.Timeout:
            log.warning("  Timeout, poging %d/%d", attempt + 1, MAX_RETRIES)
            time.sleep(5)
        except Exception as e:
            log.warning("  Fout: %s", e)
            time.sleep(2)
    return None


def fetch_gadm_mapping():
    """
    Fetch all GADM level-2 entries for the Netherlands.
    Returns dict: {normalized_name: gadm_gid}
    """
    log.info("GBIF GADM mapping ophalen voor Nederland (level %d)...", GADM_LEVEL)
    url = f"{GBIF_API}/geocode/gadm/search"
    data = gbif_get(url, {"gadmGid": GADM_COUNTRY, "gadmLevel": GADM_LEVEL, "limit": 500})

    if not data or "results" not in data:
        log.error("Kan GADM mapping niet ophalen!")
        return {}

    mapping = {}
    for entry in data["results"]:
        name = entry.get("name", "")
        gid = entry.get("id", "")
        if name and gid:
            key = normalize_naam(name)
            mapping[key] = gid
            # Also store original name for debugging
            mapping[key + "__original"] = name

    log.info("  %d GADM gemeenten gevonden", len([k for k in mapping if not k.endswith("__original")]))
    return mapping


def count_species(gadm_gid, class_key=None, kingdom_key=None):
    """
    Count unique species in a GADM area using GBIF faceted search.
    Returns (species_count, observation_count).
    """
    params = {
        "gadmGid": gadm_gid,
        "limit": 0,
        "facet": "speciesKey",
        "facetLimit": 100000,
    }
    if class_key:
        params["classKey"] = class_key
    if kingdom_key:
        params["kingdomKey"] = kingdom_key

    url = f"{GBIF_API}/occurrence/search"
    data = gbif_get(url, params)
    if not data:
        return 0, 0

    observations = data.get("count", 0)
    facets = data.get("facets", [])
    species = 0
    if facets and len(facets) > 0:
        species = len(facets[0].get("counts", []))

    return species, observations


def fetch_gemeente_data(gadm_gid, gemeente_naam):
    """Fetch all flora/fauna indicators for a single gemeente."""
    result = {}

    # 1. Total species + observations
    time.sleep(REQUEST_DELAY)
    soorten, waarnemingen = count_species(gadm_gid)
    result["ff_totaal_soorten"] = soorten
    result["ff_totaal_waarnemingen"] = waarnemingen

    # 2. Bird species
    time.sleep(REQUEST_DELAY)
    vogels, _ = count_species(gadm_gid, class_key=CLASS_AVES)
    result["ff_soorten_vogels"] = vogels

    # 3. Mammal species
    time.sleep(REQUEST_DELAY)
    zoogdieren, _ = count_species(gadm_gid, class_key=CLASS_MAMMALIA)
    result["ff_soorten_zoogdieren"] = zoogdieren

    # 4. Plant species
    time.sleep(REQUEST_DELAY)
    planten, _ = count_species(gadm_gid, kingdom_key=KINGDOM_PLANTAE)
    result["ff_soorten_planten"] = planten

    return result


def load_cache():
    """Load cached GBIF results."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache):
    """Save GBIF results to cache."""
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def enrich_file(filename, gadm_mapping, cache, skip_cached=True):
    """Enrich a single GeoJSON file with flora/fauna data."""
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

    if feat_type not in NAME_FIELD:
        log.error("Onbekend type: %s", feat_type)
        return False

    name_field = NAME_FIELD[feat_type]

    log.info("=" * 55)
    log.info("  ENRICH FLORA & FAUNA: %s %d", feat_type.upper(), year)
    log.info("=" * 55)

    # Load GeoJSON
    log.info("Laden: %s...", filename)
    with open(filename, "r", encoding="utf-8") as fh:
        geojson = json.load(fh)
    features = geojson.get("features", [])
    log.info("  %d features", len(features))

    # Collect unique gemeente names
    gemeente_namen = set()
    for f in features:
        naam = f.get("properties", {}).get(name_field, "")
        if naam:
            gemeente_namen.add(naam)
    log.info("  %d unieke gemeenten", len(gemeente_namen))

    # Fetch data per gemeente (with caching)
    ff_data = {}  # {normalized_name: {indicators}}
    total = len(gemeente_namen)
    done = 0
    new_fetched = 0

    for naam in sorted(gemeente_namen):
        done += 1
        norm = normalize_naam(naam)

        # Check cache
        if skip_cached and norm in cache:
            ff_data[norm] = cache[norm]
            continue

        # Find GADM GID
        gid = gadm_mapping.get(norm)
        if not gid:
            # Try fuzzy match
            for gadm_norm, gadm_gid in gadm_mapping.items():
                if gadm_norm.endswith("__original"):
                    continue
                if norm in gadm_norm or gadm_norm in norm:
                    gid = gadm_gid
                    break

        if not gid:
            log.warning("  [%d/%d] %s: GEEN GADM match gevonden", done, total, naam)
            continue

        log.info("  [%d/%d] %s (GADM: %s)...", done, total, naam, gid)
        data = fetch_gemeente_data(gid, naam)
        ff_data[norm] = data
        cache[norm] = data
        new_fetched += 1

        # Save cache periodically
        if new_fetched % 10 == 0:
            save_cache(cache)
            log.info("  Cache opgeslagen (%d nieuwe gemeenten)", new_fetched)

    # Save final cache
    if new_fetched > 0:
        save_cache(cache)
        log.info("  %d nieuwe gemeenten opgehaald, %d uit cache", new_fetched, done - new_fetched)

    # Merge into GeoJSON
    enriched = 0
    indicators_with_data = set()

    for f in features:
        props = f.get("properties", {})
        naam = props.get(name_field, "")
        if not naam:
            continue

        norm = normalize_naam(naam)
        row = ff_data.get(norm)
        if not row:
            continue

        any_added = False
        for key, val in row.items():
            if val is not None and val > 0:
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
    meta["gbif_source"] = "GBIF API v1 (gbif.org)"
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
        description="Verrijk GeoJSON met GBIF Flora & Fauna data"
    )
    parser.add_argument("--type", choices=["gemeenten", "buurten", "wijken"],
                        help="Feature type (default: alle)")
    parser.add_argument("--year", type=int, help="CBS jaar (default: alle)")
    parser.add_argument("--resume", action="store_true",
                        help="Gebruik cache en sla al opgehaalde gemeenten over")
    parser.add_argument("--no-cache", action="store_true",
                        help="Negeer cache, alles opnieuw ophalen")
    args = parser.parse_args()

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    files = find_geojson_files(args.type, args.year)
    if not files:
        log.error("Geen GeoJSON bestanden gevonden!")
        return 1

    log.info("Te verrijken: %d bestanden", len(files))
    log.info("GBIF API: %s", GBIF_API)
    log.info("Cache: %s\n", "aan (--resume)" if args.resume else ("uit (--no-cache)" if args.no_cache else "aan"))

    # Load GADM mapping
    gadm_mapping = fetch_gadm_mapping()
    if not gadm_mapping:
        log.error("Kan GADM mapping niet laden!")
        return 1

    # Load cache
    cache = {} if args.no_cache else load_cache()
    if cache:
        log.info("Cache geladen: %d gemeenten\n", len(cache))

    # Enrich files
    success = 0
    for filename in files:
        try:
            if enrich_file(filename, gadm_mapping, cache, skip_cached=(not args.no_cache)):
                success += 1
        except KeyboardInterrupt:
            log.info("\nOnderbroken! Cache opgeslagen.")
            save_cache(cache)
            return 1
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
