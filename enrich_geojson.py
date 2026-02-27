"""
Enrich existing GeoJSON files with extra CBS indicator data from PDOK WFS.

Instead of downloading everything (including geometry) again, this script:
1. Loads the existing GeoJSON (which already has geometry)
2. Fetches ONLY the stats properties from PDOK (no geometry = much faster)
3. Merges the new properties into existing features
4. Saves the enriched GeoJSON

Usage:
    python enrich_geojson.py --type gemeenten --year 2024
    python enrich_geojson.py --type buurten --year 2024
    python enrich_geojson.py --type wijken --year 2024
"""

import argparse
import json
import logging
import os
import sys
import time

import requests

# Import from build_geojson
from build_geojson import (
    PDOK_WFS, PDOK_FIELDS, ADMIN_FIELDS, ID_FIELD, AVAILABLE_YEARS,
    get_layer_name, discover_fields, resolve_fields, clean_value, count_total,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("enrich")

PAGE_SIZE = 1000


def download_stats_only(base_url, type_name, prop_names, id_field, expected_total=-1):
    """Download only properties (NO geometry) from PDOK WFS. Much faster!"""
    all_data = {}
    start = 0
    # Request props WITHOUT geom
    prop_str = ",".join(prop_names)

    while True:
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": type_name, "outputFormat": "application/json",
            "count": PAGE_SIZE, "startIndex": start,
            "propertyName": prop_str,
        }

        pct = f" ({len(all_data)}/{expected_total})" if expected_total > 0 else f" ({len(all_data)})"
        log.info(f"  PDOK stats page startIndex={start}{pct}...")
        resp = requests.get(base_url, params=params, timeout=300)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])

        for f in features:
            props = f.get("properties", {})
            fid = props.get(id_field, "")
            if fid:
                all_data[fid] = props

        if len(features) == 0:
            break
        nm = data.get("numberMatched", expected_total)
        if nm > 0 and len(all_data) >= nm:
            break
        if len(features) < PAGE_SIZE:
            break
        start += len(features)

    return all_data


def enrich_year(feat_type, year):
    """Enrich an existing GeoJSON file with extra PDOK indicators."""
    filename = f"{feat_type}_{year}.geojson"

    if not os.path.exists(filename):
        log.error(f"  {filename} bestaat niet! Gebruik build_geojson.py eerst.")
        return None

    base_url = PDOK_WFS.format(year=year)
    type_name = get_layer_name(feat_type, year)
    admin_fields = ADMIN_FIELDS[feat_type]
    id_field = ID_FIELD[feat_type]

    log.info("=" * 50)
    log.info(f"  ENRICHING {feat_type.upper()} {year}")
    log.info("=" * 50)

    # 1. Load existing GeoJSON
    log.info(f"Stap 1: {filename} laden...")
    with open(filename, "r", encoding="utf-8") as fh:
        geojson = json.load(fh)
    features = geojson.get("features", [])
    log.info(f"  {len(features)} features geladen")

    # Check what indicators already exist
    existing_keys = set()
    for f in features[:10]:
        existing_keys.update(f.get("properties", {}).keys())
    log.info(f"  Bestaande properties: {len(existing_keys)}")

    # 2. Discover available PDOK fields
    log.info(f"Stap 2: PDOK velden ontdekken ({type_name})...")
    try:
        available = discover_fields(base_url, type_name)
    except Exception as e:
        log.error(f"  FOUT: {e}")
        return None
    log.info(f"  {len(available)} velden beschikbaar")

    # 3. Resolve field mapping
    field_map = resolve_fields(available)
    log.info(f"Stap 3: {len(field_map)}/{len(PDOK_FIELDS)} indicatoren gevonden")

    # Determine which indicators are NEW (not in existing file)
    new_fields = {}
    for app_key, pdok_field in field_map.items():
        if app_key not in existing_keys:
            new_fields[app_key] = pdok_field
    log.info(f"  {len(new_fields)} NIEUWE indicatoren toe te voegen")
    if not new_fields:
        log.info("  Geen nieuwe indicatoren - al up-to-date!")
        return filename

    # 4. Download stats (NO geometry)
    log.info(f"Stap 4: Stats downloaden (zonder geometrie)...")
    request_props = list(set([id_field] + list(field_map.values())))
    total = count_total(base_url, type_name)
    t0 = time.time()
    stats_data = download_stats_only(base_url, type_name, request_props, id_field, total)
    log.info(f"  {len(stats_data)} gebieden in {time.time()-t0:.1f}s")

    # 5. Merge into existing features
    log.info("Stap 5: Mergen...")
    enriched = 0
    indicators_with_data = set()

    for f in features:
        props = f.get("properties", {})
        fid = props.get(id_field, "")
        if fid and fid in stats_data:
            pdok_props = stats_data[fid]
            for app_key, pdok_field in field_map.items():
                v = clean_value(pdok_props.get(pdok_field))
                if v is not None:
                    if isinstance(v, float):
                        v = round(v, 2)
                    props[app_key] = v
                    indicators_with_data.add(app_key)
            enriched += 1

    log.info(f"  {enriched}/{len(features)} features verrijkt")
    log.info(f"  Indicatoren met data: {len(indicators_with_data)}")

    # 6. Update metadata
    geojson["metadata"] = {
        "type": feat_type,
        "year": year,
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "count": len(features),
        "with_data": enriched,
        "indicators": sorted(indicators_with_data),
        "indicators_count": len(indicators_with_data),
    }

    # 7. Save
    log.info(f"Stap 6: Opslaan ({filename})...")
    with open(filename, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(filename) / (1024 * 1024)
    log.info(f"  {filename}: {size_mb:.1f} MB")
    log.info("  KLAAR!")
    return filename


def main():
    parser = argparse.ArgumentParser(description="Enrich GeoJSON with extra PDOK indicators")
    parser.add_argument("--type", choices=["buurten", "wijken", "gemeenten"], default="gemeenten")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--all", action="store_true", help="Enrich all existing GeoJSON files")
    args = parser.parse_args()

    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    if args.all:
        for ft in ["gemeenten", "buurten", "wijken"]:
            for yr in AVAILABLE_YEARS:
                fn = f"{ft}_{yr}.geojson"
                if os.path.exists(fn):
                    try:
                        enrich_year(ft, yr)
                    except Exception as e:
                        log.error(f"Fout bij {ft} {yr}: {e}")
    else:
        enrich_year(args.type, args.year)


if __name__ == "__main__":
    main()
