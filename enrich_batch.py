"""
Enrich GeoJSON with CBS indicators via batched PDOK requests.
Splits properties into groups of 20 to avoid PDOK timeouts.

Usage:
    python enrich_batch.py --type gemeenten --year 2024
"""

import argparse
import json
import logging
import os
import sys
import time
import requests

from build_geojson import (
    PDOK_WFS, PDOK_FIELDS, ADMIN_FIELDS, ID_FIELD, AVAILABLE_YEARS,
    get_layer_name, discover_fields, resolve_fields, clean_value, count_total,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("enrich")

BATCH_SIZE = 15  # properties per request (ex id_field)


def fetch_batch(base_url, type_name, id_field, prop_names, expected_total):
    """Fetch a batch of properties (no geometry) and return as dict keyed by id."""
    result = {}
    start = 0
    props_str = ",".join([id_field] + prop_names)

    while True:
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": type_name, "outputFormat": "application/json",
            "count": 2000, "startIndex": start,
            "propertyName": props_str,
        }
        resp = requests.get(base_url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])

        for f in features:
            p = f.get("properties", {})
            fid = p.get(id_field, "")
            if fid:
                result[fid] = p

        if len(features) == 0 or len(features) < 2000:
            break
        nm = data.get("numberMatched", expected_total)
        if nm > 0 and len(result) >= nm:
            break
        start += len(features)

    return result


def enrich(feat_type, year):
    filename = f"{feat_type}_{year}.geojson"
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    if not os.path.exists(filename):
        log.error(f"{filename} niet gevonden!")
        return

    base_url = PDOK_WFS.format(year=year)
    type_name = get_layer_name(feat_type, year)
    id_field = ID_FIELD[feat_type]

    log.info("=" * 50)
    log.info(f"  ENRICH {feat_type.upper()} {year} (batch mode)")
    log.info("=" * 50)

    # 1. Load existing
    log.info(f"Laden: {filename}...")
    with open(filename, "r", encoding="utf-8") as fh:
        geojson = json.load(fh)
    features = geojson.get("features", [])
    log.info(f"  {len(features)} features")

    # 2. Discover & resolve
    log.info("PDOK velden ontdekken...")
    available = discover_fields(base_url, type_name)
    field_map = resolve_fields(available)
    log.info(f"  {len(field_map)}/{len(PDOK_FIELDS)} indicatoren")

    total = count_total(base_url, type_name)

    # 3. Split into batches
    pdok_fields_list = list(field_map.values())
    batches = [pdok_fields_list[i:i+BATCH_SIZE] for i in range(0, len(pdok_fields_list), BATCH_SIZE)]
    log.info(f"  {len(batches)} batches van max {BATCH_SIZE} properties")

    # 4. Fetch all batches
    all_stats = {}  # {fid: {prop: val, ...}}
    for bi, batch in enumerate(batches):
        log.info(f"  Batch {bi+1}/{len(batches)}: {len(batch)} properties...")
        t0 = time.time()
        try:
            batch_data = fetch_batch(base_url, type_name, id_field, batch, total)
            t1 = time.time()
            log.info(f"    {len(batch_data)} gebieden in {t1-t0:.1f}s")
            for fid, props in batch_data.items():
                if fid not in all_stats:
                    all_stats[fid] = {}
                all_stats[fid].update(props)
        except Exception as e:
            log.error(f"    FOUT: {e}")
            continue

    log.info(f"  Totaal: {len(all_stats)} gebieden met data")

    # 5. Merge into features
    log.info("Mergen...")
    enriched = 0
    indicators_with_data = set()

    # Reverse field_map: pdok_field -> app_key
    reverse_map = {v: k for k, v in field_map.items()}

    for f in features:
        props = f.get("properties", {})
        fid = props.get(id_field, "")
        if fid and fid in all_stats:
            pdok_props = all_stats[fid]
            for pdok_field, val in pdok_props.items():
                app_key = reverse_map.get(pdok_field)
                if app_key:
                    v = clean_value(val)
                    if v is not None:
                        if isinstance(v, float):
                            v = round(v, 2)
                        props[app_key] = v
                        indicators_with_data.add(app_key)
            enriched += 1

    log.info(f"  {enriched}/{len(features)} verrijkt, {len(indicators_with_data)} indicatoren")

    # 6. Update metadata & save
    geojson["metadata"] = {
        "type": feat_type, "year": year,
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "count": len(features), "with_data": enriched,
        "indicators": sorted(indicators_with_data),
        "indicators_count": len(indicators_with_data),
    }

    log.info(f"Opslaan: {filename}...")
    with open(filename, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(filename) / (1024 * 1024)
    log.info(f"  {size_mb:.1f} MB - KLAAR!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--type", choices=["buurten", "wijken", "gemeenten"], default="gemeenten")
    parser.add_argument("--year", type=int, default=2024)
    args = parser.parse_args()
    enrich(args.type, args.year)
