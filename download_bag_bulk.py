"""
BAG Bulk Download per Gemeente
================================
Downloads alle verblijfsobjecten van PDOK WFS per gemeente
en slaat ze op als compacte GeoJSON bestanden in de bag/ map.

Gebruik:
    python download_bag_bulk.py                    # Alle gemeenten
    python download_bag_bulk.py --gemeente GM0363  # Alleen Amsterdam
    python download_bag_bulk.py --resume           # Hervat (skip bestaande)
    python download_bag_bulk.py --workers 4        # Parallel workers

Vereisten:
    pip install requests
"""

import argparse
import json
import logging
import os
import sys
import time
import math
import concurrent.futures
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("bag_download")

# ── Config ──────────────────────────────────────────────────────
PDOK_WFS = "https://service.pdok.nl/lv/bag/wfs/v2_0"
PAGE_SIZE = 1000       # PDOK max per request
CONCURRENCY = 6        # Concurrent requests per gemeente
REQUEST_TIMEOUT = 60   # seconds
DELAY_BETWEEN_GM = 0.5 # seconds between gemeenten (rate limit respect)
MAX_RETRIES = 3
BAG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bag")

# Minimal properties to keep (smaller files)
KEEP_PROPS = {
    "identificatie", "status", "gebruiksdoel", "oppervlakte",
    "bouwjaar", "openbare_ruimte", "huisnummer", "huisletter",
    "toevoeging", "postcode", "woonplaats",
}


def get_gemeenten_from_geojson():
    """Load gemeente codes + bboxes from the 2024 GeoJSON."""
    app_dir = os.path.dirname(os.path.abspath(__file__))
    geojson_file = os.path.join(app_dir, "gemeenten_2024.geojson")

    if not os.path.exists(geojson_file):
        log.error("gemeenten_2024.geojson niet gevonden!")
        return []

    with open(geojson_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    gemeenten = []
    for feat in data.get("features", []):
        props = feat.get("properties", {})
        code = props.get("gemeentecode", "")
        naam = props.get("gemeentenaam", "")
        if not code:
            continue

        # Calculate bounding box from geometry
        geom = feat.get("geometry", {})
        bbox = compute_bbox(geom)
        if bbox:
            gemeenten.append({
                "code": code,
                "naam": naam,
                "bbox": bbox,
            })

    return gemeenten


def compute_bbox(geometry):
    """Compute [minx, miny, maxx, maxy] from a GeoJSON geometry."""
    coords = extract_all_coords(geometry)
    if not coords:
        return None
    lngs = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    # Add small buffer (0.001 ~ 100m) to ensure edge addresses are included
    return [min(lngs) - 0.001, min(lats) - 0.001, max(lngs) + 0.001, max(lats) + 0.001]


def extract_all_coords(geom):
    """Recursively extract all [lng, lat] coordinates from a geometry."""
    gtype = geom.get("type", "")
    coords = geom.get("coordinates", [])

    if gtype == "Point":
        return [coords]
    elif gtype == "MultiPoint":
        return coords
    elif gtype == "LineString":
        return coords
    elif gtype == "MultiLineString":
        return [c for ring in coords for c in ring]
    elif gtype == "Polygon":
        return [c for ring in coords for c in ring]
    elif gtype == "MultiPolygon":
        return [c for poly in coords for ring in poly for c in ring]
    return []


def fetch_hits(bbox):
    """Get total number of verblijfsobjecten in a BBOX."""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": "bag:verblijfsobject",
        "resultType": "hits",
        "srsName": "EPSG:4326",
        "BBOX": f"{bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]},EPSG:4326",
    }
    url = f"{PDOK_WFS}?{urlencode(params)}"

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                # Parse numberMatched from XML
                text = resp.text
                import re
                match = re.search(r'numberMatched="(\d+)"', text)
                if match:
                    return int(match.group(1))
                # Try JSON format
                match = re.search(r'"numberMatched"\s*:\s*(\d+)', text)
                if match:
                    return int(match.group(1))
            log.warning("  Hits response code %d, retry %d", resp.status_code, attempt + 1)
        except Exception as e:
            log.warning("  Hits error: %s, retry %d", e, attempt + 1)
        time.sleep(2 ** attempt)

    return None


def fetch_page(bbox, start_index):
    """Fetch one page of verblijfsobjecten."""
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": "bag:verblijfsobject",
        "outputFormat": "application/json",
        "count": str(PAGE_SIZE),
        "startIndex": str(start_index),
        "sortBy": "identificatie",
        "srsName": "EPSG:4326",
        "BBOX": f"{bbox[1]},{bbox[0]},{bbox[3]},{bbox[2]},EPSG:4326",
    }
    url = f"{PDOK_WFS}?{urlencode(params)}"

    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("features", [])
            log.warning("  Page %d: HTTP %d, retry %d",
                        start_index, resp.status_code, attempt + 1)
        except requests.exceptions.Timeout:
            log.warning("  Page %d: timeout, retry %d", start_index, attempt + 1)
        except Exception as e:
            log.warning("  Page %d: error %s, retry %d", start_index, e, attempt + 1)
        time.sleep(2 ** attempt)

    return None


def strip_feature(feature):
    """Keep only essential properties for compact storage."""
    props = feature.get("properties", {})
    clean = {}
    for key in KEEP_PROPS:
        if key in props and props[key] is not None:
            val = props[key]
            # Clean whitespace
            if isinstance(val, str):
                val = val.strip()
            if val != "" and val is not None:
                clean[key] = val
    return {
        "type": "Feature",
        "geometry": feature.get("geometry"),
        "properties": clean,
    }


PDOK_MAX_INDEX = 49000  # PDOK WFS geeft HTTP 400 voor startIndex > ~50000


def split_bbox(bbox, nx=2, ny=2):
    """Split een BBOX in nx*ny sub-boxen."""
    minx, miny, maxx, maxy = bbox
    dx = (maxx - minx) / nx
    dy = (maxy - miny) / ny
    subs = []
    for ix in range(nx):
        for iy in range(ny):
            subs.append([
                minx + ix * dx,
                miny + iy * dy,
                minx + (ix + 1) * dx,
                miny + (iy + 1) * dy,
            ])
    return subs


def fetch_area(bbox, seen_ids):
    """Download alle features voor een bbox, splits als te veel resultaten."""
    total = fetch_hits(bbox)
    if total is None or total == 0:
        return []

    # Als meer dan PDOK_MAX_INDEX: splits in 4 sub-boxen
    if total > PDOK_MAX_INDEX:
        log.info("    Sub-split bbox (%s features > %d limiet)", f"{total:,}", PDOK_MAX_INDEX)
        sub_features = []
        for sub_bbox in split_bbox(bbox):
            sub_features.extend(fetch_area(sub_bbox, seen_ids))
        return sub_features

    # Fit in paginatie: download alle pagina's
    pages = math.ceil(total / PAGE_SIZE)
    features = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
        futures = {}
        for page_idx in range(pages):
            start = page_idx * PAGE_SIZE
            future = executor.submit(fetch_page, bbox, start)
            futures[future] = start

        for future in concurrent.futures.as_completed(futures):
            start = futures[future]
            try:
                page_feats = future.result()
                if page_feats is None:
                    continue
                for feat in page_feats:
                    fid = feat.get("properties", {}).get("identificatie", "")
                    if fid and fid not in seen_ids:
                        seen_ids.add(fid)
                        features.append(strip_feature(feat))
            except Exception as e:
                log.warning("    Sub-pagina %d exception: %s", start, e)

    return features


def download_gemeente(gemeente, resume=False):
    """Download all BAG verblijfsobjecten for one gemeente."""
    code = gemeente["code"]
    naam = gemeente["naam"]
    bbox = gemeente["bbox"]

    outfile = os.path.join(BAG_DIR, f"bag_{code}.geojson")

    # Resume: skip if file exists and is > 1KB
    if resume and os.path.exists(outfile) and os.path.getsize(outfile) > 1024:
        log.info("  SKIP %s %s (bestand bestaat)", code, naam)
        try:
            with open(outfile, "r", encoding="utf-8") as f:
                data = json.load(f)
            return code, len(data.get("features", [])), True
        except Exception:
            pass  # Re-download if file is corrupted

    # Step 1: Get total count
    total = fetch_hits(bbox)
    if total is None:
        log.error("  FOUT %s %s: kan hits niet ophalen", code, naam)
        return code, 0, False
    if total == 0:
        log.info("  %s %s: 0 adressen", code, naam)
        return code, 0, True

    log.info("  %s %s: %s adressen ophalen...",
             code, naam, f"{total:,}".replace(",", "."))

    # Step 2: Download - automatisch splitsen als te groot voor PDOK paginatie
    seen_ids = set()
    all_features = fetch_area(bbox, seen_ids)

    # Step 3: Save
    geojson = {
        "type": "FeatureCollection",
        "features": all_features,
        "metadata": {
            "gemeente_code": code,
            "gemeente_naam": naam,
            "count": len(all_features),
            "downloaded_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        }
    }

    with open(outfile, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(outfile) / (1024 * 1024)
    log.info("  %s %s: %s adressen -> %.1f MB",
             code, naam, f"{len(all_features):,}".replace(",", "."), size_mb)

    return code, len(all_features), True


def build_index(bag_dir):
    """Build bag_index.json from all downloaded files."""
    index = {}
    total_count = 0
    total_size = 0

    for fname in sorted(os.listdir(bag_dir)):
        if not fname.startswith("bag_GM") or not fname.endswith(".geojson"):
            continue

        filepath = os.path.join(bag_dir, fname)
        code = fname.replace("bag_", "").replace(".geojson", "")
        size_bytes = os.path.getsize(filepath)

        # Quick count from metadata
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            count = data.get("metadata", {}).get("count", len(data.get("features", [])))
            naam = data.get("metadata", {}).get("gemeente_naam", "")
        except Exception:
            count = 0
            naam = ""

        size_mb = round(size_bytes / (1024 * 1024), 2)
        index[code] = {
            "naam": naam,
            "count": count,
            "size_mb": size_mb,
        }
        total_count += count
        total_size += size_bytes

    # Save index
    index_file = os.path.join(bag_dir, "bag_index.json")
    index_data = {
        "generated_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "total_gemeenten": len(index),
        "total_adressen": total_count,
        "total_size_mb": round(total_size / (1024 * 1024), 1),
        "gemeenten": index,
    }

    with open(index_file, "w", encoding="utf-8") as f:
        json.dump(index_data, f, ensure_ascii=False, indent=2)

    log.info("\nIndex: %d gemeenten, %s adressen, %.1f MB totaal",
             len(index),
             f"{total_count:,}".replace(",", "."),
             total_size / (1024 * 1024))
    return index_data


def main():
    parser = argparse.ArgumentParser(
        description="Download BAG verblijfsobjecten per gemeente van PDOK WFS"
    )
    parser.add_argument("--gemeente", help="Specifieke gemeentecode (bijv. GM0363)")
    parser.add_argument("--resume", action="store_true",
                        help="Hervat: sla bestaande bestanden over")
    parser.add_argument("--workers", type=int, default=1,
                        help="Aantal gemeenten tegelijk (default: 1)")
    parser.add_argument("--index-only", action="store_true",
                        help="Alleen index herbouwen")
    args = parser.parse_args()

    os.makedirs(BAG_DIR, exist_ok=True)

    if args.index_only:
        build_index(BAG_DIR)
        return 0

    # Load gemeenten
    gemeenten = get_gemeenten_from_geojson()
    if not gemeenten:
        log.error("Geen gemeenten gevonden!")
        return 1

    # Filter
    if args.gemeente:
        gemeenten = [g for g in gemeenten if g["code"] == args.gemeente]
        if not gemeenten:
            log.error("Gemeente %s niet gevonden!", args.gemeente)
            return 1

    log.info("=" * 60)
    log.info("  BAG Bulk Download")
    log.info("=" * 60)
    log.info("  Gemeenten: %d", len(gemeenten))
    log.info("  Output: %s", BAG_DIR)
    log.info("  Resume: %s", "Ja" if args.resume else "Nee")
    log.info("=" * 60)

    success = 0
    total_adressen = 0
    start_time = time.time()

    for i, gemeente in enumerate(gemeenten):
        pct = (i / len(gemeenten)) * 100
        elapsed = time.time() - start_time
        eta = (elapsed / max(i, 1)) * (len(gemeenten) - i) if i > 0 else 0
        eta_min = eta / 60

        log.info("\n[%d/%d] (%.0f%%) ETA: %.0f min",
                 i + 1, len(gemeenten), pct, eta_min)

        code, count, ok = download_gemeente(gemeente, resume=args.resume)
        if ok:
            success += 1
            total_adressen += count

        # Rate limiting between gemeenten
        if i < len(gemeenten) - 1:
            time.sleep(DELAY_BETWEEN_GM)

    elapsed_total = time.time() - start_time

    log.info("\n" + "=" * 60)
    log.info("  KLAAR!")
    log.info("  %d/%d gemeenten gedownload", success, len(gemeenten))
    log.info("  %s adressen totaal",
             f"{total_adressen:,}".replace(",", "."))
    log.info("  Tijd: %.1f minuten", elapsed_total / 60)
    log.info("=" * 60)

    # Build index
    build_index(BAG_DIR)

    return 0 if success > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
