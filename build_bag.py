"""
Build BAG GeoJSON per gemeente voor GeoInzicht.
================================================
Downloads BAG verblijfsobjecten via PDOK WFS per gemeente-BBOX,
en schrijft een compact GeoJSON bestand per gemeente.

Usage:
    python build_bag.py --gemeente 0363              # Amsterdam
    python build_bag.py --gemeente 0344 --gemeente 0363  # Utrecht + Amsterdam
    python build_bag.py --naam "Utrecht"             # Zoek op naam
    python build_bag.py --all                        # Alle gemeenten
    python build_bag.py --list gemeenten.txt         # Lijst van gemeentecodes

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

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PDOK_BAG_WFS = "https://service.pdok.nl/lv/bag/wfs/v2_0"
PAGE_SIZE = 1000  # PDOK hard limit
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
CONCURRENCY = 1  # Sequential to avoid PDOK throttling

# Velden om op te slaan per adres
BAG_FIELDS = [
    "identificatie", "status", "bouwjaar", "oppervlakte",
    "gebruiksdoel", "openbare_ruimte", "huisnummer",
    "huisletter", "toevoeging", "postcode", "woonplaats",
    "pandstatus",
]

# Output directory
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bag")

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("build_bag")


def load_gemeenten_geojson():
    """Load gemeenten GeoJSON to get gemeente codes, names, and bboxes."""
    # Probeer meerdere jaren
    for year in [2024, 2023, 2022]:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            f"gemeenten_{year}.geojson")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            log.info(f"Gemeenten geladen uit gemeenten_{year}.geojson ({len(data['features'])} features)")
            return data
    raise FileNotFoundError("Geen gemeenten_YYYY.geojson gevonden. Bouw eerst: python build_geojson.py --type gemeenten --year 2022")


def get_gemeente_bbox(feature):
    """Calculate BBOX from gemeente geometry."""
    coords = []

    def extract(geom):
        if not geom:
            return
        if geom["type"] == "Polygon":
            for ring in geom["coordinates"]:
                coords.extend(ring)
        elif geom["type"] == "MultiPolygon":
            for poly in geom["coordinates"]:
                for ring in poly:
                    coords.extend(ring)

    extract(feature["geometry"])
    if not coords:
        return None

    lngs = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    # BBOX format for PDOK: south,west,north,east,EPSG:4326
    return f"{min(lats):.6f},{min(lngs):.6f},{max(lats):.6f},{max(lngs):.6f},EPSG:4326"


def count_features(bbox):
    """Get total count of BAG features in bbox via resultType=hits."""
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeName": "bag:verblijfsobject",
        "resultType": "hits",
        "srsName": "EPSG:4326",
        "BBOX": bbox,
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(PDOK_BAG_WFS, params=params, timeout=30)
            resp.raise_for_status()
            m = re.search(r'numberMatched="(\d+)"', resp.text)
            return int(m.group(1)) if m else -1
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                log.warning(f"  Hits query retry {attempt+1}: {e}")
                time.sleep(RETRY_DELAY * (attempt + 1))
            else:
                raise


def download_page(bbox, start_index, timeout=120):
    """Download a single page of BAG features."""
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeName": "bag:verblijfsobject",
        "outputFormat": "application/json",
        "count": PAGE_SIZE,
        "startIndex": start_index,
        "sortBy": "identificatie",
        "srsName": "EPSG:4326",
        "BBOX": bbox,
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(PDOK_BAG_WFS, params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            return data.get("features", [])
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAY * (attempt + 1)
                log.warning(f"  Page {start_index} retry {attempt+1}: {e} (wacht {wait}s)")
                time.sleep(wait)
            else:
                log.error(f"  Page {start_index} mislukt na {MAX_RETRIES} pogingen: {e}")
                return []


def clean_feature(feature):
    """Strip feature to only needed properties and round coordinates."""
    props = feature.get("properties", {})
    geom = feature.get("geometry")
    if not geom or not props.get("identificatie"):
        return None

    clean = {}
    for field in BAG_FIELDS:
        val = props.get(field)
        if val is not None:
            clean[field] = val

    # Round coordinates to 6 decimals (11cm precision)
    if geom.get("coordinates"):
        coords = geom["coordinates"]
        if isinstance(coords[0], (int, float)):
            coords = [round(c, 6) for c in coords]
        geom["coordinates"] = coords

    return {
        "type": "Feature",
        "properties": clean,
        "geometry": geom,
    }


def build_gemeente(gemeente_code, gemeente_naam, bbox, output_dir=None):
    """Build BAG GeoJSON for a single gemeente."""
    output_dir = output_dir or OUTPUT_DIR
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"bag_{gemeente_code}.geojson")

    log.info("=" * 50)
    log.info(f"  {gemeente_naam} ({gemeente_code})")
    log.info("=" * 50)

    # 1. Count
    log.info("Stap 1: Features tellen...")
    total = count_features(bbox)
    log.info(f"  {total} verblijfsobjecten verwacht")

    if total <= 0:
        log.warning(f"  Geen features gevonden voor {gemeente_naam}")
        return None

    # 2. Download all pages
    log.info("Stap 2: Downloaden...")
    all_features = []
    total_pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
    t0 = time.time()

    for page in range(total_pages):
        start_index = page * PAGE_SIZE
        pct = f" ({len(all_features)}/{total})" if total > 0 else ""
        log.info(f"  Page {page+1}/{total_pages}{pct}...")

        features = download_page(bbox, start_index)
        all_features.extend(features)

        if not features or len(features) < PAGE_SIZE:
            break

        # Rate limiting — kleine pauze om PDOK niet te overbelasten
        if page > 0 and page % 10 == 0:
            time.sleep(1)

    elapsed = time.time() - t0
    log.info(f"  {len(all_features)} features in {elapsed:.1f}s")

    # 3. Clean
    log.info("Stap 3: Opschonen...")
    cleaned = []
    for f in all_features:
        c = clean_feature(f)
        if c:
            cleaned.append(c)

    # Deduplicate by identificatie
    seen = set()
    unique = []
    for f in cleaned:
        fid = f["properties"].get("identificatie")
        if fid and fid not in seen:
            seen.add(fid)
            unique.append(f)

    log.info(f"  {len(unique)} unieke adressen (van {len(all_features)} raw)")

    # Statistieken
    gebruiksdoelen = {}
    statussen = {}
    for f in unique:
        gd = f["properties"].get("gebruiksdoel", "onbekend")
        st = f["properties"].get("status", "onbekend")
        gebruiksdoelen[gd] = gebruiksdoelen.get(gd, 0) + 1
        statussen[st] = statussen.get(st, 0) + 1

    # 4. Write
    log.info("Stap 4: Schrijven...")
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "gemeentecode": gemeente_code,
            "gemeentenaam": gemeente_naam,
            "count": len(unique),
            "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "source": "PDOK BAG WFS",
            "gebruiksdoelen": gebruiksdoelen,
            "statussen": statussen,
        },
        "features": unique,
    }

    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    log.info(f"  {output_path}: {size_mb:.1f} MB")
    log.info(f"  Geschat gzipped: ~{size_mb * 0.25:.1f} MB")
    log.info(f"  Gebruiksdoelen: {gebruiksdoelen}")
    log.info("  KLAAR!")
    return output_path


def find_gemeente(data, code=None, naam=None):
    """Find gemeente feature by code or name."""
    for f in data["features"]:
        p = f["properties"]
        if code and p.get("gemeentecode") == code:
            return f
        if naam and naam.lower() in (p.get("gemeentenaam", "")).lower():
            return f
    return None


def main():
    parser = argparse.ArgumentParser(description="Build BAG GeoJSON per gemeente")
    parser.add_argument("--gemeente", action="append", default=[],
                        help="Gemeentecode (bijv. 0363 voor Amsterdam). Kan meerdere keren gebruikt worden.")
    parser.add_argument("--naam", type=str, default=None,
                        help="Zoek gemeente op naam (bijv. 'Utrecht')")
    parser.add_argument("--all", action="store_true",
                        help="Bouw BAG voor alle gemeenten")
    parser.add_argument("--list", type=str, default=None,
                        help="Bestand met gemeentecodes (1 per regel)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output directory (default: ./bag/)")
    args = parser.parse_args()

    # Load gemeente data
    gem_data = load_gemeenten_geojson()

    targets = []

    if args.all:
        log.info("Alle gemeenten selecteren...")
        for f in gem_data["features"]:
            p = f["properties"]
            code = p.get("gemeentecode")
            naam = p.get("gemeentenaam")
            bbox = get_gemeente_bbox(f)
            if code and naam and bbox:
                targets.append((code, naam, bbox))
        log.info(f"  {len(targets)} gemeenten geselecteerd")

    elif args.list:
        with open(args.list, "r") as fh:
            codes = [line.strip() for line in fh if line.strip()]
        for code in codes:
            f = find_gemeente(gem_data, code=code)
            if f:
                p = f["properties"]
                bbox = get_gemeente_bbox(f)
                if bbox:
                    targets.append((p["gemeentecode"], p["gemeentenaam"], bbox))
            else:
                log.warning(f"Gemeente {code} niet gevonden")

    elif args.naam:
        f = find_gemeente(gem_data, naam=args.naam)
        if f:
            p = f["properties"]
            bbox = get_gemeente_bbox(f)
            if bbox:
                targets.append((p["gemeentecode"], p["gemeentenaam"], bbox))
        else:
            log.error(f"Gemeente '{args.naam}' niet gevonden")
            sys.exit(1)

    elif args.gemeente:
        for code in args.gemeente:
            f = find_gemeente(gem_data, code=code)
            if f:
                p = f["properties"]
                bbox = get_gemeente_bbox(f)
                if bbox:
                    targets.append((p["gemeentecode"], p["gemeentenaam"], bbox))
            else:
                log.warning(f"Gemeente {code} niet gevonden")

    else:
        parser.print_help()
        print("\nVoorbeelden:")
        print("  python build_bag.py --gemeente 0363              # Amsterdam")
        print("  python build_bag.py --naam Muiden                # Zoek op naam")
        print("  python build_bag.py --gemeente 0344 --gemeente 0363  # Meerdere")
        print("  python build_bag.py --all                        # Alle gemeenten")
        sys.exit(0)

    if not targets:
        log.error("Geen gemeenten geselecteerd")
        sys.exit(1)

    output_dir = args.output or OUTPUT_DIR

    log.info(f"{'='*50}")
    log.info(f"  BAG BUILD: {len(targets)} gemeenten")
    log.info(f"  Output: {output_dir}")
    log.info(f"{'='*50}")

    results = []
    for i, (code, naam, bbox) in enumerate(targets):
        log.info(f"\n[{i+1}/{len(targets)}] {naam} ({code})")
        try:
            path = build_gemeente(code, naam, bbox, output_dir)
            if path:
                results.append((code, naam, path))
        except Exception as e:
            log.error(f"  FOUT bij {naam}: {e}")

    # Samenvatting
    log.info("\n" + "=" * 50)
    log.info(f"  KLAAR: {len(results)}/{len(targets)} gemeenten gebouwd")
    log.info("=" * 50)
    total_size = 0
    for code, naam, path in results:
        size = os.path.getsize(path) / (1024 * 1024)
        total_size += size
        log.info(f"  {code} {naam}: {size:.1f} MB")
    log.info(f"  Totaal: {total_size:.1f} MB")

    # Schrijf index bestand voor de app
    index = {}
    for code, naam, path in results:
        with open(path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            meta = data.get("metadata", {})
            index[code] = {
                "naam": naam,
                "count": meta.get("count", 0),
                "size_mb": round(os.path.getsize(path) / (1024 * 1024), 1),
                "generated": meta.get("generated", ""),
            }

    index_path = os.path.join(output_dir, "bag_index.json")
    with open(index_path, "w", encoding="utf-8") as fh:
        json.dump(index, fh, ensure_ascii=False, indent=2)
    log.info(f"  Index: {index_path}")


if __name__ == "__main__":
    main()
