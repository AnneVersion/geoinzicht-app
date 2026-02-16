"""
Build static GeoJSON for GeoInzicht webapp.
==============================================
Downloads buurt/wijk geometrie + CBS kerncijfers from PDOK WFS
(which already includes CBS statistics as attributes),
selects only the fields the app needs, and writes a compact GeoJSON file.

Usage:
    python build_geojson.py --type buurten --year 2024
    python build_geojson.py --type wijken --year 2024 --tolerance 0.0005

Data verversen:
    python build_geojson.py --type buurten --year 2024
    python build_geojson.py --type wijken  --year 2024 --tolerance 0.0005
    git add *.geojson && git commit -m "Data update" && git push

Dependencies:
    pip install requests shapely
"""

import argparse
import json
import logging
import os
import re
import sys
import time

import requests
from shapely.geometry import shape, mapping
from shapely.validation import make_valid

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PDOK_WFS = "https://service.pdok.nl/cbs/wijkenbuurten/{year}/wfs/v1_0"
PAGE_SIZE = 1000  # PDOK max per request

# Mapping: app field key -> PDOK property name (camelCase in PDOK)
# Dezelfde indicatoren zijn beschikbaar voor buurten EN wijken
PDOK_FIELDS = {
    "aantal_inwoners":        "aantalInwoners",
    "mannen":                 "mannen",
    "vrouwen":                "vrouwen",
    "0_15":                   "percentagePersonen0Tot15Jaar",
    "15_25":                  "percentagePersonen15Tot25Jaar",
    "25_45":                  "percentagePersonen25Tot45Jaar",
    "45_65":                  "percentagePersonen45Tot65Jaar",
    "65plus":                 "percentagePersonen65JaarEnOuder",
    "huishoudens":            "aantalHuishoudens",
    "gem_huishouden_grootte": "gemiddeldeHuishoudsgrootte",
    "bevolkingsdichtheid":    "bevolkingsdichtheidInwonersPerKm2",
    "woningvoorraad":         "woningvoorraad",
    "gem_woz_waarde":         "gemiddeldeWoningwaarde",
    "koopwoningen":           "percentageKoopwoningen",
    "huurwoningen":           "percentageHuurwoningen",
    "pct_eengezins":          "percentageEengezinswoning",
    "gem_aardgas":            "gemiddeldGasverbruikTotaal",
    "gem_elektriciteit":      "gemiddeldElektriciteitsverbruikTotaal",
    "gem_inkomen_inwoner":    "gemiddeldInkomenPerInwoner",
    "arbeidsparticipatie":    "nettoArbeidsparticipatie",
    "bedrijfsvestigingen":    "aantalBedrijfsvestigingen",
    "afstand_huisarts":       "huisartsenpraktijkGemiddeldeAfstandInKm",
    "afstand_supermarkt":     "groteSupermarktGemiddeldeAfstandInKm",
    "personenautos":          "personenautosTotaal",
}

# Admin-velden per type
ADMIN_FIELDS = {
    "buurten": ["buurtcode", "buurtnaam", "gemeentecode", "gemeentenaam"],
    "wijken":  ["wijkcode", "wijknaam", "gemeentecode", "gemeentenaam"],
}

# ID-veld per type (voor skip-check)
ID_FIELD = {"buurten": "buurtcode", "wijken": "wijkcode"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("build")


def discover_fields(base_url, type_name):
    """Fetch 1 feature to discover available property names."""
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeNames": type_name, "outputFormat": "application/json",
        "count": 1, "srsName": "EPSG:4326",
    }
    resp = requests.get(base_url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if data.get("features"):
        return list(data["features"][0]["properties"].keys())
    return []


def resolve_fields(available):
    """Match app field keys to actual PDOK property names."""
    avail_lower = {f.lower(): f for f in available}
    resolved = {}
    for app_key, pdok_name in PDOK_FIELDS.items():
        if pdok_name in available:
            resolved[app_key] = pdok_name
        elif pdok_name.lower() in avail_lower:
            resolved[app_key] = avail_lower[pdok_name.lower()]
        else:
            for af in available:
                if pdok_name.lower() in af.lower():
                    resolved[app_key] = af
                    break
    return resolved


def count_total(base_url, type_name):
    """Get total number of features via resultType=hits."""
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeNames": type_name, "resultType": "hits",
    }
    resp = requests.get(base_url, params=params, timeout=30)
    resp.raise_for_status()
    m = re.search(r'numberMatched="(\d+)"', resp.text)
    return int(m.group(1)) if m else -1


def download_features(base_url, type_name, prop_names, expected_total=-1):
    """Download all features from PDOK WFS with pagination."""
    all_features = []
    start = 0
    prop_str = ",".join(prop_names + ["geom"])

    while True:
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": type_name, "outputFormat": "application/json",
            "srsName": "EPSG:4326", "count": PAGE_SIZE, "startIndex": start,
            "propertyName": prop_str,
        }
        pct = f" ({len(all_features)}/{expected_total})" if expected_total > 0 else f" ({len(all_features)})"
        log.info(f"  PDOK page startIndex={start}{pct}...")
        resp = requests.get(base_url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        all_features.extend(features)
        if len(features) == 0:
            break
        nm = data.get("numberMatched", expected_total)
        if nm > 0 and len(all_features) >= nm:
            break
        if len(features) < PAGE_SIZE:
            break
        start += len(features)

    return all_features


def clean_value(val):
    """Convert CBS/PDOK missing-data markers to None."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        if val <= -99990:
            return None
    return val


def round_coords(coords, precision=5):
    """Recursively round coordinates to save space."""
    if isinstance(coords, (list, tuple)):
        if coords and isinstance(coords[0], (int, float)):
            return [round(c, precision) for c in coords]
        return [round_coords(c, precision) for c in coords]
    return coords


def simplify_geometry(geom_dict, tolerance):
    """Simplify GeoJSON geometry using Shapely."""
    try:
        geom = shape(geom_dict)
        if not geom.is_valid:
            geom = make_valid(geom)
        simplified = geom.simplify(tolerance, preserve_topology=True)
        if simplified.is_empty:
            return geom_dict
        result = mapping(simplified)
        result["coordinates"] = round_coords(result["coordinates"], 5)
        return result
    except Exception as e:
        log.warning(f"Simplificatie mislukt: {e}")
        geom_dict["coordinates"] = round_coords(geom_dict["coordinates"], 5)
        return geom_dict


def main():
    parser = argparse.ArgumentParser(description="Build static GeoJSON for GeoInzicht")
    parser.add_argument("--type", choices=["buurten", "wijken"], default="buurten",
                        help="Type: buurten of wijken (default: buurten)")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--tolerance", type=float, default=None,
                        help="Simplificatie-tolerantie in graden. "
                             "Default: 0.0003 voor buurten, 0.0005 voor wijken")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    # Defaults per type
    if args.tolerance is None:
        args.tolerance = 0.0003 if args.type == "buurten" else 0.0005
    output = args.output or f"{args.type}_{args.year}.geojson"
    base_url = PDOK_WFS.format(year=args.year)
    type_name = args.type  # 'buurten' of 'wijken' — exact de PDOK laagnaam
    admin_fields = ADMIN_FIELDS[args.type]
    id_field = ID_FIELD[args.type]

    log.info("=" * 50)
    log.info("  GeoInzicht GeoJSON Builder")
    log.info(f"  Type: {args.type}  Jaar: {args.year}")
    log.info(f"  Tolerantie: {args.tolerance}  Output: {output}")
    log.info("=" * 50)

    # 1. Discover fields
    log.info(f"Stap 1: PDOK velden ontdekken ({type_name})...")
    available = discover_fields(base_url, type_name)
    log.info(f"  {len(available)} velden beschikbaar")

    # 2. Resolve mapping
    field_map = resolve_fields(available)
    log.info(f"Stap 2: {len(field_map)}/{len(PDOK_FIELDS)} velden gevonden")
    missing = set(PDOK_FIELDS.keys()) - set(field_map.keys())
    if missing:
        log.warning(f"  Ontbrekend: {missing}")

    # 3. Count and Download
    log.info("Stap 3: Features tellen...")
    total = count_total(base_url, type_name)
    log.info(f"  {total} {type_name} verwacht")

    request_props = list(set(admin_fields + list(field_map.values())))
    log.info(f"  Downloaden ({len(request_props)} properties)...")
    t0 = time.time()
    raw = download_features(base_url, type_name, request_props, total)
    log.info(f"  {len(raw)} features in {time.time()-t0:.1f}s")

    # 4. Clean, simplify and compact
    log.info(f"Stap 4: Opschonen (tolerantie={args.tolerance})...")
    features = []
    for i, f in enumerate(raw):
        props = f.get("properties", {})
        geom = f.get("geometry")
        if not props.get(id_field) or not geom:
            continue

        clean = {}
        for af in admin_fields:
            if props.get(af) is not None:
                clean[af] = props[af]

        for app_key, pdok_field in field_map.items():
            v = clean_value(props.get(pdok_field))
            if v is not None:
                if isinstance(v, float):
                    v = round(v, 2)
                clean[app_key] = v

        geom = simplify_geometry(geom, args.tolerance)

        features.append({
            "type": "Feature",
            "properties": clean,
            "geometry": geom,
        })

        if (i + 1) % 3000 == 0:
            log.info(f"  {i+1}/{len(raw)} verwerkt...")

    log.info(f"  {len(features)} {type_name} opgeschoond")

    # 5. Write
    log.info("Stap 5: Schrijven...")
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "type": args.type,
            "year": args.year,
            "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "count": len(features),
        },
        "features": features,
    }

    with open(output, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(output) / (1024 * 1024)
    log.info(f"  {output}: {size_mb:.1f} MB")
    log.info(f"  Geschat gzipped: ~{size_mb * 0.25:.1f} MB")
    log.info("=" * 50)
    log.info("  KLAAR!")
    log.info("=" * 50)


if __name__ == "__main__":
    main()
