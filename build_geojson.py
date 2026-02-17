"""
Build static GeoJSON for GeoInzicht webapp.
==============================================
Downloads buurt/wijk geometrie + CBS kerncijfers from PDOK WFS
(which already includes CBS statistics as attributes),
selects only the fields the app needs, and writes a compact GeoJSON file.

Usage:
    python build_geojson.py --type gemeenten --year 2022
    python build_geojson.py --type buurten --year 2024
    python build_geojson.py --type gemeenten --all-years

Beschikbare jaren op PDOK: 2012, 2017, 2018, 2019, 2022, 2023, 2024
Let op: laagnamen veranderen per era:
  - 2012-2019: cbs_buurten_YYYY, cbs_wijken_YYYY, gemeentenYYYY
  - 2022-2024: buurten, wijken, gemeenten

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

AVAILABLE_YEARS = [2012, 2017, 2018, 2019, 2022, 2023, 2024]

# Mapping: app field key -> list of possible PDOK property names (ordered by preference)
# Sommige velden veranderen van naam tussen jaren
PDOK_FIELDS = {
    "aantal_inwoners":        ["aantalInwoners"],
    "mannen":                 ["mannen"],
    "vrouwen":                ["vrouwen"],
    "0_15":                   ["percentagePersonen0Tot15Jaar"],
    "15_25":                  ["percentagePersonen15Tot25Jaar"],
    "25_45":                  ["percentagePersonen25Tot45Jaar"],
    "45_65":                  ["percentagePersonen45Tot65Jaar"],
    "65plus":                 ["percentagePersonen65JaarEnOuder"],
    "huishoudens":            ["aantalHuishoudens"],
    "gem_huishouden_grootte": ["gemiddeldeHuishoudsgrootte"],
    "bevolkingsdichtheid":    ["bevolkingsdichtheidInwonersPerKm2"],
    "woningvoorraad":         ["woningvoorraad"],
    "gem_woz_waarde":         ["gemiddeldeWoningwaarde"],
    "koopwoningen":           ["percentageKoopwoningen"],
    "huurwoningen":           ["percentageHuurwoningen"],
    "pct_eengezins":          ["percentageEengezinswoning"],
    "gem_aardgas":            ["gemiddeldGasverbruikTotaal"],
    "gem_elektriciteit":      ["gemiddeldElektriciteitsverbruikTotaal"],
    "gem_inkomen_inwoner":    ["gemiddeldInkomenPerInwoner", "gemiddeldInkomenPerInkomensontvanger"],
    "arbeidsparticipatie":    ["nettoArbeidsparticipatie"],
    "bedrijfsvestigingen":    ["aantalBedrijfsvestigingen"],
    "afstand_huisarts":       ["huisartsenpraktijkGemiddeldeAfstandInKm"],
    "afstand_supermarkt":     ["groteSupermarktGemiddeldeAfstandInKm"],
    "personenautos":          ["personenautosTotaal"],
}

# Admin-velden per type
ADMIN_FIELDS = {
    "buurten":   ["buurtcode", "buurtnaam", "gemeentecode", "gemeentenaam"],
    "wijken":    ["wijkcode", "wijknaam", "gemeentecode", "gemeentenaam"],
    "gemeenten": ["gemeentecode", "gemeentenaam"],
}

# ID-veld per type
ID_FIELD = {"buurten": "buurtcode", "wijken": "wijkcode", "gemeenten": "gemeentecode"}

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


def get_layer_name(feat_type, year):
    """Return the PDOK WFS layer name for a given type and year."""
    if year >= 2022:
        return feat_type  # 'buurten', 'wijken', 'gemeenten'
    else:
        # 2012-2019: cbs_buurten_YYYY, cbs_wijken_YYYY, gemeentenYYYY
        if feat_type == "gemeenten":
            return f"gemeenten{year}"
        else:
            return f"cbs_{feat_type}_{year}"


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
    for app_key, pdok_names in PDOK_FIELDS.items():
        for pdok_name in pdok_names:
            if pdok_name in available:
                resolved[app_key] = pdok_name
                break
            elif pdok_name.lower() in avail_lower:
                resolved[app_key] = avail_lower[pdok_name.lower()]
                break
        else:
            # Fuzzy match: zoek of het ergens in zit
            for pdok_name in pdok_names:
                for af in available:
                    if pdok_name.lower() in af.lower():
                        resolved[app_key] = af
                        break
                if app_key in resolved:
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


def build_year(feat_type, year, tolerance, output=None):
    """Build GeoJSON for a specific type and year."""
    output = output or f"{feat_type}_{year}.geojson"
    base_url = PDOK_WFS.format(year=year)
    type_name = get_layer_name(feat_type, year)
    admin_fields = ADMIN_FIELDS[feat_type]
    id_field = ID_FIELD[feat_type]

    log.info("=" * 50)
    log.info(f"  {feat_type.upper()} {year}")
    log.info(f"  Laag: {type_name}  Tolerantie: {tolerance}")
    log.info("=" * 50)

    # 1. Discover fields
    log.info(f"Stap 1: PDOK velden ontdekken ({type_name})...")
    try:
        available = discover_fields(base_url, type_name)
    except Exception as e:
        log.error(f"  FOUT: {e}")
        log.error(f"  Jaar {year} bestaat mogelijk niet voor {feat_type}")
        return None
    log.info(f"  {len(available)} velden beschikbaar")

    # 2. Resolve mapping
    field_map = resolve_fields(available)
    found_count = len(field_map)
    total_count = len(PDOK_FIELDS)
    log.info(f"Stap 2: {found_count}/{total_count} indicatoren gevonden")
    missing = set(PDOK_FIELDS.keys()) - set(field_map.keys())
    if missing:
        log.warning(f"  Ontbrekend: {sorted(missing)}")

    # 3. Count and Download
    log.info("Stap 3: Features tellen...")
    total = count_total(base_url, type_name)
    log.info(f"  {total} {feat_type} verwacht")

    request_props = list(set(admin_fields + list(field_map.values())))
    log.info(f"  Downloaden ({len(request_props)} properties)...")
    t0 = time.time()
    raw = download_features(base_url, type_name, request_props, total)
    log.info(f"  {len(raw)} features in {time.time()-t0:.1f}s")

    # 4. Clean, simplify and compact
    log.info(f"Stap 4: Opschonen (tolerantie={tolerance})...")
    features = []
    indicators_with_data = set()

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
                indicators_with_data.add(app_key)

        geom = simplify_geometry(geom, tolerance)

        features.append({
            "type": "Feature",
            "properties": clean,
            "geometry": geom,
        })

        if (i + 1) % 3000 == 0:
            log.info(f"  {i+1}/{len(raw)} verwerkt...")

    # Count features with actual CBS data (more than just admin fields)
    admin_count = len(admin_fields)
    with_data = sum(1 for f in features if len(f["properties"]) > admin_count)
    log.info(f"  {len(features)} {feat_type} opgeschoond, {with_data} met CBS data")
    log.info(f"  Indicatoren met data: {sorted(indicators_with_data)}")

    # 5. Write
    log.info("Stap 5: Schrijven...")
    geojson = {
        "type": "FeatureCollection",
        "metadata": {
            "type": feat_type,
            "year": year,
            "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "count": len(features),
            "with_data": with_data,
            "indicators": sorted(indicators_with_data),
            "indicators_count": len(indicators_with_data),
        },
        "features": features,
    }

    with open(output, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(output) / (1024 * 1024)
    log.info(f"  {output}: {size_mb:.1f} MB")
    log.info(f"  Geschat gzipped: ~{size_mb * 0.25:.1f} MB")
    log.info("  KLAAR!")
    return output


def main():
    parser = argparse.ArgumentParser(description="Build static GeoJSON for GeoInzicht")
    parser.add_argument("--type", choices=["buurten", "wijken", "gemeenten"], default="gemeenten",
                        help="Type: buurten, wijken of gemeenten (default: gemeenten)")
    parser.add_argument("--year", type=int, default=2022)
    parser.add_argument("--all-years", action="store_true",
                        help="Bouw voor alle beschikbare jaren: " + ", ".join(str(y) for y in AVAILABLE_YEARS))
    parser.add_argument("--tolerance", type=float, default=None,
                        help="Simplificatie-tolerantie in graden.")
    parser.add_argument("--output", type=str, default=None)
    args = parser.parse_args()

    # Defaults per type
    if args.tolerance is None:
        args.tolerance = {"buurten": 0.0003, "wijken": 0.0005, "gemeenten": 0.001}.get(args.type, 0.0003)

    if args.all_years:
        log.info(f"Alle jaren bouwen voor {args.type}: {AVAILABLE_YEARS}")
        for year in AVAILABLE_YEARS:
            try:
                build_year(args.type, year, args.tolerance)
            except Exception as e:
                log.error(f"Fout bij {args.type} {year}: {e}")
        log.info("=" * 50)
        log.info("  ALLE JAREN KLAAR!")
        log.info("=" * 50)
    else:
        build_year(args.type, args.year, args.tolerance, args.output)


if __name__ == "__main__":
    main()
