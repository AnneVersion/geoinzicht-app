"""
Enrich GeoJSON files with BAG address counts.
================================================
Telt het aantal BAG verblijfsobjecten per gemeente, buurt en wijk
via spatial join en voegt de telling toe aan de GeoJSON properties.

Indicators:
  - bag_adressen_totaal     : Totaal verblijfsobjecten
  - bag_adressen_woon       : Woonfunctie
  - bag_adressen_kantoor    : Kantoorfunctie
  - bag_adressen_winkel     : Winkelfunctie
  - bag_adressen_industrie  : Industriefunctie
  - bag_adressen_overig     : Overige functies
  - bag_gem_oppervlakte     : Gemiddelde oppervlakte (m²)
  - bag_med_bouwjaar        : Mediaan bouwjaar

Usage:
    python enrich_bag_counts.py                         # Alle niveaus
    python enrich_bag_counts.py --type gemeenten        # Alleen gemeenten
    python enrich_bag_counts.py --skip-spatial           # Alleen gemeenten (snel)

Dependencies:
    pip install geopandas shapely
"""

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("bag_counts")

APP_DIR = os.path.dirname(os.path.abspath(__file__))
BAG_DIR = os.path.join(APP_DIR, "bag")

# Gebruiksdoel mapping
FUNCTIE_MAP = {
    "woonfunctie": "woon",
    "kantoorfunctie": "kantoor",
    "winkelfunctie": "winkel",
    "industriefunctie": "industrie",
    "bijeenkomstfunctie": "overig",
    "gezondheidszorgfunctie": "overig",
    "logiesfunctie": "overig",
    "onderwijsfunctie": "overig",
    "sportfunctie": "overig",
    "overige gebruiksfunctie": "overig",
    "celfunctie": "overig",
}


def load_all_bag_points():
    """Load all BAG GeoJSON files and return as list of (lng, lat, functie, opp, bouwjaar)."""
    log.info("BAG bestanden laden uit %s...", BAG_DIR)
    points = []
    gem_counts = {}

    for fname in sorted(os.listdir(BAG_DIR)):
        if not fname.startswith("bag_GM") or not fname.endswith(".geojson"):
            continue
        code = fname.replace("bag_", "").replace(".geojson", "")
        filepath = os.path.join(BAG_DIR, fname)

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            features = data.get("features", [])
            gem_counts[code] = {"totaal": 0, "woon": 0, "kantoor": 0, "winkel": 0,
                                "industrie": 0, "overig": 0, "opp_sum": 0, "opp_n": 0,
                                "bouwjaren": []}

            for feat in features:
                geom = feat.get("geometry")
                props = feat.get("properties", {})
                if not geom or geom.get("type") != "Point":
                    continue

                lng, lat = geom["coordinates"][0], geom["coordinates"][1]
                functie = str(props.get("gebruiksdoel", "")).lower().strip()
                opp = props.get("oppervlakte")
                bouwjaar = props.get("bouwjaar")

                cat = FUNCTIE_MAP.get(functie, "overig")
                gem_counts[code]["totaal"] += 1
                gem_counts[code][cat] += 1

                if opp and isinstance(opp, (int, float)) and opp > 0:
                    gem_counts[code]["opp_sum"] += opp
                    gem_counts[code]["opp_n"] += 1
                if bouwjaar and isinstance(bouwjaar, (int, float)) and 1400 < bouwjaar < 2030:
                    gem_counts[code]["bouwjaren"].append(int(bouwjaar))

                points.append((lng, lat, cat, opp, bouwjaar, code))

        except Exception as e:
            log.warning("  Fout bij %s: %s", fname, e)

    log.info("  %d BAG punten geladen uit %d gemeenten", len(points), len(gem_counts))
    return points, gem_counts


def count_per_gemeente(gem_counts):
    """Build gemeente-level counts dict."""
    result = {}
    for code, counts in gem_counts.items():
        avg_opp = round(counts["opp_sum"] / counts["opp_n"]) if counts["opp_n"] > 0 else None
        bouwjaren = sorted(counts["bouwjaren"])
        med_bouwjaar = bouwjaren[len(bouwjaren) // 2] if bouwjaren else None

        result[code] = {
            "bag_adressen_totaal": counts["totaal"],
            "bag_adressen_woon": counts["woon"],
            "bag_adressen_kantoor": counts["kantoor"],
            "bag_adressen_winkel": counts["winkel"],
            "bag_adressen_industrie": counts["industrie"],
            "bag_adressen_overig": counts["overig"],
            "bag_gem_oppervlakte": avg_opp,
            "bag_med_bouwjaar": med_bouwjaar,
        }
    return result


def spatial_join_counts(points, polygon_file, code_field):
    """Count BAG points per polygon area using geopandas spatial join."""
    log.info("Spatial join met %s (veld: %s)...", polygon_file, code_field)

    # Load polygons
    with open(polygon_file, "r", encoding="utf-8") as f:
        poly_data = json.load(f)

    polys = gpd.GeoDataFrame.from_features(poly_data["features"], crs="EPSG:4326")
    if code_field not in polys.columns:
        log.error("  Veld '%s' niet gevonden in %s", code_field, polygon_file)
        return {}

    log.info("  %d polygonen geladen", len(polys))

    # Build points GeoDataFrame (efficient: only coords + cat)
    log.info("  GeoDataFrame bouwen voor %d punten...", len(points))
    pts_df = pd.DataFrame(points, columns=["lng", "lat", "cat", "opp", "bouwjaar", "gem_code"])
    geometry = [Point(row.lng, row.lat) for row in pts_df.itertuples()]
    pts_gdf = gpd.GeoDataFrame(pts_df, geometry=geometry, crs="EPSG:4326")

    # Spatial join
    log.info("  Spatial join uitvoeren...")
    joined = gpd.sjoin(pts_gdf, polys[[code_field, "geometry"]], how="inner", predicate="within")
    log.info("  %d punten gematcht", len(joined))

    # Aggregate per area code
    result = {}
    for area_code, group in joined.groupby(code_field):
        area_code = str(area_code).strip()
        if not area_code:
            continue

        totaal = len(group)
        woon = int((group["cat"] == "woon").sum())
        kantoor = int((group["cat"] == "kantoor").sum())
        winkel = int((group["cat"] == "winkel").sum())
        industrie = int((group["cat"] == "industrie").sum())
        overig = totaal - woon - kantoor - winkel - industrie

        valid_opp = group["opp"].dropna()
        valid_opp = valid_opp[valid_opp > 0]
        avg_opp = round(valid_opp.mean()) if len(valid_opp) > 0 else None

        valid_bj = group["bouwjaar"].dropna()
        valid_bj = valid_bj[(valid_bj > 1400) & (valid_bj < 2030)]
        med_bj = int(valid_bj.median()) if len(valid_bj) > 0 else None

        result[area_code] = {
            "bag_adressen_totaal": totaal,
            "bag_adressen_woon": woon,
            "bag_adressen_kantoor": kantoor,
            "bag_adressen_winkel": winkel,
            "bag_adressen_industrie": industrie,
            "bag_adressen_overig": overig,
            "bag_gem_oppervlakte": avg_opp,
            "bag_med_bouwjaar": med_bj,
        }

    log.info("  %d gebieden met BAG data", len(result))
    return result


def enrich_geojson(filename, counts, code_field):
    """Add BAG counts to GeoJSON properties."""
    log.info("Verrijken: %s...", filename)

    with open(filename, "r", encoding="utf-8") as f:
        geojson = json.load(f)

    features = geojson.get("features", [])
    enriched = 0
    indicators_added = set()

    for feat in features:
        props = feat.get("properties", {})
        code = props.get(code_field, "")
        if not code:
            continue

        bag_data = counts.get(code)
        if bag_data:
            for key, val in bag_data.items():
                if val is not None and val > 0:
                    props[key] = val
                    indicators_added.add(key)
            enriched += 1

    log.info("  %d/%d features verrijkt", enriched, len(features))

    # Update metadata
    meta = geojson.get("metadata", {})
    existing = set(meta.get("indicators", []))
    meta["indicators"] = sorted(existing | indicators_added)
    meta["indicators_count"] = len(meta["indicators"])
    meta["enriched_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    meta["bag_source"] = "PDOK BAG WFS"
    geojson["metadata"] = meta

    # Save
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(filename) / (1024 * 1024)
    log.info("  %.1f MB - %d indicatoren toegevoegd", size_mb, len(indicators_added))
    return enriched > 0


def find_geojson_files(feat_type=None, year=None):
    """Find GeoJSON files to enrich."""
    files = []
    for f in os.listdir(APP_DIR):
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
            files.append(os.path.join(APP_DIR, f))
    return sorted(files)


CODE_FIELD = {
    "gemeenten": "gemeentecode",
    "buurten": "buurtcode",
    "wijken": "wijkcode",
}


def main():
    parser = argparse.ArgumentParser(
        description="Verrijk GeoJSON met BAG adres-aantallen per gebied"
    )
    parser.add_argument("--type", choices=["gemeenten", "buurten", "wijken"],
                        help="Feature type (default: alle)")
    parser.add_argument("--year", type=int, help="CBS jaar (default: alle)")
    parser.add_argument("--skip-spatial", action="store_true",
                        help="Alleen gemeenten tellen (geen spatial join)")
    args = parser.parse_args()

    os.chdir(APP_DIR)

    # Load all BAG points
    points, gem_counts = load_all_bag_points()
    if not points:
        log.error("Geen BAG data gevonden! Draai eerst download_bag_bulk.py")
        return 1

    # Gemeente-level counts (no spatial join needed)
    gem_level_counts = count_per_gemeente(gem_counts)
    log.info("Gemeente-level: %d gemeenten met BAG data", len(gem_level_counts))

    # Find files to enrich
    types_to_process = [args.type] if args.type else ["gemeenten", "buurten", "wijken"]
    if args.skip_spatial:
        types_to_process = ["gemeenten"]

    success = 0
    for feat_type in types_to_process:
        files = find_geojson_files(feat_type, args.year)
        if not files:
            log.warning("Geen %s GeoJSON bestanden gevonden", feat_type)
            continue

        code_field = CODE_FIELD[feat_type]

        for filename in files:
            try:
                if feat_type == "gemeenten":
                    # Gemeenten: direct matchen op code
                    if enrich_geojson(filename, gem_level_counts, code_field):
                        success += 1
                else:
                    # Buurten/Wijken: spatial join
                    area_counts = spatial_join_counts(points, filename, code_field)
                    if area_counts and enrich_geojson(filename, area_counts, code_field):
                        success += 1
            except Exception as e:
                log.error("FOUT bij %s: %s", filename, e)
                import traceback
                traceback.print_exc()

    log.info("\n" + "=" * 55)
    log.info("  KLAAR: %d bestanden verrijkt met BAG tellingen", success)
    log.info("=" * 55)
    return 0 if success > 0 else 1


if __name__ == "__main__":
    sys.exit(main())
