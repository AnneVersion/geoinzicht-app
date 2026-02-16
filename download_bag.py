"""
Download BAG data (panden + verblijfsobjecten) van PDOK WFS.
=============================================================
Downloadt gebouwen en adressen per gemeente of voor heel Nederland.
Output: GeoJSON bestanden klaar voor analyse of import.

PDOK WFS limiet: max 1.000 features per request → script paginaert automatisch.

Gebruik:
    # Eén gemeente (snel, ~seconden)
    python download_bag.py --gemeente Amsterdam --type panden
    python download_bag.py --gemeente Amsterdam --type verblijfsobjecten
    python download_bag.py --gemeente Amsterdam --type beide

    # Meerdere gemeenten
    python download_bag.py --gemeente "Amsterdam,Rotterdam,Utrecht" --type beide

    # Per postcode
    python download_bag.py --postcode 1012 --type panden

    # Bounding box (xmin,ymin,xmax,ymax in WGS84)
    python download_bag.py --bbox 4.85,52.35,4.95,52.40 --type panden

    # Hele Nederland (LET OP: ~10 miljoen panden, duurt uren!)
    python download_bag.py --type panden --heel-nederland

Alternatief voor heel Nederland:
    Download de BAG GeoPackage (~7.2 GB):
    https://service.pdok.nl/kadaster/bag/atom/downloads/bag-light.gpkg

Dependencies:
    pip install requests
"""

import argparse
import json
import logging
import os
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PDOK_BAG_WFS = "https://service.pdok.nl/lv/bag/wfs/v2_0"
PAGE_SIZE = 1000  # PDOK hard limit

# Velden per type
PAND_FIELDS = [
    "identificatie", "bouwjaar", "status", "gebruiksdoel",
    "oppervlakte_min", "oppervlakte_max", "aantal_verblijfsobjecten", "geom"
]

VBO_FIELDS = [
    "identificatie", "oppervlakte", "status", "gebruiksdoel",
    "openbare_ruimte", "huisnummer", "huisletter", "toevoeging",
    "postcode", "woonplaats", "bouwjaar", "geom"
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("bag")


def count_features(type_name, cql_filter=None):
    """Tel features. NB: PDOK BAG WFS negeert CQL_FILTER bij resultType=hits,
    dus de count is alleen betrouwbaar zonder CQL filter.
    Met CQL filter retourneren we -1 (onbekend)."""
    import re
    if cql_filter:
        return -1  # Count niet betrouwbaar met CQL op BAG WFS
    params = {
        "service": "WFS", "version": "2.0.0", "request": "GetFeature",
        "typeNames": type_name, "resultType": "hits",
    }
    try:
        resp = requests.get(PDOK_BAG_WFS, params=params, timeout=30)
        resp.raise_for_status()
        m = re.search(r'numberMatched="(\d+)"', resp.text)
        return int(m.group(1)) if m else -1
    except Exception:
        return -1


def download_features(type_name, fields, cql_filter=None, bbox=None, max_features=None):
    """Download alle features met paginatie."""
    all_features = []
    start = 0
    prop_str = ",".join(fields)

    # PDOK BAG WFS: BBOX parameter werkt niet, maar CQL_FILTER BBOX() wel
    if bbox and cql_filter:
        bbox_cql = f"BBOX(geom,{bbox},'EPSG:4326')"
        cql_filter = f"{bbox_cql} AND {cql_filter}"
    elif bbox:
        cql_filter = f"BBOX(geom,{bbox},'EPSG:4326')"

    while True:
        params = {
            "service": "WFS", "version": "2.0.0", "request": "GetFeature",
            "typeNames": type_name, "outputFormat": "application/json",
            "srsName": "EPSG:4326", "count": PAGE_SIZE, "startIndex": start,
            "sortBy": "identificatie",
            "propertyName": prop_str,
        }
        if cql_filter:
            params["CQL_FILTER"] = cql_filter

        log.info(f"  Page startIndex={start} ({len(all_features)} features tot nu toe)...")

        try:
            resp = requests.get(PDOK_BAG_WFS, params=params, timeout=120)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.RequestException as e:
            log.error(f"  Request fout: {e}")
            if start > 0:
                log.info("  Opnieuw proberen na 5 seconden...")
                time.sleep(5)
                continue
            raise

        features = data.get("features", [])
        all_features.extend(features)

        if len(features) == 0:
            break
        if max_features and len(all_features) >= max_features:
            all_features = all_features[:max_features]
            break
        if len(features) < PAGE_SIZE:
            break

        start += len(features)

        # Rate limiting: 0.5s pauze tussen requests
        time.sleep(0.5)

    return all_features


def build_cql_filter(gemeente=None, postcode=None, woonplaats=None, status_filter=True):
    """Bouw een CQL filter string."""
    parts = []

    if gemeente:
        # Verblijfsobjecten hebben 'woonplaats', panden niet direct
        # We gebruiken woonplaats als proxy voor gemeente bij VBO
        parts.append(f"woonplaats='{gemeente}'")

    if postcode:
        # Postcode filter (eerste 4 cijfers)
        if len(str(postcode)) == 4:
            parts.append(f"postcode LIKE '{postcode}%'")
        else:
            parts.append(f"postcode='{postcode}'")

    if woonplaats:
        parts.append(f"woonplaats='{woonplaats}'")

    if status_filter:
        parts.append("status='Verblijfsobject in gebruik'")

    return " AND ".join(parts) if parts else None


def build_pand_cql(gemeente=None, bbox_filter=None, status_filter=True):
    """CQL filter voor panden (andere velden dan VBO)."""
    parts = []
    if status_filter:
        parts.append("status='Pand in gebruik'")
    # Panden hebben geen gemeente/woonplaats veld in WFS
    # Daarom gebruiken we bbox of geen filter
    return " AND ".join(parts) if parts else None


def get_gemeente_bbox(gemeente_naam):
    """Haal bounding box op voor een gemeente via PDOK Locatieserver."""
    import re

    # PDOK Locatieserver centroide + ruime buffer
    try:
        url = f"https://api.pdok.nl/bzk/locatieserver/search/v3_1/free?q={gemeente_naam}&fq=type:gemeente&rows=1"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("response", {}).get("docs"):
            doc = data["response"]["docs"][0]
            if "centroide_ll" in doc:
                m = re.search(r'POINT\(([^ ]+) ([^ ]+)\)', doc["centroide_ll"])
                if m:
                    lng, lat = float(m.group(1)), float(m.group(2))
                    # Buffer afhankelijk van stad (grote steden groter)
                    buf = 0.15  # ~15km — ruim genoeg voor de meeste gemeenten
                    return f"{lng-buf},{lat-buf},{lng+buf},{lat+buf}"
    except Exception:
        pass

    # Fallback: probeer als woonplaats
    try:
        url = f"https://api.pdok.nl/bzk/locatieserver/search/v3_1/free?q={gemeente_naam}&fq=type:woonplaats&rows=1"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("response", {}).get("docs"):
            doc = data["response"]["docs"][0]
            if "centroide_ll" in doc:
                m = re.search(r'POINT\(([^ ]+) ([^ ]+)\)', doc["centroide_ll"])
                if m:
                    lng, lat = float(m.group(1)), float(m.group(2))
                    buf = 0.12
                    return f"{lng-buf},{lat-buf},{lng+buf},{lat+buf}"
    except Exception:
        pass
    return None


def save_geojson(features, output_path, type_name, metadata=None):
    """Schrijf features als GeoJSON bestand."""
    geojson = {
        "type": "FeatureCollection",
        "metadata": metadata or {},
        "features": features,
    }
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(geojson, fh, ensure_ascii=False, separators=(",", ":"))

    size_mb = os.path.getsize(output_path) / (1024 * 1024)
    return size_mb


def main():
    parser = argparse.ArgumentParser(
        description="Download BAG data van PDOK WFS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Voorbeelden:
  python download_bag.py --gemeente Amsterdam --type panden
  python download_bag.py --gemeente "Den Haag" --type verblijfsobjecten
  python download_bag.py --gemeente Amsterdam --type beide
  python download_bag.py --postcode 1012 --type verblijfsobjecten
  python download_bag.py --bbox 4.85,52.35,4.95,52.40 --type panden
  python download_bag.py --woonplaats Haarlem --type beide

Voor heel Nederland: download de BAG GeoPackage (~7.2 GB):
  https://service.pdok.nl/kadaster/bag/atom/downloads/bag-light.gpkg
        """
    )
    parser.add_argument("--type", choices=["panden", "verblijfsobjecten", "beide"],
                        default="panden", help="Type BAG objecten (default: panden)")
    parser.add_argument("--gemeente", type=str, default=None,
                        help="Gemeente naam (komma-gescheiden voor meerdere)")
    parser.add_argument("--woonplaats", type=str, default=None,
                        help="Woonplaats naam")
    parser.add_argument("--postcode", type=str, default=None,
                        help="Postcode (4 cijfers of volledig)")
    parser.add_argument("--bbox", type=str, default=None,
                        help="Bounding box: minx,miny,maxx,maxy (WGS84)")
    parser.add_argument("--max", type=int, default=None,
                        help="Maximum aantal features (voor testing)")
    parser.add_argument("--output-dir", type=str, default=".",
                        help="Output directory (default: huidige map)")
    parser.add_argument("--heel-nederland", action="store_true",
                        help="Download alles (WAARSCHUWING: miljoenen features!)")
    args = parser.parse_args()

    # Validatie
    if not args.gemeente and not args.woonplaats and not args.postcode and not args.bbox and not args.heel_nederland:
        parser.error("Geef --gemeente, --woonplaats, --postcode, --bbox of --heel-nederland op")

    os.makedirs(args.output_dir, exist_ok=True)

    log.info("=" * 60)
    log.info("  BAG Data Downloader (PDOK WFS)")
    log.info("=" * 60)

    types_to_download = []
    if args.type in ("panden", "beide"):
        types_to_download.append("panden")
    if args.type in ("verblijfsobjecten", "beide"):
        types_to_download.append("verblijfsobjecten")

    # Bepaal gemeenten
    gemeenten = []
    if args.gemeente:
        gemeenten = [g.strip() for g in args.gemeente.split(",")]
    elif args.woonplaats:
        gemeenten = [args.woonplaats]  # woonplaats als filter
    elif args.heel_nederland:
        gemeenten = [None]  # None = geen filter

    for gemeente in gemeenten:
        label = gemeente or "Nederland"
        log.info(f"\n{'='*60}")
        log.info(f"  Gemeente/Woonplaats: {label}")
        log.info(f"{'='*60}")

        # Bepaal bbox voor panden (die geen woonplaats-veld hebben)
        bbox_str = args.bbox
        if gemeente and not bbox_str:
            log.info(f"  Bounding box opzoeken voor {gemeente}...")
            bbox_str = get_gemeente_bbox(gemeente)
            if bbox_str:
                log.info(f"  BBOX: {bbox_str}")
            else:
                log.warning(f"  Kon geen bbox vinden voor {gemeente}")

        for bag_type in types_to_download:
            log.info(f"\n  --- {bag_type.upper()} ---")

            if bag_type == "verblijfsobjecten":
                type_name = "bag:verblijfsobject"
                fields = VBO_FIELDS

                # VBO heeft woonplaats veld → CQL filter
                cql_parts = []
                if gemeente:
                    cql_parts.append(f"woonplaats='{gemeente}'")
                if args.postcode:
                    if len(args.postcode) == 4:
                        cql_parts.append(f"postcode LIKE '{args.postcode}%'")
                    else:
                        cql_parts.append(f"postcode='{args.postcode}'")
                cql_parts.append("status='Verblijfsobject in gebruik'")
                cql_filter = " AND ".join(cql_parts)
                use_bbox = args.bbox  # alleen expliciete bbox

            else:  # panden
                type_name = "bag:pand"
                fields = PAND_FIELDS
                cql_filter = "status='Pand in gebruik'"
                use_bbox = bbox_str  # gebruik gemeente-bbox

            log.info(f"  Type: {type_name}")
            if cql_filter:
                log.info(f"  CQL: {cql_filter}")
            if use_bbox:
                log.info(f"  BBOX: {use_bbox}")

            # Tel features (alleen betrouwbaar zonder CQL filter)
            log.info("  Downloaden...")
            max_feat = args.max

            # Download
            t0 = time.time()
            features = download_features(
                type_name, fields, cql_filter,
                bbox=use_bbox, max_features=max_feat
            )
            elapsed = time.time() - t0
            log.info(f"  {len(features):,} features gedownload in {elapsed:.1f}s")

            # Output bestandsnaam
            safe_label = (gemeente or "nederland").lower().replace(" ", "_")
            if args.postcode:
                safe_label = f"pc{args.postcode}"
            output_file = os.path.join(
                args.output_dir,
                f"bag_{bag_type}_{safe_label}.geojson"
            )

            # Metadata
            metadata = {
                "type": bag_type,
                "source": "PDOK BAG WFS v2.0",
                "api": PDOK_BAG_WFS,
                "filter": cql_filter,
                "bbox": use_bbox,
                "count": len(features),
                "total_downloaded": len(features),
                "generated": time.strftime("%Y-%m-%dT%H:%M:%S"),
                "update_frequency": "dagelijks (BAG wordt dagelijks bijgewerkt door Kadaster)",
            }
            if gemeente:
                metadata["gemeente"] = gemeente

            size_mb = save_geojson(features, output_file, bag_type, metadata)
            log.info(f"  Opgeslagen: {output_file} ({size_mb:.1f} MB)")

    log.info(f"\n{'='*60}")
    log.info("  KLAAR!")
    log.info(f"{'='*60}")
    log.info("\nTip: Open de GeoJSON in QGIS, of gebruik in de GeoInzicht app")
    log.info("Tip: Voor heel Nederland, download de BAG GeoPackage (~7.2 GB):")
    log.info("     https://service.pdok.nl/kadaster/bag/atom/downloads/bag-light.gpkg")


if __name__ == "__main__":
    main()
