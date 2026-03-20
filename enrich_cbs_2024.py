#!/usr/bin/env python3
"""
Enrich buurten_2024.geojson, wijken_2024.geojson and gemeenten_2024.geojson
with CBS Kerncijfers wijken en buurten 2024 (dataset 85618NED).
"""

import json
import urllib.request
import sys
import time

CBS_DATASET = "85618NED"
CBS_BASE = f"https://opendata.cbs.nl/ODataFeed/OData/{CBS_DATASET}"
PAGE_SIZE = 10000

# Mapping from CBS field names to GeoJSON property names
# Keys MUST match the INDICATORS array in index.html
FIELD_MAP = {
    # ── Bevolking ──
    "AantalInwoners_5": "aantal_inwoners",
    "Mannen_6": "mannen",
    "Vrouwen_7": "vrouwen",
    "Bevolkingsdichtheid_34": "bevolkingsdichtheid",
    "GeboorteRelatief_26": "geboorte_relatief",
    "SterfteRelatief_28": "sterfte_relatief",
    "MateVanStedelijkheid_125": "stedelijkheid",
    "Omgevingsadressendichtheid_126": "omgevingsadressendichtheid",
    "GeboorteTotaal_25": "geboorte_totaal",
    "SterfteTotaal_27": "sterfte_totaal",

    # ── Leeftijdsopbouw ── (index.html keys: 0_15, 15_25, etc.)
    "k_0Tot15Jaar_8": "0_15",
    "k_15Tot25Jaar_9": "15_25",
    "k_25Tot45Jaar_10": "25_45",
    "k_45Tot65Jaar_11": "45_65",
    "k_65JaarOfOuder_12": "65plus",

    # ── Huishoudens ── (index.html: huishoudens, gem_huishouden_grootte, pct_*)
    "HuishoudensTotaal_29": "huishoudens",
    "GemiddeldeHuishoudensgrootte_33": "gem_huishouden_grootte",
    "Eenpersoonshuishoudens_30": "eenpersoons_hh",
    "HuishoudensZonderKinderen_31": "hh_zonder_kinderen",
    "HuishoudensMetKinderen_32": "hh_met_kinderen",
    "Ongehuwd_13": "ongehuwd",
    "Gehuwd_14": "gehuwd",
    "Gescheiden_15": "gescheiden",
    "Verweduwd_16": "verweduwd",

    # ── Inkomen & Welvaart ──
    "GemiddeldInkomenPerInwoner_81": "gem_inkomen_inwoner",
    "GemiddeldInkomenPerInkomensontvanger_80": "gem_inkomen_ontvanger",
    "GemGestandaardiseerdInkomenVanHuish_84": "gem_gestand_inkomen",
    "MediaanVermogenVanParticuliereHuish_91": "mediaan_vermogen",
    "k_20PersonenMetHoogsteInkomen_83": "pct_hh_hoog_inkomen",
    "k_40PersonenMetLaagsteInkomen_82": "pct_hh_laag_inkomen",
    "HuishoudensMetEenLaagInkomen_87": "hh_laag_inkomen",
    "HuishOnderOfRondSociaalMinimum_88": "pct_rond_sociaal_min",
    "AantalInkomensontvangers_79": "inkomen_ontvangers",

    # ── Wonen & Woningmarkt ──
    "GemiddeldeWOZWaardeVanWoningen_36": "gem_woz_waarde",
    "Woningvoorraad_35": "woningvoorraad",
    "Koopwoningen_41": "koopwoningen",
    "HuurwoningenTotaal_42": "huurwoningen",
    "PercentageEengezinswoning_37": "pct_eengezins",
    "PercentageMeergezinswoning_38": "pct_meergezins",
    "BouwjaarVanaf2000_47": "pct_bouwjaar_na_2000",
    "PercentageOnbewoond_40": "pct_leegstand",
    "PercentageWoningenMetStadsverwarming_64": "pct_stadsverwarming",
    "InBezitWoningcorporatie_43": "pct_huurcorporatie",
    "InBezitOverigeVerhuurders_44": "huur_overig",
    "EigendomOnbekend_45": "eigendom_onbekend",
    "PercentageBewoond_39": "pct_bewoond",
    "BouwjaarVoor2000_46": "bouwjaar_voor_2000",

    # ── Energie ──
    "GemiddeldAardgasverbruikTotaal_56": "gem_aardgas",
    "GemiddeldeElektriciteitsleveringTotaal_48": "gem_elektriciteit",

    # ── Economie & Arbeid ──
    "Nettoarbeidsparticipatie_74": "arbeidsparticipatie",
    "BedrijfsvestigingenTotaal_100": "bedrijfsvestigingen",
    "PercentageWerknemers_75": "pct_werknemers",
    "PercentageZelfstandigen_78": "pct_zelfstandigen",
    "ALandbouwBosbouwEnVisserij_101": "bedrijven_landbouw",
    "BFNijverheidEnEnergie_102": "bedrijven_nijverheid",
    "GIHandelEnHoreca_103": "bedrijven_handel_horeca",
    "HJVervoerInformatieEnCommunicatie_104": "bedrijven_vervoer_ict",
    "KLFinancieleDienstenOnroerendGoed_105": "bedrijven_financieel",
    "MNZakelijkeDienstverlening_106": "bedrijven_zakelijk",
    "OQOverheidOnderwijsEnZorg_107": "bedrijven_overheid_zorg",
    "RUCultuurRecreatieOverigeDiensten_108": "bedrijven_cultuur",

    # ── Sociale Zekerheid & Zorg ──
    "WmoClientenRelatief_99": "wmo_per_1000",
    "PercentageJongerenMetJeugdzorg_97": "pct_jeugdzorg",
    "WmoClienten_98": "wmo_clienten",
    "JongerenMetJeugdzorgInNatura_96": "jeugdzorg",
    "PersonenPerSoortUitkeringBijstand_92": "uitkering_bijstand",
    "PersonenPerSoortUitkeringAO_93": "uitkering_ao",
    "PersonenPerSoortUitkeringWW_94": "uitkering_ww",
    "PersonenPerSoortUitkeringAOW_95": "uitkering_aow",

    # ── Voorzieningen ──
    "AfstandTotHuisartsenpraktijk_115": "afstand_huisarts",
    "AfstandTotGroteSupermarkt_116": "afstand_supermarkt",
    "AfstandTotKinderdagverblijf_117": "afstand_kdv",
    "AfstandTotSchool_118": "afstand_basisonderwijs",
    "ScholenBinnen3Km_119": "scholen_3km",

    # ── Mobiliteit ──
    "PersonenautoSTotaal_109": "personenautos",
    "PersonenautoSPerHuishouden_112": "auto_per_huishouden",
    "PersonenautoSNaarOppervlakte_113": "auto_per_km2",
    "Motorfietsen_114": "motorfietsen",

    # ── Oppervlakte ──
    "OppervlakteTotaal_120": "oppervlakte_totaal",
    "OppervlakteLand_121": "opp_land",
    "OppervlakteWater_122": "opp_water",
}


def fetch_all_cbs_data():
    """Fetch all rows from CBS TypedDataSet with pagination."""
    all_rows = []
    skip = 0
    while True:
        url = f"{CBS_BASE}/TypedDataSet?$top={PAGE_SIZE}&$skip={skip}&$format=json"
        print(f"  Fetching rows {skip} to {skip + PAGE_SIZE}...")
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode())
        rows = data.get("value", [])
        all_rows.extend(rows)
        print(f"  Got {len(rows)} rows (total: {len(all_rows)})")
        if len(rows) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        time.sleep(1)
    return all_rows


def safe_num(val):
    """Return numeric value or None."""
    if val is None:
        return None
    if isinstance(val, str):
        val = val.strip()
        if val in ("", "."):
            return None
        try:
            return float(val)
        except ValueError:
            return None
    return val


def safe_pct(part, total):
    """Compute percentage, return rounded float or None."""
    p = safe_num(part)
    t = safe_num(total)
    if p is None or t is None or t == 0:
        return None
    return round(p / t * 100, 1)


def build_lookup(cbs_rows):
    """Build a dict from region code -> mapped properties, including computed fields."""
    lookup = {}
    for row in cbs_rows:
        code = row.get("WijkenEnBuurten", "").strip()
        if not code:
            continue
        props = {}
        for cbs_field, short_name in FIELD_MAP.items():
            val = row.get(cbs_field)
            # Clean None and whitespace-only strings
            if val is None or (isinstance(val, str) and val.strip() in ("", ".")):
                props[short_name] = None
            else:
                props[short_name] = val

        # ── Computed percentage fields for index.html ──
        inwoners = safe_num(row.get("AantalInwoners_5"))
        huishoudens = safe_num(row.get("HuishoudensTotaal_29"))

        # Huishoudens percentages
        props["pct_eenpersoonshhd"] = safe_pct(
            row.get("Eenpersoonshuishoudens_30"), row.get("HuishoudensTotaal_29"))
        props["pct_hhd_met_kind"] = safe_pct(
            row.get("HuishoudensMetKinderen_32"), row.get("HuishoudensTotaal_29"))
        props["pct_hhd_zonder_kind"] = safe_pct(
            row.get("HuishoudensZonderKinderen_31"), row.get("HuishoudensTotaal_29"))

        # Burgerlijke staat percentages
        props["pct_gehuwd"] = safe_pct(row.get("Gehuwd_14"), row.get("AantalInwoners_5"))
        props["pct_ongehuwd"] = safe_pct(row.get("Ongehuwd_13"), row.get("AantalInwoners_5"))

        # Herkomst percentages (CBS fields: Nederland_17, EuropaExclNL_18, BuitenEuropa_19)
        props["pct_herkomst_nl"] = safe_pct(
            row.get("Nederland_17"), row.get("AantalInwoners_5"))
        props["pct_herkomst_europa"] = safe_pct(
            row.get("EuropaExclusiefNederland_18"), row.get("AantalInwoners_5"))
        props["pct_herkomst_buiten_europa"] = safe_pct(
            row.get("BuitenEuropa_19"), row.get("AantalInwoners_5"))

        lookup[code] = props
    return lookup


def enrich_geojson(filepath, lookup, code_field, prefix):
    """Enrich a GeoJSON file with CBS data."""
    print(f"\nEnriching {filepath}...")
    with open(filepath, "r", encoding="utf-8") as f:
        geojson = json.load(f)

    matched = 0
    total = len(geojson["features"])

    # All output property names (direct + computed)
    all_output_keys = set(FIELD_MAP.values()) | {
        "pct_eenpersoonshhd", "pct_hhd_met_kind", "pct_hhd_zonder_kind",
        "pct_gehuwd", "pct_ongehuwd",
        "pct_herkomst_nl", "pct_herkomst_europa", "pct_herkomst_buiten_europa",
    }

    for feature in geojson["features"]:
        props = feature["properties"]
        code = props.get(code_field, "")

        # The GeoJSON already has the prefix in the code (e.g. BU03630000)
        cbs_code = code
        if cbs_code in lookup:
            props.update(lookup[cbs_code])
            matched += 1
        else:
            # Try with spaces padding (CBS codes are sometimes space-padded)
            # CBS codes are 10 chars for BU, 8 for WK, 6 for GM
            padded = cbs_code.ljust(10) if prefix == "BU" else (
                cbs_code.ljust(8) if prefix == "WK" else cbs_code.ljust(6)
            )
            if padded in lookup:
                props.update(lookup[padded])
                matched += 1
            else:
                # Set all CBS fields to None for unmatched features
                for short_name in all_output_keys:
                    if short_name not in props:
                        props[short_name] = None

    # Build metadata with list of available indicators
    # Scan all features to find which indicator keys have at least some data
    available_indicators = set()
    for feature in geojson["features"]:
        for key in all_output_keys:
            val = feature["properties"].get(key)
            if val is not None and val != "":
                available_indicators.add(key)

    # Also include indicators from other enrichment sources (already in GeoJSON)
    # by scanning for any keys that match known indicator patterns
    for feature in geojson["features"]:
        for key in feature["properties"]:
            val = feature["properties"][key]
            if val is not None and val != "" and key not in (
                code_field, "gemeentecode", "gemeentenaam", "buurtnaam", "wijknaam"
            ):
                available_indicators.add(key)
        break  # only need one feature to find all keys

    # Preserve existing metadata and update
    metadata = geojson.get("metadata", {})
    metadata["indicators"] = sorted(available_indicators)
    metadata["enriched_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    metadata["cbs_dataset"] = CBS_DATASET
    metadata["cbs_jaar"] = 2024
    geojson["metadata"] = metadata

    print(f"  Matched {matched}/{total} features ({matched*100//total}%)")
    print(f"  Available indicators: {len(available_indicators)}")

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False)
    print(f"  Saved {filepath}")


def main():
    print("=" * 60)
    print("CBS 2024 Kerncijfers Enrichment (85618NED)")
    print("=" * 60)

    # Step 1: Fetch all CBS data
    print("\n[1/4] Fetching CBS data...")
    cbs_rows = fetch_all_cbs_data()
    print(f"Total CBS rows: {len(cbs_rows)}")

    # Step 2: Build lookup tables
    print("\n[2/4] Building lookup tables...")
    lookup = build_lookup(cbs_rows)

    # Count by type
    bu_count = sum(1 for k in lookup if k.startswith("BU"))
    wk_count = sum(1 for k in lookup if k.startswith("WK"))
    gm_count = sum(1 for k in lookup if k.startswith("GM"))
    print(f"  Buurten (BU): {bu_count}")
    print(f"  Wijken (WK): {wk_count}")
    print(f"  Gemeenten (GM): {gm_count}")
    print(f"  Other (NL, etc): {len(lookup) - bu_count - wk_count - gm_count}")

    # Step 3: Enrich GeoJSON files
    print("\n[3/4] Enriching GeoJSON files...")
    enrich_geojson("buurten_2024.geojson", lookup, "buurtcode", "BU")
    enrich_geojson("wijken_2024.geojson", lookup, "wijkcode", "WK")
    enrich_geojson("gemeenten_2024.geojson", lookup, "gemeentecode", "GM")

    # Step 4: Verify
    print("\n[4/4] Verification...")
    for fname, code_field in [
        ("buurten_2024.geojson", "buurtcode"),
        ("wijken_2024.geojson", "wijkcode"),
        ("gemeenten_2024.geojson", "gemeentecode"),
    ]:
        with open(fname, "r", encoding="utf-8") as f:
            data = json.load(f)
        feat = data["features"][1]  # Skip first (often 'Buitenland')
        props = feat["properties"]
        has_data = sum(
            1 for k in FIELD_MAP.values() if k in props and props[k] is not None
        )
        print(f"  {fname}: {len(data['features'])} features, "
              f"sample '{props.get(code_field)}' has {has_data}/{len(FIELD_MAP)} CBS fields")

    print("\nDone!")


if __name__ == "__main__":
    main()
