# Informatiemodel — overzicht en indeling (besluit 02-08-2026)

Eén DWH, één architectuur, voor álle prototypes: bronnen → `stg` → `int` → `mart` → Supabase.

## Indeling

**`int` is ingedeeld per datadomein.** Elk domein heeft een eigen kernobject (anker); er bestaat géén universeel kernobject over alle domeinen heen, en dat wordt ook niet geforceerd.

| Domein | Kernobject / anker | Bronnen | Modelbestand |
|---|---|---|---|
| geo | ADRES (nummeraanduiding/VBO) + GEBIED (buurt/wijk/gemeente per indelingsjaar), brug VBO_BUURT | BAG, CBS, Politie, Vektis, RIVM, GBIF, bekendmakingen | `relationeel_model_integratielaag.md` |
| bedrijf | kvk_nr (BEDRIJF), vestiging koppelt op adres | KvK, Northdata, CompanyInfo, XBRL | in `relationeel_model_integratielaag.md` (BEDRIJF e.o.) |
| audio | TRACK / SESSIE (DAW-project) | eigen opnames, samples, streamingmetrieken | nog te modelleren |
| astronomie | HEMELOBJECT / WAARNEMING | NASA API's | nog te modelleren |

**Koppelen tussen domeinen: alleen op gedeelde business keys waar die bestaan** — adres (na_id), gebied (BU/WK/GM-code + indelingsjaar), kvk_nr, datum. Bedrijf↔geo loopt via VESTIGING.na_id. Audio en astronomie delen alleen datum; dat is voldoende, niets forceren.

**`mart` is ingedeeld per project.** Elk prototype krijgt eigen Kimball-sterren, afgeleid uit de domeinen in `int`. Naamconventie voor modelbestanden: `ster_<project>_<onderwerp>.md` (bestaand: `ster_bag_status.md` hoort bij project geoinzicht; bij de volgende wijziging hernoemen naar `ster_geoinzicht_bag_status.md`).

**Conformed dimensions** — identiek in elke ster waarin ze voorkomen: `dim_datum` (altijd), `dim_adres`, `dim_gebied`, `dim_bedrijf`. Dit is wat de sterren van verschillende projecten vergelijkbaar houdt zonder gemeenschappelijk kernobject.

## Wat overal gelijk is (ook zonder gedeeld kernobject)

1. Laad-metadata op elke int-tabel: `geladen_op`, `bron`.
2. Business keys uit de bron; surrogaatsleutels pas in de marts.
3. Historie in aparte voorkomen-tabellen waar de bron levensloop kent.
4. Beheer in git (`docs/informatiemodel/`, Mermaid), wijzigen = commit op develop.
5. Alleen `mart` synct naar Supabase; `stg` en `int` blijven lokaal.

## Losse datasets (uploads, data.overheid, incidentele bronnen)

Niet direct modelleren. Eerst registreren in de catalogus met metadata: bron, peildatum, licentie, eigenaar. Promoveren naar een domein in `int` zodra een project de dataset structureel gebruikt. Zo blijft `int` schoon en blijft alles wel vindbaar.
