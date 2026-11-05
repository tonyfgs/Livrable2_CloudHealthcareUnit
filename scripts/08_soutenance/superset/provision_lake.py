"""Expose toutes les couches du lac dans Trino pour Superset : schemas raw (CSV externes), silver, gold, archive.

* raw     : fichiers sources du bucket lake/raw lus tels quels (CSV, toutes colonnes en texte)
* silver  : deja cree par le DDL du Livrable 2
* gold    : deja cree par le DDL du Livrable 2
* archive : resultats de la campagne sur donnees completes du 30/10/2026 (q01, q07, controles qualite,
            benchmark), uniquement necessaires quand les fichiers complets (deces, FINESS) sont absents.
Idempotent : peut etre relance.
"""
import csv
import re
import sys
from pathlib import Path

from sslib import trino

HERE = Path(__file__).resolve().parent
SOUT = HERE.parent
ARCH = SOUT / "ref" / "archive_donnees_completes"
DDL_RAW = SOUT / "cloudera" / "ddl_raw_cloudera.sql"


def ddl_raw_colonnes():
    """Colonnes des tables raw : lues dans ddl_raw_cloudera.sql (une seule source de verite)."""
    txt = DDL_RAW.read_text(encoding="utf-8")
    out = {}
    for m in re.finditer(r"CREATE EXTERNAL TABLE raw\.(\w+) \((.*?)\)\s*ROW FORMAT", txt, re.S):
        out[m.group(1)] = re.findall(r"`(\w+)` string", m.group(2))
    return out


def sqlstr(v):
    return "'" + str(v).replace("'", "''") + "'"


def raw_schema():
    trino("CREATE SCHEMA IF NOT EXISTS hive.raw WITH (location='s3a://lake/warehouse/raw.db')")
    cols = ddl_raw_colonnes()
    # (table, repertoire du lac, separateur, colonnes)
    tables = [
        ("hospitalisations", "raw/hospitalisation/", ";", cols["hospitalisations"]),
        ("etablissement_sante", "raw/etablissement/", ";", cols["etablissement_sante"]),
        ("deces", "raw/deces/", ",", cols["deces"]),
        ("referentiels_csv", "raw/referentiels/", ";", cols["satisfaction_2019"]),
    ]
    for name, d, sep, cs in tables:
        trino(f"DROP TABLE IF EXISTS hive.raw.{name}")
        coldef = ", ".join(f'"{c}" varchar' for c in cs)
        trino(f"CREATE TABLE hive.raw.{name} ({coldef}) WITH (format='CSV', csv_separator={sqlstr(sep)}, "
              f"skip_header_line_count=1, external_location='s3a://lake/{d}')")
        n = trino(f"SELECT COUNT(*) FROM hive.raw.{name}")[1][0][0]
        print(f"raw.{name}: {n} lignes")
    for an in (2019, 2020):  # vues par annee a partir du chemin du fichier
        trino(f"CREATE OR REPLACE VIEW hive.raw.satisfaction_{an} AS SELECT * FROM hive.raw.referentiels_csv "
              f"WHERE \"$path\" LIKE '%ESATIS_{an}.csv'")
    # raw sans donnees nominatives, pour Superset
    trino("CREATE OR REPLACE VIEW hive.raw.deces_sans_identite AS SELECT sexe, date_naissance, code_lieu_naissance, "
          "pays_naissance, date_deces, code_lieu_deces FROM hive.raw.deces")


def lire_csv(nom, sep=";", header=True, names=None):
    with open(ARCH / nom, encoding="utf-8") as f:
        rows = list(csv.reader(f, delimiter=sep))
    if header:
        return rows[0], rows[1:]
    return names, rows


def ctas(table, cols_types, rows):
    trino(f"DROP TABLE IF EXISTS hive.archive.{table}")
    vals = ", ".join("(" + ", ".join((sqlstr(v) if t == "varchar" else (str(float(v)) if t == "double" else str(int(v))))
                                       for v, (_, t) in zip(r, cols_types)) + ")" for r in rows)
    names = ", ".join(c for c, _ in cols_types)
    trino(f"CREATE TABLE hive.archive.{table} AS SELECT * FROM (VALUES {vals}) AS t({names})")
    print(f"archive.{table}: {len(rows)} lignes")


def pct(rows, col_key, key):
    sel = [r for r in rows if key in r[0]]
    assert sel, key
    return float(sel[0][-1])


def archive_schema():
    trino("CREATE SCHEMA IF NOT EXISTS hive.archive WITH (location='s3a://lake/warehouse/archive.db')")
    _, q01 = lire_csv("q01_consult_etab_2019.csv", sep=",", header=False, names=["etablissement", "code_commune", "region", "consultations"])
    ctas("q01_consult_etab_2019", [("etablissement", "varchar"), ("code_commune", "varchar"), ("region", "varchar"), ("consultations", "bigint")], q01)
    _, q07 = lire_csv("q07_deces_region_2019.csv", sep=",", header=False, names=["region", "deces"])
    ctas("q07_deces_region_2019", [("region", "varchar"), ("deces", "bigint")], q07)
    h, b = lire_csv("bench_synthese.csv")
    keep = ["requete", "avant_ms_median", "apres_ms_median", "apres_ml_ms_median", "avant_octets", "apres_octets", "ratio_volume_lu"]
    idx = [h.index(k) for k in keep]
    ctas("bench_synthese", [("requete", "varchar"), ("avant_ms_median", "double"), ("apres_ms_median", "double"),
                            ("apres_ml_ms_median", "double"), ("avant_octets", "bigint"), ("apres_octets", "bigint"),
                            ("ratio_volume_lu", "double")], [[r[i] for i in idx] for r in b])
    _, fk = lire_csv("dq_integrite_fk.csv"); _, ref = lire_csv("dq_referentiel.csv"); _, comp = lire_csv("dq_completude.csv")
    # attributs de dq_completude : fichier;attribut;total;non_vide;pct_complet
    sat19 = [r for r in comp if r[0] == "satisfaction_2019" and r[1] == "score_all_rea_ajust"][0][-1]
    sat20 = [r for r in comp if r[0] == "satisfaction_2020" and r[1] == "score_all_rea_ajust"][0][-1]
    fiab = [
        ("Consultations rattachées à un établissement", pct(fk, 0, "f_consultation_detail.id_etablissement")),
        ("Consultations avec professionnel connu de l'activité", pct(ref, 0, "S1.Consultation.Id_prof_sante -> activite.identifiant")),
        ("Lignes de satisfaction avec un score (2019)", float(sat19)),
        ("Lignes de satisfaction avec un score (2020)", float(sat20)),
        ("Décès avec département de lieu identifié", pct(ref, 0, "deces.code_lieu_deces")),
        ("Hospitalisations rattachées à un établissement", pct(fk, 0, "f_hospitalisation.id_etablissement")),
        ("Consultations avec diagnostic valide", pct(fk, 0, "f_consultation_detail.id_diagnostic")),
    ]
    ctas("dq_fiabilite", [("controle", "varchar"), ("taux_pct", "double")], fiab)


ISO_REGIONS = {  # codes du fond de carte france_regions de Superset (le code de l'Ile-de-France porte une tabulation dans le GeoJSON livre)
    "Auvergne-Rhône-Alpes": "FR-ARA", "Bourgogne-Franche-Comté": "FR-BFC", "Bretagne": "FR-BRE",
    "Centre-Val de Loire": "FR-CVL", "Corse": "FR-COR", "Grand Est": "FR-GES", "Hauts-de-France": "FR-HDF",
    "Île-de-France": "FR-IDF\t", "Normandie": "FR-NOR", "Nouvelle-Aquitaine": "FR-NAQ", "Occitanie": "FR-OCC",
    "Pays de la Loire": "FR-PDL", "Provence-Alpes-Côte d'Azur": "FR-PAC", "Guadeloupe": "FR-GUA",
    "Guyane": "FR-GUF", "La Réunion": "FR-LRE", "Martinique": "FR-MTQ", "Mayotte": "FR-MAY",
}
CHAPITRES = {
    "A": "Infectieuses", "B": "Infectieuses", "C": "Tumeurs malignes",
    "D": "Tumeurs bénignes", "E": "Endocrinien", "F": "Troubles mentaux",
    "G": "Système nerveux", "H": "Oeil et oreille", "I": "Appareil circulatoire", "J": "Appareil respiratoire",
    "K": "Appareil digestif", "L": "Peau", "M": "Ostéo-articulaire", "N": "Génito-urinaire",
    "O": "Grossesse", "P": "Période périnatale", "Q": "Malformations", "R": "Symptômes et signes",
    "S": "Traumatismes", "T": "Traumatismes", "U": "Codes spéciaux",
    "V": "Causes externes", "W": "Causes externes", "X": "Causes externes", "Y": "Causes externes",
    "Z": "Facteurs de santé",
}


def vues_modelisation():
    """Vues Trino de la couche de modelisation (schema vues_t) : referentiels utilises par les datasets Superset."""
    vals = ", ".join(f"({sqlstr(k)}, {sqlstr(v)})" for k, v in ISO_REGIONS.items())
    trino("CREATE OR REPLACE VIEW hive.vues_t.ref_region_iso AS SELECT * FROM (VALUES " + vals + ") AS t(region, iso_region)")
    whens = " ".join(f"WHEN {sqlstr(k)} THEN {sqlstr(v)}" for k, v in CHAPITRES.items())
    trino("CREATE OR REPLACE VIEW hive.vues_t.d_diagnostic_enrichi AS SELECT id_diagnostic, code_diag, libelle_diagnostic, "
          f"COALESCE(CASE SUBSTR(code_diag, 1, 1) {whens} END, 'Autres') AS chapitre_cim10, "
          "code_diag || ' ' || SUBSTR(libelle_diagnostic, 1, 32) AS diagnostic FROM hive.gold.d_diagnostic")
    print("vues_t.ref_region_iso, vues_t.d_diagnostic_enrichi")


if __name__ == "__main__":
    raw_schema()
    archive_schema()
    vues_modelisation()
    for s in ("raw", "silver", "gold", "archive"):
        print(s, [r[0] for r in trino(f"SHOW TABLES FROM hive.{s}")[1]])
