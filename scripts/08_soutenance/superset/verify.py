"""Verification : chaque nombre affiche par un chart Superset est compare au resultat d'une requete Trino independante
(ou aux CSV de la campagne complete pour les 3 jeux en mode archive). Echoue (code 1) au premier ecart.
Usage : python3 verify.py"""
import csv
import json
import sys
from pathlib import Path

from sslib import Superset, trino
from provision_lake import CHAPITRES, ISO_REGIONS

HERE = Path(__file__).resolve().parent
SOUT = HERE.parent
DATA = SOUT / "data"
ARCH = SOUT / "ref" / "archive_donnees_completes"
R = json.loads((HERE / "registry.json").read_text())
MODE = R["mode"]
ss = Superset()
OK = []


def lire(nom, sep=","):
    return list(csv.DictReader(open(DATA / f"{nom}.csv", encoding="utf-8"), delimiter=sep))


def chart(key):
    cid = R["charts"][key]
    qc = json.loads(ss.req("GET", f"chart/{cid}")["result"]["query_context"])
    res = ss.req("POST", "chart/data", json=qc)["result"][0]
    assert not res.get("error"), (key, res.get("error"))
    return res["data"]


def eq(label, got, want, tol=0.0):
    ok = abs(got - want) <= tol if isinstance(want, (int, float)) and isinstance(got, (int, float)) else got == want
    if not ok:
        print(f"ECART {label} : Superset={got!r} attendu={want!r}")
        sys.exit(1)
    OK.append(label)
    print(f"OK    {label} = {got!r}")


def chap_agg(rows, key_letter, val):
    out = {}
    for r in rows:
        ch = CHAPITRES.get(r[key_letter], "Autres")
        out[ch] = out.get(ch, 0) + int(r[val])
    return out


def t1(sql):
    return trino(sql)[1][0][0]


# ---- gros chiffres et couches
eq("bn_cons = silver.f_consultation_detail", chart("bn_cons")[0]["nbre_consultations"], t1("SELECT COUNT(*) FROM hive.silver.f_consultation_detail"))
eq("bn_cons = total S1 (1 027 157)", chart("bn_cons")[0]["nbre_consultations"], 1027157)
eq("bn_hosp = gold.f_hospitalisation", chart("bn_hosp")[0]["nbre_hospitalisations"], t1("SELECT COUNT(*) FROM hive.gold.f_hospitalisation"))
eq("bn_hosp = 2 479", chart("bn_hosp")[0]["nbre_hospitalisations"], 2479)
eq("bn_pat = d_patient", chart("bn_pat")[0]["nb_patients"], t1("SELECT COUNT(*) FROM hive.gold.d_patient"))
deces = int(t1("SELECT SUM(deces) FROM hive.archive.q07_deces_region_2019")) if MODE == "archive" else int(t1("SELECT SUM(nbre_deces) FROM hive.gold.f_deces_agg"))
eq("bn_deces", chart("bn_deces")[0]["nbre_deces"], deces)
if MODE == "archive":
    eq("bn_deces = 620 606 (archive 2019)", chart("bn_deces")[0]["nbre_deces"], 620606)
vol = {(r["couche"], r["table_lac"]): r["lignes"] for r in chart("tbl_volumes")}
for (couche, tab), n in vol.items():
    schema = couche.split(". ")[1].split(" ")[0]
    eq(f"volumes {schema}.{tab}", n, t1(f"SELECT COUNT(*) FROM hive.{schema}.{tab}"))
eq("volumes : 3 couches presentes", sorted({c for c, _ in vol}), ["1. raw (brut)", "2. silver (nettoyé)", "3. gold (étoile)"])

# ---- activite
d01 = {r["annee_mois"]: int(r["consultations"]) for r in lire("d01_consultations_mensuelles")}
got = {r["annee_mois"]: r["nbre_consultations"] for r in chart("line_mensuel")}
eq("line_mensuel : 94 mois", len(got), len(d01)); eq("line_mensuel : toutes les valeurs = d01", got, d01)
d04 = lire("d04_consultations_chapitre_cim10")
want = {}
for r in d04:
    k = (CHAPITRES.get(r["lettre_cim10"], "Autres"), int(r["annee"])); want[k] = want.get(k, 0) + int(r["consultations"])
got = {(r["chapitre_cim10"], r["annee"]): r["nbre_consultations"] for r in chart("bar_chapitre")}
eq("bar_chapitre : toutes les valeurs = d04", got, want)
eq("bar_chapitre : total 2019 a 2021 = 396 158", sum(got.values()), 396158)
d06 = lire("d06_top_diagnostics")
got = sorted(((r["diagnostic"].split(" ")[0], r["nbre_consultations"]) for r in chart("bar_top10_diag")), key=lambda x: (-x[1], x[0]))
want = sorted(((r["code_diag"], int(r["valeur"])) for r in d06), key=lambda x: (-x[1], x[0]))
eq("bar_top10_diag : valeurs = d06", [v for _, v in got], [v for _, v in want])
eq("bar_top10_diag : codes = d06 (hors ex aequo au rang 10)", sorted(c for c, v in got if v > want[-1][1]), sorted(c for c, v in want if v > want[-1][1]))
d07c = {int(r["consultations_par_professionnel"]): int(r["professionnels"]) for r in lire("d07c_distribution_consultations_par_professionnel_2020")}
got = {r["consultations_par_professionnel"]: r["nb_professionnels"] for r in chart("bar_distrib_pro")}
eq("bar_distrib_pro = d07c", got, d07c)
q01 = list(csv.reader(open(ARCH / "q01_consult_etab_2019.csv", encoding="utf-8")))
got = sorted(r["nbre_consultations"] for r in chart("bar_top20_etab"))
if MODE == "archive":
    eq("bar_top20_etab = q01 campagne complete (20 valeurs)", got, sorted(int(r[3]) for r in q01))
    eq("bar_top20_etab : de 146 a 371", (got[0], got[-1]), (146, 371))
else:
    want = [r[0] for r in trino("""SELECT SUM(nbre_consultation) v FROM hive.gold.f_mesures f JOIN hive.gold.d_etablissement e ON f.id_etablissement = e.id_etablissement
        WHERE f.annee = 2019 AND f.nbre_consultation > 0 GROUP BY e.raison_sociale_site, e.code_commune ORDER BY 1 DESC LIMIT 20""")[1]]
    eq("bar_top20_etab = requete Trino", got, sorted(want))

# ---- hospitalisations
d02 = {int(r["annee"]): r for r in lire("d02_hospitalisation_annuelle")}
got = {r["annee"]: r["taux_hosp_pour_100"] for r in chart("bar_taux_annuel")}
for a, r in d02.items():
    eq(f"bar_taux_annuel {a} = d02", round(got[a], 3), round(float(r["hosp_pour_100_consultations"]), 3))
got = {r["annee"]: r["nbre_hospitalisations"] for r in chart("bar_hosp_annee")}
eq("bar_hosp_annee = d02", got, {a: int(r["hospitalisations"]) for a, r in d02.items()})
got = {r["sexe"]: r["nbre_hospitalisations"] for r in chart("pie_hosp_sexe")}
eq("pie_hosp_sexe F/M = Trino", got, {"F": int(t1("SELECT COUNT(*) FROM hive.gold.f_hospitalisation WHERE sexe='F'")), "M": int(t1("SELECT COUNT(*) FROM hive.gold.f_hospitalisation WHERE sexe='M'"))})
eq("pie_hosp_sexe F = 1 425, M = 1 054", got, {"F": 1425, "M": 1054})
d03 = lire("d03_hospitalisation_sexe_age")
got = {(r["tranche_age"], r["sexe"]): r["nbre_hospitalisations"] for r in chart("bar_hosp_age_sexe")}
eq("bar_hosp_age_sexe = d03", got, {(r["tranche_age"], r["sexe"]): int(r["hospitalisations"]) for r in d03})
got = {(r["tranche_age"], r["sexe"]): round(r["taux_pour_1000"], 2) for r in chart("bar_taux_age_sexe")}
eq("bar_taux_age_sexe = d03", got, {(r["tranche_age"], r["sexe"]): round(float(r["hosp_pour_1000_consultations"]), 2) for r in d03})
d05 = lire("d05_hospitalisations_chapitre_cim10")
got = {r["chapitre_cim10"]: r["nbre_hospitalisations"] for r in chart("bar_hosp_chapitre")}
eq("bar_hosp_chapitre = d05 regroupe", got, chap_agg(d05, "lettre_cim10", "hospitalisations"))
tb = {r["chapitre_cim10"]: r for r in chart("tbl_duree_chapitre")}
want = {}
for r in d05:
    ch = CHAPITRES.get(r["lettre_cim10"], "Autres"); n = int(r["hospitalisations"]); w = want.setdefault(ch, [0, 0.0]); w[0] += n; w[1] += n * float(r["duree_moyenne_jours"])
for ch, (n, s) in want.items():
    eq(f"tbl_duree_chapitre {ch} : effectif", tb[ch]["nbre_hospitalisations"], n)
    eq(f"tbl_duree_chapitre {ch} : duree moyenne", tb[ch]["duree_moyenne_jours"], s / n, tol=0.15)
eq("duree moyenne globale 15,43 j", round(t1("SELECT AVG(duree_jours) FROM hive.gold.f_hospitalisation"), 2), 15.43)

# ---- mortalite, satisfaction, qualite
iso_to_reg = {v: k for k, v in ISO_REGIONS.items()}
got = {r["iso_region"]: r["nbre_deces"] for r in chart("map_deces")}
if MODE == "archive":
    want = {ISO_REGIONS[r[0]]: int(r[1]) for r in csv.reader(open(ARCH / "q07_deces_region_2019.csv", encoding="utf-8")) if r[0] in ISO_REGIONS}
else:
    want = {ISO_REGIONS[r[0]]: int(r[1]) for r in trino("SELECT region, SUM(nbre_deces) FROM hive.gold.f_deces_agg GROUP BY 1")[1] if r[0] in ISO_REGIONS}
want_cart = {k: v for k, v in want.items()}
got_cart = {k: v for k, v in got.items() if k != "inconnu"}
eq("map_deces = deces par region (codes ISO de la carte)", got_cart, want_cart)
if MODE == "archive":
    eq("map_deces : IDF + ARA = 24 % des deces", round(100 * (got["FR-IDF\t"] + got["FR-ARA"]) / sum(got.values())), 24)
got = {r["region"]: r["nbre_deces"] for r in chart("bar_deces_region")}
eq("bar_deces_region : meme total que la carte (hors lieu inconnu)", sum(got.values()), sum(want_cart.values()))
d08 = {(int(r["annee"]), r["region"]): float(r["score_pondere"]) for r in lire("d08_satisfaction_region") if r["score_pondere"]}
got = {(r["annee"], r["region"]): round(r["score_pondere"], 2) for r in chart("bar_satisf")}
eq("bar_satisf = d08 (hors region inconnue)", got, {k: round(v, 2) for k, v in d08.items() if k[1] != "INCONNUE"})
got = {r["iso_region"]: round(r["score_pondere"], 2) for r in chart("map_satisf")}
eq("map_satisf 2020 = d08", got, {ISO_REGIONS[k[1]]: round(v, 2) for k, v in d08.items() if k[0] == 2020 and k[1] in ISO_REGIONS and k[1] not in ("Guadeloupe", "Martinique", "Guyane", "La Réunion", "Mayotte")})
metro = [v for k, v in d08.items() if k[0] == 2020 and k[1] in ISO_REGIONS and k[1] not in ("Guadeloupe", "Guyane", "Martinique", "Mayotte", "La Réunion")]
eq("satisfaction metropole 2020 : 72,6 a 75,0", (round(min(metro), 1), round(max(metro), 1)), (72.6, 75.0))
fi = {r["controle"]: r["taux_pct"] for r in chart("bar_fiabilite")}
if MODE == "archive":
    dq = {r["controle"]: float(r["taux_pct"]) for r in (dict(zip(["controle", "taux_pct"], row)) for row in trino("SELECT controle, taux_pct FROM hive.archive.dq_fiabilite")[1])}
    eq("bar_fiabilite = dq campagne complete (7 controles)", fi, dq)
    eq("bn_rattache = 63,87", chart("bn_rattache")[0]["taux_pct"], 63.87)
else:
    eq("bar_fiabilite : rattachement consultations", round(fi["Consultations rattachées à un établissement"], 4),
       round(float(t1("SELECT 100.0 * COUNT_IF(id_etablissement > 0) / COUNT(*) FROM hive.silver.f_consultation_detail")), 4))
eq("bn_naiss_hosp = 62", chart("bn_naiss_hosp")[0]["nb_avant_naissance"], 62)
eq("bn_naiss_cons = 24 145", chart("bn_naiss_cons")[0]["nb_avant_naissance"], 24145)
eq("bn_naiss_cons = Trino", chart("bn_naiss_cons")[0]["nb_avant_naissance"], t1(
    "SELECT COUNT(*) FROM hive.silver.f_consultation_detail f JOIN hive.gold.d_temps t ON t.id_temps = f.id_temps JOIN hive.gold.d_patient p ON p.id_patient = f.id_patient WHERE t.date_jour < p.date_naissance"))

# ---- annexe benchmark
b = {r["requete"]: r for r in csv.DictReader(open(ARCH / "bench_synthese.csv", encoding="utf-8"), delimiter=";")}
got = {r["requete"]: r for r in chart("bar_bench_temps")}
for q, r in b.items():
    eq(f"bench {q} : temps avant / apres / apres+ml", (got[q]["avant_ms"], got[q]["apres_ms"], got[q]["apres_ml_ms"]),
       (float(r["avant_ms_median"]), float(r["apres_ms_median"]), float(r["apres_ml_ms_median"])))
got = {r["requete"]: r["ratio_volume"] for r in chart("bar_bench_volume")}
eq("bench : facteurs de volume", got, {q: float(r["ratio_volume_lu"]) for q, r in b.items()})

# ---- metriques de dataset non portees par un chart
ds = dict((n, (sql, m)) for n, _, sql, m, _ in __import__("datasets").virtuels(MODE))
for nom, metrique, attendu_sql in [
    ("etoile_consultations", "nb_patients", "SELECT COUNT(DISTINCT id_patient) FROM hive.silver.f_consultation_detail"),
    ("etoile_consultations", "nb_professionnels", "SELECT COUNT(DISTINCT id_professionnel) FROM hive.silver.f_consultation_detail"),
    ("etoile_consultations", "part_rattachee_pct", "SELECT 100.0 * COUNT_IF(id_etablissement > 0) / COUNT(*) FROM hive.silver.f_consultation_detail"),
    ("etoile_hospitalisations", "nb_patients_hospitalises", "SELECT COUNT(DISTINCT id_patient) FROM hive.gold.f_hospitalisation"),
    ("etoile_mesures", "consultations", "SELECT SUM(nbre_consultation) FROM hive.gold.f_mesures"),
]:
    sql, metrics = ds[nom]
    expr = next(m[2] for m in metrics if m[0] == metrique)
    got = t1(f"SELECT {expr} FROM ({sql})")
    eq(f"metrique {nom}.{metrique}", round(float(got), 6), round(float(t1(attendu_sql)), 6))

print(f"\n{len(OK)} verifications reussies (mode {MODE}).")
(HERE / "verification.txt").write_text(f"{len(OK)} verifications reussies (mode {MODE})\n" + "\n".join(OK) + "\n", encoding="utf-8")
