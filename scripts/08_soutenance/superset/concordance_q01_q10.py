"""Concordance des 10 requetes du benchmark (04_benchmark/queries/q01..q10) avec les datasets et metriques Superset.
Pour chaque requete : le resultat de la requete executee sur Trino est compare a la meme question posee a Superset
(API chart/data sur le dataset et la metrique dedies). Ecrit concordance_q01_q10.txt. Echoue au premier ecart.
Usage : python3 concordance_q01_q10.py"""
import json
import re
import sys
from pathlib import Path

from sslib import Superset, trino

HERE = Path(__file__).resolve().parent
QDIR = HERE.parent.parent / "04_benchmark" / "queries"
R = json.loads((HERE / "registry.json").read_text())
ss = Superset()
LOG = []


def sup(dataset, metrics, columns=(), filters=(), limit=1000, orderby=None):
    """Interroge Superset (chart/data) sur un dataset ; filtres = [(col, op, val)]."""
    did = R["datasets"][dataset]
    q = {"metrics": list(metrics), "columns": list(columns), "row_limit": limit, "filters": [{"col": c, "op": o, "val": v} for c, o, v in filters],
         "orderby": orderby or [], "extras": {"where": "", "having": ""}}
    body = {"datasource": {"id": did, "type": "table"}, "force": True, "queries": [q], "result_format": "json", "result_type": "full"}
    res = ss.req("POST", "chart/data", json=body)["result"][0]
    assert not res.get("error"), res.get("error")
    return res["data"]


def q(num):
    f = next(QDIR.glob(f"q{num:02d}_*.sql"))
    sql = re.sub(r"--[^\n]*", "", f.read_text(encoding="utf-8")).strip().rstrip(";")
    cols, rows = trino(sql, schema="gold")
    return [dict(zip(cols, r)) for r in rows]


def eq(label, got, want):
    if got != want:
        print(f"ECART {label}\n  Superset = {got!r}\n  requete  = {want!r}")
        sys.exit(1)
    LOG.append(label)
    print(f"OK    {label}")


# q01 : voir verify.py (campagne sur donnees completes, jeu archive). Ici : q02 a q06, q09, q10 sur le lac.
# q02 : top 10 diagnostics 2019-2021
r = q(2)
s = sup("etoile_consultations", ["nbre_consultations"], ["code_diag"], [("annee", "IN", [2019, 2020, 2021])], 50000)
s = sorted(s, key=lambda x: (-x["nbre_consultations"], x["code_diag"]))[:10]
eq("q02 top 10 diagnostics 2019-2021 (code et nombre)", [(x["code_diag"], x["nbre_consultations"]) for x in s], [(x["code_diag"], x["nbre_consultations"]) for x in r])

# q03 : taux d'hospitalisation par annee, fenetre 2015-06 a 2021-06 (maille du mois)
r = q(3)
s = {x["annee"]: x for x in sup("etoile_mesures", ["consultations", "hospitalisations"], ["annee"],
                                  [("annee_mois", ">=", "2015-06"), ("annee_mois", "<=", "2021-06")])}
eq("q03 consultations et hospitalisations par annee", {a: (x["consultations"], x["hospitalisations"]) for a, x in s.items()},
   {x["annee"]: (x["nbre_consultations"], x["nbre_hospitalisations"]) for x in r})
eq("q03 taux pour 100 consultations (arrondi 2 decimales)", {a: round(100.0 * x["hospitalisations"] / x["consultations"], 2) for a, x in s.items()},
   {x["annee"]: float(x["taux_hospitalisation_pour_100_consultations"]) for x in r})

# q04 : top 10 diagnostics d'hospitalisation, 2015-2021, avec duree moyenne
r = q(4)
s = sup("etoile_hospitalisations", ["nbre_hospitalisations", "duree_moyenne_jours"], ["code_diag"], [], 50000)
s = sorted(s, key=lambda x: (-x["nbre_hospitalisations"], x["code_diag"]))[:10]
eq("q04 top 10 diagnostics d'hospitalisation (code, nombre)", [(x["code_diag"], x["nbre_hospitalisations"]) for x in s], [(x["code_diag"], x["nbre_hospitalisations"]) for x in r])
eq("q04 duree moyenne (1 decimale)", [round(x["duree_moyenne_jours"], 1) for x in s], [float(x["duree_moyenne_jours"]) for x in r])

# q05 : hospitalisations par sexe et tranche d'age (fait consolide f_mesures)
r = q(5)
s = sup("etoile_mesures", ["hospitalisations"], ["sexe", "tranche_age"], [], 10000)
s = {(x["sexe"], x["tranche_age"]): x["hospitalisations"] for x in s if x["hospitalisations"]}
eq("q05 hospitalisations par sexe et tranche d'age", s, {(x["sexe"], x["tranche_age"]): x["nbre_hospitalisations"] for x in r})

# q06 : activite par professionnel 2020 (valeurs du top 10, sans identite)
r = q(6)
s = sup("distribution_activite_professionnels", ["nb_professionnels"], ["consultations_par_professionnel"], [("annee", "==", 2020)], 1000)
top = sorted(s, key=lambda x: -x["consultations_par_professionnel"])
vals = []
for x in top:
    vals += [x["consultations_par_professionnel"]] * int(x["nb_professionnels"])
    if len(vals) >= 10:
        break
eq("q06 valeurs du top 10 des professionnels 2020 (nombre de consultations)", vals[:10], [x["nbre_consultations"] for x in r])

# q09 : consultations mensuelles depuis 2019 (vue native Trino vues_t ; la vue ajoute une moyenne mobile, on compare le nombre)
cols, rows = trino("SELECT annee_mois, nbre_consultations FROM hive.vues_t.v_taux_consultation_mensuel WHERE annee >= 2019 ORDER BY 1")
# f_mesures contient aussi des lignes de deces ou d'hospitalisation sans consultation (mois "2020-00", 0 consultation) : ecartees
s = {x["annee_mois"]: x["consultations"] for x in sup("etoile_mesures", ["consultations"], ["annee_mois"], [("annee_mois", ">=", "2019-01")], 1000) if x["consultations"]}
eq("q09 consultations mensuelles depuis 2019 (vue metier)", s, {a: n for a, n in rows})

# q10 : consultations 2019, sexe M, par region (la requete joint d_etablissement : seules les consultations rattachees comptent).
# La tranche d'age de q10 est celle de reference du patient (d_patient.tranche_age_ref) ; les datasets Superset portent l'age a la date de
# l'evenement : la concordance porte donc sur le total par region (somme des tranches), pas sur la ventilation par tranche.
sql = (QDIR / "q10_etoile_complete.sql").read_text(encoding="utf-8")
sql = re.sub(r"--[^\n]*", "", sql).strip().rstrip(";")
sql = re.sub(r"\s+LIMIT\s+\d+\s*$", "", sql, flags=re.I)
cols, rows = trino(sql, schema="gold")
want = {}
for reg, _tr, _an, n in rows:
    want[reg] = want.get(reg, 0) + n
s = sup("etoile_mesures", ["consultations"], ["region"], [("annee", "==", 2019), ("sexe", "==", "M"), ("etablissement", "!=", "Non rattaché")], 1000)
eq("q10 consultations 2019, hommes, par region (somme des tranches)", {x["region"]: x["consultations"] for x in s if x["consultations"]}, want)

(HERE / "concordance_q01_q10.txt").write_text(f"{len(LOG)} concordances reussies\n" + "\n".join(LOG) + "\n", encoding="utf-8")
print(f"\n{len(LOG)} concordances reussies.")
