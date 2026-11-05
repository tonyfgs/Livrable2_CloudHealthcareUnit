"""Provisionne Superset (API REST) : connexion Trino, datasets, charts, dashboard a onglets, filtres natifs, permalinks.
Idempotent : relancer met a jour (recherche par nom / slug). Usage :
    python3 provision.py [--mode live|archive] [--only datasets|dashboard|permalinks]
Sortie : superset/registry.json (identifiants des objets) et superset/PERMALIENS.md (liens des etapes de l'histoire).
"""
import argparse
import json
import sys
from pathlib import Path

from sslib import Superset, SUPERSET_URL
import datasets as D

HERE = Path(__file__).resolve().parent
DB_NAME = "Trino CHU"
DB_URI = "trino://trino@trino:8080/hive"


# =========================================================================== connexion + datasets
def ensure_database(ss):
    res = ss.find("database", "database_name", DB_NAME)
    extra = json.dumps({"allows_virtual_table_explore": True, "cost_estimate_enabled": False,
                        "metadata_params": {}, "engine_params": {}, "schemas_allowed_for_file_upload": []})
    body = {"database_name": DB_NAME, "sqlalchemy_uri": DB_URI, "expose_in_sqllab": True, "allow_ctas": False,
            "allow_cvas": False, "allow_dml": False, "allow_run_async": False, "extra": extra,
            "allow_file_upload": False}
    if res:
        ss.req("PUT", f"database/{res[0]['id']}", json=body)
        return res[0]["id"]
    return ss.req("POST", "database/", json=body)["id"]


def ensure_dataset(ss, db_id, schema, table, sql=None):
    q = json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": table},
                                {"col": "database", "opr": "rel_o_m", "value": db_id}], "page_size": 50})
    found = [r for r in ss.req("GET", "dataset/", params={"q": q})["result"] if (r.get("schema") or "") == schema]
    if found:
        ds_id = found[0]["id"]
        if sql is not None:
            ss.req("PUT", f"dataset/{ds_id}", json={"sql": sql})
        return ds_id
    body = {"database": db_id, "schema": schema, "table_name": table, "owners": [1]}
    if sql is not None:
        body["sql"] = sql
    return ss.req("POST", "dataset/", json=body)["id"]


def configure_dataset(ss, ds_id, description, metrics, labels):
    """Metriques (remplacees), libelles de colonnes, colonne temporelle."""
    ss.req("PUT", f"dataset/{ds_id}/refresh")
    cur = ss.req("GET", f"dataset/{ds_id}")["result"]
    by_name = {m["metric_name"]: m for m in cur["metrics"]}
    wanted = {m[0] for m in metrics}
    out = []
    for name, verbose, expr, fmt in metrics:
        m = {"metric_name": name, "verbose_name": verbose, "expression": expr, "d3format": fmt, "metric_type": "stat"}
        if name in by_name:
            m["id"] = by_name[name]["id"]
        out.append(m)
    for name, m in by_name.items():  # on conserve le comptage par defaut s'il n'est pas redefini
        if name not in wanted and name == "count":
            out.append({"id": m["id"], "metric_name": "count", "verbose_name": "Nombre de lignes", "expression": "COUNT(*)"})
    cols = []
    for c in cur["columns"]:
        n = c["column_name"]
        cols.append({"id": c["id"], "column_name": n, "verbose_name": labels.get(n, c.get("verbose_name")),
                     "type": c.get("type"), "is_dttm": c.get("is_dttm", False), "filterable": True, "groupby": True,
                     "expression": c.get("expression"), "description": c.get("description")})
    ss.req("PUT", f"dataset/{ds_id}", json={"description": description, "metrics": out, "columns": cols})


def provision_datasets(ss, mode):
    db_id = ensure_database(ss)
    reg = {"database": db_id, "datasets": {}}
    for schema, table, desc, metrics in D.PHYSIQUES:
        ds = ensure_dataset(ss, db_id, schema, table)
        configure_dataset(ss, ds, desc, metrics, {})
        reg["datasets"][f"{schema}.{table}"] = ds
        print("dataset", schema, table, ds)
    for name, desc, sql, metrics, labels in D.virtuels(mode):
        ds = ensure_dataset(ss, db_id, "gold", name, sql)
        configure_dataset(ss, ds, desc, metrics, labels)
        reg["datasets"][name] = ds
        print("dataset virtuel", name, ds)
    return reg


# =========================================================================== charts
from charts import F, big_number, bar, line, pie, table, table_brut, country_map  # noqa: E402

A1921 = F("annee", "IN", [2019, 2020, 2021])


def chart_specs(R, mode):
    """Liste des charts : (cle, titre, dataset, params, description). R = identifiants des datasets."""
    ds = lambda n: R["datasets"][n]
    EC, EH, TA, TS = ds("etoile_consultations"), ds("etoile_hospitalisations"), ds("taux_hospitalisation_annuel"), ds("taux_hosp_sexe_age")
    ES, DR, TE, FD = ds("etoile_satisfaction"), ds("deces_par_region"), ds("top_etablissements"), ds("fiabilite_donnees")
    DP, VL, BM, PAT = ds("distribution_activite_professionnels"), ds("volumes_du_lac"), ds("benchmark_partitionnement"), ds("gold.d_patient")
    sous_deces = "campagne complète, 2019" if mode == "archive" else "toutes années du lac"
    return [
        # ---- 1. Contexte
        ("bn_cons", "Consultations (nombre)", EC, big_number(EC, "nbre_consultations", "juin 2015 à mars 2023"), "Volume total d'activité de consultation du lac (couche Silver)."),
        ("bn_hosp", "Hospitalisations (nombre)", EH, big_number(EH, "nbre_hospitalisations", "juin 2015 à juin 2021"), "Volume total d'hospitalisations du lac."),
        ("bn_pat", "Patients (nombre)", PAT, big_number(PAT, "nb_patients", "dimension patient"), "Nombre de patients de la dimension d_patient."),
        ("bn_deces", "Décès (nombre)", DR, big_number(DR, "nbre_deces", sous_deces), "Décès (INSEE) rattachés à une région."),
        ("tbl_volumes", "Volume de chaque table du lac, par couche (nombre de lignes)", VL,
         table_brut(VL, ["couche", "table_lac", "lignes"], [("couche", True), ("table_lac", True)]), "Preuve que toutes les couches du lac (raw, silver, gold) sont lisibles depuis Superset."),
        # ---- 2. Activité
        ("line_mensuel", "Consultations par mois (nombre)", EC, line(EC, "annee_mois", ["nbre_consultations"], xtitle="Mois", ytitle="Consultations par mois (nombre)"),
         "Besoin 1 : l'activité suit-elle une tendance ? Le premier mois (juin 2015) et le dernier (mars 2023) sont partiels."),
        ("bar_chapitre", "Consultations par chapitre CIM-10 et par année, 2019 à 2021 (nombre)", EC,
         bar(EC, "chapitre_cim10", ["nbre_consultations"], groupby=["annee"], horizontal=True, stack=True, filters=[A1921],
             xtitle="", ytitle="Consultations (nombre)", labels=False, legend=True, fmt="SMART_NUMBER"),
         "Besoin 2 : quelles pathologies pèsent le plus ? Chapitre = première lettre du code CIM-10."),
        ("bar_top10_diag", "10 diagnostics les plus fréquents, 2019 à 2021 (consultations)", EC,
         bar(EC, "diagnostic", ["nbre_consultations"], horizontal=True, filters=[A1921], limit=10, ytitle="Consultations (nombre)", bounds=(0, 95)),
         "Besoin 2 : aucun diagnostic ne domine. Libellés tronqués à 32 caractères pour rester lisibles sur téléphone."),
        ("bar_distrib_pro", "Professionnels selon leur nombre de consultations en 2020 (nombre de professionnels)", DP,
         bar(DP, "consultations_par_professionnel", ["nb_professionnels"], filters=[F("annee", "==", 2020)],
             xtitle="Consultations dans l'année", ytitle="Professionnels (nombre)", sort_by="consultations_par_professionnel", sort_asc=True),
         "Besoin 6 : l'activité est très dispersée entre professionnels."),
        ("bar_top20_etab", "20 établissements les plus consultés, 2019 (consultations)", TE,
         bar(TE, "etablissement_commune", ["nbre_consultations"], horizontal=True, filters=[F("annee", "==", 2019)], limit=20,
             ytitle="Consultations (nombre)", bounds=(0, 470 if mode == "archive" else None)),
         "Besoin 1 : aucun établissement ne domine. Mode %s." % mode),
        # ---- 3. Hospitalisations
        ("bar_taux_annuel", "Hospitalisations pour 100 consultations, par année (même fenêtre d'observation)", TA,
         bar(TA, "annee", ["taux_hosp_pour_100"], groupby=["couverture"], stack=True, xtitle="Année", ytitle="Hospitalisations pour 100 consultations", fmt=",.2f", sort_by="annee", sort_asc=True),
         "Besoin 3 : taux d'hospitalisation. Les années 2015 et 2021 sont partielles (fenêtre du 21/06/2015 au 19/06/2021)."),
        ("bar_hosp_annee", "Hospitalisations par année (nombre)", EH,
         bar(EH, "annee", ["nbre_hospitalisations"], xtitle="Année", ytitle="Hospitalisations (nombre)", sort_by="annee", sort_asc=True), "Volume annuel d'hospitalisations."),
        ("pie_hosp_sexe", "Répartition des hospitalisations par sexe (part en %, total au centre)", EH, pie(EH, "sexe", "nbre_hospitalisations"), "Besoin 5 : profil des patients hospitalisés."),
        ("bar_hosp_age_sexe", "Hospitalisations par tranche d'âge et par sexe (nombre)", EH,
         bar(EH, "tranche_age", ["nbre_hospitalisations"], groupby=["sexe"], xtitle="Tranche d'âge (ans)", ytitle="Hospitalisations (nombre)", sort_by="tranche_age", sort_asc=True),
         "Besoin 5. La tranche 0-14 ans contient des dates antérieures à la naissance (voir mise en garde)."),
        ("bar_taux_age_sexe", "Hospitalisations pour 1 000 consultations, par tranche d'âge et sexe", TS,
         bar(TS, "tranche_age", ["taux_pour_1000"], groupby=["sexe"], xtitle="Tranche d'âge (ans)", ytitle="Hospitalisations pour 1 000 consultations",
             fmt=",.1f", sort_by="tranche_age", sort_asc=True),
         "Besoin 5 : risque relatif d'hospitalisation, à consultations égales."),
        ("bar_hosp_chapitre", "Hospitalisations par chapitre CIM-10 (nombre)", EH,
         bar(EH, "chapitre_cim10", ["nbre_hospitalisations"], horizontal=True, ytitle="Hospitalisations (nombre)"), "Besoin 4 : pathologies les plus hospitalisées."),
        ("tbl_duree_chapitre", "Durée moyenne de séjour par chapitre CIM-10 (jours)", EH,
         table(EH, ["chapitre_cim10"], ["nbre_hospitalisations", "duree_moyenne_jours"], order=("nbre_hospitalisations", False)),
         "Besoin 4 : durée moyenne de séjour par pathologie."),
        # ---- 4. Mortalité, satisfaction, qualité
        ("map_deces", "Décès par région (nombre ; plus la teinte est foncée, plus il y a de décès)", DR, country_map(DR, "iso_region", "nbre_deces"),
         "Besoin 7 : répartition géographique des décès. Données brutes, non rapportées à la population."),
        ("bar_deces_region", "Décès par région (nombre)", DR,
         bar(DR, "region", ["nbre_deces"], horizontal=True, ytitle="Décès (nombre)", bounds=(0, 100000 if mode == "archive" else None), filters=[F("region", "!=", "Inconnue"), F("region", "!=", "INCONNUE")]),
         "Besoin 7 : valeurs brutes par région de lieu de décès, hors lieu inconnu."),
        ("bar_fiabilite", "Fiabilité des données : taux de lignes exploitables, par contrôle (%)", FD,
         bar(FD, "controle", ["taux_pct"], horizontal=True, ytitle="Taux (%)", fmt=",.1f", bounds=(0, 110), sort_by="taux_pct", sort_asc=True),
         "Contrôles qualité du Livrable 2. Mode %s." % mode),
        ("map_satisf", "Satisfaction par région de métropole, 2020 (score sur 100 ; plus la teinte est foncée, plus le score est élevé)", ES,
         country_map(ES, "iso_region", "score_pondere", filters=[F("annee", "==", 2020), F("region", "NOT IN", OUTRE_MER + ["INCONNUE"])], fmt=",.1f"), "Besoin 8 : score e-Satis pondéré par les répondants. Métropole seulement pour que l'échelle de couleur montre l'écart de 72,6 à 75,0 ; l'outre-mer figure dans les barres."),
        ("bar_satisf", "Satisfaction des patients par région et par année (score sur 100)", ES,
         bar(ES, "region", ["score_pondere"], groupby=["annee"], horizontal=True, fmt=",.1f", bounds=(0, 100),
             ytitle="Score de satisfaction (sur 100)", sort_by="score_pondere", sort_asc=True, filters=[F("region", "!=", "INCONNUE")]),
         "Besoin 8 : les écarts entre régions métropolitaines sont faibles. Axe complet de 0 à 100 volontairement."),
        ("bn_rattache", "Consultations rattachées (%)", FD,
         big_number(FD, "taux_pct", "contrôle qualité", fmt=",.2f", filters=[F("controle", "==", "Consultations rattachées à un établissement")]),
         "Le rattachement aux établissements limite les analyses par établissement."),
        ("bn_naiss_hosp", "Hospitalisations avant la naissance (nombre)", EH, big_number(EH, "nb_avant_naissance", "datées avant la naissance du patient"), "Mise en garde sur la tranche 0-14 ans."),
        ("bn_naiss_cons", "Consultations avant la naissance (nombre)", EC, big_number(EC, "nb_avant_naissance", "datées avant la naissance du patient"), "Mise en garde sur la tranche 0-14 ans."),
        # ---- 6. Annexe technique
        ("bar_bench_temps", "Temps de réponse médian par requête (millisecondes)", BM,
         bar(BM, "requete", ["avant_ms", "apres_ms", "apres_ml_ms"], xtitle="Requête", ytitle="Temps médian (ms)", fmt=",.0f", labels=False, sort_by="requete", sort_asc=True),
         "Benchmark du Livrable 2 : le partitionnement ne réduit le temps que sur 3 requêtes avec pruning metastore."),
        ("bar_bench_volume", "Volume lu avant / après partitionnement (facteur)", BM,
         bar(BM, "requete", ["ratio_volume"], xtitle="Requête", ytitle="Facteur (avant / après)", fmt=",.1f", sort_by="requete", sort_asc=True, legend=False),
         "Facteur supérieur à 1 : le partitionnement lit moins de données."),
    ]


def query_context(ds_id, p):
    """query_context equivalent au form_data (utilise par l'API /chart/data, les exports et la verification)."""
    viz = p["viz_type"]
    if viz == "table" and p.get("query_mode") == "raw":
        q = {"filters": [], "extras": {"having": "", "where": ""}, "columns": list(p["all_columns"]), "metrics": [],
             "orderby": [[json.loads(o)[0], json.loads(o)[1]] for o in p.get("order_by_cols", [])], "row_limit": 1000,
             "order_desc": True, "url_params": {}, "custom_params": {}, "custom_form_data": {}, "annotation_layers": []}
        return {"datasource": {"id": ds_id, "type": "table"}, "force": False, "queries": [q], "form_data": p,
                "result_format": "json", "result_type": "full"}
    metrics = list(p.get("metrics") or ([p["metric"]] if p.get("metric") else []))
    if viz in ("echarts_timeseries_bar", "echarts_timeseries_line"):
        columns = [p["x_axis"]] + list(p.get("groupby") or [])
    elif viz in ("pie", "table"):
        columns = list(p.get("groupby") or [])
    elif viz == "country_map":
        columns = [p["entity"]]
    else:
        columns = []
    filters = [{"col": f["subject"], "op": f["operator"], "val": f["comparator"]} for f in p.get("adhoc_filters", [])]
    q = {"filters": filters, "extras": {"having": "", "where": ""}, "applied_time_extras": {}, "columns": columns,
         "metrics": metrics, "orderby": [[metrics[0], False]] if metrics else [], "annotation_layers": [],
         "row_limit": p.get("row_limit", 1000), "series_columns": list(p.get("groupby") or []) if viz.startswith("echarts") else [],
         "order_desc": True, "url_params": {}, "custom_params": {}, "custom_form_data": {}}
    return {"datasource": {"id": ds_id, "type": "table"}, "force": False, "queries": [q], "form_data": p,
            "result_format": "json", "result_type": "full"}


def provision_charts(ss, R, mode, dash_id=None):
    prev = R.get("charts", {})  # identifiants d'une execution precedente : un chart renomme est mis a jour, pas duplique
    R["charts"] = {}
    for key, name, ds_id, params, desc in chart_specs(R, mode):
        body = {"slice_name": name, "viz_type": params["viz_type"], "datasource_id": ds_id, "datasource_type": "table",
                "params": json.dumps(params), "query_context": json.dumps(query_context(ds_id, params)),
                "description": desc, "owners": [1]}
        if dash_id:
            body["dashboards"] = dash_id if isinstance(dash_id, list) else [dash_id]
        found = ss.find("chart", "slice_name", name)
        if not found and key in prev:
            found = [{"id": prev[key]}]
        if found:
            ss.req("PUT", f"chart/{found[0]['id']}", json=body)
            cid = found[0]["id"]
        else:
            cid = ss.req("POST", "chart/", json=body)["id"]
        R["charts"][key] = cid
        print("chart", key, cid)
    if dash_id:  # nettoyage : charts rattaches au dashboard mais qui ne font plus partie de la definition
        garde = set(R["charts"].values())
        for d_id in (dash_id if isinstance(dash_id, list) else [dash_id]):
            for ch in ss.req("GET", f"dashboard/{d_id}/charts")["result"]:
                if ch["id"] not in garde:
                    ss.req("DELETE", f"chart/{ch['id']}")
                    print("chart orphelin supprime", ch["id"], ch.get("slice_name"))
    return R


# =========================================================================== dashboard
import histoire  # noqa: E402

DASH_TITLE = "CHU : de la donnée brute aux décisions"
DASH_SLUG = "chu-histoire"
DASH_TITLE_MOBILE = "CHU : histoire (version téléphone)"
DASH_SLUG_MOBILE = "chu-histoire-mobile"
FILTRES = [  # (id, libelle, colonne, dataset de reference)
    ("NATIVE_FILTER-annee", "Année", "annee"),
    ("NATIVE_FILTER-sexe", "Sexe", "sexe"),
    ("NATIVE_FILTER-age", "Tranche d'âge", "tranche_age"),
    ("NATIVE_FILTER-region", "Région", "region"),
    ("NATIVE_FILTER-etab", "Établissement", "etablissement"),
    ("NATIVE_FILTER-chapitre", "Chapitre CIM-10", "chapitre_cim10"),
]
OUTRE_MER = ["Guadeloupe", "Martinique", "Guyane", "La Réunion", "Mayotte"]
HORS_FILTRE_ANNEE = ["bar_distrib_pro", "bar_top20_etab", "map_satisf"]  # charts a annee fixee dans leur definition
HORS_FILTRE_TOUS = ["tbl_volumes", "bar_fiabilite", "bar_bench_temps", "bar_bench_volume", "bn_rattache", "bn_pat", "bar_top20_etab"]

# Etapes de l'histoire : (cle, titre de l'onglet, [lignes]) ; ligne = [(type, ref, largeur, hauteur)]
def layout_tabs(T):
    return [
        ("contexte", "1. Contexte", [
            [("md", "contexte", 12, 22)],
            [("chart", "bn_cons", 3, 17), ("chart", "bn_pat", 3, 17), ("chart", "bn_hosp", 3, 17), ("chart", "bn_deces", 3, 17)],
            [("chart", "tbl_volumes", 7, 62), ("md", "contexte_lecture", 5, 62)]]),
        ("activite", "2. Activité", [
            [("md", "activite", 12, 34)],
            [("chart", "line_mensuel", 12, 50)],
            [("chart", "bar_chapitre", 6, 76), ("chart", "bar_top10_diag", 6, 76)],
            [("chart", "bar_distrib_pro", 6, 66), ("chart", "bar_top20_etab", 6, 66)],
            [("md", "activite_garde", 12, 14)]]),
        ("hospit", "3. Hospitalisations", [
            [("md", "hospit", 12, 34)],
            [("chart", "bar_taux_annuel", 6, 50), ("chart", "bar_hosp_annee", 6, 50)],
            [("chart", "pie_hosp_sexe", 4, 50), ("chart", "bar_hosp_age_sexe", 8, 50)],
            [("chart", "bar_taux_age_sexe", 6, 76), ("chart", "bar_hosp_chapitre", 6, 76)],
            [("chart", "tbl_duree_chapitre", 12, 108)],
            [("md", "hospit_garde", 12, 14)]]),
        ("mortalite", "4. Mortalité et qualité", [
            [("md", "mortalite", 12, 30)],
            [("chart", "map_deces", 6, 56), ("chart", "bar_deces_region", 6, 56)],
            [("chart", "map_satisf", 6, 56), ("chart", "bar_satisf", 6, 90)],
            [("chart", "bn_rattache", 4, 17), ("chart", "bn_naiss_hosp", 4, 17), ("chart", "bn_naiss_cons", 4, 17)],
            [("md", "mortalite_garde", 12, 14)],
            [("chart", "bar_fiabilite", 12, 50)]]),
        ("reco", "5. Recommandations", [
            [("md", "reco", 12, 52)],
            [("md", "perspectives", 12, 30)]]),
        ("annexe", "6. Annexe technique", [
            [("md", "annexe", 12, 30)],
            [("chart", "bar_bench_temps", 6, 50), ("chart", "bar_bench_volume", 6, 50)]]),
    ]


def layout_tabs_mobile(T):
    """Version telephone : une seule colonne, gros chiffres d'abord, graphiques pleine largeur. Superset calcule la largeur
    des graphiques en JavaScript (pas en CSS) : la mise en page doit donc etre differente, pas seulement empilee par le CSS."""
    out = []
    for key, titre, rows in layout_tabs(T):
        items = [it for row in rows for it in row]
        mds = [it for it in items if it[0] == "md"]
        bns = [it for it in items if it[0] == "chart" and it[1].startswith("bn_")]
        autres = [it for it in items if it[0] == "chart" and not it[1].startswith("bn_")]
        new = []
        if mds:
            new.append([("md", mds[0][1], 12, int(mds[0][3] * 2.1) + 8)])
        for i in range(0, len(bns), 1):
            new.append([("chart", r, 12, 15) for _, r, _, _ in bns[i:i + 1]])
        for _, r, w, h in autres:
            new.append([("chart", r, 12, 125 if r == "tbl_volumes" else 150 if r.startswith("tbl_") else max(h, 60) if w < 12 else h)])
        for _, r, w, h in mds[1:]:
            new.append([("md", r, 12, int(h * 2.4) + 10)])
        out.append((key, titre, new))
    return out


def build_layout(R, T, mobile=False):
    pos = {"DASHBOARD_VERSION_KEY": "v2",
           "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
           "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["TABS-histoire"], "parents": ["ROOT_ID"]},
           "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": DASH_TITLE}}}
    tabs = layout_tabs_mobile(T) if mobile else layout_tabs(T)
    pos["TABS-histoire"] = {"type": "TABS", "id": "TABS-histoire", "children": [f"TAB-{k}" for k, _, _ in tabs],
                            "parents": ["ROOT_ID", "GRID_ID"]}
    tab_ids = {}
    for k, titre, rows in tabs:
        tid = f"TAB-{k}"
        tab_ids[k] = tid
        par = ["ROOT_ID", "GRID_ID", "TABS-histoire", tid]
        pos[tid] = {"type": "TAB", "id": tid, "children": [], "parents": par[:3], "meta": {"text": titre, "defaultText": titre, "placeholder": titre}}
        for i, row in enumerate(rows):
            rid = f"ROW-{k}-{i}"
            pos[tid]["children"].append(rid)
            pos[rid] = {"type": "ROW", "id": rid, "children": [], "parents": par, "meta": {"background": "BACKGROUND_TRANSPARENT"}}
            for j, (kind, ref, w, h) in enumerate(row):
                cid = f"{'CHART' if kind == 'chart' else 'MARKDOWN'}-{k}-{i}-{j}"
                pos[rid]["children"].append(cid)
                if kind == "chart":
                    meta = {"width": w, "height": h, "chartId": R["charts"][ref], "uuid": R["chart_uuids"][ref], "sliceName": next(n for key, n, *_ in chart_specs(R, R["mode"]) if key == ref)}
                    pos[cid] = {"type": "CHART", "id": cid, "children": [], "parents": par + [rid], "meta": meta}
                else:
                    pos[cid] = {"type": "MARKDOWN", "id": cid, "children": [], "parents": par + [rid], "meta": {"width": w, "height": h, "code": T[ref]}}
    return pos, tab_ids


def native_filters(R):
    ds = R["datasets"]["etoile_consultations"]
    out = []
    for fid, label, col in FILTRES:
        excl = []
        if col == "annee":
            excl = [R["charts"][k] for k in HORS_FILTRE_ANNEE]
        excl += [R["charts"][k] for k in HORS_FILTRE_TOUS if R["charts"][k] not in excl]
        out.append({"id": fid, "name": label, "filterType": "filter_select", "type": "NATIVE_FILTER", "description": "",
                    "targets": [{"datasetId": ds, "column": {"name": col}}], "cascadeParentIds": [],
                    "scope": {"rootPath": ["ROOT_ID"], "excluded": sorted(set(excl))},
                    "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
                    "controlValues": {"multiSelect": True, "enableEmptyFilter": False, "defaultToFirstItem": False,
                                      "inverseSelection": False, "searchAllOptions": False, "sortAscending": True}})
    return out


def ensure_dashboard(ss, title, slug):
    found = ss.find("dashboard", "slug", slug)
    base = {"dashboard_title": title, "slug": slug, "published": True, "owners": [1]}
    if found:
        ss.req("PUT", f"dashboard/{found[0]['id']}", json=base)
        return found[0]["id"]
    return ss.req("POST", "dashboard/", json=base)["id"]


def chart_uuids(ss, R, mode):
    """uuid de chaque graphique, lus dans l'export (l'API de lecture ne les expose pas) et rapproches par nom."""
    import io
    import zipfile
    import yaml
    r = ss.s.get(f"{ss.url}/api/v1/chart/export/", params={"q": json.dumps(list(R["charts"].values()))}, timeout=120)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    by_name = {}
    for n in z.namelist():
        if "/charts/" in n and n.endswith(".yaml"):
            y = yaml.safe_load(z.read(n))
            by_name[y["slice_name"]] = y["uuid"]
    return {key: by_name[nom] for key, nom, *_ in chart_specs(R, mode)}


def provision_dashboard(ss, R, mode):
    R["mode"] = mode
    did = ensure_dashboard(ss, DASH_TITLE, DASH_SLUG)
    did_m = ensure_dashboard(ss, DASH_TITLE_MOBILE, DASH_SLUG_MOBILE)
    provision_charts(ss, R, mode, dash_id=[did, did_m])
    R["chart_uuids"] = chart_uuids(ss, R, mode)  # requis pour qu'un export puis import range les graphiques dans les bons onglets
    T = histoire.textes(histoire.chiffres(mode), mode)
    css = (HERE / "dashboard.css").read_text(encoding="utf-8") if (HERE / "dashboard.css").exists() else ""
    R["tabs"] = {}
    for mobile, dash, title in ((False, did, DASH_TITLE), (True, did_m, DASH_TITLE_MOBILE)):
        pos, tab_ids = build_layout(R, T, mobile=mobile)
        meta = {"color_scheme": "", "refresh_frequency": 0, "expanded_slices": {}, "label_colors": {},
                "shared_label_colors": {}, "color_scheme_domain": [], "timed_refresh_immune_slices": [],
                "cross_filters_enabled": False, "default_filters": "{}", "native_filter_configuration": native_filters(R),
                "chart_configuration": {}}
        ss.req("PUT", f"dashboard/{dash}", json={"position_json": json.dumps(pos), "json_metadata": json.dumps(meta), "css": css,
                                                 "published": True, "dashboard_title": title})
        R["tabs"]["mobile" if mobile else "bureau"] = tab_ids
        print("dashboard", dash, f"{SUPERSET_URL}/superset/dashboard/{DASH_SLUG_MOBILE if mobile else DASH_SLUG}/")
    R["dashboard"], R["dashboard_mobile"] = did, did_m
    return R


# =========================================================================== permalinks (equivalent des signets Power BI)
ETAPES = [  # (numero, titre, onglet, filtre ou None, phrase d'accroche pour l'oral)
    (1, "Contexte", "contexte", None, "Le CHU, ses données, les trois couches du lac."),
    (2, "Activité", "activite", None, "Une activité irrégulière, sans concentration."),
    (3, "Zoom sur l'année 2019", "activite", ("NATIVE_FILTER-annee", "annee", 2019), "Le filtre Année recalcule les graphiques ; les classements à année fixe restent inchangés."),
    (4, "Hospitalisations", "hospit", None, "Peu d'hospitalisations, un taux stable, des femmes plus nombreuses."),
    (5, "Zoom sur les femmes", "hospit", ("NATIVE_FILTER-sexe", "sexe", "F"), "Le filtre Sexe : le profil par âge des femmes hospitalisées."),
    (6, "Mortalité et qualité", "mortalite", None, "Où l'on meurt, où l'on est satisfait, et ce que valent les données."),
    (7, "Zoom sur l'Île-de-France", "mortalite", ("NATIVE_FILTER-region", "region", "Île-de-France"), "Le filtre Région : la satisfaction et les volumes d'une seule région."),
    (8, "Recommandations", "reco", None, "Ce que le CHU doit faire."),
    (9, "Annexe technique", "annexe", None, "Étoile, benchmark, preuves pour les questions."),
]


def provision_permalinks(ss, R):
    out = []
    for mobile, dash_key, tabs in ((False, "dashboard", R["tabs"]["bureau"]), (True, "dashboard_mobile", R["tabs"]["mobile"])):
        for num, titre, tab, flt, accroche in ETAPES:
            mask = {}
            if flt:
                fid, col, val = flt
                mask[fid] = {"id": fid, "extraFormData": {"filters": [{"col": col, "op": "IN", "val": [val]}]},
                             "filterState": {"value": [val], "label": str(val)}, "ownState": {}}
            body = {"dataMask": mask, "activeTabs": [tabs[tab]], "anchor": tabs[tab], "urlParams": []}
            r = ss.req("POST", f"dashboard/{R[dash_key]}/permalink", json=body)
            out.append({"etape": num, "titre": titre, "mobile": mobile, "key": r["key"], "url": f"{SUPERSET_URL}/superset/dashboard/p/{r['key']}/", "tab": tab,
                        "filtre": flt[1:] if flt else None, "accroche": accroche})
    R["permaliens"] = out
    lignes = ["# Liens permanents de l'histoire (générés par provision.py)", "",
              "Ouvrir un lien restaure l'onglet et les filtres de l'étape. Les clés changent à chaque provisionnement : rejouer `python3 provision.py` puis relire ce fichier.", "",
              "| Étape | Titre | Bureau | Téléphone |", "|---|---|---|---|"]
    for num, titre, *_ in ETAPES:
        b = next(x for x in out if x["etape"] == num and not x["mobile"])
        m = next(x for x in out if x["etape"] == num and x["mobile"])
        lignes.append(f"| {num} | {titre} | {b['url']} | {m['url']}?standalone=2 |")
    (HERE / "PERMALIENS.md").write_text("\n".join(lignes) + "\n", encoding="utf-8")
    print("permaliens", len(out))
    return R


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["live", "archive"], default="live")
    ap.add_argument("--only", choices=["datasets", "dashboard", "permalinks"])
    a = ap.parse_args()
    ss = Superset()
    reg_path = HERE / "registry.json"
    R = {}
    if a.only in ("dashboard", "permalinks") and reg_path.exists():
        R = json.loads(reg_path.read_text())
    else:
        R = provision_datasets(ss, a.mode)
    if a.only == "permalinks":
        R = provision_permalinks(ss, R)
    elif a.only != "datasets":
        R = provision_dashboard(ss, R, a.mode)
        R = provision_permalinks(ss, R)
    R["mode"] = a.mode
    reg_path.write_text(json.dumps(R, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
