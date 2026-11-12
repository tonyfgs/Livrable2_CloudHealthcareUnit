# ============================================================================
# build_dashboards_test.sh — provisionne le dashboard OFFICIEL "Parcours CHU"
# (recopie de la mise en page de la soutenance 08_soutenance/visuels/
# dashboard_bureau.png) : bandeau + 4 KPI filtrables, sections 1-4, un
# graphique par visuel, AUCUN texte de commentaire (les notes de soutenance
# restent dans le script de soutenance, pas dans le dashboard).
#
# FILTRES : barre de filtres NATIFS Superset en HAUT du dashboard
# (filter_bar_orientation="HORIZONTAL" + flag HORIZONTAL_FILTER_BAR) :
# 3 filtres select multi (Année / Région / Sexe) qui s'appliquent aux
# graphiques dont le dataset contient la colonne. Le cross-filter par clic
# sur les graphiques est DÉSACTIVÉ (cross_filters_enabled=false).
# Le dashboard est PUBLIÉ et tous les autres dashboards sont supprimés.
#
# Lancer DANS le conteneur superset :
#   docker exec -i chu-superset bash -s < scripts/06_superset/build_dashboards_test.sh
# Vues Trino : scripts/06_superset/vues_test.sql (schéma vues_test)
# ============================================================================
set -u
BASE=http://superset:8088

echo "attente de /health (max 6 min)..."
for i in $(seq 1 36); do
  curl -sf "$BASE/health" >/dev/null 2>&1 && break
  sleep 10
done
curl -sf "$BASE/health" >/dev/null || { echo 'ERREUR: superset /health KO'; exit 1; }
echo "health OK"

rm -f /tmp/sup_test_c.txt
TOKEN=$(curl -sf -c /tmp/sup_test_c.txt -X POST "$BASE/api/v1/security/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin","provider":"db","refresh":true}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])') \
  || { echo 'ERREUR: login'; exit 1; }
CSRF=$(curl -sf -b /tmp/sup_test_c.txt -c /tmp/sup_test_c.txt "$BASE/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["result"])') \
  || { echo 'ERREUR: CSRF'; exit 1; }

export SUP_BASE="$BASE" SUP_TOKEN="$TOKEN" SUP_CSRF="$CSRF" SUP_COOKIE=/tmp/sup_test_c.txt

python3 - <<'PYEOF'
import json, os, sys, urllib.request, urllib.parse, urllib.error, uuid as _uuid

BASE = os.environ["SUP_BASE"]; TOKEN = os.environ["SUP_TOKEN"]
CSRF = os.environ["SUP_CSRF"]; COOKIEFILE = os.environ["SUP_COOKIE"]

def cookie_header(path):
    parts = []
    for line in open(path):
        if line.startswith("#HttpOnly_"): line = line[len("#HttpOnly_"):]
        elif line.startswith("#") or not line.strip(): continue
        f = line.rstrip("\n").split("\t")
        if len(f) >= 7: parts.append(f[5] + "=" + f[6])
    return "; ".join(parts)

HDRS = {"Authorization": "Bearer " + TOKEN, "X-CSRFToken": CSRF,
        "Content-Type": "application/json", "Cookie": cookie_header(COOKIEFILE),
        "Referer": BASE + "/"}

def call(method, path, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=HDRS)
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            raw = resp.read().decode()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as err:
        raw = err.read().decode()
        try: return err.code, json.loads(raw)
        except ValueError: return err.code, {"raw": raw}

Q = urllib.parse.quote("(page_size:200,page:0)", safe="")
def list_all(path):
    code, resp = call("GET", path + "?q=" + Q)
    if code != 200:
        print("ERREUR list", path, code, resp); sys.exit(1)
    return resp

def wait_trino(db_id, label):
    import time
    for a in range(1, 19):
        code, _ = call("GET", "/api/v1/database/%d/schemas/?q=%s" % (db_id, Q))
        if code == 200: return
        time.sleep(10)
    print("ERREUR trino KO"); sys.exit(1)

def call_retry(method, path, body=None, db_id=None, tries=4):
    import time
    last = None
    for a in range(1, tries + 1):
        code, resp = call(method, path, body)
        last = (code, resp)
        if code < 500 or a == tries: return code, resp
        if db_id: wait_trino(db_id, path)
        time.sleep(5)
    return last

db_id = next((d["id"] for d in list_all("/api/v1/database/").get("result", [])
              if d.get("database_name") == "Trino CHU"), None)
if db_id is None: print("ERREUR: pas de connexion Trino CHU"); sys.exit(1)
wait_trino(db_id, "init")
print("database Trino CHU id =", db_id)

# ---------------------------------------------------------------------------
# 1. Datasets
# ---------------------------------------------------------------------------
NOUVELLES_VUES = ["v_test_kpi", "v_test_annee", "v_test_funnel", "v_test_cim10_chapitre",
                  "v_test_satisfaction_annee_region", "v_test_carte_deces",
                  "ref_region_iso_test", "v_test_box_duree_diag",
                  "v_test_consult_chapitre_annee", "v_test_distrib_pro",
                  "v_test_carte_sat_2020", "v_test_fiabilite", "v_test_graph_groupes", "v_test_graph_hosp", "v_test_mensuel",
                  "v_test_f_annee", "v_test_f_region", "v_test_f_sexe"]
REUSE = ["f_mesures", "v_chart_consult_mensuel", "v_consultations_region_annee", "v_satisfaction_region_annee"]

existing_ds = {d.get("table_name"): d["id"] for d in list_all("/api/v1/dataset/").get("result", [])}
ds_ids = {}
for table in NOUVELLES_VUES + REUSE:
    if table in existing_ds:
        ds_ids[table] = existing_ds[table]
    else:
        code, resp = call_retry("POST", "/api/v1/dataset/",
                                {"database": db_id, "schema": "vues_test", "table_name": table}, db_id=db_id)
        if code != 201:
            print("ERREUR dataset", table, code, resp); sys.exit(1)
        ds_ids[table] = resp["id"]
    # refresh A CHAQUE run : la vue Trino peut avoir gagné des colonnes
    call_retry("PUT", "/api/v1/dataset/%d/refresh" % ds_ids[table], None, db_id=db_id)
    print("dataset %-35s id=%s" % (table, ds_ids[table]))

# ---------------------------------------------------------------------------
# 2. Charts (textes et structure repris de dashboard_bureau.png)
# ---------------------------------------------------------------------------
def agg(col, fn, label):
    return {"expressionType": "SIMPLE", "column": {"column_name": col}, "aggregate": fn, "label": label}
def sqlmetric(expr, label):
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}
def flt(col, op, val):
    return {"expressionType": "SIMPLE", "clause": "WHERE", "subject": col, "operator": op, "comparator": val}

CHARTS = [
    # KPIs filtrables : v_test_kpi expose annee/region/sexe, les métriques
    # (SUM / COUNT DISTINCT) sont calculées sur les lignes filtrées.
    ("TEST - KPI Consultations", "big_number_total", "v_test_kpi",
     {"viz_type": "big_number_total", "metric": agg("nbre_consultation", "SUM", "Consultations"), "row_limit": 1}),
    ("TEST - KPI Hospitalisations", "big_number_total", "v_test_kpi",
     {"viz_type": "big_number_total", "metric": agg("nbre_hospitalisation", "SUM", "Hospitalisations"), "row_limit": 1}),
    ("TEST - KPI Patients", "big_number_total", "v_test_kpi",
     {"viz_type": "big_number_total", "metric": sqlmetric("COUNT(DISTINCT CASE WHEN id_patient > 0 THEN id_patient END)", "Patients"), "row_limit": 1}),
    ("TEST - KPI Décès", "big_number_total", "v_test_kpi",
     {"viz_type": "big_number_total", "metric": agg("nbre_deces", "SUM", "Décès"), "row_limit": 1}),
    ("TEST - Consultations mensuelles", "mixed_timeseries", "v_test_mensuel",
     {"viz_type": "mixed_timeseries", "x_axis": "mois_date",
      "time_range": "2015-06-01 : 2023-03-31",  # plage min-max des donnees par defaut (filtres affinent ensuite)

      "metrics": [agg("consultations", "SUM", "Consultations")],
      "metrics_b": [agg("hospitalisations", "SUM", "Hospitalisations")],
      "series_type": "line", "series_type_b": "line",
      "y_axis_index_b": 1,
      "y_axis_title_secondary": "Hospitalisations",
      "groupby": [], "row_limit": 500, "show_legend": True, "legendType": "scroll", "legendOrientation": "bottom", "zoomable": True}),
    ("TEST - Chapitres consult 2019-2021", "echarts_timeseries_bar", "v_test_consult_chapitre_annee",
     {"viz_type": "echarts_timeseries_bar", "x_axis": "chapitre", "metrics": [agg("consultations", "SUM", "SUM(consultations)")],
      "groupby": ["annee"], "adhoc_filters": [], "row_limit": 200,
      "stack": "Stack", "order_desc": True, "orientation": "horizontal",
      "x_axis_force_categorical": True, "show_legend": True, "rich_tooltip": True}),
    ("TEST - Distrib pro", "dist_bar", "v_test_distrib_pro",
     {"viz_type": "dist_bar", "metrics": [agg("nb_professionnels", "SUM", "SUM(nb_professionnels)")],
      "groupby": ["consultations_par_pro"], "adhoc_filters": [], "row_limit": 12, "order_desc": False}),
    ("TEST - Taux hosp/100 par an", "echarts_timeseries_bar", "v_test_annee",
     {"viz_type": "echarts_timeseries_bar", "x_axis": "annee",
      "metrics": [agg("taux_hosp_pour_100", "AVG", "Taux moyen")], "groupby": [],
      "row_limit": 50, "order_desc": False}),
    ("TEST - Hosp par sexe et âge", "dist_bar", "f_mesures",
     {"viz_type": "dist_bar", "metrics": [agg("nbre_hospitalisation", "SUM", "SUM(nbre_hospitalisation)")],
      "groupby": ["sexe", "tranche_age"],
      "adhoc_filters": [flt("annee", ">=", 2015), flt("nbre_hospitalisation", ">", 0)],
      "row_limit": 100, "stack": "Stack", "order_desc": True}),
    ("TEST - Chapitres hospitalisations", "echarts_timeseries_bar", "v_test_cim10_chapitre",
     {"viz_type": "echarts_timeseries_bar", "x_axis": "chapitre",
      "metrics": [agg("nb_hospitalisations", "SUM", "SUM(nb_hospitalisations)")],
      "groupby": [], "adhoc_filters": [], "row_limit": 12,
      "order_desc": True, "orientation": "horizontal", "x_axis_force_categorical": True}),
    # Carte décès : l'année n'est PLUS en dur dans le chart, c'est le filtre
    # natif "Année" (barre horizontale) qui pilote la période.
    ("TEST - Carte décès par région", "country_map", "v_test_carte_deces",
     {"viz_type": "country_map", "select_country": "france_regions", "entity": "iso_region",
      "metric": agg("nbre_deces", "SUM", "SUM(nbre_deces)"), "adhoc_filters": [], "row_limit": 100}),
    ("TEST - Carte satisfaction 2020", "country_map", "v_test_carte_sat_2020",
     {"viz_type": "country_map", "select_country": "france_regions", "entity": "iso_region",
      "metric": agg("score_moyen_pondere", "SUM", "SUM(score)"), "adhoc_filters": [], "row_limit": 100}),
    ("TEST - Réseau groupes → régions 2019", "graph_chart", "v_test_graph_groupes",
     {"viz_type": "graph_chart", "metrics": [agg("value", "SUM", "value")],
      "groupby": ["source", "target"], "adhoc_filters": [], "row_limit": 100,
      "graphType": "force", "roam": True, "draggable": True,
      "label": {"show": True, "position": "right", "fontSize": 10},
      "edgeSymbol": ["none", "arrow"], "edgeSymbolSize": 6,
      "force": {"repulsion": 300, "edgeLength": 140, "layoutAnimation": True}}),
    ("TEST - Réseau groupes → régions (hosp)", "graph_chart", "v_test_graph_hosp",
     {"viz_type": "graph_chart", "metrics": [agg("value", "SUM", "value")],
      "groupby": ["source", "target"], "adhoc_filters": [], "row_limit": 100,
      "graphType": "force", "roam": True, "draggable": True,
      "label": {"show": True, "position": "right", "fontSize": 10},
      "edgeSymbol": ["none", "arrow"], "edgeSymbolSize": 6,
      "force": {"repulsion": 300, "edgeLength": 140, "layoutAnimation": True}}),
    ("TEST - Fiabilité des données", "echarts_timeseries_bar", "v_test_fiabilite",
     {"viz_type": "echarts_timeseries_bar", "x_axis": "controle",
      "metrics": [agg("pct", "SUM", "SUM(pct)")],
      "groupby": [], "adhoc_filters": [], "row_limit": 10,
      "order_desc": True, "orientation": "horizontal", "x_axis_force_categorical": True,
      "x_axis_format": "0.0", "show_value": True}),  # horizontal: les valeurs sont sur l axe X
]

# Charts des cartes par annee (module onglets Territoires par annee)
ANNEE_TABS = list(range(2015, 2021))  # décès INSEE : 1882-2020, on garde la fenêtre projet 2015-2020
for a in ANNEE_TABS:
    CHARTS.append(("TEST - Carte d\u00e9c\u00e8s %d" % a, "country_map", "v_test_carte_deces",
        {"viz_type": "country_map", "select_country": "france_regions", "entity": "iso_region",
         "metric": agg("nbre_deces", "SUM", "SUM(nbre_deces)"),
         "adhoc_filters": [flt("annee", "==", a)], "row_limit": 100}))
    CHARTS.append(("TEST - Carte satisfaction %d" % a, "country_map", "v_test_carte_sat_2020",
        {"viz_type": "country_map", "select_country": "france_regions", "entity": "iso_region",
         "metric": agg("score_moyen_pondere", "SUM", "SUM(score)"),
         "adhoc_filters": [flt("annee", "==", str(a))], "row_limit": 100}))

# Dashboard officiel : plus de préfixe "TEST - " dans les noms de charts.
CHARTS = [(n[len("TEST - "):] if n.startswith("TEST - ") else n, v, d, p) for (n, v, d, p) in CHARTS]

chart_name_to_id = {}
existing_charts = {c.get("slice_name"): c["id"] for c in list_all("/api/v1/chart/").get("result", [])}
# Nettoyage des anciens artefacts : charts FILTRE (cross-filter), ancienne carte "2019",
# anciens KPI non filtrables
for old_name in [n for n in existing_charts if n.startswith("FILTRE")] + [
        "TEST - Carte décès par région 2019", "TEST - KPI Décès 2019",
        "TEST - KPI Taux hosp/100 consult", "TEST - KPI Satisfaction /100",
        "TEST - Distrib pro 2020"]:
    if old_name in existing_charts:
        code, _ = call_retry("DELETE", "/api/v1/chart/%d" % existing_charts[old_name])
        print("chart obsolète supprimé %-42s id=%s DELETE %s" % (old_name, existing_charts[old_name], code))
        del existing_charts[old_name]
for name, viz, ds_table, params in CHARTS:
    params["color_scheme"] = "supersetColors"
    body = {"slice_name": name, "viz_type": viz, "datasource_id": ds_ids[ds_table],
            "datasource_type": "table", "params": json.dumps(params)}
    if name in existing_charts:
        cid = existing_charts[name]
    elif "TEST - " + name in existing_charts:
        cid = existing_charts["TEST - " + name]  # renommage in place via le PUT (slice_name)
    else:
        cid = None
    if cid is not None:
        code, _ = call_retry("PUT", "/api/v1/chart/%d" % cid, body, db_id=db_id)
        print("chart existant %-42s id=%s PUT %s" % (name, cid, code))
    else:
        code, resp = call_retry("POST", "/api/v1/chart/", body, db_id=db_id)
        cid = resp["id"] if code == 201 else None
        print("chart %-47s id=%s HTTP %s" % (name, cid, code))
    if cid is None:
        print("ERREUR chart", name, code, resp); sys.exit(1)
    chart_name_to_id[name] = cid

# Les charts "TEST - ..." restants sont des orphelins des anciennes itérations
# (plus référencés par aucun dashboard depuis la promotion en officiel).
for c in list_all("/api/v1/chart/").get("result", []):
    if (c.get("slice_name") or "").startswith("TEST - "):
        code, _ = call_retry("DELETE", "/api/v1/chart/%d" % c["id"])
        print("chart orphelin supprimé %-42s id=%s DELETE %s" % (c["slice_name"], c["id"], code))

# ---------------------------------------------------------------------------
# 3. Layout = structure de dashboard_bureau.png (onglets 1-4 + accueil)
# ---------------------------------------------------------------------------
CSS = """
.dashboard { background-color: #eef2f8; }
.dashboard .grid-container { background: transparent; }
.dashboard-component-chart-holder { border-radius: 12px; box-shadow: 0 2px 8px rgba(20,45,90,.10); background: #ffffff; }
.dashboard-component-markdown { background: #ffffff; border-radius: 12px; box-shadow: 0 2px 8px rgba(20,45,90,.10); padding: 12px 16px; }
.dashboard-component-header .dashboard-component-header-title { color: #0f3d6e; font-weight: 700; }
.dashboard-component-chart-holder .big-number-container, .dashboard-component-chart-holder .big-number { font-size: 2rem; }
.dashboard-component-chart-holder .subheader { text-align: center; color: #5b6b7f; }
/* colle le header bleu (row 1, markdown) et la row des 4 KPI big_number (row 2)
   dans UN SEUL bandeau bleu : les chiffres restent des requetes LIVE (big_number) */
.grid-row:nth-child(1) .dashboard-markdown { background: transparent !important; box-shadow: none !important; padding: 0 !important; }
.grid-row:nth-child(2) { background: linear-gradient(135deg,#0f3d6e,#2b86bd) !important; border-radius: 0 0 14px 14px; margin-top: -8px; padding: 16px 10px 12px; }
.grid-row:nth-child(2) .dashboard-component-chart-holder { background: rgba(255,255,255,.14) !important; box-shadow: none !important; border-radius: 10px; }
.grid-row:nth-child(2) .big-number, .grid-row:nth-child(2) .big-number-container { color: #fff !important; }
.grid-row:nth-child(2) .subheader { color: #cfe3f5 !important; }
/* barre de filtres natifs horizontale (Année / Région / Sexe) en haut */
.dashboard .filter-bar { background: #ffffff !important; border-radius: 12px; box-shadow: 0 2px 8px rgba(20,45,90,.10); padding: 8px 14px; margin-bottom: 10px; }
"""

def H(text, size="MEDIUM_HEADER"): return ("HEADER", text, size)
def M(code, w=12, h=10): return ("MARKDOWN", code, w, h)
def D(): return ("DIVIDER",)
def C(name, w, h):
    if name.startswith("TEST - "): name = name[len("TEST - "):]
    return ("CHART", name, w, h)
def R(*comps): return ("ROW", list(comps))

# Header bleu en markdown HTML (style autorise via HTML_SANITIZATION_SCHEMA_EXTENSIONS).
# Les chiffres cles NE SONT PAS en dur : ce sont les 4 charts big_number_total
# juste en dessous (row KPI), qui requetent v_test_kpi EN LIVE a chaque chargement.
HEADER_HTML = (
 '<div style="background:linear-gradient(135deg,#0f3d6e,#2b86bd);border-radius:14px;padding:24px 28px 18px;color:#fff">'
 '<h1 style="color:#fff;font-size:26px;margin:0 0 6px">Cloud Healthcare Unit : ce que disent les données du lakehouse</h1>'
 '<p style="color:#cfe3f5;margin:0">Consultations, hospitalisations, décès et satisfaction, de 2015 à 2023</p>'
 '</div>'
)

PAGE = [
    R(M(HEADER_HTML, 12, 24)),
    R(C("TEST - KPI Consultations", 3, 20), C("TEST - KPI Hospitalisations", 3, 20),
      C("TEST - KPI Patients", 3, 20), C("TEST - KPI Décès", 3, 20)),
    # Barre de filtres natifs HORIZONTALE rendue automatiquement en haut par
    # Superset (filter_bar_orientation="HORIZONTAL") : pas de composant ici.
    # Dashboard épuré : pas de textes de commentaire ni de logo.
    R(H("1. L'activit\u00e9 de consultation")),
    R(C("TEST - Consultations mensuelles", 12, 50)),
    R(H("Les motifs de consultation")),
    R(C("TEST - Chapitres consult 2019-2021", 6, 45), C("TEST - Distrib pro", 6, 45)),
    R(D()),
    R(H("2. Les hospitalisations")),
    R(C("TEST - Taux hosp/100 par an", 12, 40)),
    R(H("Qui est hospitalis\u00e9, pourquoi ?")),
    R(C("TEST - Hosp par sexe et \u00e2ge", 6, 45), C("TEST - Chapitres hospitalisations", 6, 45)),
    R(D()),
    R(H("3. Les territoires")),
    R(C("TEST - Carte décès par région", 6, 55), C("TEST - Carte satisfaction 2020", 6, 55)),
    R(D()),
    R(H("4. Ce qu'on peut affirmer")),
    R(C("TEST - Fiabilit\u00e9 des donn\u00e9es", 12, 45)),
]

def build_layout(items):
    # une seule page scrollable : ROOT -> GRID -> ROWS (pas d'onglets)
    layout = {"DASHBOARD_VERSION_KEY": "v2",
              "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
              "GRID_ID": {"type": "GRID", "id": "GRID_ID", "parents": ["ROOT_ID"], "children": []}}
    positions = {}
    def add_row(parent_id, comps):
        row_id = str(_uuid.uuid4())
        layout[parent_id]["children"].append(row_id)
        layout[row_id] = {"type": "ROW", "id": row_id, "parents": [parent_id], "children": [],
                          "meta": {"background": "BACKGROUND_WHITE"}}
        for comp in comps:
            cid = str(_uuid.uuid4())
            layout[row_id]["children"].append(cid)
            kind = comp[0]
            if kind == "CHART":
                _, name, w, h = comp
                chid = chart_name_to_id[name]
                layout[cid] = {"type": "CHART", "id": cid, "parents": [row_id], "children": [],
                               "meta": {"chartId": chid, "width": w, "height": h, "uuid": cid}}
                positions["CHART-%d" % chid] = {"type": "CHART", "id": "CHART-%d" % chid,
                                                "meta": {"chartId": chid}, "children": []}
            elif kind == "HEADER":
                _, text, size = comp
                layout[cid] = {"type": "HEADER", "id": cid, "parents": [row_id], "children": [],
                               "meta": {"text": text, "headerSize": size, "background": "BACKGROUND_TRANSPARENT",
                                        "width": 12, "height": 8}}
            elif kind == "MARKDOWN":
                _, code, w, h = comp
                layout[cid] = {"type": "MARKDOWN", "id": cid, "parents": [row_id], "children": [],
                               "meta": {"code": code, "width": w, "height": h}}
            elif kind == "DIVIDER":
                layout[cid] = {"type": "DIVIDER", "id": cid, "parents": [row_id], "children": [],
                               "meta": {"width": 12, "height": 2}}

    for item in items:
        if item[0] == "TABS":
            tabs_id = str(_uuid.uuid4())
            layout["GRID_ID"]["children"].append(tabs_id)
            layout[tabs_id] = {"type": "TABS", "id": tabs_id, "parents": ["GRID_ID"], "children": []}
            for title, tab_rows in item[1]:
                tab_id = str(_uuid.uuid4())
                layout[tabs_id]["children"].append(tab_id)
                layout[tab_id] = {"type": "TAB", "id": tab_id, "parents": [tabs_id], "children": [],
                                  "meta": {"title": title, "defaultText": title}}
                for r in tab_rows:
                    add_row(tab_id, r[1])
        else:
            add_row("GRID_ID", item[1])
    return layout, positions

layout, positions = build_layout(PAGE)

REGIONS = ["Auvergne-Rhône-Alpes","Bourgogne-Franche-Comté","Bretagne","Centre-Val de Loire","Corse",
           "Grand Est","Guadeloupe","Guyane","Hauts-de-France","Île-de-France","Martinique","Mayotte",
           "Normandie","Nouvelle-Aquitaine","Occitanie","Pays de la Loire","Provence-Alpes-Côte d'Azur","La Réunion"]
PALETTE = ["#0f3d6e","#14507f","#1a629a","#2073ad","#2b86bd","#3d9aca","#55add3","#74c0da","#93d2e2",
           "#b3e2e9","#1a629a","#2073ad","#2b86bd","#3d9aca","#55add3","#74c0da","#93d2e2","#b3e2e9"]
label_colors = {r: PALETTE[i % len(PALETTE)] for i, r in enumerate(REGIONS)}
label_colors["2015"] = "#e8762d"   # années partielles en orange (comme dashboard_bureau.png)
label_colors["2021"] = "#e8762d"

# ---------------------------------------------------------------------------
# Filtres natifs : barre HORIZONTALE rendue en haut du dashboard par Superset
# (filter_bar_orientation="HORIZONTAL" + flag HORIZONTAL_FILTER_BAR).
# 3 filtres select multi (Année / Région / Sexe). Un filtre s'applique à chaque
# chart du dashboard dont le dataset contient la colonne cible ; les valeurs
# proposées sont lues dans une petite vue agrégée (targets[0]) -> instantané.
# ---------------------------------------------------------------------------
layout_chart_ids = [int(k.split("-", 1)[1]) for k in positions if k.startswith("CHART-")]

def native_filter(name, column, target_tables):
    fid = "NATIVE_FILTER-" + str(_uuid.uuid4())
    targets = [{"datasetId": ds_ids[t], "column": {"name": column}}
               for t in target_tables if t in ds_ids]
    return {
        "id": fid,
        "name": name,
        "description": "",
        "filterType": "filter_select",
        "targets": targets,
        "controlValues": {
            "enableEmptyFilter": False,
            "defaultToFirstItem": False,
            "multiSelect": True,
            "searchAllOptions": True,
            "inverseSelection": False,
            "sortAscending": True,
        },
        "defaultDataMask": {"extraFormData": {}, "filterState": {"id": fid}, "activeTabs": []},
        "chartsInScope": layout_chart_ids,
        "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
        "adhoc_filters": [],
        "ownState": {},
        "time_range": "No filter",
    }

native_filters = [
    native_filter("Année", "annee",
                  ["v_test_f_annee", "v_test_kpi", "v_test_annee", "v_test_mensuel", "v_test_consult_chapitre_annee",
                   "v_test_distrib_pro", "v_test_carte_deces", "v_test_carte_sat_2020", "f_mesures", "v_test_cim10_chapitre"]),
    native_filter("Région", "region",
                  ["v_test_f_region", "v_test_kpi", "v_test_carte_deces", "v_test_carte_sat_2020",
                   "f_mesures", "v_test_graph_groupes", "v_test_graph_hosp"]),
    native_filter("Sexe", "sexe",
                  ["v_test_f_sexe", "v_test_kpi", "f_mesures", "v_test_cim10_chapitre"]),
]

meta = {"positions": positions, "color_scheme": "supersetColors", "label_colors": label_colors,
        "chart_configuration": {}, "native_filter_configuration": native_filters,
        "filter_bar_orientation": "HORIZONTAL",  # barre de filtres natifs EN HAUT
        "cross_filters_enabled": False,          # clic sur un graphique = aucun effet
        "global_chart_configuration": {"scope": {"rootPath": ["ROOT_ID"], "excluded": []},
                                       "chartsInScope": []}}

DASH_TITLE = "Parcours CHU"
DASH_SLUG = "parcours-chu"
existing_dash = {d.get("slug"): d["id"] for d in list_all("/api/v1/dashboard/").get("result", [])}
dash_id = existing_dash.get(DASH_SLUG) or existing_dash.get("test-parcours-chu")  # reprise de l'ancien bac a sable
if dash_id is None:
    code, resp = call_retry("POST", "/api/v1/dashboard/", {"dashboard_title": DASH_TITLE, "slug": DASH_SLUG}, db_id=db_id)
    dash_id = resp["id"] if code == 201 else None
if dash_id is None: print("ERREUR dashboard", code, resp); sys.exit(1)
print("dashboard '%s' id=%s" % (DASH_TITLE, dash_id))

# Dashboard OFFICIEL : renommé, PUBLIÉ, et tous les autres dashboards supprimés.
code, resp = call_retry("PUT", "/api/v1/dashboard/%d" % dash_id,
                        {"json_metadata": json.dumps(meta), "published": True, "css": CSS,
                         "dashboard_title": DASH_TITLE, "slug": DASH_SLUG}, db_id=db_id)
print("PUT json_metadata+css+titre+slug+published -> HTTP", code)
if code != 200: print("ERREUR:", resp); sys.exit(1)
code, resp = call_retry("PUT", "/api/v1/dashboard/%d" % dash_id, {"position_json": json.dumps(layout)}, db_id=db_id)
print("PUT position_json -> HTTP", code)
if code != 200: print("ERREUR:", resp); sys.exit(1)

for d in list_all("/api/v1/dashboard/").get("result", []):
    if d["id"] != dash_id:
        code, _ = call_retry("DELETE", "/api/v1/dashboard/%d" % d["id"])
        print("autre dashboard supprimé : id=%s '%s' -> HTTP %s" % (d["id"], d.get("dashboard_title"), code))

print("OK - dashboard officiel '%s' (slug %s, publié) : %d rows, %d charts" % (DASH_TITLE, DASH_SLUG, len(PAGE), len(chart_name_to_id)))
PYEOF
