#!/usr/bin/env bash
# ============================================================================
# build_dashboards (DAG lakehouse_init, tache apres verification_superset) :
# rend Superset auto-suffisant cote visualisation — cree via l'API REST :
#   - >= 8 datasets (tables gold + vues Trino vues_t + datasets virtuels SQL
#     pour les jointures) ;
#   - >= 8 charts (1 par besoin metier q01-q08 + 2 complementaires), avec
#     metrics SUM(...) / agrégats SQL et groupby reels (annee, region,
#     libelle_diagnostic, sexe, tranche_age...) ;
#   - 1 dashboard "CHU — Indicateurs métier" contenant tous les charts.
#
# Idempotent : 201 = cree, 422 = existe deja -> on recupere l'id par le GET
# liste correspondant. ECHEC REEL (exit 1) si les seuils ne sont pas atteints
# (dashboards >= 1, slices >= 8, tables/datasets >= 8).
#
# Pattern API reutilise de dags/verif_superset.sh (teste) : attente /health,
# login -> bearer token + cookie de session + X-CSRFToken (bearer seul ->
# 400 "CSRF token is missing"). Les appels CRUD sont faits en python3
# (urllib) avec ces memes en-tetes.
#
# Note de deploiement : le contenu de CE fichier est lu au parse time dans
# lakehouse_init.py (comme verif_superset.sh) — ne JAMAIS passer un
# bash_command qui se termine par .sh (piège Airflow 2.9.3, TemplateNotFound).
# ============================================================================
set -u
BASE=http://superset:8088

echo "attente de /health (max 6 min)..."
for i in $(seq 1 36); do
  curl -sf "$BASE/health" >/dev/null 2>&1 && break
  echo "  superset pas encore pret ($i/36), attente 10s..."
  sleep 10
done
curl -sf "$BASE/health" >/dev/null || { echo 'ERREUR: superset /health KO apres 6 min'; exit 1; }
echo "health OK"

rm -f /tmp/sup_c.txt
TOKEN=$(curl -sf -c /tmp/sup_c.txt -X POST "$BASE/api/v1/security/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin","provider":"db","refresh":true}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])') \
  || { echo 'ERREUR: login superset'; exit 1; }

CSRF=$(curl -sf -b /tmp/sup_c.txt -c /tmp/sup_c.txt "$BASE/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["result"])') \
  || { echo 'ERREUR: recuperation CSRF'; exit 1; }

export SUP_BASE="$BASE" SUP_TOKEN="$TOKEN" SUP_CSRF="$CSRF" SUP_COOKIE=/tmp/sup_c.txt

python3 - <<'PYEOF'
import json, os, sys, urllib.request, urllib.parse, urllib.error

BASE = os.environ["SUP_BASE"]
TOKEN = os.environ["SUP_TOKEN"]
CSRF = os.environ["SUP_CSRF"]
COOKIEFILE = os.environ["SUP_COOKIE"]

def cookie_header(path):
    # piège : le cookie de session Flask est HttpOnly -> ligne préfixée
    # "#HttpOnly_" dans le jar Netscape de curl ; il ne faut PAS l'ignorer
    parts = []
    with open(path) as f:
        for line in f:
            if line.startswith("#HttpOnly_"):
                line = line[len("#HttpOnly_"):]
            elif line.startswith("#") or not line.strip():
                continue
            fields = line.rstrip("\n").split("\t")
            if len(fields) >= 7:
                parts.append(fields[5] + "=" + fields[6])
    return "; ".join(parts)

HDRS = {
    "Authorization": "Bearer " + TOKEN,
    "X-CSRFToken": CSRF,
    "Content-Type": "application/json",
    "Cookie": cookie_header(COOKIEFILE),
    "Referer": BASE + "/",
}

def call(method, path, body=None):
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=HDRS)
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as err:
        raw = err.read().decode("utf-8")
        try:
            return err.code, (json.loads(raw) if raw else {})
        except ValueError:
            return err.code, {"raw": raw}

Q = urllib.parse.quote("(page_size:200,page:0)", safe="")

def list_all(path):
    code, resp = call("GET", path + "?q=" + Q)
    if code != 200:
        print("ERREUR list", path, "HTTP", code, resp)
        sys.exit(1)
    return resp

def wait_trino(db_id, label):
    # Trino peut redemarrer (OOM) : on reteste via l'API Superset jusqu'a
    # ce que Trino reponde (GET schemas interroge Trino)
    import time
    for attempt in range(1, 19):
        code, _ = call("GET", "/api/v1/database/%d/schemas/?q=%s" % (db_id, Q))
        if code == 200:
            return
        print("  trino indisponible (%s, tentative %d/18, HTTP %s), attente 10s..." % (label, attempt, code))
        time.sleep(10)
    print("ERREUR: Trino ne repond plus apres 3 min (%s)" % label)
    sys.exit(1)

def call_retry(method, path, body=None, db_id=None, tries=4):
    # retry sur les 5xx (Trino qui redemarre -> 500 transient cote Superset)
    import time
    last = None
    for attempt in range(1, tries + 1):
        code, resp = call(method, path, body)
        last = (code, resp)
        if code < 500 or attempt == tries:
            return code, resp
        print("  HTTP %s sur %s %s (tentative %d/%d), retry apres attente Trino..." % (code, method, path, attempt, tries))
        if db_id:
            wait_trino(db_id, path)
        time.sleep(5)
    return last

# ---------------------------------------------------------------------------
# 1. Connexion "Trino CHU" (creee par verification_superset ; on la retrouve)
# ---------------------------------------------------------------------------
db_id = None
for d in list_all("/api/v1/database/").get("result", []):
    if d.get("database_name") == "Trino CHU":
        db_id = d["id"]
        break
if db_id is None:
    code, resp = call("POST", "/api/v1/database/", {
        "database_name": "Trino CHU",
        "sqlalchemy_uri": "trino://trino@trino:8080/hive",
    })
    print("POST /api/v1/database/ -> HTTP", code)
    if code == 201:
        db_id = resp["id"]
    else:
        for d in list_all("/api/v1/database/").get("result", []):
            if d.get("database_name") == "Trino CHU":
                db_id = d["id"]
                break
if db_id is None:
    print("ERREUR: connexion 'Trino CHU' introuvable")
    sys.exit(1)
print("database 'Trino CHU' id =", db_id)

# Attente que Trino reponde VRAIMENT (apres un redemarrage du conteneur,
# Trino repond 500 SERVER_STARTING_UP le temps de son init) : on passe par
# l'API Superset (GET schemas interroge Trino via le driver).
wait_trino(db_id, "initiale")
print("trino OK via Superset (schemas)")

# ---------------------------------------------------------------------------
# 2. Datasets : tables gold (physiques), vues Trino vues_t, virtuels SQL
#    (SQL entierement qualifie hive.* car la connexion n'a pas de schema
#    par defaut -> les datasets virtuels executent le SQL tel quel)
# ---------------------------------------------------------------------------
DATASETS = [
    ("gold", "f_mesures", None),
    ("gold", "f_consult_diag_mois", None),
    ("gold", "f_consult_prof_mois", None),
    ("gold", "f_deces_agg", None),
    ("gold", "f_hospitalisation", None),
    ("gold", "f_satisfaction", None),
    ("gold", "d_etablissement", None),
    ("gold", "d_diagnostic", None),
    ("vues_t", "v_taux_consultation_mensuel", None),
    ("vues_t", "v_deces_region_annee", None),
    ("vues_t", "v_satisfaction_region_annee", None),
    ("vues_t", "v_consultations_region_annee", None),
    ("gold", "v_q01_consult_etab_2019",
     "SELECT e.raison_sociale_site AS raison_sociale_site, e.code_commune AS code_commune, "
     "e.region AS region, SUM(f.nbre_consultation) AS nbre_consultations "
     "FROM hive.gold.f_mesures f JOIN hive.gold.d_etablissement e "
     "ON f.id_etablissement = e.id_etablissement "
     "WHERE f.annee = 2019 AND f.nbre_consultation > 0 GROUP BY 1, 2, 3"),
    ("gold", "v_q02_consult_diag_2019_2021",
     "SELECT d.libelle_diagnostic AS libelle_diagnostic, SUM(f.nbre_consultation) AS nbre_consultations "
     "FROM hive.gold.f_consult_diag_mois f JOIN hive.gold.d_diagnostic d "
     "ON f.id_diagnostic = d.id_diagnostic "
     "WHERE f.annee BETWEEN 2019 AND 2021 GROUP BY 1"),
    ("gold", "v_q04_hosp_diag_2015_2021",
     "SELECT d.libelle_diagnostic AS libelle_diagnostic, COUNT(*) AS nbre_hospitalisations, "
     "ROUND(AVG(f.duree_jours), 1) AS duree_moyenne_jours "
     "FROM hive.gold.f_hospitalisation f JOIN hive.gold.d_diagnostic d "
     "ON f.id_diagnostic = d.id_diagnostic "
     "WHERE f.annee BETWEEN 2015 AND 2021 GROUP BY 1"),
    ("gold", "v_q06_consult_prof_2020",
     # agregat d'abord (partition annee=2020), jointure ensuite : evite de
     # charger la dimension 1M lignes dans le hash join pendant le
     # fetch_metadata (qui execute le SQL avec LIMIT 1 mais calcule tout
     # l'agregat -> sinon Trino OOM et redemarre)
     "SELECT p.identifiant AS identifiant, p.civilite AS civilite, p.nom AS nom, "
     "p.prenom AS prenom, p.profession AS profession, f.nbre_consultations AS nbre_consultations "
     "FROM (SELECT id_professionnel, SUM(nbre_consultation) AS nbre_consultations "
     "      FROM hive.gold.f_consult_prof_mois WHERE annee = 2020 GROUP BY 1) f "
     "JOIN hive.gold.d_professionnel p ON f.id_professionnel = p.identifiant"),
    ("vues_t", "v_chart_consult_mensuel",
     "SELECT CAST(f.annee_mois || '-01' AS DATE) AS mois_date, "
     "SUM(f.nbre_consultation) AS nbre_consultations "
     "FROM hive.gold.f_mesures f WHERE f.nbre_consultation > 0 GROUP BY 1"),
]

ds_ids = {}
for schema, table, sql in DATASETS:
    body = {"database": db_id, "schema": schema, "table_name": table}
    if sql:
        body["sql"] = sql
    code, resp = call_retry("POST", "/api/v1/dataset/", body, db_id=db_id)
    ds_id = None
    if code == 201:
        ds_id = resp["id"]
    elif code == 422:
        for d in list_all("/api/v1/dataset/").get("result", []):
            if d.get("table_name") == table and (d.get("schema") or "") == schema:
                ds_id = d["id"]
                break
    if ds_id is None:
        print("ERREUR dataset", schema + "." + table, "HTTP", code, resp)
        sys.exit(1)
    rcode, _ = call_retry("PUT", "/api/v1/dataset/%d/refresh" % ds_id, None, db_id=db_id)
    print("dataset %-40s id=%s (refresh HTTP %s)" % (schema + "." + table, ds_id, rcode))
    ds_ids[table] = ds_id

# ---------------------------------------------------------------------------
# 3. Charts : 1 par besoin metier q01-q08 + 2 complementaires (vues)
# ---------------------------------------------------------------------------
def agg(col, fn, label):
    return {"expressionType": "SIMPLE", "column": col, "aggregate": fn, "label": label}

def sqlmetric(expr, label):
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}

def flt(col, op, val):
    return {"expressionType": "SIMPLE", "col": col, "op": op, "val": val}

CHARTS = [
    ("Q01 - Consultations par etablissement (top 20, 2019)", "dist_bar",
     "v_q01_consult_etab_2019",
     {"viz_type": "dist_bar", "metrics": [agg("nbre_consultations", "SUM", "SUM(nbre_consultations)")],
      "groupby": ["raison_sociale_site"], "adhoc_filters": [], "row_limit": 20, "order_desc": True}),
    ("Q02 - Consultations par diagnostic (top 10, 2019-2021)", "dist_bar",
     "v_q02_consult_diag_2019_2021",
     {"viz_type": "dist_bar", "metrics": [agg("nbre_consultations", "SUM", "SUM(nbre_consultations)")],
      "groupby": ["libelle_diagnostic"], "adhoc_filters": [], "row_limit": 10, "order_desc": True}),
    ("Q03 - Taux d'hospitalisation pour 100 consultations (2015-2021)", "dist_bar",
     "f_mesures",
     {"viz_type": "dist_bar", "metrics": [sqlmetric(
          "ROUND(100.0 * SUM(nbre_hospitalisation) / NULLIF(SUM(nbre_consultation), 0), 2)",
          "taux_hosp_pour_100_consultations")],
      "groupby": ["annee"], "adhoc_filters": [flt("annee", ">=", 2015)],
      "row_limit": 50, "order_desc": False}),
    ("Q04 - Hospitalisations par diagnostic (top 10, 2015-2021)", "dist_bar",
     "v_q04_hosp_diag_2015_2021",
     {"viz_type": "dist_bar", "metrics": [agg("nbre_hospitalisations", "SUM", "SUM(nbre_hospitalisations)")],
      "groupby": ["libelle_diagnostic"], "adhoc_filters": [], "row_limit": 10, "order_desc": True}),
    ("Q05 - Hospitalisations par sexe et tranche d'age (2015-2021)", "dist_bar",
     "f_mesures",
     {"viz_type": "dist_bar", "metrics": [agg("nbre_hospitalisation", "SUM", "SUM(nbre_hospitalisation)")],
      "groupby": ["sexe", "tranche_age"],
      "adhoc_filters": [flt("annee", ">=", 2015), flt("nbre_hospitalisation", ">", 0)],
      "row_limit": 100, "order_desc": True}),
    ("Q06 - Consultations par professionnel (top 10, 2020)", "table",
     "v_q06_consult_prof_2020",
     {"viz_type": "table", "metrics": [agg("nbre_consultations", "SUM", "SUM(nbre_consultations)")],
      "groupby": ["identifiant", "civilite", "nom", "prenom", "profession"],
      "adhoc_filters": [], "row_limit": 10, "order_desc": True}),
    ("Q07 - Deces par region (2019)", "pie",
     "v_deces_region_annee",
     {"viz_type": "pie", "metrics": [agg("nbre_deces", "SUM", "SUM(nbre_deces)")],
      "groupby": ["region"], "adhoc_filters": [flt("annee", "==", 2019)],
      "row_limit": 25, "order_desc": True}),
    ("Q08 - Satisfaction moyenne par region (2020, ponderee)", "dist_bar",
     "v_satisfaction_region_annee",
     {"viz_type": "dist_bar", "metrics": [sqlmetric(
          "ROUND(SUM(score_moyen * nb_repondants) / NULLIF(SUM(nb_repondants), 0), 2)",
          "score_moyen_pondere")],
      "groupby": ["region"], "adhoc_filters": [flt("annee", "==", 2020)],
      "row_limit": 25, "order_desc": True}),
    ("Evolution mensuelle des consultations", "echarts_timeseries_line",
     "v_chart_consult_mensuel",
     {"viz_type": "echarts_timeseries_line", "x_axis": "mois_date", "time_grain_sqla": "P1M",
      "metrics": [agg("nbre_consultations", "SUM", "SUM(nbre_consultations)")],
      "groupby": [], "adhoc_filters": [], "row_limit": 500, "order_desc": False}),
    ("Consultations par region (2019)", "dist_bar",
     "v_consultations_region_annee",
     {"viz_type": "dist_bar", "metrics": [agg("nbre_consultations", "SUM", "SUM(nbre_consultations)")],
      "groupby": ["region"], "adhoc_filters": [flt("annee", "==", 2019)],
      "row_limit": 25, "order_desc": True}),
]

chart_ids = []
for name, viz, ds_table, params in CHARTS:
    body = {"slice_name": name, "viz_type": viz,
            "datasource_id": ds_ids[ds_table], "datasource_type": "table",
            "params": json.dumps(params)}
    code, resp = call_retry("POST", "/api/v1/chart/", body, db_id=db_id)
    cid = None
    if code == 201:
        cid = resp["id"]
    elif code == 422:
        for c in list_all("/api/v1/chart/").get("result", []):
            if c.get("slice_name") == name:
                cid = c["id"]
                break
    if cid is None:
        print("ERREUR chart", name, "HTTP", code, resp)
        sys.exit(1)
    print("chart %-60s id=%s" % (name, cid))
    chart_ids.append(cid)

# ---------------------------------------------------------------------------
# 4. Dashboard "CHU — Indicateurs metier" + association des charts via
#    json_metadata.positions (set_dash_metadata synchronise dashboard.slices
#    depuis les meta.chartId des positions)
# ---------------------------------------------------------------------------
DASH_TITLE = "CHU — Indicateurs métier"
DASH_SLUG = "chu-indicateurs-metier"
code, resp = call_retry("POST", "/api/v1/dashboard/",
                  {"dashboard_title": DASH_TITLE, "slug": DASH_SLUG, "published": True}, db_id=db_id)
dash_id = None
if code == 201:
    dash_id = resp["id"]
elif code == 422:
    for d in list_all("/api/v1/dashboard/").get("result", []):
        if d.get("slug") == DASH_SLUG or d.get("dashboard_title") == DASH_TITLE:
            dash_id = d["id"]
            break
if dash_id is None:
    print("ERREUR dashboard HTTP", code, resp)
    sys.exit(1)
print("dashboard '%s' id=%s" % (DASH_TITLE, dash_id))

positions = {}
for i, cid in enumerate(chart_ids, 1):
    key = "CHART-" + str(i)
    positions[key] = {"type": "CHART", "id": key, "meta": {"chartId": cid}, "children": []}
meta = {"positions": positions, "color_scheme": "supersetColors", "label_colors": {},
        "chart_configuration": {}, "native_filter_configuration": [],
        "cross_filters_enabled": False}
code, resp = call_retry("PUT", "/api/v1/dashboard/%d" % dash_id,
                  {"json_metadata": json.dumps(meta), "published": True}, db_id=db_id)
print("PUT /api/v1/dashboard/%d -> HTTP %s" % (dash_id, code))
if code != 200:
    print("ERREUR PUT dashboard:", resp)
    sys.exit(1)

# ---------------------------------------------------------------------------
# 5. Verification reelle : seuils dashboards >= 1, slices >= 8, datasets >= 8
# ---------------------------------------------------------------------------
ndash = list_all("/api/v1/dashboard/").get("count", 0)
nslices = list_all("/api/v1/chart/").get("count", 0)
ndatasets = list_all("/api/v1/dataset/").get("count", 0)
print("TOTAL dashboards=%d slices(charts)=%d datasets(tables)=%d" % (ndash, nslices, ndatasets))
if ndash < 1 or nslices < 8 or ndatasets < 8:
    print("ERREUR: seuils non atteints (attendu dashboards>=1, slices>=8, datasets>=8)")
    sys.exit(1)
print("OK - dashboard '%s' (slug=%s) avec %d charts, %d datasets, %d slices au total"
      % (DASH_TITLE, DASH_SLUG, len(chart_ids), len(ds_ids), nslices))
PYEOF
