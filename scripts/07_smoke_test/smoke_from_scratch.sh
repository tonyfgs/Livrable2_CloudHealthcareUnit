#!/usr/bin/env bash
# ============================================================================
# SMOKE TEST FROM SCRATCH — Livrable 2 CHU (lakehouse Docker)
#
# Objectif : prouver qu'un démarrage from scratch fonctionne SANS AUCUN BUG :
#   docker compose down -v && up -d -> DAGs en pause -> init 6/6 -> daily 8/8
#   -> volumes de données vérifiés un par un dans Trino -> dashboards Superset
#   -> endpoints -> lake.
#
# Usage :   cd scripts && ./07_smoke_test/smoke_from_scratch.sh
# Durée :   ~20-30 min (compose up ~3 min, init ~4 min, daily ~10 min).
# Sortie :  log dans 07_smoke_test/results/smoke_<timestamp>.log
#           + resume PASS/FAIL sur stdout. Exit 0 = 100% PASS, 1 = au moins 1 FAIL.
#
# Le script ne modifie rien d'autre que la stack docker du projet (down -v).
# ============================================================================
set -u

SCRIPTS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS_DIR="$SCRIPTS_DIR/07_smoke_test/results"
mkdir -p "$RESULTS_DIR"
LOG="$RESULTS_DIR/smoke_$(date -u +%Y%m%dT%H%M%SZ).log"

PASS=0; FAIL=0; FAILED_CHECKS=()
log()  { echo -e "$*" | tee -a "$LOG"; }
pass() { PASS=$((PASS+1)); log "PASS  | $*"; }
fail() { FAIL=$((FAIL+1)); FAILED_CHECKS+=("$1"); log "FAIL  | $*"; }
step() { log ""; log "=== $* ==="; }

# check "<description>" <commande...> : PASS si la commande retourne 0
check() { local desc="$1"; shift; if "$@" >>"$LOG" 2>&1; then pass "$desc"; else fail "$desc"; fi; }

cd "$SCRIPTS_DIR"
log "Smoke test from scratch — $(date -u +%Y-%m-%dT%H:%M:%SZ) — log: $LOG"

# ----------------------------------------------------------------------------
step "0. Preflight"
# ----------------------------------------------------------------------------
check "docker disponible" docker info -f '{{.ServerVersion}}'
[ -d "$SCRIPTS_DIR/../../DATA Projet" ] && [ -n "$(ls -A "$SCRIPTS_DIR/../../DATA Projet")" ] \
  && pass "DATA Projet présent (sources brutes)" || fail "DATA Projet présent (sources brutes)"
SRC_NB=$(find "$SCRIPTS_DIR/../../DATA Projet" -type f ! -name '.DS_Store' | wc -l | tr -d ' ')
log "INFO  | fichiers sources dans DATA Projet: $SRC_NB"

# ----------------------------------------------------------------------------
step "1. docker compose down -v (purge totale) puis up -d"
# ----------------------------------------------------------------------------
docker compose down -v >>"$LOG" 2>&1
log "INFO  | down -v effectue, lancement de up -d..."
docker compose up -d --build >>"$LOG" 2>&1
check "docker compose up -d --build retourne 0" true

# ----------------------------------------------------------------------------
step "2. Attente que les 14 services long-running soient Up (+ one-shots Exited 0)"
# ----------------------------------------------------------------------------
LONG_RUNNING="chu-airflow-db chu-airflow-scheduler chu-airflow-webserver chu-bench-duckdb chu-hive-hs2 chu-hive-metastore chu-metastore-db chu-minio chu-s1-postgres chu-spark-master chu-spark-worker chu-superset chu-superset-db chu-trino"
ONE_SHOTS="chu-minio-init chu-hive-metastore-init chu-airflow-init"
ok=0
for i in $(seq 1 90); do   # 90 x 10s = 15 min max
  all_up=1
  for c in $LONG_RUNNING; do
    st=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing)
    [ "$st" = "running" ] || { all_up=0; break; }
  done
  ones_ok=1
  if [ "$all_up" = "1" ]; then
    for c in $ONE_SHOTS; do
      st=$(docker inspect -f '{{.State.Status}}|{{.State.ExitCode}}' "$c" 2>/dev/null || echo "missing|")
      [ "$st" = "exited|0" ] || { ones_ok=0; break; }
    done
  fi
  if [ "$all_up" = "1" ] && [ "$ones_ok" = "1" ]; then ok=1; break; fi
  sleep 10
done
[ "$ok" = "1" ] && pass "14 conteneurs running + 3 one-shots exited(0) (après $((i*10))s)" \
                || { fail "14 conteneurs running + 3 one-shots exited(0)"; docker compose ps >>"$LOG" 2>&1; }

# ----------------------------------------------------------------------------
step "3. Endpoints HTTP"
# ----------------------------------------------------------------------------
# Les webservers (Airflow/Superset) mettent 2-4 min a ecouter apres le
# demarrage du conteneur : on retente chaque endpoint jusqu'a 4 min.
wait_http() { # wait_http <port> <codes-attendus-regex> <label>
  local port="$1" pat="$2" label="$3" c=""
  for i in $(seq 1 24); do
    c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://localhost:$port/" 2>/dev/null)
    echo "$c" | grep -qE "$pat" && { pass "$label -> $c (après $((i*10))s)"; return 0; }
    sleep 10
  done
  fail "$label -> $c (attendu: $pat)"
}
wait_http 8090 '^(200|302)$' "Airflow :8090"
wait_http 8088 '^(200|302)$' "Superset :8088"
wait_http 8080 '^(200|303)$' "Trino :8080"
wait_http 9000 '^(200|403)$' "MinIO API :9000"
wait_http 9001 '^(200|303)$' "MinIO console :9001"
wait_http 8085 '^200$'      "Spark master UI :8085"

# ----------------------------------------------------------------------------
step "4. DAGs enregistrés EN PAUSE, aucun run auto (fix backfill)"
# ----------------------------------------------------------------------------
psql_af() { docker exec chu-airflow-db psql -U airflow -d airflow -t -A -c "$1" 2>/dev/null; }
regs=0
for i in $(seq 1 36); do  # 6 min max (parsing scheduler)
  n=$(psql_af "SELECT count(*) FROM dag WHERE dag_id IN ('lakehouse_init','lakehouse_daily');")
  [ "$n" = "2" ] && { regs=1; break; }
  sleep 10
done
[ "$regs" = "1" ] && pass "2 DAGs enregistrés" || fail "2 DAGs enregistrés (n=$n)"
paused=$(psql_af "SELECT count(*) FROM dag WHERE dag_id IN ('lakehouse_init','lakehouse_daily') AND is_paused;")
[ "$paused" = "2" ] && pass "les 2 DAGs sont is_paused=true (pas de backfill auto)" || fail "DAGs en pause (paused=$paused)"
runs=$(psql_af "SELECT count(*) FROM dag_run;")
[ "$runs" = "0" ] && pass "0 dag_run auto au démarrage" || fail "0 dag_run auto (runs=$runs)"

# ----------------------------------------------------------------------------
step "5. lakehouse_init : unpause + trigger + attente 6/6 SUCCESS"
# ----------------------------------------------------------------------------
docker exec chu-airflow-scheduler airflow dags unpause lakehouse_init >>"$LOG" 2>&1
docker exec chu-airflow-scheduler airflow dags trigger lakehouse_init >>"$LOG" 2>&1
init_state=""
for i in $(seq 1 150); do  # 25 min max
  sleep 10
  init_state=$(psql_af "SELECT state FROM dag_run WHERE dag_id='lakehouse_init' ORDER BY execution_date DESC LIMIT 1;")
  [ "$init_state" = "success" -o "$init_state" = "failed" ] && break
done
[ "$init_state" = "success" ] && pass "lakehouse_init run = success (après ~$((i*10))s)" || fail "lakehouse_init run state=$init_state"
ti=$(psql_af "SELECT count(*) FROM task_instance WHERE dag_id='lakehouse_init' AND state='success';")
[ "$ti" = "6" ] && pass "init: 6/6 tâches success" || fail "init: tâches success=$ti/6"
ti=$(psql_af "SELECT count(*) FROM task_instance WHERE dag_id='lakehouse_init' AND state='failed';")
[ "$ti" = "0" ] && pass "init: 0 tâche failed" || fail "init: tâches failed=$ti"

# ----------------------------------------------------------------------------
step "6. lakehouse_daily : unpause (run auto, catchup=False) + attente 8/8 SUCCESS"
# ----------------------------------------------------------------------------
docker exec chu-airflow-scheduler airflow dags unpause lakehouse_daily >>"$LOG" 2>&1
# attendre qu'un run apparaisse (auto) ; sinon trigger manuel
appeared=0
for i in $(seq 1 30); do  # 5 min
  sleep 10
  n=$(psql_af "SELECT count(*) FROM dag_run WHERE dag_id='lakehouse_daily';")
  [ "${n:-0}" -ge 1 ] && { appeared=1; break; }
done
if [ "$appeared" = "0" ]; then
  log "INFO  | aucun run auto apres 5 min -> trigger manuel"
  docker exec chu-airflow-scheduler airflow dags trigger lakehouse_daily >>"$LOG" 2>&1
fi
daily_state=""
for i in $(seq 1 270); do  # 45 min max
  sleep 10
  daily_state=$(psql_af "SELECT state FROM dag_run WHERE dag_id='lakehouse_daily' ORDER BY execution_date DESC LIMIT 1;")
  [ "$daily_state" = "success" -o "$daily_state" = "failed" ] && break
done
[ "$daily_state" = "success" ] && pass "lakehouse_daily run = success (après ~$((i*10))s)" || fail "lakehouse_daily run state=$daily_state"
ti=$(psql_af "SELECT count(*) FROM task_instance WHERE dag_id='lakehouse_daily' AND state='success';")
[ "$ti" = "8" ] && pass "daily: 8/8 tâches success" || fail "daily: tâches success=$ti/8"
ti=$(psql_af "SELECT count(*) FROM task_instance WHERE dag_id='lakehouse_daily' AND state='failed';")
[ "$ti" = "0" ] && pass "daily: 0 tâche failed" || fail "daily: tâches failed=$ti"

# ----------------------------------------------------------------------------
step "7. Données : volumes exacts dans Trino (gold + silver)"
# ----------------------------------------------------------------------------
trino() { docker exec chu-trino trino --server http://localhost:8080 "$@" 2>/dev/null; }
count_is() { # count_is <schema> <table> <attendu>
  local got
  got=$(trino --catalog hive --schema "$1" --output-format CSV --execute "SELECT count(*) FROM $2" | tr -d '"\n ')
  [ "$got" = "$3" ] && pass "$1.$2 = $got" || fail "$1.$2 attendu=$3 obtenu=$got"
}
count_is gold d_temps 5844
count_is gold d_patient 100000
count_is gold d_etablissement 416665
count_is gold d_localisation 102
count_is gold d_diagnostic 15490
count_is gold d_professionnel 1048575
count_is silver f_consultation_detail 1027157
count_is gold f_mesures 1030402
count_is gold f_consult_diag_mois 979797
count_is gold f_consult_prof_mois 1001453
count_is gold f_deces_agg 69008
count_is gold f_satisfaction 1960
count_is gold f_hospitalisation 2479

step "7b. Schéma : 12 tables gold, 9 vues Trino (vues_t)"
ntab=$(trino --catalog hive --schema gold --output-format CSV --execute "SELECT count(*) FROM information_schema.tables WHERE table_schema='gold' AND table_type='BASE TABLE'" | tr -d '"\n ')
[ "$ntab" = "12" ] && pass "gold: 12 tables" || fail "gold: tables=$ntab/12"
nviews=$(trino --catalog hive --schema vues_t --output-format CSV --execute "SELECT count(*) FROM information_schema.tables WHERE table_schema='vues_t'" | tr -d '"\n ')
[ "$nviews" = "9" ] && pass "vues_t: 9 vues" || fail "vues_t: vues=$nviews/9"
nv=$(trino --catalog hive --schema vues_t --output-format CSV --execute "SELECT count(*) FROM v_taux_consultation_mensuel" | tr -d '"\n ')
[ "${nv:-0}" -gt 0 ] && pass "v_taux_consultation_mensuel non vide ($nv lignes)" || fail "v_taux_consultation_mensuel vide"
nv=$(trino --catalog hive --schema vues_t --output-format CSV --execute "SELECT count(*) FROM v_deces_region_annee" | tr -d '"\n ')
[ "${nv:-0}" -gt 0 ] && pass "v_deces_region_annee non vide ($nv lignes)" || fail "v_deces_region_annee vide"

# ----------------------------------------------------------------------------
step "8. Data lake MinIO : bucket lake, préfixes, clés normalisées, volume raw"
# ----------------------------------------------------------------------------
aws() { docker run --rm --network chu-l2_chu-net \
  -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli --endpoint-url http://chu-minio:9000 "$@" 2>/dev/null; }
aws s3 ls s3://lake/ >/dev/null 2>&1 && pass "bucket s3://lake existe" || fail "bucket s3://lake existe"
for p in raw/ silver/ gold/ warehouse/; do
  n=$(aws s3 ls "s3://lake/$p" --recursive 2>/dev/null | wc -l | tr -d ' ')
  [ "${n:-0}" -gt 0 ] && pass "prefix s3://lake/$p non vide ($n objets)" || fail "prefix s3://lake/$p vide"
done
for k in "raw/deces/deces.csv" "raw/etablissement/etablissement_sante.csv" "raw/hospitalisation/Hospitalisations.csv" "raw/satisfaction/ESATIS_2019.csv" "raw/satisfaction/ESATIS_2020.csv" "raw/referentiels/activite_professionnel_sante.csv" "gold/f_mesures/" "silver/f_consultation_detail/"; do
  aws s3 ls "s3://lake/$k" >/dev/null 2>&1 && pass "clé présente: $k" || fail "clé absente: $k"
done
raw_nb=$(aws s3 ls s3://lake/raw/ --recursive 2>/dev/null | wc -l | tr -d ' ')
[ "${raw_nb:-0}" -ge "$SRC_NB" ] && pass "raw contient >= sources ($raw_nb >= $SRC_NB objets)" || fail "raw trop petit ($raw_nb < $SRC_NB)"

# ----------------------------------------------------------------------------
step "9. Superset : connexion 'Trino CHU' + dashboards créés par l'init (build_dashboards)"
# ----------------------------------------------------------------------------
ssql() { docker exec chu-superset-db psql -U superset -d superset -t -A -c "$1" 2>/dev/null | tr -d ' '; }
n=$(ssql "SELECT count(*) FROM dbs WHERE database_name='Trino CHU';")
[ "$n" = "1" ] && pass "connexion Superset 'Trino CHU' présente" || fail "connexion Superset 'Trino CHU' (n=$n)"
n=$(ssql "SELECT count(*) FROM dashboards WHERE slug='chu-indicateurs-metier' AND published;")
[ "$n" = "1" ] && pass "dashboard 'CHU — Indicateurs métier' publié (slug chu-indicateurs-metier)" || fail "dashboard chu-indicateurs-metier (n=$n)"
n=$(ssql "SELECT count(*) FROM slices;")
[ "${n:-0}" -ge 8 ] && pass "charts (slices): $n >= 8" || fail "charts (slices) seulement $n (<8)"
n=$(ssql "SELECT count(*) FROM tables;")
[ "${n:-0}" -ge 8 ] && pass "datasets (tables): $n >= 8" || fail "datasets (tables) seulement $n (<8)"
n=$(ssql "SELECT count(*) FROM dashboard_slices ds JOIN dashboards d ON d.id=ds.dashboard_id WHERE d.slug='chu-indicateurs-metier';")
[ "${n:-0}" -ge 8 ] && pass "dashboard contient $n charts (>=8)" || fail "dashboard ne contient que $n charts (<8)"

# ----------------------------------------------------------------------------
step "10. Source S1 PostgreSQL peuplée + résultats DQ archivés"
# ----------------------------------------------------------------------------
n=$(docker exec chu-s1-postgres psql -U chu -d chu_s1 -t -A -c "SELECT count(*) FROM information_schema.tables WHERE table_schema='public';" 2>/dev/null | tr -d ' ')
[ "${n:-0}" -gt 0 ] && pass "S1 chu_s1: $n tables" || fail "S1 chu_s1 vide"
ndq=$(find "$SCRIPTS_DIR/05_qualite/results" -name 'dq_*.csv' -size +0c 2>/dev/null | wc -l | tr -d ' ')
[ "$ndq" -ge 9 ] && pass "DQ: $ndq fichiers résultats non vides (dont completude_faits + integrite_fk)" || fail "DQ: seulement $ndq fichiers (>=9 attendus)"

# ----------------------------------------------------------------------------
step "11. Re-pause des 2 DAGs (état final propre = démarrage neuf)"
# ----------------------------------------------------------------------------
docker exec chu-airflow-scheduler airflow dags pause lakehouse_init >>"$LOG" 2>&1
docker exec chu-airflow-scheduler airflow dags pause lakehouse_daily >>"$LOG" 2>&1
paused=$(psql_af "SELECT count(*) FROM dag WHERE dag_id IN ('lakehouse_init','lakehouse_daily') AND is_paused;")
[ "$paused" = "2" ] && pass "2 DAGs remis en pause" || fail "re-pause DAGs (paused=$paused)"

# ----------------------------------------------------------------------------
step "RESULTAT"
# ----------------------------------------------------------------------------
log ""
log "================ SMOKE TEST : $PASS PASS / $FAIL FAIL ================"
if [ "$FAIL" -gt 0 ]; then
  log "Checks en échec :"
  for c in "${FAILED_CHECKS[@]}"; do log "  - $c"; done
  log "Log complet : $LOG"
  exit 1
fi
log "100% PASS — from scratch OK, données conformes. Log : $LOG"
exit 0
