#!/bin/bash
# ============================================================================
# Livrable 2 - CHU - Harnais de benchmark côté Trino (plateforme Docker)
# ----------------------------------------------------------------------------
# Exécute les 10 requêtes du benchmark (queries/q01..q10) contre le schéma
# Gold déployé dans la stack docker-compose (Trino + Hive Metastore + MinIO),
# en mesurant le temps mur de chaque requête (3 répétitions) et en capturant
# le plan EXPLAIN ANALYZE de deux requêtes représentatives.
#
# Prérequis :
#   - docker compose up -d            (stack démarrée)
#   - DAG lakehouse_init exécuté      (DDL + vues métier)
#   - DAG lakehouse_daily exécuté     (données chargées par Spark)
#
# Usage :
#   cd scripts/04_benchmark
#   ./harness_trino.sh
#
# Résultats : results/trino/bench_trino.csv (temps), results/trino/qXX.csv
#            (résultats métier), results/trino/explain_qXX.txt (plans).
# ============================================================================
set -euo pipefail

cd "$(dirname "$0")"
RESULTS=results/trino
mkdir -p "$RESULTS"

TRINO="docker compose -f ../docker-compose.yml exec -T trino trino --server http://trino:8080 --catalog hive --schema gold"
CATALOG_FLAGS="--catalog hive --schema gold"
REPS=3

echo "== Vérification de Trino =="
docker compose -f ../docker-compose.yml exec -T trino trino \
  --server http://trino:8080 --execute 'SELECT version()' | head -1

echo "== Création des vues métier (si absentes) =="
docker compose -f ../docker-compose.yml exec -T trino trino \
  --server http://trino:8080 $CATALOG_FLAGS \
  --file ../01_ddl/03_vues_metier.sql || echo "(vues déjà présentes ou créées via Hive)"

echo "== Benchmark des 10 requêtes (${REPS} répétitions) =="
printf 'requete;run;temps_ms;lignes\n' > "$RESULTS/bench_trino.csv"

for q in queries/q*.sql; do
  name=$(basename "$q" .sql)
  for run in $(seq 1 "$REPS"); do
    start=$(date +%s%N)
    $TRINO --file "$q" > "$RESULTS/${name}_r${run}.csv" 2>"$RESULTS/${name}_r${run}.err" \
      || { echo "ERREUR sur $name (run $run) :"; cat "$RESULTS/${name}_r${run}.err"; exit 1; }
    end=$(date +%s%N)
    ms=$(( (end - start) / 1000000 ))
    # trino-cli : 1ère ligne = en-têtes ; lignes = (wc -l) - 1
    lignes=$(($(wc -l < "$RESULTS/${name}_r${run}.csv") - 1))
    printf '%s;%s;%s;%s\n' "$name" "$run" "$ms" "$lignes" >> "$RESULTS/bench_trino.csv"
    echo "  $name run $run : ${ms} ms, ${lignes} lignes"
  done
  # Le résultat de référence est celui de la dernière répétition
  cp "$RESULTS/${name}_r${REPS}.csv" "$RESULTS/${name}.csv"
done

echo "== Plans EXPLAIN ANALYZE (q01, q10) =="
for name in q01_consult_etab_2019 q10_etoile_complete; do
  { echo "EXPLAIN ANALYZE"; cat "queries/${name}.sql"; } > "$RESULTS/_explain_${name}.sql"
  docker compose -f ../docker-compose.yml exec -T trino trino \
    --server http://trino:8080 $CATALOG_FLAGS \
    --file "$RESULTS/_explain_${name}.sql" > "$RESULTS/explain_${name}.txt" 2>&1 || true
  echo "  plan ${name} -> $RESULTS/explain_${name}.txt"
done

echo "== Synthèse =="
cat "$RESULTS/bench_trino.csv"
echo "Terminé. Résultats dans $RESULTS/"
