#!/bin/sh
# ============================================================================
# Harnais de benchmark - variante "après avec pruning metastore simulé"
# ----------------------------------------------------------------------------
# Le listing des partitions retenues par le filtre est fait EN AMONT (équivalent
# du pruning Hive Metastore / Trino) : la requête ne reçoit que la LISTE
# EXPLICITE des fichiers concernés (générée par gen_listes.js dans
# poc/queries/ml/). DuckDB local n'ayant pas de metastore, le glob
# `**/*.parquet` pénaliserait sinon le scénario "après" (coût de listing).
#
# Prérequis : node poc/gen_listes.js   (génère poc/queries/ml/_apres_<q>.sql)
# Usage     : ./run_bench_ml.sh
# ============================================================================
L2WORK=${L2WORK:-/tmp/l2work}
cd "$L2WORK" || exit 1
export LC_ALL=C
RES=poc/results/bench_results.csv
mkdir -p poc/results
[ -f "$RES" ] || echo "requete;scenario;run;temps_ms" > "$RES"
[ -f poc/queries/ml/_apres_q01.sql ] || node poc/gen_listes.js
for q in q01 q02 q03 q04 q05 q06 q07 q08 q09 q10; do
  qf=$(ls poc/queries/${q}_*.sql)
  # --- run 1 : cache froid (process dédié) ---
  cat poc/queries/_timer.sql poc/queries/ml/_apres_$q.sql "$qf" > poc/results/_run.sql
  t=$(./run-duckdb.sh poc/results/_bench.duckdb < poc/results/_run.sql 2>/dev/null | grep -o 'real [0-9.]*' | head -1 | cut -d' ' -f2)
  echo "$q;apres_ml;1;$(awk -v t="$t" 'BEGIN{printf "%.1f", t*1000}')" >> "$RES"
  # --- runs 2-5 : chauds (même process) ---
  { cat poc/queries/_timer.sql poc/queries/ml/_apres_$q.sql
    for i in 2 3 4 5; do cat "$qf"; done
  } > poc/results/_run.sql
  ./run-duckdb.sh poc/results/_bench.duckdb < poc/results/_run.sql 2>/dev/null \
    | grep -o 'real [0-9.]*' | cut -d' ' -f2 \
    | awk -v q="$q" '{printf "%s;apres_ml;%d;%.1f\n", q, NR+1, $1*1000}' >> "$RES"
done
echo "terminé : $RES (apres_ml)"
