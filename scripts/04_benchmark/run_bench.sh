#!/bin/sh
# ============================================================================
# Harnais de benchmark PoC - Livrable 2 (scénarios avant / apres)
# ----------------------------------------------------------------------------
# Pour chaque requête (q01..q10) et chaque scénario :
#   - run 1  : process DuckDB dédié (1er accès, cache froid)
#   - runs 2-5 : même process, 4 exécutions chaudes
# Les temps sont les "real" mesurés autour de chaque requête (hors startup).
#
# Variables d'environnement :
#   L2WORK   workspace contenant staging/, duckdb-linux/, run-duckdb.sh,
#            et le lien/symlink `poc` -> ce répertoire   (défaut /tmp/l2work)
#
# Usage :  ./run_bench.sh [avant|apres|"avant apres"]
# ============================================================================
L2WORK=${L2WORK:-/tmp/l2work}
cd "$L2WORK" || exit 1
export LC_ALL=C
SCENARIOS=${1:-"avant apres"}
RES=poc/results/bench_results.csv
mkdir -p poc/results
if [ ! -f "$RES" ]; then echo "requete;scenario;run;temps_ms" > "$RES"; fi
for q in q01 q02 q03 q04 q05 q06 q07 q08 q09 q10; do
  qf=$(ls poc/queries/${q}_*.sql)
  for sc in $SCENARIOS; do
    # --- run 1 : cache froid (process dédié) ---
    cat poc/queries/_timer.sql poc/queries/_$sc.sql "$qf" > poc/results/_run.sql
    t=$(./run-duckdb.sh poc/results/_bench.duckdb < poc/results/_run.sql 2>/dev/null | grep -o 'real [0-9.]*' | head -1 | cut -d' ' -f2)
    echo "$q;$sc;1;$(awk -v t="$t" 'BEGIN{printf "%.1f", t*1000}')" >> "$RES"
    # --- runs 2-5 : chauds (même process) ---
    { cat poc/queries/_timer.sql poc/queries/_$sc.sql
      for i in 2 3 4 5; do cat "$qf"; done
    } > poc/results/_run.sql
    ./run-duckdb.sh poc/results/_bench.duckdb < poc/results/_run.sql 2>/dev/null \
      | grep -o 'real [0-9.]*' | cut -d' ' -f2 \
      | awk -v q="$q" -v sc="$sc" '{printf "%s;%s;%d;%.1f\n", q, sc, NR+1, $1*1000}' >> "$RES"
  done
done
echo "terminé : $RES"
