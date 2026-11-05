#!/bin/bash
# ============================================================================
# Soutenance (Livrable 3) - export des jeux de donnees des visuels depuis Trino
# (lakehouse Docker : MinIO + Hive Metastore + Trino). Chaque requete de sql/
# produit un CSV dans data/. Prerequis : stack demarree, lakehouse_init et
# lakehouse_daily executes.
# Usage : cd scripts/08_soutenance && ./export_donnees.sh
# ============================================================================
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p data
for f in sql/d*.sql; do
  n=$(basename "$f" .sql)
  docker exec -i chu-trino trino --server http://localhost:8080 --output-format CSV_HEADER < "$f" > "data/$n.csv"
  echo "$n -> data/$n.csv ($(($(wc -l < data/$n.csv) - 1)) lignes)"
done
