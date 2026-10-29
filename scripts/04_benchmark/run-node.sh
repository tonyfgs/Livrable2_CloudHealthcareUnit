#!/bin/sh
# ============================================================================
# Livrable 2 - CHU - Exécute node dans le conteneur chu-bench-duckdb (image
# du projet, node embarqué) : rien n'est installé sur le poste hôte.
#
# Utilisé par run_bench_ml.sh (gen_listes.js) et pour la synthèse (synthese.js).
#
# Variables d'environnement (surchargées) :
#   L2WORK       workspace du benchmark        (défaut /tmp/l2work)
#   COMPOSE_FILE chemin du docker-compose.yml  (défaut: ../docker-compose.yml
#                                              relatif à ce script)
#
# Usage :  ./run-node.sh <script.js> [args...]
# ============================================================================
L2WORK=${L2WORK:-/tmp/l2work}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
COMPOSE_FILE=${COMPOSE_FILE:-"$SCRIPT_DIR/../docker-compose.yml"}

if ! docker image inspect chu-bench-duckdb:latest >/dev/null 2>&1; then
  echo "image chu-bench-duckdb absente : construction (docker compose build bench-duckdb) ..."
  docker compose -f "$COMPOSE_FILE" build bench-duckdb || exit 1
fi

exec docker run --rm \
  --network none \
  --entrypoint node \
  -v "$L2WORK:$L2WORK" \
  -w "$L2WORK" \
  chu-bench-duckdb:latest "$@"
