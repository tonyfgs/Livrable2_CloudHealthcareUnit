#!/bin/sh
# ============================================================================
# Livrable 2 - CHU - Exécute le CLI DuckDB dans un conteneur Docker (sans
# aucune installation sur le poste hôte : DuckDB tourne DANS l'image du
# projet chu-bench-duckdb, construite par docker compose à partir de
# Dockerfile.duckdb).
#
# - l'image chu-bench-duckdb embarque le binaire DuckDB (et node) : aucun
#   téléchargement sur le poste hôte
# - les données brutes du projet ("DATA Projet", variable DATA_PROJET) sont
#   montées en bind au point fixe /data-projet (même convention que le
#   service bench-duckdb du docker-compose.yml)
# - le workspace $L2WORK est monté en bind (scripts, staging, résultats)
# - le réseau permet de joindre le conteneur PostgreSQL S1 (service
#   "s1-postgres", conteneur chu-s1-postgres)
# - -i : transmet stdin (pour `./run-duckdb.sh < script.sql`)
#
# Variables d'environnement (surchargées) :
#   L2WORK       workspace du benchmark          (défaut /tmp/l2work)
#   DATA_PROJET  chemin absolu de "DATA Projet"  (défaut: voir ci-dessous)
#   NETWORK      réseau Docker                   (défaut: auto - chu-l2_chu-net
#                                                si la stack compose tourne,
#                                                sinon chu-net)
#   COMPOSE_FILE chemin du docker-compose.yml    (défaut: ../docker-compose.yml
#                                                relatif à ce script)
# ============================================================================
L2WORK=${L2WORK:-/tmp/l2work}
DATA_PROJET=${DATA_PROJET:-"/Users/tonyfages/CESI/4A/Big Data/projet/DATA Projet"}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
COMPOSE_FILE=${COMPOSE_FILE:-"$SCRIPT_DIR/../docker-compose.yml"}

if [ -z "$NETWORK" ]; then
  if docker network ls --format '{{.Name}}' 2>/dev/null | grep -qx 'chu-l2_chu-net'; then
    NETWORK=chu-l2_chu-net
  else
    NETWORK=chu-net
  fi
fi

if ! docker image inspect chu-bench-duckdb:latest >/dev/null 2>&1; then
  echo "image chu-bench-duckdb absente : construction (docker compose build bench-duckdb) ..."
  docker compose -f "$COMPOSE_FILE" build bench-duckdb || exit 1
fi

exec docker run --rm -i \
  --network "$NETWORK" \
  --entrypoint duckdb \
  -v "$DATA_PROJET:/data-projet:ro" \
  -v "$L2WORK:$L2WORK" \
  -w "$L2WORK" \
  chu-bench-duckdb:latest "$@"
