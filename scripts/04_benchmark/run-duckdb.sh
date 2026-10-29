#!/bin/sh
# ============================================================================
# Livrable 2 - CHU - Exécute le CLI DuckDB dans un conteneur Docker (sans
# aucune installation sur le poste hôte : DuckDB tourne DANS l'image
# postgres:15, comme un "sidecar" jetable).
#
# - le binaire DuckDB (Linux) est lu dans $L2WORK/duckdb-linux/duckdb
# - les données brutes du projet ("DATA Projet", variable DATA_PROJET) sont
#   montées en bind au point fixe /data-projet (même convention que le
#   service bench-duckdb du docker-compose.yml)
# - le workspace $L2WORK est monté en bind (scripts, staging, résultats)
# - le réseau chu-net permet de joindre le conteneur PostgreSQL S1
#   (service "s1-postgres", conteneur chu-s1-postgres)
# - -i : transmet stdin (pour `./run-duckdb.sh < script.sql`)
#
# Variables d'environnement (surchargées) :
#   L2WORK       workspace du benchmark          (défaut /tmp/l2work)
#   DATA_PROJET  chemin absolu de "DATA Projet"  (défaut: voir ci-dessous)
#   DUCKDB_BIN   binaire DuckDB                  (défaut $L2WORK/duckdb-linux/duckdb)
#   NETWORK      réseau Docker                   (défaut chu-net)
#
# Le binaire est téléchargé automatiquement s'il est absent :
#   https://github.com/duckdb/duckdb/releases (v2.0.0, linux amd64/arm64)
# ============================================================================
L2WORK=${L2WORK:-/tmp/l2work}
DATA_PROJET=${DATA_PROJET:-"/Users/tonyfages/CESI/4A/Big Data/projet/DATA Projet"}
DUCKDB_BIN=${DUCKDB_BIN:-$L2WORK/duckdb-linux/duckdb}
NETWORK=${NETWORK:-chu-net}

if [ ! -x "$DUCKDB_BIN" ]; then
  echo "binaire DuckDB absent : téléchargement dans $L2WORK/duckdb-linux/ ..."
  mkdir -p "$L2WORK/duckdb-linux"
  ARCH=$(uname -m)
  case "$ARCH" in
    arm64)  DUCKDB_ARCH=aarch64 ;;
    *)      DUCKDB_ARCH=amd64 ;;
  esac
  curl -sfL -o "$L2WORK/duckdb.zip" \
    "https://github.com/duckdb/duckdb/releases/download/v2.0.0/duckdb_cli-linux-${DUCKDB_ARCH}.zip" \
    || { echo "échec du téléchargement de DuckDB"; exit 1; }
  unzip -j -o "$L2WORK/duckdb.zip" -d "$L2WORK/duckdb-linux/" >/dev/null
  rm -f "$L2WORK/duckdb.zip"
fi

exec docker run --rm -i \
  --network "$NETWORK" \
  --entrypoint "$DUCKDB_BIN" \
  -e HOME="$L2WORK" \
  -v "$DATA_PROJET:/data-projet:ro" \
  -v "$L2WORK:$L2WORK" \
  -w "$L2WORK" \
  postgres:15 "$@"
