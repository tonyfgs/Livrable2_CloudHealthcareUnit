#!/bin/bash
# ============================================================================
# Livrable 2 - CHU - Initialisation de la source S1 (PostgreSQL ChuSimulation)
# ----------------------------------------------------------------------------
# Le dump `DATA2023` est un pg_dump au format CUSTOM (PGDMP). Il est restauré
# dans la base chu_s1 créée par l'image postgres (POSTGRES_DB=chu_s1).
#   --no-owner : le dump a été produit par l'utilisateur `postgres` ; on
#                restaure en tant que `chu` sans recréer les propriétaires.
# ============================================================================
set -e

echo "[s1-postgres] restauration du dump ChuSimulation (format custom) ..."
pg_restore --no-owner --role="${POSTGRES_USER}" -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" \
  /docker-entrypoint-initdb.d/DATA2023.dump

echo "[s1-postgres] restauration terminée."
psql -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -c "SELECT 'Patient' AS table, COUNT(*) FROM \"Patient\";"
