# ============================================================================
# Livrable 2 - CHU - Image Superset du projet
# ----------------------------------------------------------------------------
# Image officielle Apache Superset 4.1.1 + driver PostgreSQL Python (psycopg2),
# absent de l'image officielle mais requis pour la base de métadonnées
# `superset-db` (service PostgreSQL du docker-compose.yml).
# ============================================================================
FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.9
USER superset
