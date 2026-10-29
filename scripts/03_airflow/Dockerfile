# ============================================================================
# Livrable 2 - CHU - Image Airflow du projet
# ----------------------------------------------------------------------------
# Image officielle Apache Airflow + client Docker CLI : les DAGs pilotent les
# services de la stack (beeline Hive, trino-cli, conteneur DuckDB de benchmark)
# via `docker exec`, en utilisant le socket Docker monté dans le scheduler
# (/var/run/docker.sock, voir docker-compose.yml).
# ============================================================================
FROM apache/airflow:2.9.3

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io \
 && rm -rf /var/lib/apt/lists/*
USER airflow
