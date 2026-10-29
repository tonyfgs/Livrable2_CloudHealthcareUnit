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
# docker.io : client Docker CLI (les DAGs pilotent beeline/trino/duckdb via
# `docker exec`, socket monté dans le scheduler).
# default-jre-headless : JRE indispensable au SparkSubmitOperator — le
# provider appelle `spark-submit` (embarqué par le package pyspark), qui
# exige JAVA_HOME (échec « JAVA_HOME is not set » sans JRE).
RUN apt-get update \
 && apt-get install -y --no-install-recommends docker.io default-jre-headless \
 && rm -rf /var/lib/apt/lists/*

# default-java = symlink géré par Debian vers la JVM installée (pointe vers
# java-17-openjdk-<arch> : portable amd64 / arm64)
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Provider Spark (SparkSubmitOperator du DAG lakehouse_daily) : installé au
# BUILD (pas de pip au démarrage des conteneurs, démarrage plus rapide et
# reproductible). Version 3.0.0 = dernière version compatible Airflow 2.9.x
# (les providers >= 4.x sont réservés à Airflow 3.x : ils tirent
# apache-airflow-core / apache-airflow 3.x et cassent l'image 2.9.3).
# `python -m pip` (et non `pip`) : l'image officielle installe un wrapper
# `pip` qui refuse de s'exécuter en root.
USER root
RUN python -m pip install --no-cache-dir apache-airflow-providers-apache-spark==3.0.0
USER airflow
