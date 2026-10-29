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
# pyspark==3.5.1 : le spark-submit embarqué par pyspark DOIT matcher la
# version du cluster Spark standalone (services spark-master/spark-worker,
# image apache/spark:3.5.1). L'image officielle embarque pyspark 4.x :
# sans ce pin, le driver et le master ne parlent pas le même protocole RPC
# (InvalidClassException serialVersionUID -> « All masters are unresponsive »).
USER root
RUN python -m pip install --no-cache-dir apache-airflow-providers-apache-spark==3.0.0 pyspark==3.5.1

# Jars S3A/JDBC aussi dans le pyspark embarqué (driver du spark-submit lancé
# par SparkSubmitOperator) : mêmes versions que l'image chu-spark:3.5.1-s3a
# (hadoop-aws 3.3.4 = Hadoop embarqué par Spark 3.5.1, AWS SDK v1 bundle).
# Sans eux, --packages streamerait 280 Mo de AWS SDK driver->worker (échec).
ENV PYSPARK_JARS=/home/airflow/.local/lib/python3.12/site-packages/pyspark/jars
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar ${PYSPARK_JARS}/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar ${PYSPARK_JARS}/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar ${PYSPARK_JARS}/
# ADD depuis une URL distante pose les droits 0600 (root) : le user airflow
# (driver spark-submit) ne pourrait pas lire les jars -> ClassNotFoundException
RUN chmod 644 ${PYSPARK_JARS}/hadoop-aws-3.3.4.jar ${PYSPARK_JARS}/aws-java-sdk-bundle-1.12.262.jar ${PYSPARK_JARS}/postgresql-42.7.3.jar
USER airflow
