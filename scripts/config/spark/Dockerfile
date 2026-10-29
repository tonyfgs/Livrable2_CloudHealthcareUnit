# ============================================================================
# Image Spark du projet = apache/spark:3.5.1 + Python 3.12 + jars S3A/JDBC
# posés directement dans $SPARK_HOME/jars (téléchargés au BUILD depuis Maven
# Central).
#
# Pourquoi Python 3.12 embarqué : le driver spark-submit tourne dans le
# conteneur Airflow (Python 3.12) ; PySpark exige la MÊME version mineure
# côté driver et workers (PYTHON_VERSION_MISMATCH sinon). L'image
# apache/spark:3.5.1 (Ubuntu 20.04) ne fournit que Python 3.8 -> on embarque
# un interpréteur standalone 3.12 (astral-sh/python-build-standalone, asset
# choisi selon l'arch du build : x86_64 ou arm64).
#
# Pourquoi les jars dans $SPARK_HOME/jars : avec `--packages` côté DAG,
# Ivy résout côté driver puis STREAM le jar AWS SDK (280 Mo) vers les workers
# -> échec réseau/mémoire. Avec les jars déjà présents chez master ET
# workers, aucun transfert. hadoop-aws 3.3.4 = version du Hadoop embarqué
# par Spark 3.5.1 (avec AWS SDK v1 bundle 1.12.262).
# ============================================================================
FROM apache/spark:3.5.1
USER root

# --- Python 3.12 standalone (multi-arch) ------------------------------------
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "arm64" ]; then PYARCH="aarch64-unknown-linux-gnu"; \
        else PYARCH="x86_64-unknown-linux-gnu"; fi \
    && curl -fsSL "https://github.com/astral-sh/python-build-standalone/releases/download/20241016/cpython-3.12.7+20241016-${PYARCH}-install_only.tar.gz" -o /tmp/py312.tgz \
    && mkdir -p /opt/python3.12 \
    && tar -xzf /tmp/py312.tgz -C /opt/python3.12 --strip-components=1 \
    && rm /tmp/py312.tgz \
    && /opt/python3.12/bin/python3.12 --version
ENV PYSPARK_PYTHON=/opt/python3.12/bin/python3.12
ENV PYSPARK_DRIVER_PYTHON=/opt/python3.12/bin/python3.12

# --- Jars S3A (MinIO) + JDBC PostgreSQL, depuis Maven Central ----------------
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar /opt/spark/jars/
# ADD depuis une URL distante pose les droits 0600 (root seul) : le user spark
# ne pourrait pas lire les jars -> ClassNotFoundException S3AFileSystem
RUN chmod 644 /opt/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/postgresql-42.7.3.jar
USER spark
