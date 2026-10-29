# ============================================================================
# Livrable 2 - CHU - Airflow : DAG QUOTIDIEN du lakehouse (orchestration)
# ----------------------------------------------------------------------------
# Rôle d'Apache Airflow dans la stack : couche ORCHESTRATION. Ce DAG enchaîne
# les traitements Spark (couche traitement) qui alimentent le data lake MinIO
# (couche stockage), publient les métadonnées dans le Hive Metastore (couche
# catalogue), puis exécute les contrôles qualité et valide la disponibilité
# de Trino pour Superset (couche visualisation).
#
# Pipeline :
#   load_dimensions ──┬──> load_faits_detail ──┬──> build_f_mesures ──┐
#                     │                         │                      ├──> dq_checks --> dispo_trino_superset
#                     └──> build_f_deces_agg ──┘                      │
#                                             └──> build_agregats ───┘
#
# Ordre d'exécution :
#   1. load_dimensions.py    : 6 dimensions (d_temps, d_patient, d_etablissement,
#                              d_localisation, d_diagnostic, d_professionnel)
#   2. load_faits_detail.py  : silver.f_consultation_detail, gold.f_hospitalisation,
#                              gold.f_satisfaction  (dépend de 1 : d_diagnostic,
#                              d_etablissement)
#   3. build_f_deces_agg.py  : gold.f_deces_agg (25,09 M décès INSEE -> grain annuel,
#                              indépendant de 1 et 2, exécuté en parallèle)
#   4. build_f_mesures.py    : gold.f_mesures (fait consolidé, dépend de 2 et 3)
#   5. build_agregats.py     : gold.f_consult_diag_mois, gold.f_consult_prof_mois
#                              (dépend de 2)
#   6. dq_checks             : 6 familles de contrôles qualité (script DuckDB du
#                              benchmark, exécuté dans son conteneur dédié)
#   7. dispo_trino_superset  : santé Trino + rappel dashboards live
#
# Variables d'environnement transmises aux jobs Spark (surchargées via
# Airflow > Admin > Variables si besoin) :
#   LAKE_ROOT=s3a://lake  MINIO_ENDPOINT=http://minio:9000
#   HIVE_METASTORE_URI=thrift://hive-metastore:9083
#   PG_HOST=s1-postgres PG_DB=chu_s1 PG_USER=chu PG_PASSWORD=chu
# ============================================================================
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_CONN = os.environ.get("SPARK_CONN_ID", "spark_default")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
JOBS = "/opt/spark/jobs"

# Packages nécessaires aux jobs : driver JDBC PostgreSQL (source S1) et
# connecteur Hadoop S3A (data lake MinIO). Hadoop 3.3 (Spark 3.5).
SPARK_PACKAGES = (
    "org.postgresql:postgresql:42.7.3,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

# Variables d'environnement communes à tous les jobs Spark (cf. 02_spark/common.py)
SPARK_ENV = {
    "LAKE_ROOT": "s3a://lake",
    "MINIO_ENDPOINT": "http://minio:9000",
    "MINIO_ACCESS_KEY": "minioadmin",
    "MINIO_SECRET_KEY": "minioadmin",
    "HIVE_METASTORE_URI": "thrift://hive-metastore:9083",
    "WAREHOUSE_DIR": "s3a://lake/warehouse",
    "PG_HOST": "s1-postgres",
    "PG_PORT": "5432",
    "PG_DB": "chu_s1",
    "PG_USER": "chu",
    "PG_PASSWORD": "chu",
}


def spark_job(task_id: str, application: str) -> SparkSubmitOperator:
    """Job SparkSubmitOperator standardisé (master, packages, env, logs)."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN,
        application=f"{JOBS}/{application}",
        conf={
            "spark.master": SPARK_MASTER,
            "spark.submit.deployMode": "client",
            # S3A côté cluster (workers) : même configuration que le driver
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minioadmin",
            "spark.hadoop.fs.s3a.secret.key": "minioadmin",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            # Hive Metastore côté workers
            "spark.hive.metastore.uris": "thrift://hive-metastore:9083",
        },
        packages=SPARK_PACKAGES,
        env_vars=SPARK_ENV,
        verbose=True,
    )


default_args = {
    "owner": "chu-data-team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lakehouse_daily",
    description="Pipeline quotidien CHU : dimensions -> faits -> mesures -> DQ",
    default_args=default_args,
    schedule="0 2 * * *",   # tous les jours à 02:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["chu", "livrable2", "quotidien"],
) as dag:

    load_dimensions = spark_job("load_dimensions", "load_dimensions.py")
    load_faits_detail = spark_job("load_faits_detail", "load_faits_detail.py")
    build_f_deces_agg = spark_job("build_f_deces_agg", "build_f_deces_agg.py")
    build_f_mesures = spark_job("build_f_mesures", "build_f_mesures.py")
    build_agregats = spark_job("build_agregats", "build_agregats.py")

    # -------------------------------------------------------------------------
    # 6. Contrôles qualité (6 familles : complétude, validité, doublons,
    #    référentiel, temporalité, volumes). Le script dq_checks.sql est écrit
    #    pour le moteur DuckDB du conteneur de benchmark (mêmes règles que le
    #    rapport qualité) ; les résultats CSV sont archivés dans
    #    05_qualite/results/. Le conteneur chu-bench-duckdb monte : le script
    #    (/opt/l2work/dq), les résultats (/opt/l2work/dq/results), les sources
    #    brutes (/data-projet) et le staging du workspace (/opt/l2work/staging).
    # -------------------------------------------------------------------------
    dq_checks = BashOperator(
        task_id="dq_checks",
        bash_command=(
            "docker exec chu-bench-duckdb sh -c '"
            "cd /opt/l2work && duckdb /tmp/dq.duckdb < /opt/l2work/dq/dq_checks.sql"
            "' && echo 'DQ OK - résultats dans 05_qualite/results/'"
        ),
    )

    # -------------------------------------------------------------------------
    # 7. Disponibilité Trino pour Superset. Les tableaux de bord Superset
    #    interrogent Trino EN DIRECT (datasets SQL virtuels, cache désactivé
    #    dans superset_config.py) : aucune materialisation à rafraîchir. Cette
    #    tâche valide la santé de Trino et la visibilité du schéma gold.
    # -------------------------------------------------------------------------
    dispo_trino_superset = BashOperator(
        task_id="dispo_trino_superset",
        bash_command=(
            "docker exec chu-trino trino --server http://localhost:8080 "
            "--execute 'SELECT version()' && "
            "docker exec chu-trino trino --server http://localhost:8080 "
            "--catalog hive --schema gold "
            "--execute 'SELECT COUNT(*) AS lignes_f_mesures FROM gold.f_mesures'"
        ),
    )

    # Dépendances
    load_dimensions >> load_faits_detail
    load_dimensions >> build_f_deces_agg
    [load_faits_detail, build_f_deces_agg] >> build_f_mesures
    load_faits_detail >> build_agregats
    [build_f_mesures, build_agregats] >> dq_checks >> dispo_trino_superset
