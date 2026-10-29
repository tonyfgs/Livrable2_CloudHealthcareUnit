# ============================================================================
# Livrable 2 - Cloud Healthcare Unit (CHU) - Module commun PySpark
# ----------------------------------------------------------------------------
# Rôle d'Apache Spark dans la stack : couche TRAITEMENT (ETL/ELT) - lecture
# des sources (PostgreSQL S1 via JDBC, CSV du data lake MinIO), nettoyage,
# transformation, écriture des tables Gold/Silver dans le data lake (MinIO)
# au format Parquet partitionné. Le catalogue (Hive Metastore) est
# alimenté SÉPARÉMENT par le DAG Airflow (MSCK REPAIR TABLE via beeline) :
# Spark 3.5.1 embarque un client Hive 2.3 incompatible du Hive Metastore 4.x.
#
# Toute la stack tourne dans Docker (voir scripts/docker-compose.yml) :
# aucune installation sur le poste hôte.
#
# Variables d'environnement (surchargées par le DAG Airflow) :
#   LAKE_ROOT          racine du data lake          (défaut s3a://lake)
#   MINIO_ENDPOINT     endpoint MinIO                (défaut http://minio:9000)
#   MINIO_ACCESS_KEY / MINIO_SECRET_KEY             (défaut minioadmin)
#   HIVE_METASTORE_URI thrift Hive Metastore         (défaut thrift://hive-metastore:9083)
#   PG_HOST / PG_PORT / PG_DB / PG_USER / PG_PASSWORD  source S1 (défaut s1-postgres:5432/chu_s1/chu/chu)
#   WAREHOUSE_DIR      répertoire entrepôt Spark   (défaut s3a://lake/warehouse)
# ============================================================================
import os

from pyspark.sql import SparkSession, functions as F

LAKE_ROOT = os.environ.get("LAKE_ROOT", "s3a://lake")
MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
HIVE_METASTORE_URI = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")
WAREHOUSE_DIR = os.environ.get("WAREHOUSE_DIR", f"{LAKE_ROOT}/warehouse")

PG_HOST = os.environ.get("PG_HOST", "s1-postgres")
PG_PORT = os.environ.get("PG_PORT", "5432")
PG_DB = os.environ.get("PG_DB", "chu_s1")
PG_USER = os.environ.get("PG_USER", "chu")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "chu")
PG_JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# Racine des données brutes (zone RAW du lake) : sources S2/S3/S4 déposées
# telles quelles dans MinIO par le script d'ingestion (hors périmètre de ce
# livrable, cf. rapport §Stack - couche ingestion).
RAW = f"{LAKE_ROOT}/raw"
GOLD = f"{LAKE_ROOT}/gold"
SILVER = f"{LAKE_ROOT}/silver"


def get_spark(app_name: str) -> SparkSession:
    """Session Spark configurée pour MinIO (S3A).

    Le catalogue est IN-MEMORY (pas de Hive Metastore côté Spark) : Spark 3.5.1
    embarque un client Hive 2.3 dont le protocole Thrift est incompatible du
    Hive Metastore 4.x du cluster (« Invalid method name: get_table »).
    Architecture retenue : Spark écrit les fichiers Parquet partitionnés
    directement sur le lake (save_gold -> saveAsTable avec option
    path, sans metastore) ; le Hive Metastore est rafraîchi SÉPARÉMENT par le
    DAG (MSCK REPAIR TABLE via beeline, tâche msck_repair_metastore), puis
    Trino/Superset lisent via le catalogue.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.sql.catalogImplementation", "in-memory")
        # S3A : accès au data lake MinIO
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        # Parquet : compression + statistiques pour le pruning Trino
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.parquet.writeLegacyFormat", "false")
        # Performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.files.maxRecordsPerFile", "5000000")
        .getOrCreate()
    )


def read_lake(spark: SparkSession, table: str):
    """Lit une table du lake directement en Parquet, SANS le Hive Metastore.
    'gold.f_mesures' -> s3a://lake/gold/f_mesures. Les colonnes de partition
    (annee=.../sexe=...) sont découvertes automatiquement (hive_partitioning)."""
    path = f"{LAKE_ROOT}/{table.replace('.', '/')}"
    return spark.read.parquet(path)


def read_postgres(spark: SparkSession, table: str):
    """Lecture d'une table de la source S1 (PostgreSQL) via JDBC."""
    return (
        spark.read.format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", f'public."{table}"')
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def read_raw_csv(spark: SparkSession, relative_path: str):
    """Lecture d'un CSV de la zone RAW (séparateur ';', encodage UTF-8).

    `relative_path` est relatif à RAW, ex. 'hospitalisation/Hospitalisations.csv'.
    """
    return (
        spark.read.option("header", "true")
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("nullValue", "")
        .option("maxLineSize", "10000000")
        .csv(f"{RAW}/{relative_path}")
    )


def age_revolu(event_col, naissance_col):
    """Âge révolu à la date d'événement (équivalent Spark de la macro DuckDB
    date_diff('year', dn, de) - ajustement anniversaire du PoC).

    floor(months_between(event, naissance) / 12) == âge révolu.
    """
    return F.floor(F.months_between(event_col, naissance_col) / 12.0)


def tranche_age(age_col):
    """Tranche d'âge (grille du projet) à partir d'un âge révolu.

    Équivalent de la macro DuckDB tranche_of_age du PoC :
    NULL -> 'INCONNUE', <15 -> '0-14', <30 -> '15-29', <45 -> '30-44',
    <60 -> '45-59', <75 -> '60-74', sinon '75+'.
    """
    return (
        F.when(age_col.isNull(), F.lit("INCONNUE"))
        .when(age_col < 15, F.lit("0-14"))
        .when(age_col < 30, F.lit("15-29"))
        .when(age_col < 45, F.lit("30-44"))
        .when(age_col < 60, F.lit("45-59"))
        .when(age_col < 75, F.lit("60-74"))
        .otherwise(F.lit("75+"))
    )


def dept_of(code_commune_col):
    """Règle INSEE : code commune (5 car.) -> département.

    2 premiers caractères, sauf 97x -> 3 caractères (Outre-mer),
    20x -> '2A' (Corse-du-Sud ; la distinction 2A/2B du référentiel est
    volontairement simplifiée, cf. rapport qualité - concordance 99,74 %).
    """
    c = F.trim(code_commune_col)
    return (
        F.when(c.isNull() | (c == ""), F.lit(None).cast("string"))
        # F.substring (pos/len en int) et pas F.left : depuis pyspark 3.5,
        # F.left exige une Column pour len (NOT_COLUMN_OR_STR sinon)
        .when(F.substring(c, 1, 2) == "97", F.substring(c, 1, 3))
        .when(F.substring(c, 1, 2) == "20", F.lit("2A"))
        .otherwise(F.substring(c, 1, 2))
    )


def sexe_normalise(sexe_col):
    """Normalisation S1 : 'male'/'female' -> 'M'/'F', sinon 'I'."""
    return (
        F.when(F.lower(F.trim(sexe_col)) == "male", F.lit("M"))
        .when(F.lower(F.trim(sexe_col)) == "female", F.lit("F"))
        .otherwise(F.lit("I"))
    )


def save_gold(df, table: str, path: str, partition_cols=None):
    """Écrit une table Gold/Silver : EXTERNAL (chemin explicite sur le lake),
    Parquet, partitionnée, triée sur les clés de jointure (min/max intra-fichier).

    - table        : nom qualifié 'gold.f_mesures' (catalogue IN-MEMORY Spark :
    #                   la définition n'est PAS poussée dans le Hive Metastore,
    #                   qui est rafraîchi séparément par MSCK REPAIR côté DAG)
    - path         : chemin S3A, ex. 's3a://lake/gold/f_mesures'
    - partition_cols : liste de colonnes de partition (pruning côté Trino)

    PAS de bucketing (bucketBy) : Spark n'écrit que les buckets non vides et
    Trino 484 rejette alors la table bucketée ("number of files does not match
    the declared bucket count"). Le tri intra-fichier est conservé via
    sortWithinPartitions sur les clés de jointure (quand elles existent).
    """
    # Pas de DROP TABLE : le catalogue est in-memory (pas de metastore Hive,
    # incompatible Hive 4.x) ; le mode overwrite vide le répertoire cible
    # (pas de partitions obsolètes). Le catalogue in-memory ne crée pas les
    # schémas tout seul : CREATE DATABASE IF NOT EXISTS avant saveAsTable.
    spark = df.sparkSession
    if "." in table:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {table.split('.')[0]}")
    # Tri intra-partition sur les clés de jointure -> statistiques min/max par
    # fichier Parquet -> pruning intra-fichier côté Trino (sans bucketing)
    if {"id_etablissement", "id_patient"} <= set(df.columns):
        df = df.sortWithinPartitions("id_etablissement", "id_patient")
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    # option("path") + saveAsTable => table EXTERNE (les données restent dans
    # le lake même après DROP TABLE ; conforme au DDL 01_schema_gold_hive.sql)
    writer.option("path", path).saveAsTable(table)
