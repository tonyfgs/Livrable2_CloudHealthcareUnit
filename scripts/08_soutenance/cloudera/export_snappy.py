"""Export des tables du lakehouse vers des Parquet SNAPPY non partitionnes, lisibles par Hive 1.1 (Cloudera CDH 5.12).

Pourquoi : Spark ecrit en zstd, que Hive 1.1 de la VM Cloudera Quickstart ne sait pas lire ; les colonnes de
partition (annee, sexe...) sont dans les chemins, pas dans les fichiers ; le type DATE n'est pas lu en Parquet
par Hive 1.1. Ce script relit chaque table du lac, la reecrit en un jeu de fichiers snappy contenant TOUTES les colonnes
(dates converties en texte AAAA-MM-JJ) et genere le DDL Hive 1.1 correspondant.

Lancement (depuis le dossier scripts, stack Docker demarree) : voir ../GUIDE_POWERBI_CLOUDERA.md, section 1.2
"""
import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DateType, TimestampType, IntegerType, LongType, DoubleType, BooleanType, ShortType, FloatType

LAKE = os.environ.get("LAKE_ROOT", "s3a://lake")
OUT = os.environ.get("EXPORT_DIR", "file:///tmp/export_cloudera")
TABLES = [  # (base, table)
    ("gold", "d_temps"), ("gold", "d_patient"), ("gold", "d_etablissement"), ("gold", "d_localisation"),
    ("gold", "d_diagnostic"), ("gold", "d_professionnel"), ("gold", "f_mesures"), ("gold", "f_hospitalisation"),
    ("gold", "f_deces_agg"), ("gold", "f_satisfaction"), ("gold", "f_consult_diag_mois"), ("gold", "f_consult_prof_mois"),
    ("silver", "f_consultation_detail"),
]
HDFS_ROOT = "/user/cloudera/lake"

spark = (SparkSession.builder.appName("export-cloudera-snappy").master("local[2]")
         .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("MINIO_ENDPOINT", "http://minio:9000"))
         .config("spark.hadoop.fs.s3a.access.key", os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
         .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("MINIO_SECRET_KEY", "minioadmin"))
         .config("spark.hadoop.fs.s3a.path.style.access", "true")
         .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.sql.parquet.compression.codec", "snappy")
         .config("spark.sql.parquet.writeLegacyFormat", "true")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())


def hive_type(t):
    if isinstance(t, (IntegerType, ShortType)): return "int"
    if isinstance(t, LongType): return "bigint"
    if isinstance(t, (DoubleType, FloatType)): return "double"
    if isinstance(t, BooleanType): return "boolean"
    return "string"


ddl = ["-- Genere par export_snappy.py : DDL Hive 1.1 (Cloudera CDH 5.12). A executer dans Hue ou beeline.",
       "CREATE DATABASE IF NOT EXISTS gold;", "CREATE DATABASE IF NOT EXISTS silver;", ""]
resume = []
for base, name in TABLES:
    df = spark.read.option("basePath", f"{LAKE}/{base}/{name}").parquet(f"{LAKE}/{base}/{name}")
    for c in df.schema.fields:  # DATE / TIMESTAMP -> texte (Hive 1.1 ne lit pas DATE en Parquet)
        if isinstance(c.dataType, (DateType, TimestampType)):
            df = df.withColumn(c.name, F.date_format(F.col(c.name), "yyyy-MM-dd").cast("string"))
    n = df.count()
    df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(f"{OUT}/{base}/{name}")
    cols = ",\n  ".join(f"`{c.name}` {hive_type(c.dataType)}" for c in df.schema.fields)
    ddl.append(f"DROP TABLE IF EXISTS {base}.{name};\nCREATE EXTERNAL TABLE {base}.{name} (\n  {cols}\n)\n"
               f"STORED AS PARQUET\nLOCATION '{HDFS_ROOT}/{base}/{name}';\n")
    resume.append(f"{base}.{name};{n}")
    print(f"OK {base}.{name} : {n} lignes", flush=True)

with open("/tmp/export_cloudera/ddl_hive_cloudera.sql", "w", encoding="utf-8") as f:
    f.write("\n".join(ddl))
with open("/tmp/export_cloudera/volumes_export.csv", "w", encoding="utf-8") as f:
    f.write("table;lignes\n" + "\n".join(resume) + "\n")
spark.stop()
