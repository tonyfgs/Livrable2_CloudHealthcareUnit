# ============================================================================
# Livrable 2 - CHU - Spark : fait CONSOLIDÉ f_mesures (couche Gold, étoile)
# ----------------------------------------------------------------------------
# Équivalent PySpark du script PoC 02c_f_mesures.sql (DuckDB).
#
# Grain : annee_mois x id_etablissement x id_patient x sexe x tranche_age
#         x id_localisation
#   - consultations / hospitalisations : grain MENSUEL (annee_mois = 'AAAA-MM')
#   - décès / satisfaction            : grain ANNUEL  (sentinelle 'AAAA-00')
# 4 mesures : nbre_consultation, nbre_hospitalisation, total_satisfaction,
#             nbre_deces.
# sexe / tranche_age / annee = colonnes de partition (pruning démographique
# et temporel) ; region dénormalisée depuis d_localisation (filtre direct).
#
# Lancement : spark-submit --master local[*] 02_spark/build_f_mesures.py
# (dépend de load_faits_detail.py, build_f_deces_agg.py, load_dimensions.py)
# ============================================================================
import sys

sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common import get_spark, GOLD, save_gold  # noqa: E402

spark = get_spark("chu-l2-build-f-mesures")

KEYS = ["annee_mois", "id_etablissement", "id_patient",
        "sexe", "tranche_age", "id_localisation"]

# ---------------------------------------------------------------------------
# 1. Quatre sources de mesures, agrégées à la clé de grain
# ---------------------------------------------------------------------------
consult = (
    spark.table("silver.f_consultation_detail")
    .groupBy(*KEYS)
    .agg(F.count(F.lit(1)).alias("nbre_consultation"))
)

hosp = (
    spark.table("gold.f_hospitalisation")
    .groupBy(*KEYS)
    .agg(F.count(F.lit(1)).alias("nbre_hospitalisation"))
)

# Périmètre d'analyse du projet : 2015-2023 (les autres faits couvrent
# 2015-2023). Les décès antérieurs (1882-2014) restent disponibles dans
# f_deces_agg (couche Gold). Sentinelle 'AAAA-00' = agrégat annuel.
deces = (
    spark.table("gold.f_deces_agg")
    .where(F.col("annee") >= 2015)
    .groupBy(
        F.concat(F.col("annee").cast("string"), F.lit("-00")).alias("annee_mois"),
        F.lit(-1).cast("long").alias("id_etablissement"),
        F.lit(-1).cast("long").alias("id_patient"),
        "sexe", "tranche_age", "id_localisation",
    )
    .agg(F.sum("nbre_deces").alias("nbre_deces"))
)

# Satisfaction : grain juridique x année -> rattachée à la région (pas de clé
# établissement de site), sexe/tranche_age = sentinelles 'TOUS'/'TOUTES',
# id_localisation = 'INCONNU'.
sat = (
    spark.table("gold.f_satisfaction")
    .where(F.col("score_satisfaction").isNotNull())
    .groupBy(
        F.concat(F.col("annee"), F.lit("-00")).alias("annee_mois"),
        F.lit(-1).cast("long").alias("id_etablissement"),
        F.lit(-1).cast("long").alias("id_patient"),
        F.lit("TOUS").alias("sexe"),
        F.lit("TOUTES").alias("tranche_age"),
        F.lit("INCONNU").alias("id_localisation"),
        "region",
    )
    .agg(F.avg("score_satisfaction").alias("total_satisfaction"))
    .withColumnRenamed("region", "region_sat")
)

# ---------------------------------------------------------------------------
# 2. Clés du grain = union des clés des 4 sources, puis jointures externes
# ---------------------------------------------------------------------------
cles = (
    consult.select(*KEYS)
    .unionByName(hosp.select(*KEYS))
    .unionByName(deces.select(*KEYS))
    .unionByName(sat.select(*KEYS))
    .distinct()
)

f_mesures = (
    cles
    .join(consult, on=KEYS, how="left")
    .join(hosp, on=KEYS, how="left")
    .join(deces, on=KEYS, how="left")
    .join(sat, on=KEYS, how="left")
    # id_temps = AAAAMMJJ du 1er jour du mois (NULL pour le grain annuel :
    # 'AAAA-00-01' ne parse pas -> NULL, conforme au DDL)
    .withColumn("id_temps",
                F.date_format(
                    F.to_date(F.concat(F.col("annee_mois"), F.lit("-01")), "yyyy-MM-dd"),
                    "yyyyMMdd").cast("int"))
    .withColumn("annee", F.substring("annee_mois", 1, 4).cast("int"))
    .join(
        spark.table("gold.d_localisation").select("id_localisation", "region"),
        on="id_localisation", how="left",
    )
    .select(
        "annee_mois", "id_temps", "annee",
        "id_etablissement", "id_patient", "sexe", "tranche_age", "id_localisation",
        F.coalesce(F.col("nbre_consultation"), F.lit(0)).alias("nbre_consultation"),
        F.coalesce(F.col("nbre_hospitalisation"), F.lit(0)).alias("nbre_hospitalisation"),
        F.coalesce(F.col("nbre_deces"), F.lit(0)).alias("nbre_deces"),
        F.col("total_satisfaction"),
        F.coalesce(F.col("region_sat"), F.col("region"), F.lit("INCONNUE")).alias("region"),
    )
)

# ---------------------------------------------------------------------------
# 3. Écriture : PARTITIONED BY (annee, sexe, tranche_age) + bucketing 32 sur
#    id_etablissement + tri (id_etablissement, id_patient).
#    -> pruning temporel/démographique + jointures map-side (bucketing).
# ---------------------------------------------------------------------------
save_gold(f_mesures, "gold.f_mesures", f"{GOLD}/f_mesures",
          partition_cols=["annee", "sexe", "tranche_age"],
          bucket_col="id_etablissement", num_buckets=32,
          sort_cols=["id_etablissement", "id_patient"])

spark.sql("MSCK REPAIR TABLE gold.f_mesures")
spark.sql("ANALYZE TABLE gold.f_mesures COMPUTE STATISTICS")

print("f_mesures :")
spark.sql("""
    SELECT COUNT(*) AS lignes,
           SUM(nbre_consultation) AS total_consultations,
           SUM(nbre_hospitalisation) AS total_hospitalisations,
           SUM(nbre_deces) AS total_deces
    FROM gold.f_mesures
""").show()

spark.stop()
