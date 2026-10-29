# ============================================================================
# Livrable 2 - CHU - Spark : agrégats PRÉ-CALCULÉS (couche Gold)
# ----------------------------------------------------------------------------
# Équivalent PySpark du script PoC 02d_agregats.sql (DuckDB).
# Équivalents matérialisés (dénombrements les plus demandés par les besoins
# métier du Livrable 1) pour accélérer les tableaux de bord Superset :
#   gold.f_consult_diag_mois  : consultations par diagnostic x mois x etab.
#   gold.f_consult_prof_mois  : consultations par professionnel x mois
#
# Lancement : spark-submit --master local[*] 02_spark/build_agregats.py
# (dépend de load_faits_detail.py)
# ============================================================================
import sys

sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common import get_spark, GOLD, save_gold  # noqa: E402

spark = get_spark("chu-l2-build-agregats")

f_consultation_detail = spark.table("silver.f_consultation_detail")

# ---------------------------------------------------------------------------
# f_consult_diag_mois : PARTITIONED BY (annee), bucketing 8 sur id_diagnostic
# (jointures fréquentes fait <-> d_diagnostic sur cette clé)
# ---------------------------------------------------------------------------
f_consult_diag_mois = (
    f_consultation_detail
    .groupBy("annee_mois", "id_diagnostic", "id_etablissement")
    .agg(F.count(F.lit(1)).alias("nbre_consultation"))
    .withColumn("annee", F.substring("annee_mois", 1, 4).cast("int"))
    .select("annee_mois", "annee", "id_diagnostic", "id_etablissement",
            "nbre_consultation")
)
save_gold(f_consult_diag_mois, "gold.f_consult_diag_mois",
          f"{GOLD}/f_consult_diag_mois",
          partition_cols=["annee"],
          bucket_col="id_diagnostic", num_buckets=8,
          sort_cols=["id_diagnostic"])

# ---------------------------------------------------------------------------
# f_consult_prof_mois : PARTITIONED BY (annee), bucketing 32 sur
# id_professionnel (jointures fait <-> d_professionnel)
# ---------------------------------------------------------------------------
f_consult_prof_mois = (
    f_consultation_detail
    .groupBy("annee_mois", "id_professionnel")
    .agg(F.count(F.lit(1)).alias("nbre_consultation"))
    .withColumn("annee", F.substring("annee_mois", 1, 4).cast("int"))
    .select("annee_mois", "annee", "id_professionnel", "nbre_consultation")
)
save_gold(f_consult_prof_mois, "gold.f_consult_prof_mois",
          f"{GOLD}/f_consult_prof_mois",
          partition_cols=["annee"],
          bucket_col="id_professionnel", num_buckets=32,
          sort_cols=["id_professionnel"])

for table in ["f_consult_diag_mois", "f_consult_prof_mois"]:
    spark.sql(f"MSCK REPAIR TABLE gold.{table}")
    spark.sql(f"ANALYZE TABLE gold.{table} COMPUTE STATISTICS")

print("Agrégats pré-calculés :")
spark.sql("""
    SELECT 'f_consult_diag_mois' t, COUNT(*) n FROM gold.f_consult_diag_mois
    UNION ALL SELECT 'f_consult_prof_mois', COUNT(*) FROM gold.f_consult_prof_mois
""").show()

spark.stop()
