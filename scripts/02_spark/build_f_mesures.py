# ============================================================================
# Livrable 2 - CHU - Spark : fait CONSOLIDÉ f_mesures (couche Gold, étoile)
# ----------------------------------------------------------------------------
# Équivalent PySpark du script PoC 02c_f_mesures.sql (DuckDB).
#
# Grain : annee_mois x id_etablissement x id_patient x sexe x tranche_age
#         x id_localisation x region
#   - consultations / hospitalisations : grain MENSUEL (annee_mois = 'AAAA-MM')
#   - décès / satisfaction            : grain ANNUEL  (sentinelle 'AAAA-00')
#   - region dans le grain : la satisfaction est rattachée à la RÉGION (pas
#     à un établissement de site, cf. load_faits_detail.py) ; sans region dans
#     la clé de jointure, la jointure sat (1 ligne par région x année) créait
#     un fan-out (31 doublons, mesuré en DQ C2 et corrigé).
# 4 mesures : nbre_consultation, nbre_hospitalisation, total_satisfaction,
#             nbre_deces.
# sexe / tranche_age / annee = colonnes de partition (pruning démographique
# et temporel) ; region dans le grain (filtre direct, dénormalisée).
#
# Lancement : spark-submit --master local[*] 02_spark/build_f_mesures.py
# (dépend de load_faits_detail.py, build_f_deces_agg.py, load_dimensions.py)
# ============================================================================
import sys

sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common import get_spark, GOLD, save_gold, read_lake  # noqa: E402

spark = get_spark("chu-l2-build-f-mesures")

KEYS = ["annee_mois", "id_etablissement", "id_patient",
        "sexe", "tranche_age", "id_localisation", "region"]

# ---------------------------------------------------------------------------
# 1. Quatre sources de mesures, agrégées à la clé de grain
# ---------------------------------------------------------------------------
consult = (
    read_lake(spark, "silver.f_consultation_detail")
    .groupBy(*KEYS)
    .agg(F.count(F.lit(1)).alias("nbre_consultation"))
)

hosp = (
    read_lake(spark, "gold.f_hospitalisation")
    .groupBy(*KEYS)
    .agg(F.count(F.lit(1)).alias("nbre_hospitalisation"))
)

# Périmètre d'analyse du projet : 2015-2023 (les autres faits couvrent
# 2015-2023). Les décès antérieurs (1882-2014) restent disponibles dans
# f_deces_agg (couche Gold). Sentinelle 'AAAA-00' = agrégat annuel.
# region : rattachée via d_localisation (les décès n'ont pas d'établissement).
# f_deces_agg porte déjà sa région (coalesce INCONNUE en amont) : pas de
# re-jointure avec d_localisation, qui créerait une colonne region ambiguë
deces = (
    read_lake(spark, "gold.f_deces_agg")
    .where(F.col("annee") >= 2015)
    .groupBy(
        F.concat(F.col("annee").cast("string"), F.lit("-00")).alias("annee_mois"),
        F.lit(-1).cast("long").alias("id_etablissement"),
        F.lit(-1).cast("long").alias("id_patient"),
        "sexe", "tranche_age", "id_localisation",
    )
    .agg(
        F.sum("nbre_deces").alias("nbre_deces"),
        F.min("region").alias("region"),
    )
    .withColumn("region", F.coalesce(F.col("region"), F.lit("INCONNUE")))
)

# Satisfaction : grain juridique x année -> rattachée à la région (pas de clé
# établissement de site), sexe/tranche_age = sentinelles 'TOUS'/'TOUTES',
# id_localisation = 'INCONNU'. Moyenne pondérée par les répondants des finess
# juridiques de la région.
sat = (
    read_lake(spark, "gold.f_satisfaction")
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
    .agg(
        (F.sum(F.col("score_satisfaction") * F.col("nb_repondants"))
         / F.sum("nb_repondants")).alias("total_satisfaction")
    )
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
    .select(
        "annee_mois", "id_temps", "annee",
        "id_etablissement", "id_patient", "sexe", "tranche_age", "id_localisation",
        F.coalesce(F.col("nbre_consultation"), F.lit(0)).alias("nbre_consultation"),
        F.coalesce(F.col("nbre_hospitalisation"), F.lit(0)).alias("nbre_hospitalisation"),
        F.coalesce(F.col("nbre_deces"), F.lit(0)).alias("nbre_deces"),
        F.col("total_satisfaction"),
        F.coalesce(F.col("region"), F.lit("INCONNUE")).alias("region"),
    )
)

# ---------------------------------------------------------------------------
# 3. Écriture : PARTITIONED BY (annee, sexe, tranche_age) + tri intra-partition
#    (id_etablissement, id_patient) -> pruning temporel/démographique par
#    partition + pruning intra-fichier (min/max Parquet) côté Trino.
# ---------------------------------------------------------------------------
save_gold(f_mesures, "gold.f_mesures", f"{GOLD}/f_mesures",
          partition_cols=["annee", "sexe", "tranche_age"])


print("f_mesures :")
read_lake(spark, "gold.f_mesures").agg(
    F.count("*").alias("lignes"),
    F.sum("nbre_consultation").alias("total_consultations"),
    F.sum("nbre_hospitalisation").alias("total_hospitalisations"),
    F.sum("nbre_deces").alias("total_deces"),
).show()

spark.stop()
