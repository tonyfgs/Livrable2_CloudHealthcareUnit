# ============================================================================
# Livrable 2 - CHU - Spark : fait DÉCÈS AGRÉGÉ (depuis les 25,09 M décès INSEE)
# ----------------------------------------------------------------------------
# Équivalent PySpark du script PoC 02b_deces_agg.sql (DuckDB).
#
# Grain : ANNUEL = annee x sexe x tranche_age x département (id_localisation).
# Le fichier couvre 1882-2020 : un grain mensuel produirait 8,6 M de lignes
# pour un besoin métier ANNUEL (décès par région sur 2019) -> grain annuel
# retenu, stocké dans f_mesures avec la sentinelle annee_mois = 'AAAA-00'.
#
# Traitement des 25 M de lignes en Spark (pas en mémoire driver) :
#   - filtre précoce (date_deces non nulle, 646 dates ISO invalides exclues)
#   - agrégat local (map-side) puis shuffle sur les 6 clés de grain
#   - jointure du référentiel départements (101 lignes, broadcast)
#
# Lancement : spark-submit --master local[*] 02_spark/build_f_deces_agg.py
# ============================================================================
import sys

sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common import (  # noqa: E402
    get_spark, read_raw_csv, RAW, GOLD,
    age_revolu, tranche_age, dept_of, save_gold, read_lake,
)

spark = get_spark("chu-l2-build-f-deces-agg")

# ---------------------------------------------------------------------------
# Lecture du fichier INSEE (séparateur ',', quote '"', dates ISO yyyy-MM-dd,
# sexe '1'/'2'). Schéma forcé en string pour éviter toute inférence coûteuse
# sur 25 M de lignes (les casts sont poussés dans les colonnes typées).
# ---------------------------------------------------------------------------
deces = (
    spark.read.option("header", "true")
    .option("sep", ",")
    .option("quote", '"')
    .option("encoding", "UTF-8")
    .csv(f"{RAW}/deces/deces.csv")
    .select(
        F.col("sexe"),
        F.to_date(F.col("date_deces"), "yyyy-MM-dd").alias("d_deces"),
        F.to_date(F.col("date_naissance"), "yyyy-MM-dd").alias("d_naissance"),
        F.trim(F.col("code_lieu_deces")).alias("code_lieu_deces"),
    )
    .where(F.col("d_deces").isNotNull())  # exclut les 646 dates invalides (DQ)
)

# ---------------------------------------------------------------------------
# Référentiel départements (broadcast : 101 lignes) pour le rattachement
# géographique du lieu de décès (concordance code_lieu_deces -> département
# mesurée à 99,2 % en DQ).
# ---------------------------------------------------------------------------
ref_dept = (
    read_raw_csv(spark, "referentiels/referentiel_departements.csv")
    .select("code_departement", "nom_region")
    .distinct()
)

f_deces_agg = (
    deces
    .withColumn("annee", F.year("d_deces").cast("int"))
    .withColumn("sexe_norm",
                F.when(F.col("sexe") == "1", F.lit("M"))
                .when(F.col("sexe") == "2", F.lit("F"))
                .otherwise(F.lit("I")))
    .withColumn("age_deces", age_revolu(F.col("d_deces"), F.col("d_naissance")))
    .withColumn("tranche_age", tranche_age(F.col("age_deces")))
    .withColumn("dept", dept_of(F.col("code_lieu_deces")))
    .join(F.broadcast(ref_dept), F.col("dept") == F.col("code_departement"), "left")
    # Le département fait partie du GRAIN (cf. PoC 02b : GROUP BY 1..6)
    .withColumn("id_localisation",
                F.coalesce(F.col("code_departement"), F.lit("INCONNU")))
    .withColumn("region", F.coalesce(F.col("nom_region"), F.lit("INCONNUE")))
    .groupBy(
        (F.year("d_deces") * 10000 + 101).cast("int").alias("id_temps"),  # 1er janvier
        "annee", "sexe_norm", "tranche_age", "id_localisation", "region",
    )
    .agg(F.count(F.lit(1)).alias("nbre_deces"))
    .select("id_temps", "annee", F.col("sexe_norm").alias("sexe"),
            "tranche_age", "id_localisation", "region", "nbre_deces")
)

# ---------------------------------------------------------------------------
# Écriture : PARTITIONED BY (annee) SEULEMENT. Justification (cf. rapport) :
# un partitionnement par (annee, sexe, tranche_age) produirait
# 1 251 combinaisons réelles (~55 lignes ; borne 133 x 2 x 7 = 1 862) pour
# 69 008 lignes (small-files problem) ; le grain annuel + annee en partition
# donnent 133 partitions de ~519 lignes. Le tri par (sexe, tranche_age,
# id_localisation) compense
# l'absence de partitionnement sur ces colonnes (min/max par fichier ->
# pruning intra-partition côté Trino).
# ---------------------------------------------------------------------------
save_gold(f_deces_agg, "gold.f_deces_agg", f"{GOLD}/f_deces_agg",
          partition_cols=["annee"])


print("f_deces_agg :")
read_lake(spark, "gold.f_deces_agg").agg(
    F.count("*").alias("lignes"),
    F.sum("nbre_deces").alias("total_deces"),
    F.min("annee").alias("annee_min"),
    F.max("annee").alias("annee_max"),
).show()

spark.stop()
