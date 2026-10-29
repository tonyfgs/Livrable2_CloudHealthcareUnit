# ============================================================================
# Livrable 2 - CHU - Spark : chargement des 6 DIMENSIONS (couche Gold)
# ----------------------------------------------------------------------------
# Équivalent PySpark du script PoC 01_dims.sql (DuckDB). Produit les tables
# du schéma en étoile déclarées dans 01_ddl/01_schema_gold_hive.sql :
#   d_temps (grain jour, 2015-2030, PARTITIONED BY annee)
#   d_patient (S1 PostgreSQL.Patient, PARTITIONED BY tranche_age_ref)
#   d_etablissement (référentiel FINESS, bucketing 32 sur id_etablissement)
#   d_localisation (101 départements + Inconnu, PARTITIONED BY region)
#   d_diagnostic (S1 PostgreSQL.Diagnostic, bucketing 8)
#   d_professionnel (S1 PostgreSQL.Professionnel_de_sante, bucketing 32)
#
# Lancement (dans le conteneur spark, stack docker-compose) :
#   spark-submit --master local[*] 02_spark/load_dimensions.py
# ============================================================================
import sys

sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F, Window
from common import (  # noqa: E402
    get_spark, read_postgres, read_raw_csv, GOLD,
    age_revolu, tranche_age, dept_of, sexe_normalise, save_gold,
)

spark = get_spark("chu-l2-load-dimensions")

# ---------------------------------------------------------------------------
# d_temps : grain = jour, couverture 2015-2030 (faits 2015-2023, décès
# 1882-2020 ; la dimension est prolongée pour les besoins de planification).
# id_temps = AAAAMMJJ (clé de substitution entière, jointure avec les faits).
# ---------------------------------------------------------------------------
d_temps = (
    spark.range(0, 1)
    .select(F.explode(F.expr(
        "sequence(to_date('2015-01-01'), to_date('2030-12-31'), interval 1 day)"
    )).alias("date_jour"))
    .withColumn("id_temps", F.date_format("date_jour", "yyyyMMdd").cast("int"))
    .withColumn("annee", F.year("date_jour"))
    .withColumn("mois", F.month("date_jour"))
    .withColumn("annee_mois", F.date_format("date_jour", "yyyy-MM"))
    .withColumn("trimestre", F.quarter("date_jour"))
    # Spark dayofweek : 1=dimanche..7=samedi -> on décalle pour avoir 0=dimanche
    .withColumn("jour_semaine", (F.dayofweek("date_jour") - 1).cast("int"))
    .withColumn("est_weekend", F.dayofweek("date_jour").isin(1, 7))
)
save_gold(d_temps, "gold.d_temps", f"{GOLD}/d_temps",
          partition_cols=["annee"], sort_cols=["id_temps"])

# ---------------------------------------------------------------------------
# d_patient : depuis S1 PostgreSQL.Patient (100 000 patients).
# sexe normalisé M/F/I ; date_naissance parsée ('M/J/AAAA' -> date) ;
# tranche_age_ref = tranche à la date de référence 2023-12-31.
# L'attribut Age de S1 n'est PAS retenu comme âge de référence : incohérent
# avec la date de naissance (46 % de cohérence, cf. rapport qualité). La
# tranche d'âge des FAITS est recalculée à la date de l'événement.
# ---------------------------------------------------------------------------
patient = read_postgres(spark, "Patient")
d_patient = (
    patient
    .withColumn("id_patient", F.col("Id_patient").cast("long"))
    .withColumn("sexe", sexe_normalise(F.col("Sexe")))
    .withColumn("date_naissance", F.to_date(F.col("Date"), "M/d/yyyy"))
    .withColumn("age_ref_calc", age_revolu(F.to_date(F.lit("2023-12-31")), F.to_date(F.col("Date"), "M/d/yyyy")))
    .withColumn("tranche_age_ref", tranche_age(F.col("age_ref_calc")))
    .withColumn("age_ref", F.col("Age").cast("int"))
    .withColumn("code_postal", F.nullif(F.trim(F.col("Code_postal")), F.lit("")))
    .withColumn("ville", F.nullif(F.trim(F.col("Ville")), F.lit("")))
    .select("id_patient", "sexe", "date_naissance", "age_ref",
            "code_postal", "ville", "tranche_age_ref")
)
save_gold(d_patient, "gold.d_patient", f"{GOLD}/d_patient",
          partition_cols=["tranche_age_ref"],
          bucket_col="id_patient", num_buckets=32, sort_cols=["id_patient"])

# ---------------------------------------------------------------------------
# Référentiel départements (geo.api.gouv.fr, 101 départements) - zone RAW.
# Sert à la fois à d_etablissement (code_commune -> département) et à
# d_localisation (grain département).
# ---------------------------------------------------------------------------
ref_dept = read_raw_csv(spark, "referentiels/referentiel_departements.csv").select(
    F.col("code_departement"), F.col("nom_departement"),
    F.col("code_region"), F.col("nom_region"),
)

# ---------------------------------------------------------------------------
# d_etablissement : référentiel FINESS (416 665 sites), clé de substitution
# surrogate id_etablissement (row_number sur la clé naturelle ordonnée).
# Rattachement géographique : code_commune -> département (règle INSEE,
# 99,74 % de concordance mesurée en DQ) -> région dénormalisée.
# ---------------------------------------------------------------------------
etab = read_raw_csv(spark, "etablissement/etablissement_sante.csv")
d_etablissement = (
    etab
    .withColumn("identifiant_organisation", F.trim(F.col("identifiant_organisation")))
    .withColumn("raison_sociale_site", F.trim(F.col("raison_sociale_site")))
    .withColumn("id_etablissement",
                F.row_number().over(Window.orderBy("identifiant_organisation", "raison_sociale_site"))
                .cast("long"))
    .withColumn("finess_etablissement_juridique",
                F.nullif(F.trim(F.col("finess_etablissement_juridique")), F.lit("")))
    .withColumn("finess_site", F.nullif(F.trim(F.col("finess_site")), F.lit("")))
    .withColumn("code_commune", F.nullif(F.trim(F.col("code_commune")), F.lit("")))
    .join(
        ref_dept.withColumn("dept", F.col("code_departement")),
        dept_of(F.col("code_commune")) == F.col("dept"),
        "left",
    )
    .withColumn("id_localisation", F.coalesce(F.col("code_departement"), F.lit("INCONNU")))
    .withColumn("region", F.coalesce(F.col("nom_region"), F.lit("INCONNUE")))
    .select("id_etablissement", "identifiant_organisation",
            "finess_etablissement_juridique", "finess_site", "raison_sociale_site",
            "code_commune", "id_localisation", "region")
)
save_gold(d_etablissement, "gold.d_etablissement", f"{GOLD}/d_etablissement",
          bucket_col="id_etablissement", num_buckets=32, sort_cols=["id_etablissement"])

# ---------------------------------------------------------------------------
# d_localisation : 101 départements + membre 'Inconnu' (clé 'INCONNU'),
# grain = département, partitionnée par région (colonne la plus filtrante).
# ---------------------------------------------------------------------------
d_localisation = (
    ref_dept.select(
        F.col("code_departement").alias("id_localisation"),
        "code_departement", "nom_departement", "code_region", "nom_region",
    )
    .distinct()
    .unionByName(
        spark.createDataFrame(
            [("INCONNU", "INCONNU", "Inconnu", "99", "Inconnue")],
            ["id_localisation", "code_departement", "nom_departement",
             "code_region", "nom_region"],
        )
    )
    .withColumnRenamed("nom_region", "region")
)
save_gold(d_localisation, "gold.d_localisation", f"{GOLD}/d_localisation",
          partition_cols=["region"], sort_cols=["id_localisation"])

# ---------------------------------------------------------------------------
# d_diagnostic : depuis S1 PostgreSQL.Diagnostic (15 490 diagnostics CIM-10).
# ---------------------------------------------------------------------------
d_diagnostic = (
    read_postgres(spark, "Diagnostic")
    .withColumn("id_diagnostic",
                F.row_number().over(Window.orderBy("Code_diag")).cast("long"))
    .withColumn("code_diag", F.col("Code_diag"))
    .withColumn("libelle_diagnostic", F.trim(F.col("Diagnostic")))
    .select("id_diagnostic", "code_diag", "libelle_diagnostic")
)
save_gold(d_diagnostic, "gold.d_diagnostic", f"{GOLD}/d_diagnostic",
          bucket_col="id_diagnostic", num_buckets=8, sort_cols=["id_diagnostic"])

# ---------------------------------------------------------------------------
# d_professionnel : depuis S1 PostgreSQL.Professionnel_de_sante (1 048 575).
# Clé naturelle = identifiant (stable et unique) : pas de surrogate.
# ---------------------------------------------------------------------------
d_professionnel = (
    read_postgres(spark, "Professionnel_de_sante")
    .withColumn("identifiant", F.trim(F.col("Identifiant")))
    .withColumn("civilite", F.nullif(F.trim(F.col("Civilite")), F.lit("")))
    .withColumn("nom", F.col("Nom"))
    .withColumn("prenom", F.col("Prenom"))
    .withColumn("profession", F.col("Profession"))
    .withColumn("categorie_professionnelle", F.col("Categorie_professionnelle"))
    .withColumn("code_specialite", F.col("Code_specialite"))
    .select("identifiant", "civilite", "nom", "prenom", "profession",
            "categorie_professionnelle", "code_specialite")
)
save_gold(d_professionnel, "gold.d_professionnel", f"{GOLD}/d_professionnel",
          bucket_col="identifiant", num_buckets=32, sort_cols=["identifiant"])

# ---------------------------------------------------------------------------
# Rafraîchissement des métadonnées de partitions (les écritures Spark en
# partitionBy dynamique créent les répertoires annee=.../ ; le metastore doit
# les connaître pour le pruning Trino).
# ---------------------------------------------------------------------------
for table in ["d_temps", "d_patient", "d_localisation"]:
    spark.sql(f"MSCK REPAIR TABLE gold.{table}")

spark.sql("ANALYZE TABLE gold.d_temps COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE gold.d_patient COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE gold.d_etablissement COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE gold.d_localisation COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE gold.d_diagnostic COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE gold.d_professionnel COMPUTE STATISTICS")

print("Dimensions chargées :")
spark.sql("""
    SELECT 'd_temps' t, COUNT(*) n FROM gold.d_temps
    UNION ALL SELECT 'd_patient', COUNT(*) FROM gold.d_patient
    UNION ALL SELECT 'd_etablissement', COUNT(*) FROM gold.d_etablissement
    UNION ALL SELECT 'd_localisation', COUNT(*) FROM gold.d_localisation
    UNION ALL SELECT 'd_diagnostic', COUNT(*) FROM gold.d_diagnostic
    UNION ALL SELECT 'd_professionnel', COUNT(*) FROM gold.d_professionnel
""").show()

spark.stop()
