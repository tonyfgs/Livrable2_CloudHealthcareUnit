# ============================================================================
# Livrable 2 - CHU - Spark : faits de DÉTAIL et satisfaction (Silver/Gold)
# ----------------------------------------------------------------------------
# Équivalent PySpark du script PoC 02a_faits_detail.sql (DuckDB) :
#   silver.f_consultation_detail : grain = 1 consultation (S1, 1 027 157)
#   gold.f_hospitalisation       : grain = 1 hospitalisation (S2, 2 479)
#   gold.f_satisfaction          : grain = établissement JURIDIQUE x année
#                                  (S3 ESATIS 2019 + 2020, 2 295 lignes source
#                                   agrégées en 1 960 finess juridiques)
#
# Lancement : spark-submit --master local[*] 02_spark/load_faits_detail.py
# (dépend de load_dimensions.py : d_diagnostic, d_etablissement, d_patient)
# ============================================================================
import sys

sys.path.insert(0, "/opt/spark/jobs")
from pyspark.sql import functions as F
from common import (  # noqa: E402
    get_spark, read_postgres, read_raw_csv, GOLD, SILVER,
    age_revolu, tranche_age, sexe_normalise, save_gold,
)

spark = get_spark("chu-l2-load-faits-detail")

# ---------------------------------------------------------------------------
# silver.f_consultation_detail (grain = 1 consultation, couche Silver)
# Rattachement établissement : Consultation.Id_prof_sante -> fichier
# d'activité (identifiant, identifiant_organisation) -> d_etablissement.
# Un professionnel exerce souvent dans PLUSIEURS établissements (fichier
# d'activité 1-N) : la jointure est ramenée à UNE organisation par
# professionnel (MIN) pour préserver le grain "1 consultation" (un fan-out
# 1-N dupliquerait les consultations : +346 437 lignes, +33,7 %, corrigé).
# Taux mesurés en DQ : 71,87 % des consultations ont leur professionnel dans
# le fichier d'activité ; 63,87 % sont rattachées à un établissement (chaîne
# complète) ; les autres -> -1 (Inconnu).
# ---------------------------------------------------------------------------
consultation = read_postgres(spark, "Consultation")
patient = read_postgres(spark, "Patient").select(
    F.col("Id_patient").alias("id_patient_src"),
    sexe_normalise(F.col("Sexe")).alias("sexe"),
    F.to_date(F.col("Date"), "M/d/yyyy").alias("date_naissance"),
)

c2 = (
    consultation
    .withColumn("id_consultation", F.col("Num_consultation").cast("long"))
    .withColumn("id_temps", F.date_format(F.col("Date"), "yyyyMMdd").cast("int"))
    .withColumn("annee_mois", F.date_format(F.col("Date"), "yyyy-MM"))
    .withColumn("annee", F.year(F.col("Date")).cast("int"))
    .withColumn("id_patient", F.col("Id_patient").cast("long"))
    .withColumn("id_professionnel", F.col("Id_prof_sante"))
    .withColumn("code_diag", F.col("Code_diag"))
    .withColumn("date_event", F.col("Date"))
    .join(patient, F.col("Id_patient") == F.col("id_patient_src"))
)

# Fichier d'activité (référentiel S4) : lien professionnel -> établissement
activite = (
    read_raw_csv(spark, "referentiels/activite_professionnel_sante.csv")
    .select(
        F.trim(F.col("identifiant")).alias("identifiant"),
        F.trim(F.col("identifiant_organisation")).alias("identifiant_organisation"),
    )
    .where(F.col("identifiant").isNotNull() & (F.col("identifiant") != ""))
    .groupBy("identifiant")
    .agg(F.min("identifiant_organisation").alias("identifiant_organisation"))
)

f_consultation_detail = (
    c2
    .withColumn("age_event", age_revolu(F.col("date_event"), F.col("date_naissance")))
    .withColumn("tranche_age", tranche_age(F.col("age_event")))
    .join(spark.table("gold.d_diagnostic").select("code_diag", "id_diagnostic"),
          on="code_diag")
    .join(activite, c2["id_professionnel"] == activite["identifiant"], "left")
    .join(
        spark.table("gold.d_etablissement")
        .select("identifiant_organisation", "id_etablissement",
                "id_localisation", "region")
        .where(F.col("identifiant_organisation").isNotNull()),
        on="identifiant_organisation", how="left",
    )
    .select(
        "id_consultation", "id_temps", "annee_mois", "annee",
        "id_patient", "sexe", "tranche_age", "id_diagnostic", "id_professionnel",
        F.coalesce(F.col("id_etablissement"), F.lit(-1)).alias("id_etablissement"),
        F.coalesce(F.col("id_localisation"), F.lit("INCONNU")).alias("id_localisation"),
        F.coalesce(F.col("region"), F.lit("INCONNUE")).alias("region"),
    )
)
save_gold(f_consultation_detail, "silver.f_consultation_detail",
          f"{SILVER}/f_consultation_detail",
          partition_cols=["annee"],
          bucket_col="id_patient", num_buckets=32,
          sort_cols=["id_patient", "id_temps"])

# ---------------------------------------------------------------------------
# gold.f_hospitalisation (grain = 1 hospitalisation, source S2 CSV)
# Rattachement établissement DIRECT : identifiant_organisation ->
# d_etablissement (100 % de rattachement mesuré en DQ).
# ---------------------------------------------------------------------------
hosp = (
    read_raw_csv(spark, "hospitalisation/Hospitalisations.csv")
    .withColumn("id_hospitalisation", F.trim(F.col("Num_Hospitalisation")))
    .withColumn("d_entree", F.to_date(F.col("Date_Entree"), "d/M/yyyy"))
    .withColumn("id_patient", F.col("Id_patient").cast("long"))
    .withColumn("identifiant_organisation", F.trim(F.col("identifiant_organisation")))
    .withColumn("code_diagnostic", F.trim(F.col("Code_diagnostic")))
    .withColumn("duree_jours", F.col("Jour_Hospitalisation").cast("int"))
)

f_hospitalisation = (
    hosp
    .join(patient, hosp["id_patient"] == patient["id_patient_src"])
    .withColumn("id_temps", F.date_format(F.col("d_entree"), "yyyyMMdd").cast("int"))
    .withColumn("annee_mois", F.date_format(F.col("d_entree"), "yyyy-MM"))
    .withColumn("annee", F.year(F.col("d_entree")).cast("int"))
    .withColumn("age_event", age_revolu(F.col("d_entree"), F.col("date_naissance")))
    .withColumn("tranche_age", tranche_age(F.col("age_event")))
    .join(spark.table("gold.d_diagnostic").select("code_diag", "id_diagnostic"),
          hosp["code_diagnostic"] == F.col("code_diag"))
    .join(
        spark.table("gold.d_etablissement")
        .select("identifiant_organisation", "id_etablissement",
                "id_localisation", "region")
        .where(F.col("identifiant_organisation").isNotNull()),
        on="identifiant_organisation", how="left",
    )
    .select(
        "id_hospitalisation", "id_temps", "annee_mois", "annee",
        hosp["id_patient"].alias("id_patient"),
        "sexe", "tranche_age", "id_diagnostic",
        F.coalesce(F.col("id_etablissement"), F.lit(-1)).alias("id_etablissement"),
        F.coalesce(F.col("id_localisation"), F.lit("INCONNU")).alias("id_localisation"),
        F.coalesce(F.col("region"), F.lit("INCONNUE")).alias("region"),
        "duree_jours",
    )
)
save_gold(f_hospitalisation, "gold.f_hospitalisation",
          f"{GOLD}/f_hospitalisation",
          partition_cols=["annee"],
          bucket_col="id_etablissement", num_buckets=8,
          sort_cols=["id_etablissement", "id_patient"])

# ---------------------------------------------------------------------------
# gold.f_satisfaction (grain = établissement JURIDIQUE x année, source ESATIS)
# - Le score brut utilise la VIRGULE DÉCIMALE dans les deux millésimes
#   (mesuré en DQ) -> replace(',', '.') avant cast DOUBLE.
# - Le finess ESATIS est un finess JURIDIQUE : le fichier source contient
#   plusieurs lignes (sites) par finess juridique (2 295 lignes -> 1 960
#   finess juridiques, mesuré en DQ). Rattachement site impossible (0 % en
#   DQ) -> agrégation au grain (annee, finess juridique) :
#   score = moyenne PONDÉRÉE par le nombre de répondants, nb_repondants =
#   somme des répondants, nb_sites = nombre de lignes source agrégées.
# - 6 finess juridiques ont 2 régions dans la source (1 966 couples
#   annee/finess/region pour 1 960 finess) : région retenue = MIN(region),
#   la pondération mélange les sites (documenté).
# - La région provient du fichier ESATIS, normalisée par mapping (18 régions ;
#   "Océan Indien" ambigu volontairement exclu -> INCONNUE, cf. rapport DQ).
# ---------------------------------------------------------------------------
region_map = spark.createDataFrame(
    [
        ("Auvergne-Rhône-Alpes", "Auvergne-Rhône-Alpes"),
        ("Bourgogne-Franche-Comté", "Bourgogne-Franche-Comté"),
        ("Bretagne", "Bretagne"),
        ("Centre-Val de Loire", "Centre-Val de Loire"),
        ("Corse", "Corse"),
        ("Grand Est", "Grand Est"),
        ("Guadeloupe", "Guadeloupe"),
        ("Guyane", "Guyane"),
        ("Hauts de France", "Hauts-de-France"),
        ("Ile de France", "Île-de-France"),
        ("Martinique", "Martinique"),
        ("Normandie", "Normandie"),
        ("Nouvelle Aquitaine", "Nouvelle-Aquitaine"),
        ("Occitanie", "Occitanie"),
        ("PACA", "Provence-Alpes-Côte d'Azur"),
        ("Pays de la Loire", "Pays de la Loire"),
        ("La Réunion", "La Réunion"),
        ("Mayotte", "Mayotte"),
    ],
    ["region_brute", "region_norm"],
)

sat = None
for annee, fichier in [("2019", "satisfaction/ESATIS_2019.csv"),
                       ("2020", "satisfaction/ESATIS_2020.csv")]:
    s = (
        read_raw_csv(spark, fichier)
        .withColumn("annee", F.lit(annee))
        .select(
            "annee",
            F.trim(F.col("finess")).alias("finess"),
            F.trim(F.col("region")).alias("region_brute"),
            # virgule décimale -> point (normalisation DQ)
            F.regexp_replace(F.col("score_all_rea_ajust"), ",", ".")
            .cast("double").alias("score_satisfaction"),
            F.col("nb_rep_score_all_rea_ajust").cast("int").alias("nb_repondants"),
        )
    )
    sat = s if sat is None else sat.unionByName(s)

f_satisfaction = (
    sat.join(region_map, on="region_brute", how="left")
    .withColumn("region", F.coalesce(F.col("region_norm"), F.lit("INCONNUE")))
    # Agrégation au grain juridique (annee, finess) : moyenne pondérée par
    # le nombre de répondants (évite le fan-out 1-N des sites ESATIS).
    .groupBy("annee", "finess")
    .agg(
        F.min("region").alias("region"),
        (F.sum(F.col("score_satisfaction") * F.col("nb_repondants"))
         / F.sum("nb_repondants")).alias("score_satisfaction"),
        F.sum("nb_repondants").alias("nb_repondants"),
        F.count(F.lit(1)).alias("nb_sites"),
    )
    .select("annee", "finess", "region", "score_satisfaction",
            "nb_repondants", "nb_sites")
)
save_gold(f_satisfaction, "gold.f_satisfaction",
          f"{GOLD}/f_satisfaction",
          partition_cols=["annee", "region"], sort_cols=["finess"])

# ---------------------------------------------------------------------------
# Rafraîchissement métadonnées + statistiques
# ---------------------------------------------------------------------------
for table in ["f_consultation_detail", "f_hospitalisation", "f_satisfaction"]:
    schema = "silver" if table == "f_consultation_detail" else "gold"
    spark.sql(f"MSCK REPAIR TABLE {schema}.{table}")
    spark.sql(f"ANALYZE TABLE {schema}.{table} COMPUTE STATISTICS")

print("Faits de détail chargés :")
spark.sql("""
    SELECT 'f_consultation_detail' t, COUNT(*) n FROM silver.f_consultation_detail
    UNION ALL SELECT 'f_hospitalisation', COUNT(*) FROM gold.f_hospitalisation
    UNION ALL SELECT 'f_satisfaction', COUNT(*) FROM gold.f_satisfaction
""").show()

spark.stop()
