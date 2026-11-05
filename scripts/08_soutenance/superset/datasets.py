"""Couche de modelisation Superset : datasets physiques (etoile Gold + Silver + Raw) et datasets virtuels
(jointures de l'etoile, taux, referentiels). Une seule source de verite pour provision.py et verify.py.

Deux modes pour trois datasets dont les fichiers complets (FINESS, deces) ne sont pas dans le lac de demonstration :
  live    : calcul direct sur le lac (donnees presentes dans le lac au moment du provisionnement)
  archive : resultats de la campagne sur donnees completes du 30/10/2026 (schema hive.archive)
"""

# --------------------------------------------------------------------------- datasets physiques
PHYSIQUES = [
    # (schema, table, description, metrics[(nom, libelle, expression, format)])
    ("gold", "d_temps", "Dimension temps (jour, mois, trimestre, année).", []),
    ("gold", "d_patient", "Dimension patient (date de naissance, sexe, tranche d'âge de référence).", [("nb_patients", "Patients", "COUNT(*)", ",d")]),
    ("gold", "d_etablissement", "Dimension établissement (FINESS, raison sociale, commune, région).", [("nb_etablissements", "Établissements", "COUNT(*)", ",d")]),
    ("gold", "d_localisation", "Dimension localisation (département, région).", []),
    ("gold", "d_diagnostic", "Dimension diagnostic (code et libellé CIM-10).", [("nb_diagnostics", "Diagnostics", "COUNT(*)", ",d")]),
    ("gold", "d_professionnel", "Dimension professionnel de santé (profession, spécialité). Les colonnes nominatives ne sont pas utilisées dans les tableaux de bord.", [("nb_professionnels", "Professionnels", "COUNT(*)", ",d")]),
    ("silver", "f_consultation_detail", "Fait consultation au grain 1 consultation (couche Silver).", [("nbre_consultations", "Consultations", "COUNT(*)", ",d")]),
    ("gold", "f_hospitalisation", "Fait hospitalisation au grain 1 hospitalisation.", [("nbre_hospitalisations", "Hospitalisations", "COUNT(*)", ",d"), ("duree_moyenne_jours", "Durée moyenne de séjour (jours)", "AVG(duree_jours)", ",.1f")]),
    ("gold", "f_deces_agg", "Fait décès agrégé (jour x sexe x tranche d'âge x département).", [("nbre_deces_total", "Décès", "SUM(nbre_deces)", ",d")]),
    ("gold", "f_satisfaction", "Fait satisfaction e-Satis (établissement juridique x année).", [("score_pondere", "Score de satisfaction pondéré", "SUM(score_satisfaction * nb_repondants) / SUM(CASE WHEN score_satisfaction IS NOT NULL THEN nb_repondants END)", ",.2f")]),
    ("gold", "f_mesures", "Table de faits consolidée (grain jour x établissement x patient x département).", [("nbre_consultations", "Consultations", "SUM(nbre_consultation)", ",d"), ("nbre_hospitalisations", "Hospitalisations", "SUM(nbre_hospitalisation)", ",d"), ("nbre_deces", "Décès", "SUM(nbre_deces)", ",d")]),
    ("gold", "f_consult_diag_mois", "Agrégat consultations par diagnostic, établissement et mois.", [("nbre_consultations", "Consultations", "SUM(nbre_consultation)", ",d")]),
    ("gold", "f_consult_prof_mois", "Agrégat consultations par professionnel et mois.", [("nbre_consultations", "Consultations", "SUM(nbre_consultation)", ",d")]),
    ("raw", "hospitalisations", "Fichier source des hospitalisations, lu tel quel (CSV, tout en texte).", [("lignes", "Lignes", "COUNT(*)", ",d")]),
    ("raw", "etablissement_sante", "Fichier source FINESS des établissements, lu tel quel.", [("lignes", "Lignes", "COUNT(*)", ",d")]),
    ("raw", "deces_sans_identite", "Fichier source des décès (INSEE) sans nom ni prénom.", [("lignes", "Lignes", "COUNT(*)", ",d")]),
    ("raw", "satisfaction_2019", "Fichier source e-Satis 2019, lu tel quel.", [("lignes", "Lignes", "COUNT(*)", ",d")]),
    ("raw", "satisfaction_2020", "Fichier source e-Satis 2020, lu tel quel.", [("lignes", "Lignes", "COUNT(*)", ",d")]),
]

# --------------------------------------------------------------------------- jointure de l'etoile (consultations)
_JOIN_CONSULT = """FROM hive.silver.f_consultation_detail f
JOIN hive.gold.d_temps t ON t.id_temps = f.id_temps
JOIN hive.gold.d_patient p ON p.id_patient = f.id_patient
JOIN hive.vues_t.d_diagnostic_enrichi d ON d.id_diagnostic = f.id_diagnostic
LEFT JOIN hive.gold.d_etablissement e ON e.id_etablissement = f.id_etablissement
LEFT JOIN hive.gold.d_localisation l ON l.id_localisation = f.id_localisation"""

_JOIN_HOSP = """FROM hive.gold.f_hospitalisation f
JOIN hive.gold.d_temps t ON t.id_temps = f.id_temps
JOIN hive.gold.d_patient p ON p.id_patient = f.id_patient
JOIN hive.vues_t.d_diagnostic_enrichi d ON d.id_diagnostic = f.id_diagnostic
LEFT JOIN hive.gold.d_etablissement e ON e.id_etablissement = f.id_etablissement
LEFT JOIN hive.gold.d_localisation l ON l.id_localisation = f.id_localisation"""

_COLS_COMMUNES = """f.annee, f.annee_mois, t.date_jour, f.sexe, f.tranche_age,
 d.code_diag, d.libelle_diagnostic, d.diagnostic, d.chapitre_cim10,
 CASE WHEN f.id_etablissement > 0 THEN e.raison_sociale_site ELSE 'Non rattaché' END AS etablissement,
 CASE WHEN f.id_etablissement > 0 THEN 'Oui' ELSE 'Non' END AS rattache_etablissement,
 f.region, l.nom_departement AS departement,
 CASE WHEN t.date_jour < p.date_naissance THEN 'Oui' ELSE 'Non' END AS avant_naissance"""


def sql_etoile_consultations():
    return f"SELECT f.id_consultation, f.id_patient, f.id_professionnel,\n {_COLS_COMMUNES}\n{_JOIN_CONSULT}"


def sql_etoile_hospitalisations():
    return f"SELECT f.id_hospitalisation, f.id_patient, f.duree_jours,\n {_COLS_COMMUNES}\n{_JOIN_HOSP}"


SQL_TAUX_ANNUEL = """WITH h AS (
  SELECT annee, COUNT(*) AS hospitalisations, COUNT(DISTINCT id_patient) AS patients_hospitalises
  FROM hive.gold.f_hospitalisation GROUP BY annee
), c AS (
  SELECT annee, COUNT_IF(id_temps BETWEEN 20150621 AND 20210619) AS consultations_fenetre
  FROM hive.silver.f_consultation_detail GROUP BY annee
)
SELECT c.annee, h.hospitalisations, h.patients_hospitalises, c.consultations_fenetre,
       CASE WHEN c.annee IN (2015, 2021) THEN 'Année partielle' ELSE 'Année complète' END AS couverture
FROM c JOIN h ON h.annee = c.annee"""

SQL_TAUX_SEXE_AGE = """WITH h AS (
  SELECT annee, sexe, tranche_age, COUNT(*) AS hospitalisations FROM hive.gold.f_hospitalisation GROUP BY 1, 2, 3
), c AS (
  SELECT annee, sexe, tranche_age, COUNT(*) AS consultations FROM hive.silver.f_consultation_detail
  WHERE id_temps BETWEEN 20150621 AND 20210619 GROUP BY 1, 2, 3
)
SELECT h.annee, h.sexe, h.tranche_age, h.hospitalisations, c.consultations
FROM h JOIN c ON h.annee = c.annee AND h.sexe = c.sexe AND h.tranche_age = c.tranche_age"""

SQL_SATISFACTION = """SELECT s.annee, s.region, COALESCE(r.iso_region, 'inconnu') AS iso_region, s.finess,
       s.score_satisfaction, s.nb_repondants, s.nb_sites
FROM hive.gold.f_satisfaction s LEFT JOIN hive.vues_t.ref_region_iso r ON r.region = s.region"""

SQL_MESURES = """SELECT f.annee, f.annee_mois, f.sexe, f.tranche_age, f.region,
       CASE WHEN f.id_etablissement > 0 THEN e.raison_sociale_site ELSE 'Non rattaché' END AS etablissement,
       l.nom_departement AS departement, f.nbre_consultation, f.nbre_hospitalisation, f.nbre_deces
FROM hive.gold.f_mesures f
LEFT JOIN hive.gold.d_etablissement e ON e.id_etablissement = f.id_etablissement
LEFT JOIN hive.gold.d_localisation l ON l.id_localisation = f.id_localisation"""

SQL_DISTRIB_PRO = """SELECT annee, n AS consultations_par_professionnel, COUNT(*) AS nb_professionnels
FROM (SELECT annee, id_professionnel, SUM(nbre_consultation) AS n FROM hive.gold.f_consult_prof_mois GROUP BY 1, 2)
GROUP BY 1, 2"""

SQL_VOLUMES = """SELECT CASE ordre WHEN 1 THEN '1. raw (brut)' WHEN 2 THEN '2. silver (nettoyé)' ELSE '3. gold (étoile)' END AS couche,
 table_name AS table_lac, lignes FROM (
 SELECT 1 AS ordre, 'raw' AS couche, 'hospitalisations' AS table_name, COUNT(*) AS lignes FROM hive.raw.hospitalisations UNION ALL
 SELECT 1, 'raw', 'etablissement_sante', COUNT(*) FROM hive.raw.etablissement_sante UNION ALL
 SELECT 1, 'raw', 'deces', COUNT(*) FROM hive.raw.deces UNION ALL
 SELECT 1, 'raw', 'satisfaction_2019', COUNT(*) FROM hive.raw.satisfaction_2019 UNION ALL
 SELECT 1, 'raw', 'satisfaction_2020', COUNT(*) FROM hive.raw.satisfaction_2020 UNION ALL
 SELECT 2, 'silver', 'f_consultation_detail', COUNT(*) FROM hive.silver.f_consultation_detail UNION ALL
 SELECT 3, 'gold', 'd_temps', COUNT(*) FROM hive.gold.d_temps UNION ALL
 SELECT 3, 'gold', 'd_patient', COUNT(*) FROM hive.gold.d_patient UNION ALL
 SELECT 3, 'gold', 'd_etablissement', COUNT(*) FROM hive.gold.d_etablissement UNION ALL
 SELECT 3, 'gold', 'd_localisation', COUNT(*) FROM hive.gold.d_localisation UNION ALL
 SELECT 3, 'gold', 'd_diagnostic', COUNT(*) FROM hive.gold.d_diagnostic UNION ALL
 SELECT 3, 'gold', 'd_professionnel', COUNT(*) FROM hive.gold.d_professionnel UNION ALL
 SELECT 3, 'gold', 'f_mesures', COUNT(*) FROM hive.gold.f_mesures UNION ALL
 SELECT 3, 'gold', 'f_hospitalisation', COUNT(*) FROM hive.gold.f_hospitalisation UNION ALL
 SELECT 3, 'gold', 'f_deces_agg', COUNT(*) FROM hive.gold.f_deces_agg UNION ALL
 SELECT 3, 'gold', 'f_satisfaction', COUNT(*) FROM hive.gold.f_satisfaction UNION ALL
 SELECT 3, 'gold', 'f_consult_diag_mois', COUNT(*) FROM hive.gold.f_consult_diag_mois UNION ALL
 SELECT 3, 'gold', 'f_consult_prof_mois', COUNT(*) FROM hive.gold.f_consult_prof_mois)"""

SQL_BENCH = """SELECT requete, CAST(avant_ms_median AS double) AS avant_ms_median, CAST(apres_ms_median AS double) AS apres_ms_median,
       CAST(apres_ml_ms_median AS double) AS apres_ml_ms_median, CAST(ratio_volume_lu AS double) AS ratio_volume_lu
FROM hive.archive.bench_synthese"""


def sql_top_etablissements(mode):
    if mode == "archive":
        return """SELECT 2019 AS annee, SUBSTR(etablissement, 1, 26) || ' (' || code_commune || ')' AS etablissement_commune, region,
       consultations AS nbre_consultations FROM hive.archive.q01_consult_etab_2019"""
    return """SELECT f.annee, SUBSTR(e.raison_sociale_site, 1, 26) || ' (' || COALESCE(e.code_commune, '?') || ')' AS etablissement_commune,
       e.region, f.nbre_consultation AS nbre_consultations
FROM hive.gold.f_mesures f JOIN hive.gold.d_etablissement e ON f.id_etablissement = e.id_etablissement
WHERE f.nbre_consultation > 0"""


def sql_deces_region(mode):
    if mode == "archive":
        return """SELECT 2019 AS annee, q.region, COALESCE(r.iso_region, 'inconnu') AS iso_region, q.deces AS nbre_deces
FROM hive.archive.q07_deces_region_2019 q LEFT JOIN hive.vues_t.ref_region_iso r ON r.region = q.region"""
    return """SELECT a.annee, a.sexe, a.tranche_age, a.region, COALESCE(r.iso_region, 'inconnu') AS iso_region, a.nbre_deces
FROM hive.gold.f_deces_agg a LEFT JOIN hive.vues_t.ref_region_iso r ON r.region = a.region"""


def sql_fiabilite(mode):
    if mode == "archive":
        return "SELECT controle, CAST(taux_pct AS double) AS taux_pct FROM hive.archive.dq_fiabilite"
    return """SELECT 'Consultations rattachées à un établissement' AS controle,
       100.0 * COUNT_IF(id_etablissement > 0) / COUNT(*) AS taux_pct FROM hive.silver.f_consultation_detail UNION ALL
SELECT 'Hospitalisations rattachées à un établissement', 100.0 * COUNT_IF(id_etablissement > 0) / COUNT(*) FROM hive.gold.f_hospitalisation UNION ALL
SELECT 'Consultations avec diagnostic valide', 100.0 * COUNT_IF(d.id_diagnostic IS NOT NULL) / COUNT(*)
  FROM hive.silver.f_consultation_detail f LEFT JOIN hive.gold.d_diagnostic d ON d.id_diagnostic = f.id_diagnostic UNION ALL
SELECT 'Lignes de satisfaction avec un score (2019)', 100.0 * COUNT_IF(TRIM(score_all_rea_ajust) <> '') / COUNT(*) FROM hive.raw.satisfaction_2019 UNION ALL
SELECT 'Lignes de satisfaction avec un score (2020)', 100.0 * COUNT_IF(TRIM(score_all_rea_ajust) <> '') / COUNT(*) FROM hive.raw.satisfaction_2020"""


# --------------------------------------------------------------------------- datasets virtuels
def virtuels(mode):
    """(nom, description, sql, metriques[(nom, libelle, expression, format)], libelles de colonnes {col: libelle})."""
    M = lambda *a: a
    comm = {"annee": "Année", "annee_mois": "Mois", "sexe": "Sexe", "tranche_age": "Tranche d'âge",
            "region": "Région", "etablissement": "Établissement", "chapitre_cim10": "Chapitre CIM-10",
            "code_diag": "Code diagnostic", "libelle_diagnostic": "Libellé du diagnostic", "diagnostic": "Diagnostic",
            "departement": "Département", "avant_naissance": "Date avant la naissance du patient",
            "rattache_etablissement": "Rattaché à un établissement", "date_jour": "Date"}
    return [
        ("etoile_consultations",
         "Étoile des consultations : f_consultation_detail (Silver) jointe à d_temps, d_patient, d_diagnostic, d_etablissement, d_localisation. 1 ligne = 1 consultation.",
         sql_etoile_consultations(),
         [M("nbre_consultations", "Consultations", "COUNT(*)", ",d"),
          M("nb_patients", "Patients distincts", "COUNT(DISTINCT id_patient)", ",d"),
          M("nb_professionnels", "Professionnels distincts", "COUNT(DISTINCT id_professionnel)", ",d"),
          M("part_rattachee_pct", "Part rattachée à un établissement (%)", "100.0 * COUNT_IF(rattache_etablissement = 'Oui') / COUNT(*)", ",.2f"),
          M("nb_avant_naissance", "Consultations datées avant la naissance", "COUNT_IF(avant_naissance = 'Oui')", ",d")],
         comm),
        ("etoile_hospitalisations",
         "Étoile des hospitalisations : f_hospitalisation jointe à d_temps, d_patient, d_diagnostic, d_etablissement, d_localisation. 1 ligne = 1 hospitalisation.",
         sql_etoile_hospitalisations(),
         [M("nbre_hospitalisations", "Hospitalisations", "COUNT(*)", ",d"),
          M("duree_moyenne_jours", "Durée moyenne de séjour (jours)", "AVG(duree_jours)", ",.1f"),
          M("nb_patients_hospitalises", "Patients hospitalisés", "COUNT(DISTINCT id_patient)", ",d"),
          M("nb_avant_naissance", "Hospitalisations datées avant la naissance", "COUNT_IF(avant_naissance = 'Oui')", ",d")],
         comm),
        ("taux_hospitalisation_annuel",
         "Hospitalisations rapportées aux consultations de la même fenêtre d'observation (2015-06-21 au 2021-06-19).",
         SQL_TAUX_ANNUEL,
         [M("taux_hosp_pour_100", "Hospitalisations pour 100 consultations", "100.0 * SUM(hospitalisations) / SUM(consultations_fenetre)", ",.3f"),
          M("hospitalisations", "Hospitalisations", "SUM(hospitalisations)", ",d"),
          M("consultations_fenetre", "Consultations de la fenêtre", "SUM(consultations_fenetre)", ",d")],
         {"annee": "Année", "couverture": "Couverture de l'année"}),
        ("taux_hosp_sexe_age",
         "Hospitalisations pour 1 000 consultations, par sexe et tranche d'âge (même fenêtre d'observation).",
         SQL_TAUX_SEXE_AGE,
         [M("taux_pour_1000", "Hospitalisations pour 1 000 consultations", "1000.0 * SUM(hospitalisations) / SUM(consultations)", ",.2f"),
          M("hospitalisations", "Hospitalisations", "SUM(hospitalisations)", ",d"),
          M("consultations", "Consultations", "SUM(consultations)", ",d")],
         comm),
        ("etoile_satisfaction", "Satisfaction e-Satis (gold.f_satisfaction) avec le code ISO de région.", SQL_SATISFACTION,
         [M("score_pondere", "Score de satisfaction pondéré par les répondants", "SUM(score_satisfaction * nb_repondants) / SUM(CASE WHEN score_satisfaction IS NOT NULL THEN nb_repondants END)", ",.2f"),
          M("nb_repondants_total", "Répondants", "SUM(nb_repondants)", ",d")],
         comm),
        ("deces_par_region", "Décès par région. Mode %s." % mode, sql_deces_region(mode),
         [M("nbre_deces", "Décès", "SUM(nbre_deces)", ",d")], comm),
        ("top_etablissements", "Consultations par établissement. Mode %s." % mode, sql_top_etablissements(mode),
         [M("nbre_consultations", "Consultations", "SUM(nbre_consultations)", ",d")],
         {"annee": "Année", "etablissement_commune": "Établissement (commune)", "region": "Région"}),
        ("fiabilite_donnees", "Taux de fiabilité des données (contrôles qualité). Mode %s." % mode, sql_fiabilite(mode),
         [M("taux_pct", "Taux (%)", "AVG(taux_pct)", ",.2f")], {"controle": "Contrôle"}),
        ("etoile_mesures", "Table de faits consolidée f_mesures jointe aux dimensions établissement et localisation.", SQL_MESURES,
         [M("consultations", "Consultations", "SUM(nbre_consultation)", ",d"),
          M("hospitalisations", "Hospitalisations", "SUM(nbre_hospitalisation)", ",d"),
          M("deces", "Décès", "SUM(nbre_deces)", ",d")], comm),
        ("distribution_activite_professionnels", "Nombre de professionnels selon leur nombre de consultations dans l'année.", SQL_DISTRIB_PRO,
         [M("nb_professionnels", "Professionnels", "SUM(nb_professionnels)", ",d")],
         {"annee": "Année", "consultations_par_professionnel": "Consultations par professionnel"}),
        ("volumes_du_lac", "Nombre de lignes de chaque table du lac, par couche (raw, silver, gold).", SQL_VOLUMES,
         [M("lignes_total", "Lignes", "SUM(lignes)", ",d")], {"couche": "Couche", "table_lac": "Table"}),
        ("benchmark_partitionnement", "Benchmark avant et après partitionnement (médiane de 5 répétitions).", SQL_BENCH,
         [M("avant_ms", "Avant (tables plates), ms", "AVG(avant_ms_median)", ",.0f"),
          M("apres_ms", "Après (partitionné), ms", "AVG(apres_ms_median)", ",.0f"),
          M("apres_ml_ms", "Après + pruning metastore, ms", "AVG(apres_ml_ms_median)", ",.0f"),
          M("ratio_volume", "Volume lu avant / après", "AVG(ratio_volume_lu)", ",.1f")],
         {"requete": "Requête"}),
    ]
