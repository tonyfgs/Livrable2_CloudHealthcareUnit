-- ============================================================================
-- Livrable 2 - CHU - Superset : requêtes des 8 tableaux de bord (SQL Lab)
-- ----------------------------------------------------------------------------
-- Connexion : base de données "Trino CHU" (trino://trino@trino:8080/hive),
-- schéma par défaut : gold. Chaque requête ci-dessous = 1 chart du dashboard
-- "CHU - Cloud Healthcare Unit" (1 chart par besoin métier du Livrable 1).
-- Coller chaque requête dans SQL Lab (Superset > SQL > SQL Lab), sélectionner
-- la base "Trino CHU", schéma "gold", puis "Visualize".
-- ============================================================================

-- ---------------------------------------------------------------------------
-- Chart 1 (besoin 1) : consultations par établissement - top 20, 2019
-- Type recommandé : Bar chart (horizontal) | X = nbre_consultations | Y = raison_sociale_site
-- ---------------------------------------------------------------------------
SELECT e.raison_sociale_site,
       e.code_commune,
       e.region,
       SUM(f.nbre_consultation) AS nbre_consultations
FROM gold.f_mesures f
JOIN gold.d_etablissement e ON f.id_etablissement = e.id_etablissement
WHERE f.annee = 2019 AND f.nbre_consultation > 0
GROUP BY 1, 2, 3
ORDER BY nbre_consultations DESC
LIMIT 20;

-- ---------------------------------------------------------------------------
-- Chart 2 (besoin 2) : consultations par diagnostic - top 10, 2019-2021
-- (s'appuie sur l'agrégat pré-calculé f_consult_diag_mois)
-- Type recommandé : Bar chart | X = nbre_consultations | Y = libelle_diagnostic
-- ---------------------------------------------------------------------------
SELECT d.libelle_diagnostic,
       SUM(f.nbre_consultation) AS nbre_consultations
FROM gold.f_consult_diag_mois f
JOIN gold.d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
WHERE f.annee BETWEEN 2019 AND 2021
GROUP BY 1
ORDER BY nbre_consultations DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Chart 3 (besoin 3) : taux d'hospitalisation pour 100 consultations, 2015-2021
-- Type recommandé : Line chart | X = annee | Y = taux_hospitalisation_pour_100_consultations
-- ---------------------------------------------------------------------------
SELECT f.annee,
       SUM(f.nbre_hospitalisation) AS nbre_hospitalisations,
       SUM(f.nbre_consultation) AS nbre_consultations,
       ROUND(100.0 * SUM(f.nbre_hospitalisation) / NULLIF(SUM(f.nbre_consultation), 0), 2)
         AS taux_hospitalisation_pour_100_consultations
FROM gold.f_mesures f
WHERE f.annee BETWEEN 2015 AND 2021
GROUP BY f.annee
ORDER BY f.annee;

-- ---------------------------------------------------------------------------
-- Chart 4 (besoin 4) : hospitalisations par diagnostic - top 10, 2015-2021
-- Type recommandé : Bar chart | X = nbre_hospitalisations | Y = libelle_diagnostic
-- ---------------------------------------------------------------------------
SELECT d.libelle_diagnostic,
       COUNT(*) AS nbre_hospitalisations,
       ROUND(AVG(f.duree_jours), 1) AS duree_moyenne_jours
FROM gold.f_hospitalisation f
JOIN gold.d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
WHERE f.annee BETWEEN 2015 AND 2021
GROUP BY 1
ORDER BY nbre_hospitalisations DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Chart 5 (besoin 5) : hospitalisations par sexe et tranche d'âge, 2015-2021
-- Type recommandé : Bar chart groupé | X = tranche_age | Y = nbre_hospitalisations | séries = sexe
-- ---------------------------------------------------------------------------
SELECT f.sexe,
       f.tranche_age,
       SUM(f.nbre_hospitalisation) AS nbre_hospitalisations
FROM gold.f_mesures f
WHERE f.annee BETWEEN 2015 AND 2021 AND f.nbre_hospitalisation > 0
GROUP BY 1, 2
ORDER BY f.sexe, f.tranche_age;

-- ---------------------------------------------------------------------------
-- Chart 6 (besoin 6) : consultations par professionnel - top 10, 2020
-- (s'appuie sur l'agrégat pré-calculé f_consult_prof_mois)
-- Type recommandé : Table | colonnes = identifiant, civilite, nom, prenom, profession, nbre_consultations
-- ---------------------------------------------------------------------------
SELECT p.identifiant,
       p.civilite,
       p.nom,
       p.prenom,
       p.profession,
       SUM(f.nbre_consultation) AS nbre_consultations
FROM gold.f_consult_prof_mois f
JOIN gold.d_professionnel p ON f.id_professionnel = p.identifiant
WHERE f.annee = 2020
GROUP BY 1, 2, 3, 4, 5
ORDER BY nbre_consultations DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- Chart 7 (besoin 7) : décès par région, 2019
-- Type recommandé : Bar chart (ou carte) | X = nbre_deces | Y = region
-- ---------------------------------------------------------------------------
SELECT l.region AS region,
       SUM(f.nbre_deces) AS nbre_deces
FROM gold.f_deces_agg f
JOIN gold.d_localisation l ON f.id_localisation = l.id_localisation
WHERE f.annee = 2019
GROUP BY 1
ORDER BY nbre_deces DESC;

-- ---------------------------------------------------------------------------
-- Chart 8 (besoin 8) : satisfaction moyenne par région, 2020
-- Type recommandé : Bar chart | X = score_moyen_satisfaction | Y = region
-- ---------------------------------------------------------------------------
SELECT f.region,
       COUNT(*) AS nbre_etablissements,
       ROUND(AVG(f.score_satisfaction), 2) AS score_moyen_satisfaction,
       SUM(f.nb_repondants) AS total_repondants
FROM gold.f_satisfaction f
WHERE f.annee = 2020 AND f.score_satisfaction IS NOT NULL
GROUP BY 1
ORDER BY score_moyen_satisfaction DESC;

-- ============================================================================
-- Charts complémentaires (hors 8 besoins, disponibles via les vues métier) :
--   * Évolution mensuelle des consultations + moyenne mobile 3 mois :
--       SELECT * FROM vues_t.v_taux_consultation_mensuel ORDER BY annee_mois;
--       (vues_t = vues NATIVES Trino ; Trino ne lit pas les vues Hive du
--        metastore, cf. 01_ddl/04_vues_metier_trino.sql)
--   * Vue d'ensemble étoile (jointure fait + 4 dimensions) : cf. requête q10
--     du benchmark (scripts/04_benchmark/queries/q10_etoile_complete.sql)
-- ============================================================================
