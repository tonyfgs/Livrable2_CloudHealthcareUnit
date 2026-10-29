-- ============================================================================
-- Livrable 2 - Cloud Healthcare Unit (CHU) - Vues métier (couche Gold)
-- SQL portable : exécutable sous Hive (beeline) ET sous Trino (trino-cli).
--
-- Les vues exposent les 4 faits métier du Livrable 1 (consultation,
-- hospitalisation, décès, satisfaction) dérivés du fait consolidé f_mesures,
-- plus une vue analytique (taux de consultation mensuel avec moyenne mobile).
--
-- Exécution Hive :   docker exec chu-hive-metastore beeline \
--                      -u jdbc:hive2://localhost:10000/gold \
--                      -f /opt/scripts/01_ddl/03_vues_metier.sql
-- Exécution Trino :  trino --server http://localhost:8080 --catalog hive \
--                      --schema gold -f scripts/01_ddl/03_vues_metier.sql
-- (Sous Trino, les vues sont stockées dans le schéma gold du catalogue hive ;
--  en alternative, les déclarer comme datasets virtuels dans Superset.)
--
-- Conventions rappelées (cf. 01_schema_gold_hive.sql) :
--   * f_mesures : grain mensuel ('AAAA-MM') pour consultations/hospitalisations,
--     annuel (sentinelle 'AAAA-00') pour décès/satisfaction ;
--   * sexe / tranche_age = 'TOUS' / 'TOUTES' quand non applicable (décès,
--     satisfaction) ; id_patient = -1, id_etablissement = -1 (membre Inconnu) ;
--   * annee, sexe, tranche_age = colonnes de partition (pruning) ;
--   * region dénormalisée dans les faits (filtre sans jointure).
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Fait métier CONSULTATION (dérivé de f_mesures, périmètre mensuel)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS v_fait_consultation;
CREATE VIEW v_fait_consultation AS
SELECT annee_mois, id_temps, annee, sexe, tranche_age,
       id_etablissement, id_patient, id_localisation, region,
       nbre_consultation
FROM f_mesures
WHERE nbre_consultation > 0;

-- ---------------------------------------------------------------------------
-- 2. Fait métier HOSPITALISATION (dérivé de f_mesures, périmètre mensuel)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS v_fait_hospitalisation;
CREATE VIEW v_fait_hospitalisation AS
SELECT annee_mois, id_temps, annee, sexe, tranche_age,
       id_etablissement, id_patient, id_localisation, region,
       nbre_hospitalisation
FROM f_mesures
WHERE nbre_hospitalisation > 0;

-- ---------------------------------------------------------------------------
-- 3. Fait métier DÉCÈS (grain annuel : annee_mois = 'AAAA-00')
--    Les décès sont stockés au grain département (pas de clé patient/étab.)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS v_fait_deces;
CREATE VIEW v_fait_deces AS
SELECT annee_mois, annee, sexe, tranche_age,
       id_localisation, region,
       nbre_deces
FROM f_mesures
WHERE nbre_deces > 0;

-- ---------------------------------------------------------------------------
-- 4. Fait métier SATISFACTION (grain annuel, rattaché à la région)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS v_fait_satisfaction;
CREATE VIEW v_fait_satisfaction AS
SELECT annee_mois, annee, region,
       total_satisfaction
FROM f_mesures
WHERE total_satisfaction IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 5. Vue analytique : taux de consultation mensuel national + moyenne mobile
--    3 mois (fonction fenêtrée sur agrégat - supportée Hive 2.2+ et Trino)
-- ---------------------------------------------------------------------------
DROP VIEW IF EXISTS v_taux_consultation_mensuel;
CREATE VIEW v_taux_consultation_mensuel AS
SELECT annee,
       annee_mois,
       SUM(nbre_consultation) AS nbre_consultations,
       ROUND(AVG(SUM(nbre_consultation)) OVER (
               ORDER BY annee_mois
               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0) AS moyenne_mobile_3mois
FROM f_mesures
WHERE nbre_consultation > 0
GROUP BY annee, annee_mois;

-- ---------------------------------------------------------------------------
-- 6. Vues d'agrégat prêtes pour le tableau de bord Superset
-- ---------------------------------------------------------------------------

-- Consultations par région et par année (top régions)
DROP VIEW IF EXISTS v_consultations_region_annee;
CREATE VIEW v_consultations_region_annee AS
SELECT annee, region,
       SUM(nbre_consultation) AS nbre_consultations,
       COUNT(DISTINCT id_etablissement) AS nb_etablissements
FROM f_mesures
WHERE nbre_consultation > 0
GROUP BY annee, region;

-- Décès par région et par année (source INSEE, via f_deces_agg)
DROP VIEW IF EXISTS v_deces_region_annee;
CREATE VIEW v_deces_region_annee AS
SELECT f.annee, l.region,
       SUM(f.nbre_deces) AS nbre_deces
FROM f_deces_agg f
JOIN d_localisation l ON f.id_localisation = l.id_localisation
GROUP BY f.annee, l.region;

-- Satisfaction moyenne par région et par année (source ESATIS)
DROP VIEW IF EXISTS v_satisfaction_region_annee;
CREATE VIEW v_satisfaction_region_annee AS
SELECT annee, region,
       AVG(score_satisfaction) AS score_moyen,
       SUM(nb_repondants) AS nb_repondants
FROM f_satisfaction
GROUP BY annee, region;

-- Hospitalisations par établissement et par année (avec diagnostic)
DROP VIEW IF EXISTS v_hospitalisations_etab_annee;
CREATE VIEW v_hospitalisations_etab_annee AS
SELECT f.annee, f.id_etablissement, e.raison_sociale_site, e.region,
       dg.code_diag, dg.libelle_diagnostic,
       COUNT(*) AS nbre_hospitalisations,
       AVG(f.duree_jours) AS duree_moyenne_jours
FROM f_hospitalisation f
JOIN d_etablissement e ON f.id_etablissement = e.id_etablissement
JOIN d_diagnostic dg ON f.id_diagnostic = dg.id_diagnostic
GROUP BY f.annee, f.id_etablissement, e.raison_sociale_site, e.region,
         dg.code_diag, dg.libelle_diagnostic;

-- ============================================================================
-- Vérification rapide (à exécuter après création) :
--   SELECT * FROM v_taux_consultation_mensuel ORDER BY annee_mois LIMIT 5;
--   SELECT * FROM v_fait_consultation LIMIT 5;
--   SELECT * FROM v_deces_region_annee WHERE annee = 2019 ORDER BY nbre_deces DESC LIMIT 5;
-- ============================================================================
