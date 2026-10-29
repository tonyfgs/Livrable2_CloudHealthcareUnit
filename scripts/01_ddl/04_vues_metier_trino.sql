-- ============================================================================
-- Livrable 2 - CHU - Vues métier côté TRINO (schéma `vues_t`)
-- ----------------------------------------------------------------------------
-- Le connecteur Hive de Trino ne sait PAS lire les vues Hive stockées dans le
-- metastore (« Hive views are not supported ») : 03_vues_metier.sql (exécuté
-- via beeline/HS2) crée les vues HIVE, illisibles depuis Trino/Superset.
-- Ce fichier crée les MÊMES vues comme vues NATIVES Trino (schéma `vues_t`),
-- utilisées par Superset et le harnais de benchmark Trino. Les tables sont
-- qualifiées hive.gold.* car la vue vit dans un autre schéma.
--
-- Exécution : docker exec chu-trino trino --server http://localhost:8080 \
--              --catalog hive --file /opt/scripts/01_ddl/04_vues_metier_trino.sql
-- (intégré au DAG lakehouse_init, tâche `vues_metier_trino`)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS hive.vues_t
  WITH (location = 's3a://lake/warehouse/vues_t');

DROP VIEW IF EXISTS hive.vues_t.v_fait_consultation;
CREATE VIEW hive.vues_t.v_fait_consultation AS
SELECT annee_mois, id_temps, annee, sexe, tranche_age,
       id_etablissement, id_patient, id_localisation, region,
       nbre_consultation
FROM hive.gold.f_mesures
WHERE nbre_consultation > 0;

DROP VIEW IF EXISTS hive.vues_t.v_fait_hospitalisation;
CREATE VIEW hive.vues_t.v_fait_hospitalisation AS
SELECT annee_mois, id_temps, annee, sexe, tranche_age,
       id_etablissement, id_patient, id_localisation, region,
       nbre_hospitalisation
FROM hive.gold.f_mesures
WHERE nbre_hospitalisation > 0;

DROP VIEW IF EXISTS hive.vues_t.v_fait_deces;
CREATE VIEW hive.vues_t.v_fait_deces AS
SELECT annee_mois, annee, sexe, tranche_age,
       id_localisation, region,
       nbre_deces
FROM hive.gold.f_mesures
WHERE nbre_deces > 0;

DROP VIEW IF EXISTS hive.vues_t.v_fait_satisfaction;
CREATE VIEW hive.vues_t.v_fait_satisfaction AS
SELECT annee_mois, annee, region,
       total_satisfaction
FROM hive.gold.f_mesures
WHERE total_satisfaction IS NOT NULL;

DROP VIEW IF EXISTS hive.vues_t.v_taux_consultation_mensuel;
CREATE VIEW hive.vues_t.v_taux_consultation_mensuel AS
SELECT annee,
       annee_mois,
       SUM(nbre_consultation) AS nbre_consultations,
       ROUND(AVG(SUM(nbre_consultation)) OVER (
               ORDER BY annee_mois
               ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 0) AS moyenne_mobile_3mois
FROM hive.gold.f_mesures
WHERE nbre_consultation > 0
GROUP BY annee, annee_mois;

DROP VIEW IF EXISTS hive.vues_t.v_consultations_region_annee;
CREATE VIEW hive.vues_t.v_consultations_region_annee AS
SELECT annee, region,
       SUM(nbre_consultation) AS nbre_consultations,
       COUNT(DISTINCT id_etablissement) AS nb_etablissements
FROM hive.gold.f_mesures
WHERE nbre_consultation > 0
GROUP BY annee, region;

DROP VIEW IF EXISTS hive.vues_t.v_deces_region_annee;
CREATE VIEW hive.vues_t.v_deces_region_annee AS
SELECT f.annee, l.region,
       SUM(f.nbre_deces) AS nbre_deces
FROM hive.gold.f_deces_agg f
JOIN hive.gold.d_localisation l ON f.id_localisation = l.id_localisation
GROUP BY f.annee, l.region;

DROP VIEW IF EXISTS hive.vues_t.v_satisfaction_region_annee;
CREATE VIEW hive.vues_t.v_satisfaction_region_annee AS
SELECT annee, region,
       AVG(score_satisfaction) AS score_moyen,
       SUM(nb_repondants) AS nb_repondants
FROM hive.gold.f_satisfaction
GROUP BY annee, region;

DROP VIEW IF EXISTS hive.vues_t.v_hospitalisations_etab_annee;
CREATE VIEW hive.vues_t.v_hospitalisations_etab_annee AS
SELECT f.annee, f.id_etablissement, e.raison_sociale_site, e.region,
       dg.code_diag, dg.libelle_diagnostic,
       COUNT(*) AS nbre_hospitalisations,
       AVG(f.duree_jours) AS duree_moyenne_jours
FROM hive.gold.f_hospitalisation f
JOIN hive.gold.d_etablissement e ON f.id_etablissement = e.id_etablissement
JOIN hive.gold.d_diagnostic dg ON f.id_diagnostic = dg.id_diagnostic
GROUP BY f.annee, f.id_etablissement, e.raison_sociale_site, e.region,
         dg.code_diag, dg.libelle_diagnostic;
