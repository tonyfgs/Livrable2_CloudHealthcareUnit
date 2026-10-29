-- ============================================================================
-- Livrable 2 - Cloud Healthcare Unit (CHU) - Entrepôt de données décisionnel
-- Côté Trino : VÉRIFICATION du schéma GOLD (pas de DDL dupliqué)
--
-- Rôle de Trino dans la stack : moteur de REQUÊtage distribué (couche
-- "requête"), en lecture seule sur le data lake MinIO via le connecteur Hive.
-- Le catalogue (Hive Metastore) est la source unique de vérité pour les
-- métadonnées : le schéma est créé UNE SEULE FOIS par 01_schema_gold_hive.sql,
-- Trino le lit tel quel. Ce fichier vérifie le déploiement et calcule les
-- statistiques de l'optimiseur (équivalent Trino de ANALYZE TABLE Hive).
--
-- Configuration du connecteur (docker-compose.yml, volume
-- config/trino/catalog/hive.properties) :
--   connector.name=hive
--   hive.metastore.uri=thrift://hive-metastore:9083
--   fs.s3.enabled=true                   -- filesystem S3 NATIF Trino (depuis
--                                          Trino 460+, l'ancien S3A legacy
--                                          hive.s3.* est supprimé)
--   s3.endpoint=http://minio:9000
--   s3.aws-access-key=minioadmin
--   s3.aws-secret-key=minioadmin
--   s3.path-style-access=true
--   s3.region=us-east-1
--   hive.metastore-cache-ttl=1h
--   hive.parquet.use-column-names=true   -- lit les colonnes par NOM (pas par position)
--
-- Exécution : trino-cli --server http://localhost:8080 --catalog hive --schema gold
--   trino --file scripts/01_ddl/02_schema_gold_trino.sql
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Vérification du catalogue : les bases silver/gold et les 12 tables
-- ---------------------------------------------------------------------------
SHOW SCHEMAS FROM hive;
-- attendu : default, gold, information_schema, silver, sys

SHOW TABLES FROM hive.silver;
-- attendu : f_consultation_detail

SHOW TABLES FROM hive.gold;
-- attendu : d_diagnostic, d_etablissement, d_localisation, d_patient,
--          d_professionnel, d_temps, f_consult_diag_mois, f_consult_prof_mois,
--          f_deces_agg, f_hospitalisation, f_mesures, f_satisfaction

-- ---------------------------------------------------------------------------
-- 2. Vérification des structures (colonnes + colonnes de partition)
-- ---------------------------------------------------------------------------
DESCRIBE hive.gold.f_mesures;
-- colonnes de données : annee_mois, id_temps, id_etablissement, id_patient,
--   id_localisation, nbre_consultation, nbre_hospitalisation, nbre_deces,
--   total_satisfaction, region
-- colonnes de partition : annee (int), sexe (varchar), tranche_age (varchar)

DESCRIBE hive.gold.f_deces_agg;    -- partition : annee (int)
DESCRIBE hive.gold.f_satisfaction;  -- partitions : annee (varchar), region (varchar)
DESCRIBE hive.gold.f_hospitalisation;   -- partition : annee (int)
DESCRIBE hive.gold.f_consultation_detail; -- partition : annee (int) [silver]
DESCRIBE hive.gold.d_temps;         -- partition : annee (int)
DESCRIBE hive.gold.d_patient;       -- partitions : sexe (varchar), tranche_age_ref (varchar)
DESCRIBE hive.gold.d_localisation;  -- partition : region (varchar)
DESCRIBE hive.gold.d_etablissement; -- pas de partition (bucketing Hive)

-- ---------------------------------------------------------------------------
-- 3. Vérification des volumes (doit correspondre au rapport §Qualité)
-- ---------------------------------------------------------------------------
SELECT 'd_temps' AS table_name, COUNT(*) AS lignes FROM hive.gold.d_temps
UNION ALL SELECT 'd_patient',        COUNT(*) FROM hive.gold.d_patient
UNION ALL SELECT 'd_etablissement',   COUNT(*) FROM hive.gold.d_etablissement
UNION ALL SELECT 'd_localisation',    COUNT(*) FROM hive.gold.d_localisation
UNION ALL SELECT 'd_diagnostic',     COUNT(*) FROM hive.gold.d_diagnostic
UNION ALL SELECT 'd_professionnel',   COUNT(*) FROM hive.gold.d_professionnel
UNION ALL SELECT 'f_mesures',         COUNT(*) FROM hive.gold.f_mesures
UNION ALL SELECT 'f_consultation_detail', COUNT(*) FROM hive.silver.f_consultation_detail
UNION ALL SELECT 'f_hospitalisation', COUNT(*) FROM hive.gold.f_hospitalisation
UNION ALL SELECT 'f_deces_agg',       COUNT(*) FROM hive.gold.f_deces_agg
UNION ALL SELECT 'f_satisfaction',    COUNT(*) FROM hive.gold.f_satisfaction
UNION ALL SELECT 'f_consult_diag_mois',  COUNT(*) FROM hive.gold.f_consult_diag_mois
UNION ALL SELECT 'f_consult_prof_mois',  COUNT(*) FROM hive.gold.f_consult_prof_mois;
-- attendu : d_temps 5 844 ; d_patient 100 000 ; d_etablissement 416 665 ;
--   d_localisation 102 ; d_diagnostic 15 490 ; d_professionnel 1 048 575 ;
--   f_mesures 1 030 402 ; f_consultation_detail 1 027 157 ;
--   f_hospitalisation 2 479 ; f_deces_agg 69 008 ; f_satisfaction 1 960 ;
--   f_consult_diag_mois 979 797 ; f_consult_prof_mois 1 001 453
-- (f_mesures : grain 7 colonnes dont region ; 0 doublon sur ce grain.
--  f_satisfaction : 1 960 finess juridiques, 2 295 lignes source agrégées
--  en moyenne pondérée par les répondants.)

-- ---------------------------------------------------------------------------
-- 4. Statistiques pour l'optimiseur Trino (équivalent de ANALYZE TABLE Hive)
--    -> alimentent le choix des plans (pruning de partitions, jointures,
--       distribution des agrégations). À relancer après chaque chargement Spark.
-- ---------------------------------------------------------------------------
ANALYZE hive.gold.f_mesures;
ANALYZE hive.gold.f_deces_agg;
ANALYZE hive.gold.f_hospitalisation;
ANALYZE hive.gold.f_satisfaction;
ANALYZE hive.silver.f_consultation_detail;
ANALYZE hive.gold.f_consult_diag_mois;
ANALYZE hive.gold.f_consult_prof_mois;
ANALYZE hive.gold.d_temps;
ANALYZE hive.gold.d_patient;
ANALYZE hive.gold.d_etablissement;
ANALYZE hive.gold.d_localisation;
ANALYZE hive.gold.d_diagnostic;
ANALYZE hive.gold.d_professionnel;

-- ---------------------------------------------------------------------------
-- 5. Vérification du pruning de partitions (EXPLAIN doit montrer moins de
--    partitions scannées qu'au total) - ex. avec le filtre annee = 2019 :
-- ---------------------------------------------------------------------------
EXPLAIN (TYPE IO)
SELECT SUM(nbre_consultation)
FROM hive.gold.f_mesures
WHERE annee = 2019 AND nbre_consultation > 0;
-- le champ "parquet" doit indiquer un sous-ensemble des 9 partitions annee
-- (2015-2023) de f_mesures, et non les 9 x 3 x 8 = 216 combinaisons
-- (annee, sexe, tranche_age ; 122 réellement présentes dans les données).

-- ---------------------------------------------------------------------------
-- 6. Requête de bout en bout (jointure étoile) - sanity check Superset
-- ---------------------------------------------------------------------------
SELECT l.region, COUNT(*) AS nb_lignes, SUM(f.nbre_consultation) AS consultations
FROM hive.gold.f_mesures f
JOIN hive.gold.d_localisation l ON f.id_localisation = l.id_localisation
WHERE f.annee = 2019 AND f.nbre_consultation > 0
GROUP BY l.region
ORDER BY consultations DESC
LIMIT 5;
-- ============================================================================
