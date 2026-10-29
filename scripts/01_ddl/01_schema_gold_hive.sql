-- ============================================================================
-- Livrable 2 - Cloud Healthcare Unit (CHU) - Entrepôt de données décisionnel
-- DDL Hive - couche GOLD (schéma en étoile) sur data lake MinIO (s3a://lake)
-- Moteur : Hive 4 (tables externes) - lecture : Trino (connecteur Hive)
--
-- Conventions :
--   * Tables EXTERNES : les données appartiennent au data lake (MinIO) ; le
--     catalogue (Hive Metastore) ne référence que les métadonnées. Un DROP TABLE
--     ne supprime jamais les données.
--   * LOCATION 's3a://lake/...' : chemin MinIO via le connecteur S3A (Hadoop).
--   * Toutes les tables sont STORED AS PARQUET : format colonnaire compressé,
--     pushdown des prédicats, statistiques par fichier (min/max) pour le pruning.
--   * Clés de substitution (surrogate keys) dans toutes les dimensions.
--
-- Justification du partitionnement (cf. rapport §Partitionnement) :
--   f_mesures          PARTITIONED BY (annee, sexe, tranche_age)
--                      -> pruning temporel (annee) + démographique (sexe,
--                         tranche_age) ; conforme à la grille du livrable.
--   f_consultation_detail PARTITIONED BY (annee) -> pruning temporel.
--   f_hospitalisation  PARTITIONED BY (annee).
--   f_deces_agg        PARTITIONED BY (annee) SEULEMENT : un partitionnement par
--                      (annee, sexe, tranche_age) produirait 1 251 combinaisons
--                      réelles (~55 lignes chacune ; borne 133 x 2 x 7 = 1 862)
--                      pour 69 008 lignes (small-files problem) ; annee seul
--                      donne 133 partitions (~519 lignes). Le tri intra-partition
--                      compense.
--   f_satisfaction     PARTITIONED BY (annee, code_region) : grain juridique x
--                      année ; code_region (et non region) car les noms de
--                      régions contiennent espaces/accents/apostrophes, que
--                      Hive refuse dans les noms de répertoires de partitions.
--   d_temps            PARTITIONED BY (annee)    (grain jour, 2015-2030,
--                      couvre le périmètre 2015-2021 de la grille + marge).
--   d_patient          PARTITIONED BY (sexe, tranche_age_ref) : partitionnement
--                      démographique (grille : sexe + tranche d'âge sur la
--                      dimension patient).
--   d_localisation     PARTITIONED BY (code_region) (grain département, 101 +
--                      Inconnu ; code_region car les noms de régions contiennent
--                      des caractères invalides dans les chemins de partitions).
--   d_etablissement, d_diagnostic, d_professionnel : pas de partitionnement
--                      (tables de taille moyenne, une seule "partition" racine).
--
-- PAS de bucketing (CLUSTERED BY ... INTO n BUCKETS) dans ce schéma, volontairement :
-- Spark n'écrit un fichier par bucket NON VIDE (avec AQE, une petite partition
-- produit p. ex. 2 fichiers pour 32 buckets déclarés), et Trino 484 refuse alors
-- de lire la table ("Hive table is corrupt ... number of files does not match the
-- declared bucket count"), même avec hive.validate-bucketing=false. Le schéma
-- retenu est donc : PARTITIONNEMENT Hive (pruning par partition côté Trino) +
-- tri intra-fichier sur les clés de jointure (pruning intra-fichier via les
-- statistiques min/max Parquet) — combinaison supportée nativement et robuste.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS silver
  LOCATION 's3a://lake/silver';

CREATE DATABASE IF NOT EXISTS gold
  LOCATION 's3a://lake/gold';

USE gold;

-- ---------------------------------------------------------------------------
-- DIMENSIONS (clés de substitution = identifiants surrogate)
-- ---------------------------------------------------------------------------

-- d_temps : grain = jour, couverture 2015-2030 (les faits couvrent 2015-2023,
-- les décès 1882-2020 ; la dimension est volontairement prolongée pour les
-- besoins de planification). Clé de substitution : id_temps = AAAAMMJJ.
CREATE EXTERNAL TABLE IF NOT EXISTS gold.d_temps (
  id_temps      INT,          -- AAAAMMJJ (clé de substitution)
  date_jour     DATE,
  mois          INT,
  annee_mois    STRING,       -- 'AAAA-MM'
  trimestre     INT,
  jour_semaine  INT,          -- 0 = dimanche
  est_weekend   BOOLEAN
)
PARTITIONED BY (annee INT)
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_temps';

-- d_patient : depuis S1 PostgreSQL.Patient (100 000 patients).
-- sexe normalisé M/F ; tranche_age_ref = tranche à la date de référence 2023-12-31.
-- L'attribut Age de S1 n'est PAS retenu (incohérent avec la date de naissance,
-- cf. rapport qualité : 46 % de cohérence seulement) : la tranche d'âge est
-- recalculée à la date de l'événement dans les faits.
CREATE EXTERNAL TABLE IF NOT EXISTS gold.d_patient (
  id_patient      BIGINT,     -- clé de substitution (identique à S1, clé naturelle)
  date_naissance  DATE,
  age_ref         INT,        -- âge à la date de référence (indicatif, non utilisé)
  code_postal     STRING,
  ville           STRING
  -- 'sexe' et 'tranche_age_ref' = colonnes de partition (chemins Hive
  -- sexe=.../tranche_age_ref=...), pas stockées dans les fichiers
)
PARTITIONED BY (sexe STRING, tranche_age_ref STRING)
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_patient';

-- d_etablissement : référentiel FINESS (416 665 sites), clé de substitution
-- surrogate id_etablissement. id_localisation = département (clé de
-- d_localisation), region dénormalisée pour les filtres directs.
CREATE EXTERNAL TABLE IF NOT EXISTS gold.d_etablissement (
  id_etablissement                BIGINT,   -- clé de substitution (surrogate)
  identifiant_organisation        STRING,   -- clé naturelle FINESS (site)
  finess_etablissement_juridique  STRING,
  finess_site                     STRING,
  raison_sociale_site             STRING,
  code_commune                    STRING,   -- code INSEE 5 caractères
  id_localisation                 STRING,   -- département ('01'..'976', 'INCONNU')
  region                          STRING    -- dénormalisée depuis d_localisation
)
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_etablissement';

-- d_localisation : 101 départements (référentiel geo.api.gouv.fr) + membre
-- 'Inconnu' (clé -1 / 'INCONNU'), grain = département, partitionnée par
-- code_region (les noms de régions contiennent des caractères invalides
-- dans les noms de répertoires de partitions Hive).
CREATE EXTERNAL TABLE IF NOT EXISTS gold.d_localisation (
  id_localisation   STRING,   -- code département ('01'..'2B', '971'..'976', 'INCONNU')
  code_departement  STRING,
  nom_departement   STRING,
  region            STRING    -- nom de région (18 régions + 'Inconnue'), colonne data
  -- 'code_region' = colonne de partition : fournie par le chemin Hive
  -- (code_region=...), pas stockée dans les fichiers
)
PARTITIONED BY (code_region STRING)
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_localisation';

-- d_diagnostic : depuis S1 PostgreSQL.Diagnostic (15 490 diagnostics CIM-10).
CREATE EXTERNAL TABLE IF NOT EXISTS gold.d_diagnostic (
  id_diagnostic      BIGINT,  -- clé de substitution (surrogate)
  code_diag          STRING,  -- code CIM-10 (clé naturelle)
  libelle_diagnostic STRING
)
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_diagnostic';

-- d_professionnel : depuis S1 PostgreSQL.Professionnel_de_sante (1 048 575).
-- Clé naturelle = identifiant (pas de surrogate nécessaire : clé stable et unique).
CREATE EXTERNAL TABLE IF NOT EXISTS gold.d_professionnel (
  identifiant               STRING,  -- clé naturelle (clé de substitution = clé naturelle)
  civilite                  STRING,
  nom                       STRING,
  prenom                    STRING,
  profession                STRING,
  categorie_professionnelle STRING,
  code_specialite           STRING
)
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_professionnel';

-- ---------------------------------------------------------------------------
-- FAITS
-- ---------------------------------------------------------------------------

-- f_mesures : FAIT CONSOLIDÉ (couche Gold) - grain :
--   annee_mois x id_etablissement x id_patient x sexe x tranche_age x id_localisation
--   x region
--   - consultations / hospitalisations : grain MENSUEL  (annee_mois = 'AAAA-MM')
--   - décès / satisfaction            : grain ANNUEL   (annee_mois = 'AAAA-00',
--     sentinelle 'mois = 00' = agrégat annuel)
-- 4 mesures : nbre_consultation, nbre_hospitalisation, total_satisfaction, nbre_deces.
-- sexe et tranche_age sont dénormalisés dans le fait (colonnes de partition) :
--   - ils proviennent de la dimension patient mais servent de clés de
--     partitionnement (pruning démographique) ;
--   - pour les lignes sans patient (décès, satisfaction) : 'TOUS' / 'TOUTES'.
-- region est dans le GRAIN : la satisfaction est rattachée à la région (pas à
-- un établissement de site) ; sans region dans la clé, la jointure sat créait
-- un fan-out (31 doublons, corrigé, cf. rapport qualité). Pour les consultations
-- et hospitalisations, region est dénormalisée depuis l'établissement (filtre
-- direct sans jointure).
CREATE EXTERNAL TABLE IF NOT EXISTS gold.f_mesures (
  annee_mois          STRING,   -- 'AAAA-MM' (mensuel) ou 'AAAA-00' (annuel)
  id_temps            INT,      -- AAAAMMJJ du 1er jour du mois (NULL si annuel)
  id_etablissement    BIGINT,   -- -1 = non rattaché (membre Inconnu)
  id_patient          BIGINT,   -- -1 = non applicable (décès, satisfaction)
  id_localisation     STRING,   -- département ('INCONNU' si non rattaché)
  nbre_consultation   BIGINT,
  nbre_hospitalisation BIGINT,
  total_satisfaction  DOUBLE,   -- score moyen ajusté ESATIS (0-100), grain annuel
  nbre_deces          BIGINT,
  region              STRING    -- dénormalisée depuis d_localisation
)
PARTITIONED BY (annee INT, sexe STRING, tranche_age STRING)
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_mesures';

-- f_consultation_detail : couche SILVER, grain = 1 consultation (1 027 157 lignes).
-- Rattachement établissement : Id_prof_sante -> activite (1 organisation par
-- professionnel : MIN(identifiant_organisation) si multi-établissements, pour
-- préserver le grain 1 consultation) -> etablissement (taux de rattachement
-- chaîne complète 63,87 %, cf. rapport qualité).
CREATE EXTERNAL TABLE IF NOT EXISTS silver.f_consultation_detail (
  id_consultation   BIGINT,
  id_temps          INT,
  annee_mois        STRING,
  id_patient        BIGINT,
  sexe              STRING,
  tranche_age       STRING,   -- calculée à la date de la consultation
  id_diagnostic     BIGINT,
  id_professionnel  STRING,
  id_etablissement  BIGINT,
  id_localisation   STRING,
  region            STRING
)
PARTITIONED BY (annee INT)
STORED AS PARQUET
LOCATION 's3a://lake/silver/f_consultation_detail';

-- f_hospitalisation : grain = 1 hospitalisation (2 479 lignes, source S2 CSV).
CREATE EXTERNAL TABLE IF NOT EXISTS gold.f_hospitalisation (
  id_hospitalisation STRING,
  id_temps           INT,
  annee_mois         STRING,
  id_patient         BIGINT,
  sexe               STRING,
  tranche_age        STRING,
  id_diagnostic      BIGINT,
  id_etablissement   BIGINT,
  id_localisation    STRING,
  region             STRING,
  duree_jours        INT
)
PARTITIONED BY (annee INT)
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_hospitalisation';

-- f_deces_agg : grain ANNUEL = annee x sexe x tranche_age x département.
-- Source : 25 088 208 décès INSEE (fichier 1882-2020) agrégés en amont (Spark).
-- Partitionné par annee SEULEMENT (justification small-files en en-tête).
CREATE EXTERNAL TABLE IF NOT EXISTS gold.f_deces_agg (
  id_temps        INT,       -- AAAA0101 (1er janvier de l'année)
  sexe            STRING,
  tranche_age     STRING,
  id_localisation STRING,
  region          STRING,
  nbre_deces      BIGINT
)
PARTITIONED BY (annee INT)
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_deces_agg';

-- f_satisfaction : grain = établissement JURIDIQUE x année (source ESATIS :
-- 2 295 lignes source agrégées en 1 960 finess juridiques, moyenne pondérée
-- par les répondants, cf. 02a_faits_detail.sql / load_faits_detail.py).
-- Le finess ESATIS est un finess juridique (correspond à plusieurs sites FINESS) :
-- pas de clé d'établissement de site -> la mesure est rattachée à la région
-- (colonne region normalisée depuis le fichier ESATIS, cf. rapport qualité).
CREATE EXTERNAL TABLE IF NOT EXISTS gold.f_satisfaction (
  finess             STRING,  -- finess juridique ESATIS
  region             STRING,  -- région normalisée (18 régions + INCONNUE), colonne data
  score_satisfaction DOUBLE,  -- score moyen ajusté ESATIS, pondéré (0-100)
  nb_repondants      BIGINT,  -- somme des répondants des sites agrégés
  nb_sites           INT     -- nombre de sites ESATIS agrégés (lignes source)
  -- 'annee' et 'code_region' = colonnes de partition (chemins Hive
  -- annee=.../code_region=...) ; code_region et pas region car les noms de
  -- régions contiennent des caractères invalides dans les chemins Hive
)
PARTITIONED BY (annee STRING, code_region STRING)
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_satisfaction';

-- ---------------------------------------------------------------------------
-- AGRÉGATS PRÉ-CALCULÉS (couche Gold, équivalents matérialisés)
-- ---------------------------------------------------------------------------

-- f_consult_diag_mois : consultations par diagnostic x mois x établissement.
CREATE EXTERNAL TABLE IF NOT EXISTS gold.f_consult_diag_mois (
  annee_mois         STRING,
  id_diagnostic      BIGINT,
  id_etablissement   BIGINT,
  nbre_consultation  BIGINT
)
PARTITIONED BY (annee INT)
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_consult_diag_mois';

-- f_consult_prof_mois : consultations par professionnel x mois.
CREATE EXTERNAL TABLE IF NOT EXISTS gold.f_consult_prof_mois (
  annee_mois         STRING,
  id_professionnel   STRING,
  nbre_consultation  BIGINT
)
PARTITIONED BY (annee INT)
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_consult_prof_mois';

-- ---------------------------------------------------------------------------
-- Rafraîchissement des métadonnées après peuplement (Spark / Airflow) :
--   MSCK REPAIR TABLE gold.f_mesures;   -- découvre les partitions écrites
--   ANALYZE TABLE gold.f_mesures COMPUTE STATISTICS;               -- stats globales
--   ANALYZE TABLE gold.f_mesures PARTITION(annee=2019) COMPUTE STATISTICS; -- stats par partition
-- Les statistiques (min/max par colonne, cardinalités) alimentent l'optimiseur
-- Trino pour le choix des plans (jointures, pruning, bucketed join).
-- ============================================================================
