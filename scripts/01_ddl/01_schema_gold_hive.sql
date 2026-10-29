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
--   f_satisfaction     PARTITIONED BY (annee, region) : grain juridique x année,
--                      la région est la colonne la plus filtrante (besoin métier).
--   d_temps            PARTITIONED BY (annee)    (grain jour, 2015-2030,
--                      couvre le périmètre 2015-2021 de la grille + marge).
--   d_patient          PARTITIONED BY (sexe, tranche_age_ref) : partitionnement
--                      démographique (grille : sexe + tranche d'âge sur la
--                      dimension patient).
--   d_localisation     PARTITIONED BY (region)   (grain département, 101 + Inconnu).
--   d_etablissement, d_diagnostic, d_professionnel : pas de partitionnement
--                      (tables de taille moyenne) -> bucketing seul.
--
-- Justification du bucketing (Hive/Spark UNIQUEMENT - le bucketing n'existe pas
-- en PostgreSQL) : CLUSTERED BY (cle de jointure) INTO N BUCKETS
--   * co-localise les lignes partageant la même clé de jointure dans le même
--     fichier -> jointures map-side / merge join sans shuffle complet ;
--   * pré-tri intra-bucket -> agrégations et ORDER BY + LIMIT efficaces ;
--   * nombre de buckets = multiple de la cardinalité de la clé (32 pour les
--     faits/dimensions volumineux, 8 pour les petites dimensions).
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
CREATE EXTERNAL TABLE IF NOT EXISTS d_temps (
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
CREATE EXTERNAL TABLE IF NOT EXISTS d_patient (
  id_patient      BIGINT,     -- clé de substitution (identique à S1, clé naturelle)
  sexe            STRING,     -- M / F / I
  date_naissance  DATE,
  age_ref         INT,        -- âge à la date de référence (indicatif, non utilisé)
  code_postal     STRING,
  ville           STRING
  -- 'sexe' et 'tranche_age_ref' = colonnes de partition (chemins Hive
  -- sexe=.../tranche_age_ref=...), pas stockées dans les fichiers
)
PARTITIONED BY (sexe STRING, tranche_age_ref STRING)
CLUSTERED BY (id_patient) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_patient';

-- d_etablissement : référentiel FINESS (416 665 sites), clé de substitution
-- surrogate id_etablissement. id_localisation = département (clé de
-- d_localisation), region dénormalisée pour les filtres directs.
CREATE EXTERNAL TABLE IF NOT EXISTS d_etablissement (
  id_etablissement                BIGINT,   -- clé de substitution (surrogate)
  identifiant_organisation        STRING,   -- clé naturelle FINESS (site)
  finess_etablissement_juridique  STRING,
  finess_site                     STRING,
  raison_sociale_site             STRING,
  code_commune                    STRING,   -- code INSEE 5 caractères
  id_localisation                 STRING,   -- département ('01'..'976', 'INCONNU')
  region                          STRING    -- dénormalisée depuis d_localisation
)
CLUSTERED BY (id_etablissement) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_etablissement';

-- d_localisation : 101 départements (référentiel geo.api.gouv.fr) + membre
-- 'Inconnu' (clé -1 / 'INCONNU'), grain = département, partitionnée par région.
CREATE EXTERNAL TABLE IF NOT EXISTS d_localisation (
  id_localisation   STRING,   -- code département ('01'..'2B', '971'..'976', 'INCONNU')
  code_departement  STRING,
  nom_departement   STRING,
  code_region       STRING
  -- 'region' (nom de région, 18 régions) = colonne de partition :
  -- fournie par le chemin Hive (region=...), pas stockée dans les fichiers
)
PARTITIONED BY (region STRING)
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_localisation';

-- d_diagnostic : depuis S1 PostgreSQL.Diagnostic (15 490 diagnostics CIM-10).
CREATE EXTERNAL TABLE IF NOT EXISTS d_diagnostic (
  id_diagnostic      BIGINT,  -- clé de substitution (surrogate)
  code_diag          STRING,  -- code CIM-10 (clé naturelle)
  libelle_diagnostic STRING
)
CLUSTERED BY (id_diagnostic) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_diagnostic';

-- d_professionnel : depuis S1 PostgreSQL.Professionnel_de_sante (1 048 575).
-- Clé naturelle = identifiant (pas de surrogate nécessaire : clé stable et unique).
CREATE EXTERNAL TABLE IF NOT EXISTS d_professionnel (
  identifiant               STRING,  -- clé naturelle (clé de substitution = clé naturelle)
  civilite                  STRING,
  nom                       STRING,
  prenom                    STRING,
  profession                STRING,
  categorie_professionnelle STRING,
  code_specialite           STRING
)
CLUSTERED BY (identifiant) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_professionnel';

-- ---------------------------------------------------------------------------
-- FAITS
-- ---------------------------------------------------------------------------

-- f_mesures : FAIT CONSOLIDÉ (couche Gold) - grain :
--   annee_mois x id_etablissement x id_patient x sexe x tranche_age x id_localisation
--   - consultations / hospitalisations : grain MENSUEL  (annee_mois = 'AAAA-MM')
--   - décès / satisfaction            : grain ANNUEL   (annee_mois = 'AAAA-00',
--     sentinelle 'mois = 00' = agrégat annuel)
-- 4 mesures : nbre_consultation, nbre_hospitalisation, total_satisfaction, nbre_deces.
-- sexe et tranche_age sont dénormalisés dans le fait (colonnes de partition) :
--   - ils proviennent de la dimension patient mais servent de clés de
--     partitionnement (pruning démographique) ;
--   - pour les lignes sans patient (décès, satisfaction) : 'TOUS' / 'TOUTES'.
-- region est dénormalisée depuis d_localisation (filtre direct sans jointure).
CREATE EXTERNAL TABLE IF NOT EXISTS f_mesures (
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
CLUSTERED BY (id_etablissement) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_mesures';

-- f_consultation_detail : couche SILVER, grain = 1 consultation (1 373 594 lignes).
-- Rattachement établissement : Id_prof_sante -> activite -> etablissement
-- (taux de rattachement 57,7 %, cf. rapport qualité).
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
CLUSTERED BY (id_patient) INTO 32 BUCKETS
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
CLUSTERED BY (id_etablissement) INTO 8 BUCKETS
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

-- f_satisfaction : grain = établissement JURIDIQUE x année (source ESATIS).
-- Le finess ESATIS est un finess juridique (correspond à plusieurs sites FINESS) :
-- pas de clé d'établissement de site -> la mesure est rattachée à la région
-- (colonne region normalisée depuis le fichier ESATIS, cf. rapport qualité).
CREATE EXTERNAL TABLE IF NOT EXISTS gold.f_satisfaction (
  finess             STRING,  -- finess juridique ESATIS
  score_satisfaction DOUBLE,  -- score_all_rea_ajust (virgule décimale normalisée)
  nb_repondants      INT
  -- 'annee' et 'region' (région normalisée, 18 régions + INCONNUE) =
  -- colonnes de partition (chemins Hive annee=.../region=...)
)
PARTITIONED BY (annee STRING, region STRING)
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
CLUSTERED BY (id_diagnostic) INTO 8 BUCKETS
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_consult_diag_mois';

-- f_consult_prof_mois : consultations par professionnel x mois.
CREATE EXTERNAL TABLE IF NOT EXISTS gold.f_consult_prof_mois (
  annee_mois         STRING,
  id_professionnel   STRING,
  nbre_consultation  BIGINT
)
PARTITIONED BY (annee INT)
CLUSTERED BY (id_professionnel) INTO 32 BUCKETS
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
