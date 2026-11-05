-- Genere par export_snappy.py : DDL Hive 1.1 (Cloudera CDH 5.12). A executer dans Hue ou beeline.
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS silver;

DROP TABLE IF EXISTS gold.d_temps;
CREATE EXTERNAL TABLE gold.d_temps (
  `date_jour` string,
  `id_temps` int,
  `mois` int,
  `annee_mois` string,
  `trimestre` int,
  `jour_semaine` int,
  `est_weekend` boolean,
  `annee` int
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/d_temps';

DROP TABLE IF EXISTS gold.d_patient;
CREATE EXTERNAL TABLE gold.d_patient (
  `id_patient` bigint,
  `date_naissance` string,
  `age_ref` int,
  `code_postal` string,
  `ville` string,
  `sexe` string,
  `tranche_age_ref` string
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/d_patient';

DROP TABLE IF EXISTS gold.d_etablissement;
CREATE EXTERNAL TABLE gold.d_etablissement (
  `id_etablissement` bigint,
  `identifiant_organisation` string,
  `finess_etablissement_juridique` string,
  `finess_site` string,
  `raison_sociale_site` string,
  `code_commune` string,
  `id_localisation` string,
  `region` string
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/d_etablissement';

DROP TABLE IF EXISTS gold.d_localisation;
CREATE EXTERNAL TABLE gold.d_localisation (
  `id_localisation` string,
  `code_departement` string,
  `nom_departement` string,
  `region` string,
  `code_region` int
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/d_localisation';

DROP TABLE IF EXISTS gold.d_diagnostic;
CREATE EXTERNAL TABLE gold.d_diagnostic (
  `id_diagnostic` bigint,
  `code_diag` string,
  `libelle_diagnostic` string
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/d_diagnostic';

DROP TABLE IF EXISTS gold.d_professionnel;
CREATE EXTERNAL TABLE gold.d_professionnel (
  `identifiant` string,
  `civilite` string,
  `nom` string,
  `prenom` string,
  `profession` string,
  `categorie_professionnelle` string,
  `code_specialite` string
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/d_professionnel';

DROP TABLE IF EXISTS gold.f_mesures;
CREATE EXTERNAL TABLE gold.f_mesures (
  `annee_mois` string,
  `id_temps` int,
  `id_etablissement` bigint,
  `id_patient` bigint,
  `id_localisation` string,
  `nbre_consultation` bigint,
  `nbre_hospitalisation` bigint,
  `nbre_deces` bigint,
  `total_satisfaction` double,
  `region` string,
  `annee` int,
  `sexe` string,
  `tranche_age` string
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/f_mesures';

DROP TABLE IF EXISTS gold.f_hospitalisation;
CREATE EXTERNAL TABLE gold.f_hospitalisation (
  `id_hospitalisation` string,
  `id_temps` int,
  `annee_mois` string,
  `id_patient` bigint,
  `sexe` string,
  `tranche_age` string,
  `id_diagnostic` bigint,
  `id_etablissement` bigint,
  `id_localisation` string,
  `region` string,
  `duree_jours` int,
  `annee` int
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/f_hospitalisation';

DROP TABLE IF EXISTS gold.f_deces_agg;
CREATE EXTERNAL TABLE gold.f_deces_agg (
  `id_temps` int,
  `sexe` string,
  `tranche_age` string,
  `id_localisation` string,
  `region` string,
  `nbre_deces` bigint,
  `annee` int
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/f_deces_agg';

DROP TABLE IF EXISTS gold.f_satisfaction;
CREATE EXTERNAL TABLE gold.f_satisfaction (
  `finess` string,
  `region` string,
  `score_satisfaction` double,
  `nb_repondants` bigint,
  `nb_sites` bigint,
  `annee` int,
  `code_region` int
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/f_satisfaction';

DROP TABLE IF EXISTS gold.f_consult_diag_mois;
CREATE EXTERNAL TABLE gold.f_consult_diag_mois (
  `annee_mois` string,
  `id_diagnostic` bigint,
  `id_etablissement` bigint,
  `nbre_consultation` bigint,
  `annee` int
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/f_consult_diag_mois';

DROP TABLE IF EXISTS gold.f_consult_prof_mois;
CREATE EXTERNAL TABLE gold.f_consult_prof_mois (
  `annee_mois` string,
  `id_professionnel` string,
  `nbre_consultation` bigint,
  `annee` int
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/gold/f_consult_prof_mois';

DROP TABLE IF EXISTS silver.f_consultation_detail;
CREATE EXTERNAL TABLE silver.f_consultation_detail (
  `id_consultation` bigint,
  `id_temps` int,
  `annee_mois` string,
  `id_patient` bigint,
  `sexe` string,
  `tranche_age` string,
  `id_diagnostic` bigint,
  `id_professionnel` string,
  `id_etablissement` bigint,
  `id_localisation` string,
  `region` string,
  `annee` int
)
STORED AS PARQUET
LOCATION '/user/cloudera/lake/silver/f_consultation_detail';
