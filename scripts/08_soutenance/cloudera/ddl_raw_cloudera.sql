-- Couche raw (bronze) du lac : fichiers sources bruts exposes dans Hive 1.1 (Cloudera CDH 5.12).
-- Toutes les colonnes sont en texte (OpenCSVSerde) ; l'en-tete de chaque fichier est ignore.
-- A executer dans Hue ou beeline apres le chargement HDFS (charger_cloudera.sh le fait).
CREATE DATABASE IF NOT EXISTS raw;

DROP TABLE IF EXISTS raw.hospitalisations;
CREATE EXTERNAL TABLE raw.hospitalisations (
  `num_hospitalisation` string,
  `id_patient` string,
  `identifiant_organisation` string,
  `code_diagnostic` string,
  `suite_diagnostic_consultation` string,
  `date_entree` string,
  `jour_hospitalisation` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ";", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION '/user/cloudera/lake/raw/hospitalisation/Hospitalisations'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS raw.satisfaction_2019;
CREATE EXTERNAL TABLE raw.satisfaction_2019 (
  `finess` string,
  `rs_finess` string,
  `finess_geo` string,
  `rs_finess_geo` string,
  `region` string,
  `participation` string,
  `depot` string,
  `nb_rep_score_all_rea_ajust` string,
  `score_all_rea_ajust` string,
  `classement` string,
  `evolution` string,
  `nb_rep_score_accueil_rea_ajust` string,
  `score_accueil_rea_ajust` string,
  `nb_rep_score_pecinf_rea_ajust` string,
  `score_pecinf_rea_ajust` string,
  `nb_rep_score_pecmed_rea_ajust` string,
  `score_pecmed_rea_ajust` string,
  `nb_rep_score_chambre_rea_ajust` string,
  `score_chambre_rea_ajust` string,
  `nb_rep_score_repas_rea_ajust` string,
  `score_repas_rea_ajust` string,
  `nb_rep_score_sortie_rea_ajust` string,
  `score_sortie_rea_ajust` string,
  `taux_reco_brut` string,
  `nb_reco_brut` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ";", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION '/user/cloudera/lake/raw/satisfaction/ESATIS_2019'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS raw.satisfaction_2020;
CREATE EXTERNAL TABLE raw.satisfaction_2020 (
  `finess` string,
  `rs_finess` string,
  `finess_geo` string,
  `rs_finess_geo` string,
  `region` string,
  `participation` string,
  `depot` string,
  `nb_rep_score_all_rea_ajust` string,
  `score_all_rea_ajust` string,
  `classement` string,
  `evolution` string,
  `nb_rep_score_accueil_rea_ajust` string,
  `score_accueil_rea_ajust` string,
  `nb_rep_score_pecinf_rea_ajust` string,
  `score_pecinf_rea_ajust` string,
  `nb_rep_score_pecmed_rea_ajust` string,
  `score_pecmed_rea_ajust` string,
  `nb_rep_score_chambre_rea_ajust` string,
  `score_chambre_rea_ajust` string,
  `nb_rep_score_repas_rea_ajust` string,
  `score_repas_rea_ajust` string,
  `nb_rep_score_sortie_rea_ajust` string,
  `score_sortie_rea_ajust` string,
  `taux_reco_brut` string,
  `nb_reco_brut` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ";", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION '/user/cloudera/lake/raw/satisfaction/ESATIS_2020'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS raw.referentiel_departements;
CREATE EXTERNAL TABLE raw.referentiel_departements (
  `code_departement` string,
  `nom_departement` string,
  `code_region` string,
  `nom_region` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ";", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION '/user/cloudera/lake/raw/referentiels/referentiel_departements'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS raw.activite_professionnel_sante;
CREATE EXTERNAL TABLE raw.activite_professionnel_sante (
  `identifiant` string,
  `identifiant_organisation` string,
  `mode_exercice` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ";", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION '/user/cloudera/lake/raw/referentiels/activite_professionnel_sante'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS raw.etablissement_sante;
CREATE EXTERNAL TABLE raw.etablissement_sante (
  `adresse` string,
  `cedex` string,
  `code_commune` string,
  `code_postal` string,
  `commune` string,
  `complement_destinataire` string,
  `complement_point_geographique` string,
  `email` string,
  `enseigne_commerciale_site` string,
  `finess_etablissement_juridique` string,
  `finess_site` string,
  `identifiant_organisation` string,
  `indice_repetition_voie` string,
  `mention_distribution` string,
  `numero_voie` string,
  `pays` string,
  `raison_sociale_site` string,
  `siren_site` string,
  `siret_site` string,
  `telecopie` string,
  `telephone` string,
  `telephone_2` string,
  `type_voie` string,
  `voie` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ";", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION '/user/cloudera/lake/raw/etablissement/etablissement_sante'
TBLPROPERTIES ('skip.header.line.count' = '1');

DROP TABLE IF EXISTS raw.deces;
CREATE EXTERNAL TABLE raw.deces (
  `nom` string,
  `prenom` string,
  `sexe` string,
  `date_naissance` string,
  `code_lieu_naissance` string,
  `lieu_naissance` string,
  `pays_naissance` string,
  `date_deces` string,
  `code_lieu_deces` string,
  `numero_acte_deces` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ",", "quoteChar" = "\"", "escapeChar" = "\\")
STORED AS TEXTFILE
LOCATION '/user/cloudera/lake/raw/deces/deces'
TBLPROPERTIES ('skip.header.line.count' = '1');
