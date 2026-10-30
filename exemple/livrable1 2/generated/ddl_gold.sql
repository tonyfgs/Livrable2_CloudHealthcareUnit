-- DDL Gold (HiveQL, tables EXTERNES en TEXTFILE) -- généré par model/ddl.py depuis model/model.py
-- Exécution : beeline -u jdbc:hive2://quickstart.cloudera:10000 --hivevar racine=... --hivevar chargement=AAAAMM -f ddl_gold.sql
-- Hive 1.1 n'impose ni clé primaire, ni clé étrangère, ni NOT NULL : unicité, intégrité référentielle
-- et non-nullité sont vérifiées par les requêtes de contrôle du job J_PUB (RQ-08, RQ-20, RQ-21) avant publication.
-- Dates au format yyyy-MM-dd, horodatages yyyy-MM-dd HH:mm:ss, booléens true/false, champ vide = NULL.

CREATE DATABASE IF NOT EXISTS chu_gold LOCATION '${hivevar:racine}/gold';
USE chu_gold;

-- Dimension Temps (calendrier) | grain : un jour calendaire | PK contrôlée : (date_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
  date_sk                        INT,
  date_complete                  DATE,
  annee                          SMALLINT,
  semestre                       SMALLINT,
  trimestre                      SMALLINT,
  mois                           SMALLINT,
  libelle_mois                   VARCHAR(12),
  annee_mois                     VARCHAR(7),
  semaine_iso                    SMALLINT,
  annee_iso                      SMALLINT,
  semaine_epidemio               INT,
  jour_mois                      SMALLINT,
  jour_semaine_iso               SMALLINT,
  libelle_jour                   VARCHAR(10),
  est_week_end                   BOOLEAN,
  est_jour_ferie                 BOOLEAN,
  est_jour_ouvre                 BOOLEAN
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_date/chargement=${hivevar:chargement}';

-- Dimension Patient pseudonymisé | grain : un patient (Id_patient source) | PK contrôlée : (patient_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_patient (
  patient_sk                     BIGINT,
  patient_pseudo                 VARCHAR(64),
  code_departement_residence     VARCHAR(3),
  code_region_residence          VARCHAR(2),
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_patient/chargement=${hivevar:chargement}';

-- Dimension Sexe (conforme) | grain : une modalité de sexe | PK contrôlée : (sexe_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_sexe (
  sexe_sk                        SMALLINT,
  sexe_code                      VARCHAR(1),
  sexe_libelle                   VARCHAR(20)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_sexe/chargement=${hivevar:chargement}';

-- Dimension Tranche d'âge (conforme) | grain : une tranche quinquennale d'âge révolu | PK contrôlée : (tranche_age_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_tranche_age (
  tranche_age_sk                 SMALLINT,
  age_min                        SMALLINT,
  age_max                        SMALLINT,
  libelle_tranche                VARCHAR(16),
  classe_age                     VARCHAR(20),
  ordre_tri                      SMALLINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_tranche_age/chargement=${hivevar:chargement}';

-- Dimension Géographie (COG INSEE) | grain : une commune du COG courant, ou un pays pour l'étranger | PK contrôlée : (geographie_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_geographie (
  geographie_sk                  INT,
  code_commune                   VARCHAR(5),
  libelle_commune                VARCHAR(100),
  code_departement               VARCHAR(3),
  libelle_departement            VARCHAR(60),
  code_region                    VARCHAR(2),
  libelle_region                 VARCHAR(60),
  est_etranger                   BOOLEAN,
  millesime_cog                  SMALLINT,
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_geographie/chargement=${hivevar:chargement}';

-- Dimension Établissement / organisation | grain : une version d'une organisation de l'annuaire santé (FINESS géographique ou cabinet SIRET) | PK contrôlée : (etablissement_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_etablissement (
  etablissement_sk               INT,
  identifiant_organisation       VARCHAR(20),
  type_organisation              VARCHAR(1),
  finess_geographique            VARCHAR(9),
  finess_juridique               VARCHAR(9),
  siret                          VARCHAR(14),
  raison_sociale                 VARCHAR(150),
  categorie_libelle              VARCHAR(100),
  code_postal                    VARCHAR(5),
  code_commune                   VARCHAR(5),
  libelle_commune                VARCHAR(100),
  code_departement               VARCHAR(3),
  code_region                    VARCHAR(2),
  libelle_region                 VARCHAR(60),
  est_groupe_chu                 BOOLEAN,
  date_debut_validite            DATE,
  date_fin_validite              DATE,
  est_version_courante           BOOLEAN,
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_etablissement/chargement=${hivevar:chargement}';

-- Dimension Diagnostic (CIM-10) | grain : un code diagnostic | PK contrôlée : (diagnostic_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_diagnostic (
  diagnostic_sk                  INT,
  code_diag                      VARCHAR(6),
  libelle_diagnostic             VARCHAR(255),
  code_categorie                 VARCHAR(3),
  code_chapitre                  VARCHAR(5),
  libelle_chapitre               VARCHAR(255),
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_diagnostic/chargement=${hivevar:chargement}';

-- Dimension Professionnel de santé | grain : une version d'un professionnel | PK contrôlée : (professionnel_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_professionnel (
  professionnel_sk               INT,
  professionnel_pseudo           VARCHAR(64),
  type_identifiant               VARCHAR(5),
  profession                     VARCHAR(60),
  categorie_professionnelle      VARCHAR(20),
  specialite_code                VARCHAR(10),
  specialite_libelle             VARCHAR(100),
  fonction                       VARCHAR(60),
  mode_exercice                  VARCHAR(20),
  date_debut_validite            DATE,
  date_fin_validite              DATE,
  est_version_courante           BOOLEAN,
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_professionnel/chargement=${hivevar:chargement}';

-- Dimension Mutuelle (organisme complémentaire) | grain : un organisme complémentaire | PK contrôlée : (mutuelle_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_mutuelle (
  mutuelle_sk                    SMALLINT,
  id_mutuelle                    INT,
  nom_mutuelle                   VARCHAR(150),
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_mutuelle/chargement=${hivevar:chargement}';

-- Dimension Acte de consultation (junk) | grain : une combinaison motif × plage horaire | PK contrôlée : (acte_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_acte (
  acte_sk                        SMALLINT,
  motif                          VARCHAR(60),
  famille_motif                  VARCHAR(30),
  plage_horaire                  VARCHAR(12),
  heure_debut                    STRING,
  heure_fin                      STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_acte/chargement=${hivevar:chargement}';

-- Dimension Campagne de mesure (HAS) | grain : une campagne d'un dispositif de mesure (millésime de fichier) | PK contrôlée : (campagne_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_campagne (
  campagne_sk                    SMALLINT,
  dispositif                     VARCHAR(40),
  annee_publication              SMALLINT,
  annee_donnees                  SMALLINT,
  fichier_source                 VARCHAR(120)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_campagne/chargement=${hivevar:chargement}';

-- Dimension Indicateur (canonique) | grain : un indicateur canonique, indépendant des noms de colonnes des millésimes | PK contrôlée : (indicateur_sk)
CREATE EXTERNAL TABLE IF NOT EXISTS dim_indicateur (
  indicateur_sk                  SMALLINT,
  code_indicateur                VARCHAR(40),
  libelle_indicateur             VARCHAR(200),
  famille                        VARCHAR(30),
  secteur                        VARCHAR(10),
  unite                          VARCHAR(20),
  sens_favorable                 VARCHAR(1)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/dim_indicateur/chargement=${hivevar:chargement}';

-- Fait Consultation (transactionnel) | grain : une consultation (Num_consultation) | PK contrôlée : (consultation_id)
-- FK contrôlées : date_consultation_sk->dim_date, patient_sk->dim_patient, professionnel_sk->dim_professionnel, etablissement_sk->dim_etablissement, geographie_etablissement_sk->dim_geographie, diagnostic_sk->dim_diagnostic, mutuelle_sk->dim_mutuelle, acte_sk->dim_acte, sexe_sk->dim_sexe, tranche_age_sk->dim_tranche_age
CREATE EXTERNAL TABLE IF NOT EXISTS fait_consultation (
  consultation_id                BIGINT,
  date_consultation_sk           INT,
  patient_sk                     BIGINT,
  professionnel_sk               INT,
  etablissement_sk               INT,
  geographie_etablissement_sk    INT,
  diagnostic_sk                  INT,
  mutuelle_sk                    SMALLINT,
  acte_sk                        SMALLINT,
  sexe_sk                        SMALLINT,
  tranche_age_sk                 SMALLINT,
  age_patient                    SMALLINT,
  duree_plage_minutes            SMALLINT,
  nb_consultation                SMALLINT,
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
PARTITIONED BY (chargement STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/fait_consultation';

-- Fait Hospitalisation (séjour) | grain : un séjour hospitalier (Num_Hospitalisation) | PK contrôlée : (sejour_id)
-- FK contrôlées : date_entree_sk->dim_date, date_sortie_sk->dim_date, patient_sk->dim_patient, etablissement_sk->dim_etablissement, geographie_etablissement_sk->dim_geographie, diagnostic_sk->dim_diagnostic, sexe_sk->dim_sexe, tranche_age_sk->dim_tranche_age
CREATE EXTERNAL TABLE IF NOT EXISTS fait_hospitalisation (
  sejour_id                      BIGINT,
  date_entree_sk                 INT,
  date_sortie_sk                 INT,
  patient_sk                     BIGINT,
  etablissement_sk               INT,
  geographie_etablissement_sk    INT,
  diagnostic_sk                  INT,
  sexe_sk                        SMALLINT,
  tranche_age_sk                 SMALLINT,
  age_entree                     SMALLINT,
  duree_sejour_jours             SMALLINT,
  nb_sejour                      SMALLINT,
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/fait_hospitalisation/chargement=${hivevar:chargement}';

-- Fait Décès (open data INSEE) | grain : un acte de décès (après dédoublonnage) | PK contrôlée : (deces_id)
-- FK contrôlées : date_deces_sk->dim_date, geographie_deces_sk->dim_geographie, sexe_sk->dim_sexe, tranche_age_sk->dim_tranche_age
CREATE EXTERNAL TABLE IF NOT EXISTS fait_deces (
  deces_id                       VARCHAR(64),
  date_deces_sk                  INT,
  geographie_deces_sk            INT,
  sexe_sk                        SMALLINT,
  tranche_age_sk                 SMALLINT,
  annee_deces                    SMALLINT,
  age_deces                      SMALLINT,
  nb_deces                       SMALLINT,
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
PARTITIONED BY (chargement STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/fait_deces';

-- Fait Indicateur qualité & satisfaction (instantané périodique) | grain : un résultat d'indicateur pour un établissement et une campagne ; unicité aussi testée sur (identifiant_organisation, campagne, indicateur) | PK contrôlée : (etablissement_sk, campagne_sk, indicateur_sk)
-- FK contrôlées : etablissement_sk->dim_etablissement, campagne_sk->dim_campagne, indicateur_sk->dim_indicateur, date_reference_sk->dim_date, geographie_etablissement_sk->dim_geographie
CREATE EXTERNAL TABLE IF NOT EXISTS fait_indicateur_qualite (
  etablissement_sk               INT,
  campagne_sk                    SMALLINT,
  indicateur_sk                  SMALLINT,
  date_reference_sk              INT,
  geographie_etablissement_sk    INT,
  valeur                         DECIMAL(9,4),
  effectif_base                  INT,
  ic_bas                         DECIMAL(9,4),
  ic_haut                        DECIMAL(9,4),
  classe                         VARCHAR(4),
  evolution                      VARCHAR(30),
  est_participation_obligatoire  BOOLEAN,
  est_diffusable                 BOOLEAN,
  id_lot                         VARCHAR(36),
  dt_chargement                  TIMESTAMP
)
PARTITIONED BY (chargement STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''
STORED AS TEXTFILE
LOCATION '${hivevar:racine}/gold/fait_indicateur_qualite';

-- Publication d'un lot (job J_PUB), après succès des contrôles sur les tables de travail *_stg :
--   dimensions et fait_hospitalisation : ALTER TABLE <t> SET LOCATION '${hivevar:racine}/gold/<t>/chargement=${hivevar:chargement}';
--   faits partitionnés : ALTER TABLE <t> ADD IF NOT EXISTS PARTITION (chargement='${hivevar:chargement}')
--                        LOCATION '${hivevar:racine}/gold/<t>/chargement=${hivevar:chargement}';
-- Retour arrière : SET LOCATION sur le répertoire du lot précédent, ou DROP PARTITION (table externe : fichiers conservés).