.mode list
.headers on
.timer off
-- ============================================================
-- PoC Livrable 2 - dimensions du schéma en étoile (couche Gold)
-- Export double : poc/avant (Parquet plat, 1 fichier) et
--                 poc/apres (Parquet partitionné/bucketé, trié)
-- ============================================================
INSTALL postgres; LOAD postgres;
ATTACH 'dbname=chu_s1 host=chu-s1 user=chu password=chu' AS s1 (TYPE postgres);

-- ---------- d_temps : grain jour, 2015-2030 ----------
CREATE OR REPLACE TABLE d_temps AS
SELECT CAST(strftime(d,'%Y%m%d') AS INTEGER) AS id_temps,
       CAST(d AS DATE) AS date_jour,
       EXTRACT(YEAR FROM d)::INTEGER AS annee,
       EXTRACT(MONTH FROM d)::INTEGER AS mois,
       CAST(strftime(d,'%Y-%m') AS VARCHAR) AS annee_mois,
       EXTRACT(QUARTER FROM d)::INTEGER AS trimestre,
       EXTRACT(DOW FROM d)::INTEGER AS jour_semaine,
       CASE WHEN EXTRACT(DOW FROM d) IN (0,6) THEN true ELSE false END AS est_weekend
FROM generate_series(DATE '2015-01-01', DATE '2030-12-31', INTERVAL 1 DAY) AS t(d);

-- ---------- d_patient : depuis S1.PostgreSQL.Patient ----------
-- sexe normalisé M/F ; tranche_age_ref = tranche à la date de référence 2023-12-31
-- (âge révolu calculé inline : date_diff('year', dn, de) - ajustement anniversaire)
CREATE OR REPLACE TABLE d_patient AS
SELECT id_patient, sexe, date_naissance, tranche_of_age(age_ref_calc) AS tranche_age_ref,
       age_ref, code_postal, ville
FROM (
  SELECT p."Id_patient" AS id_patient,
         CASE p."Sexe" WHEN 'male' THEN 'M' WHEN 'female' THEN 'F' ELSE 'I' END AS sexe,
         CAST(try_strptime(p."Date",'%m/%d/%Y') AS DATE) AS date_naissance,
         date_diff('year', CAST(try_strptime(p."Date",'%m/%d/%Y') AS DATE), DATE '2023-12-31')
           - (CASE WHEN EXTRACT(MONTH FROM DATE '2023-12-31')*100 + EXTRACT(DAY FROM DATE '2023-12-31')
                        < EXTRACT(MONTH FROM CAST(try_strptime(p."Date",'%m/%d/%Y') AS DATE))*100
                        + EXTRACT(DAY FROM CAST(try_strptime(p."Date",'%m/%d/%Y') AS DATE))
                   THEN 1 ELSE 0 END) AS age_ref_calc,
         p."Age" AS age_ref,
         NULLIF(TRIM(p."Code_postal"),'') AS code_postal,
         NULLIF(TRIM(p."Ville"),'') AS ville
  FROM s1.public."Patient" p
);

-- ---------- d_etablissement : référentiel FINESS (416 665 sites) ----------
CREATE OR REPLACE TABLE d_etablissement AS
SELECT ROW_NUMBER() OVER (ORDER BY TRIM(e.identifiant_organisation), TRIM(e.raison_sociale_site)) AS id_etablissement,
       TRIM(e.identifiant_organisation) AS identifiant_organisation,
       NULLIF(TRIM(e.finess_etablissement_juridique),'') AS finess_etablissement_juridique,
       NULLIF(TRIM(e.finess_site),'') AS finess_site,
       TRIM(e.raison_sociale_site) AS raison_sociale_site,
       NULLIF(TRIM(e.code_commune),'') AS code_commune,
       COALESCE(d.code_departement,'INCONNU') AS id_localisation,
       COALESCE(d.nom_region,'INCONNUE') AS region
FROM src_etab e
LEFT JOIN ref_dept d ON dept_of(e.code_commune) = d.code_departement;

-- ---------- d_localisation : 101 départements + membre "Inconnu" ----------
CREATE OR REPLACE TABLE d_localisation AS
SELECT code_departement AS id_localisation, code_departement, nom_departement, code_region, nom_region AS region
FROM (SELECT DISTINCT code_departement, nom_departement, code_region, nom_region FROM ref_dept)
UNION ALL
SELECT 'INCONNU','INCONNU','Inconnu','99','Inconnue';

-- ---------- d_diagnostic : depuis S1.PostgreSQL.Diagnostic ----------
CREATE OR REPLACE TABLE d_diagnostic AS
SELECT ROW_NUMBER() OVER (ORDER BY "Code_diag") AS id_diagnostic,
       "Code_diag" AS code_diag,
       TRIM("Diagnostic") AS libelle_diagnostic
FROM s1.public."Diagnostic";

-- ---------- d_professionnel : depuis S1.PostgreSQL.Professionnel_de_sante ----------
CREATE OR REPLACE TABLE d_professionnel AS
SELECT TRIM("Identifiant") AS identifiant,
       NULLIF(TRIM("Civilite"),'') AS civilite,
       NULLIF(TRIM("Nom"),'') AS nom,
       NULLIF(TRIM("Prenom"),'') AS prenom,
       NULLIF(TRIM("Profession"),'') AS profession,
       NULLIF(TRIM("Categorie_professionnelle"),'') AS categorie_professionnelle,
       NULLIF(TRIM("Code_specialite"),'') AS code_specialite
FROM s1.public."Professionnel_de_sante";

CHECKPOINT;

-- ============================================================
-- EXPORT "AVANT" : Parquet plat, 1 fichier par table, aucun ordre garanti
-- ============================================================
COPY (SELECT * FROM d_temps) TO 'poc/avant/d_temps.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM d_patient) TO 'poc/avant/d_patient.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM d_etablissement) TO 'poc/avant/d_etablissement.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM d_localisation) TO 'poc/avant/d_localisation.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM d_diagnostic) TO 'poc/avant/d_diagnostic.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM d_professionnel) TO 'poc/avant/d_professionnel.parquet' (FORMAT PARQUET);

-- ============================================================
-- EXPORT "APRES" : partitionnement Hive + bucketing + tri sur les clés de jointure
--  d_temps          PARTITIONED BY (annee)                      -> 16 partitions
--  d_patient        PARTITIONED BY (tranche_age_ref)            -> 7 partitions
--  d_etablissement  CLUSTERED BY (id_etablissement) INTO 32 BUCKETS (simulé : 32 fichiers bucket=N)
--  d_localisation   PARTITIONED BY (region)                     -> 19 partitions
--  d_diagnostic     CLUSTERED BY (id_diagnostic) INTO 8 BUCKETS
--  d_professionnel  CLUSTERED BY (identifiant) INTO 32 BUCKETS
-- ============================================================
COPY (SELECT * FROM d_temps ORDER BY id_temps) TO 'poc/apres/d_temps' (FORMAT PARQUET, PARTITION_BY (annee), OVERWRITE_OR_IGNORE 1);
COPY (SELECT * FROM d_patient ORDER BY id_patient) TO 'poc/apres/d_patient' (FORMAT PARQUET, PARTITION_BY (tranche_age_ref), OVERWRITE_OR_IGNORE 1);
COPY (SELECT *, (hash(id_etablissement) % 32)::INTEGER AS bucket FROM (SELECT * FROM d_etablissement ORDER BY id_etablissement)) TO 'poc/apres/d_etablissement' (FORMAT PARQUET, PARTITION_BY (bucket), OVERWRITE_OR_IGNORE 1);
COPY (SELECT * FROM d_localisation ORDER BY id_localisation) TO 'poc/apres/d_localisation' (FORMAT PARQUET, PARTITION_BY (region), OVERWRITE_OR_IGNORE 1);
COPY (SELECT *, (hash(id_diagnostic) % 8)::INTEGER AS bucket FROM (SELECT * FROM d_diagnostic ORDER BY id_diagnostic)) TO 'poc/apres/d_diagnostic' (FORMAT PARQUET, PARTITION_BY (bucket), OVERWRITE_OR_IGNORE 1);
COPY (SELECT *, (hash(identifiant) % 32)::INTEGER AS bucket FROM (SELECT * FROM d_professionnel ORDER BY identifiant)) TO 'poc/apres/d_professionnel' (FORMAT PARQUET, PARTITION_BY (bucket), OVERWRITE_OR_IGNORE 1);

-- Récapitulatif des volumes
SELECT 'd_temps' AS dim, COUNT(*) AS lignes FROM d_temps
UNION ALL SELECT 'd_patient', COUNT(*) FROM d_patient
UNION ALL SELECT 'd_etablissement', COUNT(*) FROM d_etablissement
UNION ALL SELECT 'd_localisation', COUNT(*) FROM d_localisation
UNION ALL SELECT 'd_diagnostic', COUNT(*) FROM d_diagnostic
UNION ALL SELECT 'd_professionnel', COUNT(*) FROM d_professionnel;
