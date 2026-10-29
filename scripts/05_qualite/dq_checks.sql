.mode list
.headers on
.timer off
-- ============================================================
-- Contrôles qualité Livrable 2 - 6 familles (complétude, validité,
-- doublons, cohérence référentielle FINESS, temporalité, volumes).
-- Sources : PostgreSQL S1 (service Docker "s1-postgres") + CSV sous
-- /data-projet (montage fixe de "DATA Projet", voir run-duckdb.sh) +
-- staging/ (CSV dérivés UTF-8, lus en relatif depuis le workspace $L2WORK).
-- Résultats : dq/results/*.csv (relatifs au workspace).
-- ============================================================
INSTALL postgres; LOAD postgres;
ATTACH 'dbname=chu_s1 host=s1-postgres user=chu password=chu' AS s1 (TYPE postgres);
CREATE OR REPLACE VIEW src_hosp AS SELECT * FROM read_csv('/data-projet/Hospitalisation/Hospitalisations.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW src_etab AS SELECT * FROM read_csv('/data-projet/Etablissement de SANTE/etablissement_sante.csv', header=true, delim=';', nullstr='', all_varchar=true, max_line_size=10000000);
CREATE OR REPLACE VIEW src_pro AS SELECT * FROM read_csv('staging/professionnel_sante_utf8.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW src_act AS SELECT * FROM read_csv('staging/activite_professionnel_sante_utf8.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW src_sat19 AS SELECT * FROM read_csv('staging/satisfaction_2019.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW src_sat20 AS SELECT * FROM read_csv('staging/satisfaction_2020_clean.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW src_deces AS SELECT * FROM read_csv('/data-projet/DECES EN FRANCE/deces.csv', header=true, delim=',', quote='"', nullstr='', types={'sexe':'VARCHAR','date_deces':'VARCHAR','date_naissance':'VARCHAR','code_lieu_deces':'VARCHAR','code_lieu_naissance':'VARCHAR','numero_acte_deces':'VARCHAR'});
-- matérialisation physique intermédiaire : coupe la poussée de cast dans le lecteur CSV
-- (un try_cast poussé dans read_csv provoque une erreur dure, ex. "75,09" -> DOUBLE ou "1983" -> DATE)
CREATE OR REPLACE TABLE t_hosp AS SELECT * FROM src_hosp;
CREATE OR REPLACE TABLE t_sat19 AS SELECT * FROM src_sat19;
CREATE OR REPLACE TABLE t_sat20 AS SELECT * FROM src_sat20;
CREATE OR REPLACE TABLE t_deces AS SELECT * FROM read_csv('/data-projet/DECES EN FRANCE/deces.csv', header=true, delim=',', quote='"', nullstr='', all_varchar=true);
-- vues typées sûres : le cast s'applique sur une table DuckDB, jamais dans le lecteur CSV
CREATE OR REPLACE VIEW v_deces_d AS SELECT *, try_cast(date_deces AS DATE) AS d_deces, try_cast(date_naissance AS DATE) AS d_naissance FROM t_deces;
CREATE OR REPLACE VIEW v_hosp_d AS SELECT *, try_strptime("Date_Entree",'%d/%m/%Y') AS d_entree FROM t_hosp;
CREATE OR REPLACE VIEW v_sat19_n AS SELECT *, try_cast(replace(score_all_rea_ajust,',','.') AS DOUBLE) AS score19 FROM t_sat19;
CREATE OR REPLACE VIEW v_sat20_n AS SELECT *, try_cast(replace(score_all_rea_ajust,',','.') AS DOUBLE) AS score20 FROM t_sat20;
-- référentiels géographiques (geo.api.gouv.fr : 101 départements, 18 régions)
CREATE OR REPLACE VIEW ref_dept AS SELECT * FROM read_csv('staging/referentiel_departements.csv', header=true, delim=';', nullstr='', all_varchar=true);
-- normalisation des libellés de région hétérogènes du fichier ESATIS ("PACA", "Hauts de France", "Océan Indien" ambigu -> exclu)
CREATE OR REPLACE VIEW ref_region_map AS SELECT * FROM (VALUES
 ('Auvergne-Rhône-Alpes','Auvergne-Rhône-Alpes'),
 ('Bourgogne-Franche-Comté','Bourgogne-Franche-Comté'),
 ('Bretagne','Bretagne'),
 ('Centre-Val de Loire','Centre-Val de Loire'),
 ('Corse','Corse'),
 ('Grand Est','Grand Est'),
 ('Guadeloupe','Guadeloupe'),
 ('Guyane','Guyane'),
 ('Hauts de France','Hauts-de-France'),
 ('Ile de France','Île-de-France'),
 ('Martinique','Martinique'),
 ('Normandie','Normandie'),
 ('Nouvelle Aquitaine','Nouvelle-Aquitaine'),
 ('Occitanie','Occitanie'),
 ('PACA','Provence-Alpes-Côte d''Azur'),
 ('Pays de la Loire','Pays de la Loire'),
 ('La Réunion','La Réunion'),
 ('Mayotte','Mayotte')
) AS t(region_brute, region_norm);

-- ============ A. COMPLETUDE PAR ATTRIBUT ============
COPY (
  SELECT 'Hospitalisations' AS fichier, 'Num_Hospitalisation' AS attribut, COUNT(*) AS total, COUNT("Num_Hospitalisation") AS non_vide, ROUND(100.0*COUNT("Num_Hospitalisation")/COUNT(*),2) AS pct_complet FROM src_hosp
  UNION ALL SELECT 'Hospitalisations','Id_patient',COUNT(*),COUNT("Id_patient"),ROUND(100.0*COUNT("Id_patient")/COUNT(*),2) FROM src_hosp
  UNION ALL SELECT 'Hospitalisations','identifiant_organisation',COUNT(*),COUNT("identifiant_organisation"),ROUND(100.0*COUNT("identifiant_organisation")/COUNT(*),2) FROM src_hosp
  UNION ALL SELECT 'Hospitalisations','Code_diagnostic',COUNT(*),COUNT("Code_diagnostic"),ROUND(100.0*COUNT("Code_diagnostic")/COUNT(*),2) FROM src_hosp
  UNION ALL SELECT 'Hospitalisations','Date_Entree',COUNT(*),COUNT("Date_Entree"),ROUND(100.0*COUNT("Date_Entree")/COUNT(*),2) FROM src_hosp
  UNION ALL SELECT 'Hospitalisations','Jour_Hospitalisation',COUNT(*),COUNT("Jour_Hospitalisation"),ROUND(100.0*COUNT("Jour_Hospitalisation")/COUNT(*),2) FROM src_hosp
  UNION ALL SELECT 'S1_Patient','Sexe',COUNT(*),COUNT(NULLIF(TRIM("Sexe"),'')) ,ROUND(100.0*COUNT(NULLIF(TRIM("Sexe"),''))/COUNT(*),2) FROM s1.public."Patient"
  UNION ALL SELECT 'S1_Patient','Date_naissance',COUNT(*),COUNT(NULLIF(TRIM("Date"),'')),ROUND(100.0*COUNT(NULLIF(TRIM("Date"),''))/COUNT(*),2) FROM s1.public."Patient"
  UNION ALL SELECT 'S1_Patient','Age',COUNT(*),COUNT("Age"),ROUND(100.0*COUNT("Age")/COUNT(*),2) FROM s1.public."Patient"
  UNION ALL SELECT 'S1_Patient','Num_Secu',COUNT(*),COUNT(NULLIF(TRIM("Num_Secu"),'')),ROUND(100.0*COUNT(NULLIF(TRIM("Num_Secu"),''))/COUNT(*),2) FROM s1.public."Patient"
  UNION ALL SELECT 'S1_Consultation','Date',COUNT(*),COUNT("Date"),ROUND(100.0*COUNT("Date")/COUNT(*),2) FROM s1.public."Consultation"
  UNION ALL SELECT 'S1_Consultation','Code_diag',COUNT(*),COUNT(NULLIF(TRIM("Code_diag"),'')),ROUND(100.0*COUNT(NULLIF(TRIM("Code_diag"),''))/COUNT(*),2) FROM s1.public."Consultation"
  UNION ALL SELECT 'S1_Consultation','Id_prof_sante',COUNT(*),COUNT(NULLIF(TRIM("Id_prof_sante"),'')),ROUND(100.0*COUNT(NULLIF(TRIM("Id_prof_sante"),''))/COUNT(*),2) FROM s1.public."Consultation"
  UNION ALL SELECT 'deces','sexe',COUNT(*),COUNT(NULLIF(TRIM(sexe),'')),ROUND(100.0*COUNT(NULLIF(TRIM(sexe),''))/COUNT(*),2) FROM src_deces
  UNION ALL SELECT 'deces','date_deces',COUNT(*),COUNT(NULLIF(TRIM(date_deces),'')),ROUND(100.0*COUNT(NULLIF(TRIM(date_deces),''))/COUNT(*),2) FROM src_deces
  UNION ALL SELECT 'deces','date_naissance',COUNT(*),COUNT(NULLIF(TRIM(date_naissance),'')),ROUND(100.0*COUNT(NULLIF(TRIM(date_naissance),''))/COUNT(*),2) FROM src_deces
  UNION ALL SELECT 'deces','code_lieu_deces',COUNT(*),COUNT(NULLIF(TRIM(code_lieu_deces),'')),ROUND(100.0*COUNT(NULLIF(TRIM(code_lieu_deces),''))/COUNT(*),2) FROM src_deces
  UNION ALL SELECT 'etablissement','finess_site',COUNT(*),COUNT(NULLIF(TRIM(finess_site),'')),ROUND(100.0*COUNT(NULLIF(TRIM(finess_site),''))/COUNT(*),2) FROM src_etab
  UNION ALL SELECT 'etablissement','finess_etablissement_juridique',COUNT(*),COUNT(NULLIF(TRIM(finess_etablissement_juridique),'')),ROUND(100.0*COUNT(NULLIF(TRIM(finess_etablissement_juridique),''))/COUNT(*),2) FROM src_etab
  UNION ALL SELECT 'etablissement','identifiant_organisation',COUNT(*),COUNT(NULLIF(TRIM(identifiant_organisation),'')),ROUND(100.0*COUNT(NULLIF(TRIM(identifiant_organisation),''))/COUNT(*),2) FROM src_etab
  UNION ALL SELECT 'etablissement','code_commune',COUNT(*),COUNT(NULLIF(TRIM(code_commune),'')),ROUND(100.0*COUNT(NULLIF(TRIM(code_commune),''))/COUNT(*),2) FROM src_etab
  UNION ALL SELECT 'professionnel_sante','identifiant',COUNT(*),COUNT(NULLIF(TRIM(identifiant),'')),ROUND(100.0*COUNT(NULLIF(TRIM(identifiant),''))/COUNT(*),2) FROM src_pro
  UNION ALL SELECT 'satisfaction_2019','score_all_rea_ajust',COUNT(*),COUNT(NULLIF(TRIM(score_all_rea_ajust),'')),ROUND(100.0*COUNT(NULLIF(TRIM(score_all_rea_ajust),''))/COUNT(*),2) FROM src_sat19
  UNION ALL SELECT 'satisfaction_2020','score_all_rea_ajust',COUNT(*),COUNT(NULLIF(TRIM(score_all_rea_ajust),'')),ROUND(100.0*COUNT(NULLIF(TRIM(score_all_rea_ajust),''))/COUNT(*),2) FROM src_sat20
  UNION ALL SELECT 'satisfaction_2019','score_all_rea_ajust normalise (non NULL)',COUNT(*),COUNT(score19),ROUND(100.0*COUNT(score19)/COUNT(*),2) FROM v_sat19_n
  UNION ALL SELECT 'satisfaction_2020','score_all_rea_ajust normalise (non NULL)',COUNT(*),COUNT(score20),ROUND(100.0*COUNT(score20)/COUNT(*),2) FROM v_sat20_n
) TO 'dq/results/dq_completude.csv' (HEADER, DELIMITER ';');

-- ============ B. VALIDITE DES FORMATS ============
COPY (
  SELECT 'deces' AS fichier, 'date_deces format ISO AAA-MM-JJ' AS controle, COUNT(*) AS total, COUNT(*) FILTER (WHERE d_deces IS NULL AND NULLIF(TRIM(date_deces),'') IS NOT NULL) AS invalides, ROUND(100.0*COUNT(*) FILTER (WHERE d_deces IS NULL AND NULLIF(TRIM(date_deces),'') IS NOT NULL)/COUNT(*),4) AS pct_invalide FROM v_deces_d
  UNION ALL SELECT 'deces','date_naissance format ISO',COUNT(*),COUNT(*) FILTER (WHERE d_naissance IS NULL AND NULLIF(TRIM(date_naissance),'') IS NOT NULL),ROUND(100.0*COUNT(*) FILTER (WHERE d_naissance IS NULL AND NULLIF(TRIM(date_naissance),'') IS NOT NULL)/COUNT(*),4) FROM v_deces_d
  UNION ALL SELECT 'deces','sexe dans {1,2}',COUNT(*),COUNT(*) FILTER (WHERE sexe NOT IN ('1','2')),ROUND(100.0*COUNT(*) FILTER (WHERE sexe NOT IN ('1','2'))/COUNT(*),4) FROM src_deces
  UNION ALL SELECT 'Hospitalisations','Date_Entree format JJ/MM/AAAA',COUNT(*),COUNT(*) FILTER (WHERE d_entree IS NULL),ROUND(100.0*COUNT(*) FILTER (WHERE d_entree IS NULL)/COUNT(*),4) FROM v_hosp_d
  UNION ALL SELECT 'Hospitalisations','Code_diagnostic format CIM-10',COUNT(*),COUNT(*) FILTER (WHERE "Code_diagnostic" !~ '^[A-Z][0-9][0-9][A-Z0-9]?[A-Z0-9]?$'),ROUND(100.0*COUNT(*) FILTER (WHERE "Code_diagnostic" !~ '^[A-Z][0-9][0-9][A-Z0-9]?[A-Z0-9]?$')/COUNT(*),4) FROM src_hosp
  UNION ALL SELECT 'S1_Patient','Sexe dans {male,female}',COUNT(*),COUNT(*) FILTER (WHERE "Sexe" NOT IN ('male','female')),ROUND(100.0*COUNT(*) FILTER (WHERE "Sexe" NOT IN ('male','female'))/COUNT(*),4) FROM s1.public."Patient"
  UNION ALL SELECT 'S1_Patient','Date_naissance format M/J/AAAA',COUNT(*),COUNT(*) FILTER (WHERE try_strptime("Date",'%m/%d/%Y') IS NULL),ROUND(100.0*COUNT(*) FILTER (WHERE try_strptime("Date",'%m/%d/%Y') IS NULL)/COUNT(*),4) FROM s1.public."Patient"
  UNION ALL SELECT 'etablissement','finess_site 9 caracteres (non vides)',COUNT(*) FILTER (WHERE NULLIF(TRIM(finess_site),'') IS NOT NULL) AS total, COUNT(*) FILTER (WHERE NULLIF(TRIM(finess_site),'') IS NOT NULL AND finess_site !~ '^[0-9]{9}$') AS invalides, ROUND(100.0*COUNT(*) FILTER (WHERE NULLIF(TRIM(finess_site),'') IS NOT NULL AND finess_site !~ '^[0-9]{9}$')/NULLIF(COUNT(*) FILTER (WHERE NULLIF(TRIM(finess_site),'') IS NOT NULL),1),4) AS pct_invalide FROM src_etab
  UNION ALL SELECT 'satisfaction_2019','finess 9 caracteres',COUNT(*),COUNT(*) FILTER (WHERE finess !~ '^[0-9]{9}$'),ROUND(100.0*COUNT(*) FILTER (WHERE finess !~ '^[0-9]{9}$')/COUNT(*),4) FROM src_sat19
  UNION ALL SELECT 'satisfaction_2019','score_all_rea_ajust decimal (virgule)',COUNT(*),COUNT(*) FILTER (WHERE NULLIF(TRIM(score_all_rea_ajust),'') IS NOT NULL AND score19 IS NULL),ROUND(100.0*COUNT(*) FILTER (WHERE NULLIF(TRIM(score_all_rea_ajust),'') IS NOT NULL AND score19 IS NULL)/COUNT(*),4) FROM v_sat19_n
  UNION ALL SELECT 'satisfaction_2020','finess 9 caracteres',COUNT(*),COUNT(*) FILTER (WHERE finess !~ '^[0-9]{9}$'),ROUND(100.0*COUNT(*) FILTER (WHERE finess !~ '^[0-9]{9}$')/COUNT(*),4) FROM src_sat20
  UNION ALL SELECT 'satisfaction_2020','score_all_rea_ajust decimal (virgule)',COUNT(*),COUNT(*) FILTER (WHERE NULLIF(TRIM(score_all_rea_ajust),'') IS NOT NULL AND score20 IS NULL),ROUND(100.0*COUNT(*) FILTER (WHERE NULLIF(TRIM(score_all_rea_ajust),'') IS NOT NULL AND score20 IS NULL)/COUNT(*),4) FROM v_sat20_n
) TO 'dq/results/dq_validite.csv' (HEADER, DELIMITER ';');

-- ============ C. DOUBLONS ============
COPY (
  SELECT 'Hospitalisations' AS fichier, 'Num_Hospitalisation' AS cle, COUNT(*) AS total, COUNT(*)-COUNT(DISTINCT "Num_Hospitalisation") AS doublons, ROUND(100.0*(COUNT(*)-COUNT(DISTINCT "Num_Hospitalisation"))/COUNT(*),4) AS pct FROM src_hosp
  UNION ALL SELECT 'S1_Consultation','Num_consultation',COUNT(*),COUNT(*)-COUNT(DISTINCT "Num_consultation"),ROUND(100.0*(COUNT(*)-COUNT(DISTINCT "Num_consultation"))/COUNT(*),4) FROM s1.public."Consultation"
  UNION ALL SELECT 'S1_Patient','Id_patient',COUNT(*),COUNT(*)-COUNT(DISTINCT "Id_patient"),ROUND(100.0*(COUNT(*)-COUNT(DISTINCT "Id_patient"))/COUNT(*),4) FROM s1.public."Patient"
  UNION ALL SELECT 'etablissement (finess_site non vide)','finess_site',COUNT(*) FILTER (WHERE NULLIF(TRIM(finess_site),'') IS NOT NULL),COUNT(*) FILTER (WHERE NULLIF(TRIM(finess_site),'') IS NOT NULL)-COUNT(DISTINCT finess_site),0.0 FROM src_etab
  UNION ALL SELECT 'etablissement (toutes cles)','identifiant_organisation',COUNT(*),COUNT(*)-COUNT(DISTINCT NULLIF(TRIM(identifiant_organisation),'')),ROUND(100.0*(COUNT(*)-COUNT(DISTINCT NULLIF(TRIM(identifiant_organisation),'')))/COUNT(*),4) FROM src_etab
  UNION ALL SELECT 'satisfaction_2019','finess (grain brut)',COUNT(*),COUNT(*)-COUNT(DISTINCT finess),ROUND(100.0*(COUNT(*)-COUNT(DISTINCT finess))/COUNT(*),4) FROM src_sat19
  UNION ALL SELECT 'satisfaction_2019','finess + finess_geo (grain reel)',COUNT(*),COUNT(*)-COUNT(DISTINCT finess||'|'||finess_geo),ROUND(100.0*(COUNT(*)-COUNT(DISTINCT finess||'|'||finess_geo))/COUNT(*),4) FROM src_sat19
  UNION ALL SELECT 'satisfaction_2020','finess + finess_geo (grain reel)',COUNT(*),COUNT(*)-COUNT(DISTINCT finess||'|'||finess_geo),ROUND(100.0*(COUNT(*)-COUNT(DISTINCT finess||'|'||finess_geo))/COUNT(*),4) FROM src_sat20
  -- COALESCE sur chaque champ : en DuckDB, NULL || 'x' = NULL, une clé avec un
  -- champ NULL serait exclue du COUNT(DISTINCT) et gonflerait artificiellement
  -- le nombre de doublons (les décès ont des champs facultatifs : acte, lieu).
  UNION ALL SELECT 'deces','cle complete (nom,prenom,date_naissance,date_deces,lieu,acte)',COUNT(*),COUNT(*)-COUNT(DISTINCT COALESCE(nom,'')||'|'||COALESCE(prenom,'')||'|'||COALESCE(date_naissance,'')||'|'||COALESCE(date_deces,'')||'|'||COALESCE(code_lieu_deces,'')||'|'||COALESCE(numero_acte_deces,'')),ROUND(100.0*(COUNT(*)-COUNT(DISTINCT COALESCE(nom,'')||'|'||COALESCE(prenom,'')||'|'||COALESCE(date_naissance,'')||'|'||COALESCE(date_deces,'')||'|'||COALESCE(code_lieu_deces,'')||'|'||COALESCE(numero_acte_deces,'')))/COUNT(*),4) FROM src_deces
) TO 'dq/results/dq_doublons.csv' (HEADER, DELIMITER ';');

-- ============ C2. DOUBLONS SUR LES FAITS INTÉGRÉS (PoC) ============
-- Le sujet exige le contrôle des doublons sur les données INTÉGRÉES, pas
-- seulement sur les sources. Lecture des exports Parquet partitionnés du
-- PoC (chemin relatif au workspace $L2WORK ; en production, le même contrôle
-- porte sur les tables Gold/Silver du lake via Trino/Hive Metastore).
-- Grains : f_consultation_detail = 1 consultation ; f_hospitalisation = 1
-- hospitalisation ; f_satisfaction = établissement juridique x année ;
-- f_mesures = annee_mois x id_etablissement x id_patient x sexe x tranche_age
-- x id_localisation x region (region dans le grain : la satisfaction est
-- rattachée à la région, pas à un établissement de site).
COPY (
  SELECT 'f_consultation_detail (fait intégré, Silver)' AS table_cible, 'id_consultation (grain 1 consultation)' AS cle, COUNT(*) AS total, COUNT(*)-COUNT(DISTINCT id_consultation) AS doublons, ROUND(100.0*(COUNT(*)-COUNT(DISTINCT id_consultation))/COUNT(*),4) AS pct FROM read_parquet('poc/apres/f_consultation_detail/**/*.parquet', hive_partitioning=true)
  UNION ALL SELECT 'f_hospitalisation (fait intégré, Gold)', 'id_hospitalisation (grain 1 hospitalisation)', COUNT(*), COUNT(*)-COUNT(DISTINCT id_hospitalisation), ROUND(100.0*(COUNT(*)-COUNT(DISTINCT id_hospitalisation))/COUNT(*),4) FROM read_parquet('poc/apres/f_hospitalisation/**/*.parquet', hive_partitioning=true)
  UNION ALL SELECT 'f_satisfaction (fait intégré, Gold)', 'annee + finess (grain etab. juridique x annee)', COUNT(*), COUNT(*)-COUNT(DISTINCT annee||'|'||finess), ROUND(100.0*(COUNT(*)-COUNT(DISTINCT annee||'|'||finess))/COUNT(*),4) FROM read_parquet('poc/apres/f_satisfaction/**/*.parquet', hive_partitioning=true)
  UNION ALL SELECT 'f_mesures (fait consolidé, Gold)', 'annee_mois + id_etablissement + id_patient + sexe + tranche_age + id_localisation + region', COUNT(*), COUNT(*)-COUNT(DISTINCT annee_mois||'|'||id_etablissement||'|'||id_patient||'|'||sexe||'|'||tranche_age||'|'||id_localisation||'|'||region), ROUND(100.0*(COUNT(*)-COUNT(DISTINCT annee_mois||'|'||id_etablissement||'|'||id_patient||'|'||sexe||'|'||tranche_age||'|'||id_localisation||'|'||region))/COUNT(*),4) FROM read_parquet('poc/apres/f_mesures/**/*.parquet', hive_partitioning=true)
) TO 'dq/results/dq_doublons_faits.csv' (HEADER, DELIMITER ';');

-- ============ D. COHERENCE REFERENTIELLE ============
COPY (
  SELECT 'Hospitalisations.identifiant_organisation -> etab.identifiant_organisation' AS controle, COUNT(*) AS total, COUNT(e.identifiant_organisation) AS trouves, ROUND(100.0*COUNT(e.identifiant_organisation)/COUNT(*),2) AS pct_trouve FROM src_hosp h LEFT JOIN (SELECT DISTINCT identifiant_organisation FROM src_etab WHERE NULLIF(TRIM(identifiant_organisation),'') IS NOT NULL) e ON h."identifiant_organisation"=e.identifiant_organisation
  UNION ALL SELECT 'Hospitalisations.identifiant_organisation -> etab.finess_etablissement_juridique', COUNT(*), COUNT(e.finess_etablissement_juridique), ROUND(100.0*COUNT(e.finess_etablissement_juridique)/COUNT(*),2) FROM src_hosp h LEFT JOIN (SELECT DISTINCT finess_etablissement_juridique FROM src_etab WHERE NULLIF(TRIM(finess_etablissement_juridique),'') IS NOT NULL) e ON h."identifiant_organisation"=e.finess_etablissement_juridique
  UNION ALL SELECT 'Hospitalisations.identifiant_organisation -> etab.finess_site', COUNT(*), COUNT(e.finess_site), ROUND(100.0*COUNT(e.finess_site)/COUNT(*),2) FROM src_hosp h LEFT JOIN (SELECT DISTINCT finess_site FROM src_etab WHERE NULLIF(TRIM(finess_site),'') IS NOT NULL) e ON h."identifiant_organisation"=e.finess_site
  UNION ALL SELECT 'Hospitalisations.Id_patient -> S1.Patient.Id_patient', COUNT(*), COUNT(p."Id_patient"), ROUND(100.0*COUNT(p."Id_patient")/COUNT(*),2) FROM src_hosp h LEFT JOIN s1.public."Patient" p ON TRY_CAST(h."Id_patient" AS INTEGER)=p."Id_patient"
  UNION ALL SELECT 'Hospitalisations.Code_diagnostic -> S1.Diagnostic.Code_diag', COUNT(*), COUNT(d."Code_diag"), ROUND(100.0*COUNT(d."Code_diag")/COUNT(*),2) FROM src_hosp h LEFT JOIN s1.public."Diagnostic" d ON h."Code_diagnostic"=d."Code_diag"
  UNION ALL SELECT 'satisfaction_2019.finess -> etab.finess_etablissement_juridique', COUNT(*), COUNT(e.finess_etablissement_juridique), ROUND(100.0*COUNT(e.finess_etablissement_juridique)/COUNT(*),2) FROM src_sat19 s LEFT JOIN (SELECT DISTINCT finess_etablissement_juridique FROM src_etab WHERE NULLIF(TRIM(finess_etablissement_juridique),'') IS NOT NULL) e ON s.finess=e.finess_etablissement_juridique
  UNION ALL SELECT 'satisfaction_2019.finess_geo -> etab.finess_site', COUNT(*), COUNT(e.finess_site), ROUND(100.0*COUNT(e.finess_site)/COUNT(*),2) FROM src_sat19 s LEFT JOIN (SELECT DISTINCT finess_site FROM src_etab WHERE NULLIF(TRIM(finess_site),'') IS NOT NULL) e ON s.finess_geo=e.finess_site
  UNION ALL SELECT 'satisfaction_2020.finess -> etab.finess_etablissement_juridique', COUNT(*), COUNT(e.finess_etablissement_juridique), ROUND(100.0*COUNT(e.finess_etablissement_juridique)/COUNT(*),2) FROM src_sat20 s LEFT JOIN (SELECT DISTINCT finess_etablissement_juridique FROM src_etab WHERE NULLIF(TRIM(finess_etablissement_juridique),'') IS NOT NULL) e ON s.finess=e.finess_etablissement_juridique
  UNION ALL SELECT 'satisfaction_2020.finess_geo -> etab.finess_site', COUNT(*), COUNT(e.finess_site), ROUND(100.0*COUNT(e.finess_site)/COUNT(*),2) FROM src_sat20 s LEFT JOIN (SELECT DISTINCT finess_site FROM src_etab WHERE NULLIF(TRIM(finess_site),'') IS NOT NULL) e ON s.finess_geo=e.finess_site
  UNION ALL SELECT 'activite.identifiant -> pro_referentiel.identifiant', COUNT(*), COUNT(p.identifiant), ROUND(100.0*COUNT(p.identifiant)/COUNT(*),2) FROM src_act a LEFT JOIN (SELECT DISTINCT identifiant FROM src_pro WHERE NULLIF(TRIM(identifiant),'') IS NOT NULL) p ON a.identifiant=p.identifiant
  UNION ALL SELECT 'activite.identifiant_organisation -> etab.identifiant_organisation', COUNT(*), COUNT(e.identifiant_organisation), ROUND(100.0*COUNT(e.identifiant_organisation)/COUNT(*),2) FROM src_act a LEFT JOIN (SELECT DISTINCT identifiant_organisation FROM src_etab WHERE NULLIF(TRIM(identifiant_organisation),'') IS NOT NULL) e ON a."identifiant_organisation"=e.identifiant_organisation
  UNION ALL SELECT 'S1.Consultation.Id_prof_sante -> pro_referentiel.identifiant', COUNT(*), COUNT(p.identifiant), ROUND(100.0*COUNT(p.identifiant)/COUNT(*),2) FROM s1.public."Consultation" c LEFT JOIN (SELECT DISTINCT identifiant FROM src_pro WHERE NULLIF(TRIM(identifiant),'') IS NOT NULL) p ON c."Id_prof_sante"=p.identifiant
  UNION ALL SELECT 'S1.Consultation.Id_prof_sante -> activite.identifiant', COUNT(*), COUNT(a.identifiant), ROUND(100.0*COUNT(a.identifiant)/COUNT(*),2) FROM s1.public."Consultation" c LEFT JOIN (SELECT DISTINCT identifiant FROM src_act WHERE NULLIF(TRIM(identifiant),'') IS NOT NULL) a ON c."Id_prof_sante"=a.identifiant
  UNION ALL SELECT 'satisfaction_2019.finess -> etab.identifiant_organisation', COUNT(*), COUNT(e.identifiant_organisation), ROUND(100.0*COUNT(e.identifiant_organisation)/COUNT(*),2) FROM src_sat19 s LEFT JOIN (SELECT DISTINCT identifiant_organisation FROM src_etab WHERE NULLIF(TRIM(identifiant_organisation),'') IS NOT NULL) e ON s.finess=e.identifiant_organisation
  UNION ALL SELECT 'satisfaction_2020.finess -> etab.identifiant_organisation', COUNT(*), COUNT(e.identifiant_organisation), ROUND(100.0*COUNT(e.identifiant_organisation)/COUNT(*),2) FROM src_sat20 s LEFT JOIN (SELECT DISTINCT identifiant_organisation FROM src_etab WHERE NULLIF(TRIM(identifiant_organisation),'') IS NOT NULL) e ON s.finess=e.identifiant_organisation
  UNION ALL SELECT 'etablissement.code_commune -> departement (regle INSEE, non vides)', COUNT(*) FILTER (WHERE NULLIF(TRIM(code_commune),'') IS NOT NULL), COUNT(d.code_departement), ROUND(100.0*COUNT(d.code_departement)/NULLIF(COUNT(*) FILTER (WHERE NULLIF(TRIM(code_commune),'') IS NOT NULL),1),2) FROM src_etab e LEFT JOIN (SELECT DISTINCT code_departement FROM ref_dept) d ON CASE WHEN LEFT(TRIM(e.code_commune),2)='97' THEN LEFT(TRIM(e.code_commune),3) WHEN LEFT(TRIM(e.code_commune),2)='20' THEN '2A' ELSE LEFT(TRIM(e.code_commune),2) END=d.code_departement
  UNION ALL SELECT 'deces.code_lieu_deces -> departement (regle INSEE, non vides)', COUNT(*) FILTER (WHERE NULLIF(TRIM(code_lieu_deces),'') IS NOT NULL), COUNT(d.code_departement), ROUND(100.0*COUNT(d.code_departement)/NULLIF(COUNT(*) FILTER (WHERE NULLIF(TRIM(code_lieu_deces),'') IS NOT NULL),1),2) FROM src_deces x LEFT JOIN (SELECT DISTINCT code_departement FROM ref_dept) d ON CASE WHEN LEFT(TRIM(x.code_lieu_deces),2)='97' THEN LEFT(TRIM(x.code_lieu_deces),3) WHEN LEFT(TRIM(x.code_lieu_deces),2)='20' THEN '2A' ELSE LEFT(TRIM(x.code_lieu_deces),2) END=d.code_departement
  UNION ALL SELECT 'satisfaction_2019.region brute -> region normalisee (mapping)', COUNT(*), COUNT(m.region_norm), ROUND(100.0*COUNT(m.region_norm)/COUNT(*),2) FROM src_sat19 s LEFT JOIN ref_region_map m ON TRIM(s.region)=m.region_brute
  UNION ALL SELECT 'satisfaction_2020.region brute -> region normalisee (mapping)', COUNT(*), COUNT(m.region_norm), ROUND(100.0*COUNT(m.region_norm)/COUNT(*),2) FROM src_sat20 s LEFT JOIN ref_region_map m ON TRIM(s.region)=m.region_brute
  UNION ALL SELECT 'satisfaction_2019.region normalisee -> referentiel region', COUNT(*), COUNT(d.code_region), ROUND(100.0*COUNT(d.code_region)/COUNT(*),2) FROM src_sat19 s JOIN ref_region_map m ON TRIM(s.region)=m.region_brute LEFT JOIN (SELECT DISTINCT code_region, nom_region FROM ref_dept) d ON m.region_norm=d.nom_region
  UNION ALL SELECT 'satisfaction_2020.region normalisee -> referentiel region', COUNT(*), COUNT(d.code_region), ROUND(100.0*COUNT(d.code_region)/COUNT(*),2) FROM src_sat20 s JOIN ref_region_map m ON TRIM(s.region)=m.region_brute LEFT JOIN (SELECT DISTINCT code_region, nom_region FROM ref_dept) d ON m.region_norm=d.nom_region
  UNION ALL SELECT 'satisfaction_2020: concordance region ESATIS vs region etab (via finess juridique)', COUNT(*), COUNT(*) FILTER (WHERE m.region_norm=d.nom_region), ROUND(100.0*COUNT(*) FILTER (WHERE m.region_norm=d.nom_region)/COUNT(*),2) FROM src_sat20 s JOIN ref_region_map m ON TRIM(s.region)=m.region_brute JOIN src_etab e ON s.finess=e.finess_etablissement_juridique LEFT JOIN ref_dept d ON CASE WHEN LEFT(TRIM(e.code_commune),2)='97' THEN LEFT(TRIM(e.code_commune),3) WHEN LEFT(TRIM(e.code_commune),2)='20' THEN '2A' ELSE LEFT(TRIM(e.code_commune),2) END=d.code_departement AND d.nom_region=m.region_norm
) TO 'dq/results/dq_referentiel.csv' (HEADER, DELIMITER ';');

-- ============ E. COHERENCE TEMPORELLE ============
COPY (
  SELECT 'Hospitalisations' AS fichier, 'Date_Entree dans [2015,2023]' AS controle, COUNT(*) AS total, COUNT(*) FILTER (WHERE d_entree BETWEEN DATE '2015-01-01' AND DATE '2023-12-31') AS conformes, ROUND(100.0*COUNT(*) FILTER (WHERE d_entree BETWEEN DATE '2015-01-01' AND DATE '2023-12-31')/COUNT(*),2) AS pct_conforme, CAST(MIN(d_entree) AS VARCHAR)||' -> '||CAST(MAX(d_entree) AS VARCHAR) AS plage_obseevee FROM v_hosp_d
  UNION ALL SELECT 'S1_Consultation','Date dans [2015,2023]',COUNT(*),COUNT(*) FILTER (WHERE "Date" BETWEEN DATE '2015-01-01' AND DATE '2023-12-31'),ROUND(100.0*COUNT(*) FILTER (WHERE "Date" BETWEEN DATE '2015-01-01' AND DATE '2023-12-31')/COUNT(*),2),CAST(MIN("Date") AS VARCHAR)||' -> '||CAST(MAX("Date") AS VARCHAR) FROM s1.public."Consultation"
  UNION ALL SELECT 'deces','date_deces >= date_naissance',COUNT(*),COUNT(*) FILTER (WHERE d_deces >= d_naissance),ROUND(100.0*COUNT(*) FILTER (WHERE d_deces >= d_naissance)/COUNT(*),2),CAST(MIN(d_deces) AS VARCHAR)||' -> '||CAST(MAX(d_deces) AS VARCHAR) FROM v_deces_d
  UNION ALL SELECT 'S1_Patient','coherence Age vs date_naissance (|ecart|<=2 ans)',COUNT(*),COUNT(*) FILTER (WHERE abs("Age"-(2023-EXTRACT(YEAR FROM try_strptime("Date",'%m/%d/%Y'))))<=2),ROUND(100.0*COUNT(*) FILTER (WHERE abs("Age"-(2023-EXTRACT(YEAR FROM try_strptime("Date",'%m/%d/%Y'))))<=2)/COUNT(*),2),'age 0..100 observe' FROM s1.public."Patient"
  UNION ALL SELECT 'satisfaction_2019','score_all_rea_ajust dans [0,100]',COUNT(*),COUNT(*) FILTER (WHERE score19 BETWEEN 0 AND 100),ROUND(100.0*COUNT(*) FILTER (WHERE score19 BETWEEN 0 AND 100)/COUNT(*),2),'scores 0-100' FROM v_sat19_n
  UNION ALL SELECT 'satisfaction_2020','score_all_rea_ajust dans [0,100]',COUNT(*),COUNT(*) FILTER (WHERE score20 BETWEEN 0 AND 100),ROUND(100.0*COUNT(*) FILTER (WHERE score20 BETWEEN 0 AND 100)/COUNT(*),2),'scores 0-100' FROM v_sat20_n
) TO 'dq/results/dq_temporel.csv' (HEADER, DELIMITER ';');

-- ============ F. VOLUMES (profilage) ============
COPY (
  SELECT 'S1.PostgreSQL (dump DATA2023)' AS source, 'Patient' AS table_ou_fichier, COUNT(*) AS lignes, NULL AS plage FROM s1.public."Patient"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Consultation',COUNT(*),CAST(MIN("Date") AS VARCHAR)||' -> '||CAST(MAX("Date") AS VARCHAR) FROM s1.public."Consultation"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Diagnostic',COUNT(*),NULL FROM s1.public."Diagnostic"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Professionnel_de_sante',COUNT(*),NULL FROM s1.public."Professionnel_de_sante"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Specialites',COUNT(*),NULL FROM s1.public."Specialites"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Salle',COUNT(*),NULL FROM s1.public."Salle"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Adher',COUNT(*),NULL FROM s1.public."Adher"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Mutuelle',COUNT(*),NULL FROM s1.public."Mutuelle"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Prescription',COUNT(*),NULL FROM s1.public."Prescription"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Medicaments',COUNT(*),NULL FROM s1.public."Medicaments"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','Laboratoire',COUNT(*),NULL FROM s1.public."Laboratoire"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','AAAA',COUNT(*),NULL FROM s1.public."AAAA"
  UNION ALL SELECT 'S1.PostgreSQL (dump DATA2023)','date',COUNT(*),NULL FROM s1.public."date"
  UNION ALL SELECT 'S2.Hospitalisations.csv','Hospitalisations.csv',COUNT(*),CAST(MIN(d_entree) AS VARCHAR)||' -> '||CAST(MAX(d_entree) AS VARCHAR) FROM v_hosp_d
  UNION ALL SELECT 'S3/ESATIS','satisfaction_2019.csv',COUNT(*),'2019' FROM src_sat19
  UNION ALL SELECT 'S3/ESATIS','satisfaction_2020.csv',COUNT(*),'2020' FROM src_sat20
  UNION ALL SELECT 'S4.INSEE','deces.csv',COUNT(*),CAST(MIN(d_deces) AS VARCHAR)||' -> '||CAST(MAX(d_deces) AS VARCHAR) FROM v_deces_d
  UNION ALL SELECT 'Referentiel FINESS','etablissement_sante.csv',COUNT(*),NULL FROM src_etab
  UNION ALL SELECT 'Referentiel FINESS','professionnel_sante.csv',COUNT(*),NULL FROM src_pro
  UNION ALL SELECT 'Referentiel FINESS','activite_professionnel_sante.csv',COUNT(*),NULL FROM src_act
) TO 'dq/results/dq_volumes.csv' (HEADER, DELIMITER ';');
