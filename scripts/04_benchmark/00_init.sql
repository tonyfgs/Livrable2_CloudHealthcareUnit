.mode list
.headers on
.timer off
-- ============================================================
-- PoC Livrable 2 - initialisation : sources + matérialisation
-- Base persistante : poc/warehouse.duckdb (ouverte en argument CLI)
--
-- Chemins : les 3 chemins absolus "DATA Projet" ci-dessous correspondent au
-- montage du volume Docker chu-l2-data (même chemin absolu hôte/conteneur,
-- voir run-duckdb.sh, variable DATA_PROJET). Adapter si le projet est ailleurs.
-- Les CSV de staging/ (satisfaction nettoyés, référentiels UTF-8) sont lus
-- en relatif depuis le workspace ($L2WORK).
-- ============================================================
INSTALL postgres; LOAD postgres;
ATTACH 'dbname=chu_s1 host=chu-s1 user=chu password=chu' AS s1 (TYPE postgres);

CREATE OR REPLACE VIEW src_hosp AS SELECT * FROM read_csv('/Users/tonyfages/CESI/4A/Big Data/projet/DATA Projet/Hospitalisation/Hospitalisations.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW src_etab AS SELECT * FROM read_csv('/Users/tonyfages/CESI/4A/Big Data/projet/DATA Projet/Etablissement de SANTE/etablissement_sante.csv', header=true, delim=';', nullstr='', all_varchar=true, max_line_size=10000000);
CREATE OR REPLACE VIEW src_pro AS SELECT * FROM read_csv('staging/professionnel_sante_utf8.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW src_act AS SELECT * FROM read_csv('staging/activite_professionnel_sante_utf8.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW src_sat19 AS SELECT * FROM read_csv('staging/satisfaction_2019.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW src_sat20 AS SELECT * FROM read_csv('staging/satisfaction_2020_clean.csv', header=true, delim=';', nullstr='', all_varchar=true);
CREATE OR REPLACE VIEW ref_dept AS SELECT * FROM read_csv('staging/referentiel_departements.csv', header=true, delim=';', nullstr='', all_varchar=true);

-- Normalisation des libellés de région hétérogènes ESATIS ("Océan Indien" ambigu -> volontairement absent)
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

-- Matérialisation physique intermédiaire (coupe la poussée de cast dans le lecteur CSV)
CREATE OR REPLACE TABLE t_hosp AS SELECT * FROM src_hosp;
CREATE OR REPLACE TABLE t_sat19 AS SELECT * FROM src_sat19;
CREATE OR REPLACE TABLE t_sat20 AS SELECT * FROM src_sat20;
CREATE OR REPLACE TABLE t_deces AS SELECT * FROM read_csv('/Users/tonyfages/CESI/4A/Big Data/projet/DATA Projet/DECES EN FRANCE/deces.csv', header=true, delim=',', quote='"', nullstr='', all_varchar=true);
CHECKPOINT;

-- Vues typées sûres (cast sur table DuckDB, jamais dans le lecteur CSV)
CREATE OR REPLACE VIEW v_deces_d AS SELECT *, try_cast(date_deces AS DATE) AS d_deces, try_cast(date_naissance AS DATE) AS d_naissance FROM t_deces;
CREATE OR REPLACE VIEW v_hosp_d AS SELECT *, try_strptime("Date_Entree",'%d/%m/%Y') AS d_entree FROM t_hosp;

-- Tranche d'âge à partir de l'âge révolu en années. L'âge révolu est calculé inline
-- (date_diff('year', dn, de) - ajustement anniversaire) : le binder DuckDB ne résout pas
-- les surcharges de try_strptime/date à l'intérieur d'une macro scalaire.
-- L'attribut Age de S1 s'est révélé incohérent avec la date de naissance à la date de
-- référence (contrôle DQ temporel : 46 % seulement) -> la tranche est calculée à la date
-- de l'événement, jamais lue depuis Age.
CREATE OR REPLACE MACRO tranche_of_age(a) AS
  CASE WHEN a IS NULL THEN 'INCONNUE'
       WHEN a < 15 THEN '0-14'
       WHEN a < 30 THEN '15-29'
       WHEN a < 45 THEN '30-44'
       WHEN a < 60 THEN '45-59'
       WHEN a < 75 THEN '60-74'
       ELSE '75+'
  END;

-- Règle INSEE : code commune (5 car.) -> département (2 car., 3 car. pour les 97x, '2A' pour la Corse 20xxx)
CREATE OR REPLACE MACRO dept_of(code_commune) AS
  CASE
    WHEN code_commune IS NULL OR TRIM(code_commune) = '' THEN NULL
    WHEN LEFT(TRIM(code_commune),2) = '97' THEN LEFT(TRIM(code_commune),3)
    WHEN LEFT(TRIM(code_commune),2) = '20' THEN '2A'
    ELSE LEFT(TRIM(code_commune),2)
  END;

SELECT 't_deces' AS table_duckdb, COUNT(*) AS lignes FROM t_deces
UNION ALL SELECT 't_hosp', COUNT(*) FROM t_hosp
UNION ALL SELECT 't_sat19', COUNT(*) FROM t_sat19
UNION ALL SELECT 't_sat20', COUNT(*) FROM t_sat20;
