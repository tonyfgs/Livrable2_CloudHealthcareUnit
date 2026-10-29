.mode list
.headers on
.timer off
-- ============================================================
-- PoC Livrable 2 - faits de détail et satisfaction (couches Silver/Gold)
-- ============================================================
INSTALL postgres; LOAD postgres;
ATTACH 'dbname=chu_s1 host=s1-postgres user=chu password=chu' AS s1 (TYPE postgres);

-- ---------- f_consultation_detail (grain = 1 consultation, couche Silver) ----------
-- Rattachement établissement : Consultation.Id_prof_sante -> activite (identifiant, identifiant_organisation)
--   -> etablissement.identifiant_organisation.
--   Un professionnel exerce souvent dans PLUSIEURS établissements (fichier d'activité 1-N) :
--   la jointure est ramenée à UNE organisation par professionnel (MIN(identifiant_organisation))
--   pour préserver le grain "1 consultation" (un fan-out 1-N dupliquerait les consultations :
--   +346 437 lignes, +33,7 %, mesuré en v1 et corrigé).
--   Taux mesurés en DQ : 71,87 % des consultations ont leur professionnel dans le fichier
--   d'activité ; 63,87 % sont rattachées à un établissement (chaîne complète) ; les autres
--   -> id_etablissement = -1 (membre Inconnu).
CREATE OR REPLACE TABLE f_consultation_detail AS
WITH c2 AS (
  SELECT c."Num_consultation" AS id_consultation,
         CAST(strftime(c."Date",'%Y%m%d') AS INTEGER) AS id_temps,
         CAST(strftime(c."Date",'%Y-%m') AS VARCHAR) AS annee_mois,
         EXTRACT(YEAR FROM c."Date")::INTEGER AS annee,
         c."Id_patient" AS id_patient,
         c."Id_prof_sante" AS id_professionnel,
         c."Code_diag" AS code_diag,
         c."Date" AS date_event,
         CAST(try_strptime(p."Date",'%m/%d/%Y') AS DATE) AS date_naissance,
         CASE p."Sexe" WHEN 'male' THEN 'M' WHEN 'female' THEN 'F' ELSE 'I' END AS sexe
  FROM s1.public."Consultation" c
  JOIN s1.public."Patient" p ON c."Id_patient" = p."Id_patient"
)
SELECT c2.id_consultation, c2.id_temps, c2.annee_mois, c2.annee,
       c2.id_patient, c2.sexe,
       tranche_of_age(date_diff('year', c2.date_naissance, c2.date_event)
         - (CASE WHEN EXTRACT(MONTH FROM c2.date_event)*100 + EXTRACT(DAY FROM c2.date_event)
                      < EXTRACT(MONTH FROM c2.date_naissance)*100 + EXTRACT(DAY FROM c2.date_naissance)
                 THEN 1 ELSE 0 END)) AS tranche_age,
       dg.id_diagnostic,
       c2.id_professionnel,
       COALESCE(e.id_etablissement, -1) AS id_etablissement,
       COALESCE(e.id_localisation, 'INCONNU') AS id_localisation,
       COALESCE(e.region, 'INCONNUE') AS region
FROM c2
JOIN d_diagnostic dg ON c2.code_diag = dg.code_diag
LEFT JOIN (SELECT identifiant, MIN(identifiant_organisation) AS identifiant_organisation
           FROM src_act
           WHERE NULLIF(TRIM(identifiant),'') IS NOT NULL
           GROUP BY identifiant) a
       ON c2.id_professionnel = a.identifiant
LEFT JOIN d_etablissement e ON a.identifiant_organisation = e.identifiant_organisation;

-- ---------- f_hospitalisation (grain = 1 hospitalisation) ----------
-- Rattachement établissement direct : identifiant_organisation -> etablissement (100 % en DQ).
CREATE OR REPLACE TABLE f_hospitalisation AS
WITH h2 AS (
  SELECT TRIM(h."Num_Hospitalisation") AS id_hospitalisation,
         try_strptime(h."Date_Entree",'%d/%m/%Y') AS d_entree,
         TRY_CAST(h."Id_patient" AS INTEGER) AS id_patient,
         TRIM(h."identifiant_organisation") AS identifiant_organisation,
         TRIM(h."Code_diagnostic") AS code_diagnostic,
         TRY_CAST(h."Jour_Hospitalisation" AS INTEGER) AS duree_jours
  FROM t_hosp h
)
SELECT h2.id_hospitalisation,
       CAST(strftime(h2.d_entree,'%Y%m%d') AS INTEGER) AS id_temps,
       CAST(strftime(h2.d_entree,'%Y-%m') AS VARCHAR) AS annee_mois,
       EXTRACT(YEAR FROM h2.d_entree)::INTEGER AS annee,
       h2.id_patient,
       CASE p."Sexe" WHEN 'male' THEN 'M' WHEN 'female' THEN 'F' ELSE 'I' END AS sexe,
       tranche_of_age(date_diff('year', CAST(try_strptime(p."Date",'%m/%d/%Y') AS DATE), h2.d_entree)
         - (CASE WHEN EXTRACT(MONTH FROM h2.d_entree)*100 + EXTRACT(DAY FROM h2.d_entree)
                      < EXTRACT(MONTH FROM CAST(try_strptime(p."Date",'%m/%d/%Y') AS DATE))*100
                      + EXTRACT(DAY FROM CAST(try_strptime(p."Date",'%m/%d/%Y') AS DATE))
                 THEN 1 ELSE 0 END)) AS tranche_age,
       dg.id_diagnostic,
       COALESCE(e.id_etablissement,-1) AS id_etablissement,
       COALESCE(e.id_localisation,'INCONNU') AS id_localisation,
       COALESCE(e.region,'INCONNUE') AS region,
       h2.duree_jours
FROM h2
JOIN s1.public."Patient" p ON h2.id_patient = p."Id_patient"
JOIN d_diagnostic dg ON h2.code_diagnostic = dg.code_diag
LEFT JOIN d_etablissement e ON h2.identifiant_organisation = e.identifiant_organisation;

-- ---------- f_satisfaction (grain = établissement juridique x année, source ESATIS) ----------
-- Le score brut utilise la virgule décimale dans les deux millésimes (mesuré en DQ).
-- Grain juridique, PAS site : le finess ESATIS est un finess d'établissement juridique, qui
-- correspond à plusieurs sites dans le fichier source (2 295 lignes -> 1 960 finess juridiques
-- distincts, mesuré en DQ : le grain brut du fichier est (finess, finess_geo) = grain SITE).
-- Le rattachement site est impossible (finess_geo -> finess_site = 0 % en DQ) : la mesure est
-- donc agrégée au grain (annee, finess juridique), sans clé d'établissement de site :
--   - score_satisfaction = moyenne PONDÉRÉE par le nombre de répondants
--     (SUM(score * nb_repondants) / SUM(nb_repondants))
--   - nb_repondants = somme des répondants des sites agrégés
--   - nb_sites = nombre de lignes source agrégées (sites ESATIS du même finess juridique)
--   - region = région de l'établissement juridique ; 6 finess juridiques apparaissent avec
--     2 régions dans la source (1 966 couples annee/finess/region pour 1 960 finess) :
--     la région retenue est MIN(region) et la pondération mélange les sites (documenté).
-- La région provient de la colonne `region` du fichier ESATIS, normalisée par mapping.
CREATE OR REPLACE TABLE f_satisfaction AS
SELECT annee,
       finess,
       MIN(region) AS region,
       SUM(score_satisfaction * nb_repondants) / NULLIF(SUM(nb_repondants),0) AS score_satisfaction,
       SUM(nb_repondants) AS nb_repondants,
       COUNT(*) AS nb_sites
FROM (
  SELECT s.annee,
         s.finess,
         COALESCE(m.region_norm,'INCONNUE') AS region,
         try_cast(replace(s.score_all_rea_ajust,',','.') AS DOUBLE) AS score_satisfaction,
         TRY_CAST(s.nb_rep_score_all_rea_ajust AS INTEGER) AS nb_repondants
  FROM (
    SELECT '2019' AS annee, TRIM(finess) AS finess, region, score_all_rea_ajust, nb_rep_score_all_rea_ajust FROM t_sat19
    UNION ALL
    SELECT '2020' AS annee, TRIM(finess) AS finess, region, score_all_rea_ajust, nb_rep_score_all_rea_ajust FROM t_sat20
  ) s
  LEFT JOIN ref_region_map m ON TRIM(s.region) = m.region_brute
)
GROUP BY annee, finess;

CHECKPOINT;

-- ---------- exports AVANT (Parquet plat) ----------
COPY (SELECT * FROM f_consultation_detail) TO 'poc/avant/f_consultation_detail.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM f_hospitalisation) TO 'poc/avant/f_hospitalisation.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM f_satisfaction) TO 'poc/avant/f_satisfaction.parquet' (FORMAT PARQUET);

-- ---------- exports APRES (partitionnés + triés) ----------
-- f_consultation_detail : PARTITIONED BY (annee), tri (id_patient, id_temps)
COPY (SELECT * FROM f_consultation_detail ORDER BY id_patient, id_temps) TO 'poc/apres/f_consultation_detail' (FORMAT PARQUET, PARTITION_BY (annee), OVERWRITE_OR_IGNORE 1);
-- f_hospitalisation : PARTITIONED BY (annee), tri (id_etablissement, id_patient)
COPY (SELECT * FROM f_hospitalisation ORDER BY id_etablissement, id_patient) TO 'poc/apres/f_hospitalisation' (FORMAT PARQUET, PARTITION_BY (annee), OVERWRITE_OR_IGNORE 1);
-- f_satisfaction : PARTITIONED BY (annee, region), tri (finess)
COPY (SELECT * FROM f_satisfaction ORDER BY finess) TO 'poc/apres/f_satisfaction' (FORMAT PARQUET, PARTITION_BY (annee, region), OVERWRITE_OR_IGNORE 1);

SELECT 'f_consultation_detail' AS fait, COUNT(*) AS lignes FROM f_consultation_detail
UNION ALL SELECT 'f_hospitalisation', COUNT(*) FROM f_hospitalisation
UNION ALL SELECT 'f_satisfaction', COUNT(*) FROM f_satisfaction;
