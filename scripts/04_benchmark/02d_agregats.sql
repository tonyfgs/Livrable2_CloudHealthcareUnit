.mode list
.headers on
.timer off
-- ============================================================
-- PoC Livrable 2 - agrégats pré-calculés (couche Gold, équivalents matérialisés)
-- f_consult_diag_mois   : consultations par diagnostic x mois x établissement
-- f_consult_prof_mois   : consultations par professionnel x mois
-- ============================================================
INSTALL postgres; LOAD postgres;
ATTACH 'dbname=chu_s1 host=chu-s1 user=chu password=chu' AS s1 (TYPE postgres);

CREATE OR REPLACE TABLE f_consult_diag_mois AS
SELECT annee_mois, CAST(substring(annee_mois,1,4) AS INTEGER) AS annee, id_diagnostic, id_etablissement, COUNT(*) AS nbre_consultation
FROM f_consultation_detail GROUP BY 1,2,3,4;

CREATE OR REPLACE TABLE f_consult_prof_mois AS
SELECT annee_mois, CAST(substring(annee_mois,1,4) AS INTEGER) AS annee, id_professionnel, COUNT(*) AS nbre_consultation
FROM f_consultation_detail GROUP BY 1,2,3;

CHECKPOINT;

-- ---------- exports ----------
COPY (SELECT * FROM f_consult_diag_mois) TO 'poc/avant/f_consult_diag_mois.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM f_consult_prof_mois) TO 'poc/avant/f_consult_prof_mois.parquet' (FORMAT PARQUET);
COPY (SELECT * FROM f_consult_diag_mois ORDER BY id_diagnostic) TO 'poc/apres/f_consult_diag_mois' (FORMAT PARQUET, PARTITION_BY (annee), OVERWRITE_OR_IGNORE 1);
COPY (SELECT * FROM f_consult_prof_mois ORDER BY id_professionnel) TO 'poc/apres/f_consult_prof_mois' (FORMAT PARQUET, PARTITION_BY (annee), OVERWRITE_OR_IGNORE 1);

SELECT 'f_consult_diag_mois' AS agg, COUNT(*) AS lignes FROM f_consult_diag_mois
UNION ALL SELECT 'f_consult_prof_mois', COUNT(*) FROM f_consult_prof_mois;
