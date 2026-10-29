.mode list
.headers on
.timer off
-- ============================================================
-- PoC Livrable 2 - fait consolidé f_mesures (couche Gold, modèle en étoile)
-- Grain : annee_mois x id_etablissement x id_patient x sexe x tranche_age x id_localisation
--         x region
--   - consultations / hospitalisations : grain MENSUEL (annee_mois = 'AAAA-MM')
--   - décès / satisfaction            : grain ANNUEL  (annee_mois = 'AAAA-00', sentinelle)
--   - region dans le grain : la satisfaction est rattachée à la RÉGION (pas à un
--     établissement de site, cf. 02a) ; sans region dans la clé de jointure, la
--     jointure sat (1 ligne par région x année) créait un fan-out (31 doublons,
--     mesuré en DQ C2 et corrigé).
-- 4 mesures : nbre_consultation, nbre_hospitalisation, total_satisfaction, nbre_deces
-- ============================================================
INSTALL postgres; LOAD postgres;
ATTACH 'dbname=chu_s1 host=s1-postgres user=chu password=chu' AS s1 (TYPE postgres);

CREATE OR REPLACE TABLE f_mesures AS
WITH consult AS (
  SELECT annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region, COUNT(*) AS nbre_consultation
  FROM f_consultation_detail GROUP BY 1,2,3,4,5,6,7
),
hosp AS (
  SELECT annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region, COUNT(*) AS nbre_hospitalisation
  FROM f_hospitalisation GROUP BY 1,2,3,4,5,6,7
),
deces AS (
  -- Périmètre d'analyse du projet : 2015-2023 (les autres faits couvrent 2015-2023).
  -- Les décès antérieurs (1882-2014) restent disponibles dans f_deces_agg (couche Gold).
  -- region : rattachée via d_localisation (les décès n'ont pas d'établissement).
  SELECT CAST(annee AS VARCHAR) || '-00' AS annee_mois,   -- sentinelle agrégat annuel
         -1 AS id_etablissement, -1 AS id_patient, sexe, tranche_age, d.id_localisation,
         SUM(nbre_deces) AS nbre_deces,
         COALESCE(MIN(l.region),'INCONNUE') AS region
  FROM f_deces_agg d
  LEFT JOIN d_localisation l ON d.id_localisation = l.id_localisation
  WHERE annee >= 2015 GROUP BY 1,2,3,4,5,6
),
sat AS (
  SELECT annee || '-00' AS annee_mois,                   -- sentinelle agrégat annuel
         -1 AS id_etablissement, -1 AS id_patient, 'TOUS' AS sexe, 'TOUTES' AS tranche_age,
         'INCONNU' AS id_localisation,
         -- moyenne pondérée par les répondants des finess juridiques de la région
         SUM(score_satisfaction * nb_repondants) / NULLIF(SUM(nb_repondants),0) AS total_satisfaction,
         region
  FROM f_satisfaction WHERE score_satisfaction IS NOT NULL GROUP BY 1, region
),
cles AS (
  SELECT annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region FROM consult
  UNION
  SELECT annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region FROM hosp
  UNION
  SELECT annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region FROM deces
  UNION
  SELECT annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region FROM sat
)
SELECT k.annee_mois,
       CAST(strftime(try_cast(k.annee_mois || '-01' AS DATE),'%Y%m%d') AS INTEGER) AS id_temps,
       CAST(substring(k.annee_mois,1,4) AS INTEGER) AS annee,
       k.id_etablissement, k.id_patient, k.sexe, k.tranche_age, k.id_localisation,
       COALESCE(c.nbre_consultation,0) AS nbre_consultation,
       COALESCE(h.nbre_hospitalisation,0) AS nbre_hospitalisation,
       COALESCE(d.nbre_deces,0) AS nbre_deces,
       s.total_satisfaction,
       COALESCE(k.region, 'INCONNUE') AS region
FROM cles k
LEFT JOIN consult c USING (annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region)
LEFT JOIN hosp h USING (annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region)
LEFT JOIN deces d USING (annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region)
LEFT JOIN sat s USING (annee_mois, id_etablissement, id_patient, sexe, tranche_age, id_localisation, region);

-- ---------- vues dérivées : équivalents des 4 faits métier du Livrable 1 ----------
CREATE OR REPLACE VIEW v_fait_consultation AS SELECT * FROM f_mesures WHERE nbre_consultation > 0;
CREATE OR REPLACE VIEW v_fait_hospitalisation AS SELECT * FROM f_mesures WHERE nbre_hospitalisation > 0;
CREATE OR REPLACE VIEW v_fait_deces AS SELECT * FROM f_mesures WHERE nbre_deces > 0;
CREATE OR REPLACE VIEW v_fait_satisfaction AS SELECT * FROM f_mesures WHERE total_satisfaction IS NOT NULL;

CHECKPOINT;

-- ---------- exports ----------
COPY (SELECT * FROM f_mesures) TO 'poc/avant/f_mesures.parquet' (FORMAT PARQUET);
-- APRES : PARTITIONED BY (annee, sexe, tranche_age), tri (id_etablissement, id_patient)
COPY (SELECT * FROM f_mesures ORDER BY id_etablissement, id_patient) TO 'poc/apres/f_mesures' (FORMAT PARQUET, PARTITION_BY (annee, sexe, tranche_age), OVERWRITE_OR_IGNORE 1);

SELECT 'f_mesures' AS fait, COUNT(*) AS lignes,
       SUM(nbre_consultation) AS total_consultations,
       SUM(nbre_hospitalisation) AS total_hospitalisations,
       SUM(nbre_deces) AS total_deces
FROM f_mesures;
