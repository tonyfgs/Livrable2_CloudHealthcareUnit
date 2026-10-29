.mode list
.headers on
.timer off
-- ============================================================
-- PoC Livrable 2 - fait décès agrégé (depuis les 25,09 M de décès INSEE)
-- Grain : ANNEE x sexe x tranche_age x département (id_localisation)
-- Le fichier couvre 1882-2020 : le grain mensuel produirait 8,6 M de lignes pour un
-- besoin métier annuel (décès par région sur 2019) -> grain annuel retenu, stocké dans
-- f_mesures avec la sentinelle annee_mois = 'AAAA-00' (agrégat annuel, comme la satisfaction).
-- ============================================================
INSTALL postgres; LOAD postgres;
ATTACH 'dbname=chu_s1 host=chu-s1 user=chu password=chu' AS s1 (TYPE postgres);

CREATE OR REPLACE TABLE f_deces_agg AS
SELECT x.annee * 10000 + 101 AS id_temps,          -- 1er janvier de l'année (jointure d_temps)
       x.annee,
       CASE x.sexe WHEN '1' THEN 'M' WHEN '2' THEN 'F' ELSE 'I' END AS sexe,
       tranche_of_age(date_diff('year', x.d_naissance, x.d_deces)
         - (CASE WHEN EXTRACT(MONTH FROM x.d_deces)*100 + EXTRACT(DAY FROM x.d_deces)
                      < EXTRACT(MONTH FROM x.d_naissance)*100 + EXTRACT(DAY FROM x.d_naissance)
                 THEN 1 ELSE 0 END)) AS tranche_age,
       COALESCE(d.code_departement,'INCONNU') AS id_localisation,
       COALESCE(d.nom_region,'INCONNUE') AS region,
       COUNT(*) AS nbre_deces
FROM (SELECT sexe, code_lieu_deces,
             EXTRACT(YEAR FROM try_cast(date_deces AS DATE))::INTEGER AS annee,
             try_cast(date_deces AS DATE) AS d_deces,
             try_cast(date_naissance AS DATE) AS d_naissance
      FROM t_deces) x
LEFT JOIN ref_dept d ON dept_of(x.code_lieu_deces) = d.code_departement
WHERE x.d_deces IS NOT NULL
GROUP BY 1,2,3,4,5,6;

CHECKPOINT;

-- ---------- exports ----------
COPY (SELECT * FROM f_deces_agg) TO 'poc/avant/f_deces_agg.parquet' (FORMAT PARQUET);
-- APRES : PARTITIONED BY (annee) seul. Justification : un partitionnement par
-- (annee, sexe, tranche_age) produirait 138 x 2 x 6 = 1 656 partitions de ~40 lignes
-- (small-files problem) pour 69 008 lignes ; le grain annuel + la colonne annee en
-- partition donnent 138 partitions de ~500 lignes. Le tri par (sexe, tranche_age,
-- id_localisation) compense l'absence de partitionnement sur ces colonnes.
COPY (SELECT * FROM f_deces_agg ORDER BY sexe, tranche_age, id_localisation) TO 'poc/apres/f_deces_agg' (FORMAT PARQUET, PARTITION_BY (annee), OVERWRITE_OR_IGNORE 1);

SELECT 'f_deces_agg' AS fait, COUNT(*) AS lignes, SUM(nbre_deces) AS total_deces FROM f_deces_agg;
