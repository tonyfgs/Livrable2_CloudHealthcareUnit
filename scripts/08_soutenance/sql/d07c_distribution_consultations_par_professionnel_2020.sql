-- D07c (besoin 6) : distribution de l'activite 2020 : combien de professionnels ont n consultations
WITH par_pro AS (
  SELECT id_professionnel, SUM(nbre_consultation) AS n
  FROM hive.gold.f_consult_prof_mois WHERE annee = 2020 GROUP BY id_professionnel
)
SELECT n AS consultations_par_professionnel, COUNT(*) AS professionnels
FROM par_pro GROUP BY n ORDER BY n
