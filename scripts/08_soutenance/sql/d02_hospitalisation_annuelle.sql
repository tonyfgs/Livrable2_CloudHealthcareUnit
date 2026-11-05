-- D02 (besoin 3) : taux global d'hospitalisation par annee.
-- Fenetre d'observation commune des hospitalisations : 2015-06-21 -> 2021-06-19.
-- Les consultations sont donc comptees sur la MEME fenetre (comparaison a periode egale).
WITH h AS (
  SELECT annee, COUNT(*) AS hospitalisations, COUNT(DISTINCT id_patient) AS patients_hospitalises,
         ROUND(AVG(duree_jours), 1) AS duree_moyenne_jours
  FROM hive.gold.f_hospitalisation GROUP BY annee
), c AS (
  SELECT annee,
         COUNT(*) AS consultations_annee,
         COUNT_IF(id_temps BETWEEN 20150621 AND 20210619) AS consultations_fenetre
  FROM hive.silver.f_consultation_detail GROUP BY annee
)
SELECT c.annee, h.hospitalisations, c.consultations_annee, c.consultations_fenetre,
       h.patients_hospitalises, h.duree_moyenne_jours,
       ROUND(100.0 * h.hospitalisations / c.consultations_fenetre, 3) AS hosp_pour_100_consultations,
       ROUND(1000.0 * h.patients_hospitalises / (SELECT COUNT(*) FROM hive.gold.d_patient), 2) AS patients_hosp_pour_1000_patients
FROM c JOIN h ON h.annee = c.annee
ORDER BY c.annee
