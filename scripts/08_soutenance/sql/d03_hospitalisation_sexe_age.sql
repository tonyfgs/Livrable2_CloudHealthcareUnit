-- D03 (besoin 5) : hospitalisations par sexe et tranche d'age, rapportees aux consultations
-- (meme sexe, meme tranche, meme fenetre d'observation 2015-06-21 -> 2021-06-19)
WITH h AS (
  SELECT sexe, tranche_age, COUNT(*) AS hospitalisations
  FROM hive.gold.f_hospitalisation GROUP BY sexe, tranche_age
), c AS (
  SELECT sexe, tranche_age, COUNT(*) AS consultations
  FROM hive.silver.f_consultation_detail
  WHERE id_temps BETWEEN 20150621 AND 20210619
  GROUP BY sexe, tranche_age
)
SELECT h.sexe, h.tranche_age, h.hospitalisations, c.consultations,
       ROUND(1000.0 * h.hospitalisations / c.consultations, 2) AS hosp_pour_1000_consultations
FROM h JOIN c ON h.sexe = c.sexe AND h.tranche_age = c.tranche_age
ORDER BY h.sexe, h.tranche_age
