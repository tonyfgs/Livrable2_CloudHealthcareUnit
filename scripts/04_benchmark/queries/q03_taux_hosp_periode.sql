-- Q03 (métier 3) : taux global d'hospitalisation sur la période 2015-2021
SELECT f.annee,
       SUM(f.nbre_hospitalisation) AS nbre_hospitalisations,
       SUM(f.nbre_consultation) AS nbre_consultations,
       ROUND(100.0 * SUM(f.nbre_hospitalisation) / NULLIF(SUM(f.nbre_consultation),0), 2) AS taux_hospitalisation_pour_100_consultations
FROM f_mesures f
WHERE f.annee BETWEEN 2015 AND 2021
GROUP BY f.annee
ORDER BY f.annee;
