-- Q05 (métier 5) : hospitalisation par sexe et tranche d'âge, 2015-2021
SELECT f.sexe, f.tranche_age,
       SUM(f.nbre_hospitalisation) AS nbre_hospitalisations
FROM f_mesures f
WHERE f.annee BETWEEN 2015 AND 2021 AND f.nbre_hospitalisation > 0
GROUP BY 1,2
ORDER BY f.sexe, f.tranche_age;
