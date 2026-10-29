-- Q08 (métier 8) : taux global de satisfaction par région sur 2020
SELECT f.region,
       COUNT(*) AS nbre_etablissements,
       ROUND(AVG(f.score_satisfaction),2) AS score_moyen_satisfaction,
       SUM(f.nb_repondants) AS total_repondants
FROM f_satisfaction f
WHERE f.annee = 2020 AND f.score_satisfaction IS NOT NULL
GROUP BY 1
ORDER BY score_moyen_satisfaction DESC;
