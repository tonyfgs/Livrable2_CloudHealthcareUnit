-- Q08 (métier 8) : taux global de satisfaction par région sur 2020
-- Score = moyenne PONDÉRÉE par les répondants (SUM(score x nb_repondants) /
-- SUM(nb_repondants)) : cohérent avec le grain de f_satisfaction, où
-- score_satisfaction est elle-même la moyenne pondérée des sites au niveau
-- du finess juridique. Une moyenne simple des établissements sur-pondérerait
-- les petits établissements.
SELECT f.region,
       COUNT(*) AS nbre_etablissements,
       ROUND(SUM(f.score_satisfaction * f.nb_repondants) / SUM(f.nb_repondants), 2) AS score_moyen_satisfaction,
       SUM(f.nb_repondants) AS total_repondants
FROM f_satisfaction f
WHERE f.annee = 2020 AND f.score_satisfaction IS NOT NULL
GROUP BY 1
ORDER BY score_moyen_satisfaction DESC;
