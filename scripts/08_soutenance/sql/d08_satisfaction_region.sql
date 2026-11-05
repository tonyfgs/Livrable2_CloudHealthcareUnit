-- D08 (besoin 8) : satisfaction par region et annee, moyenne ponderee par les repondants
SELECT annee, region, COUNT(*) AS etablissements_juridiques, COUNT(score_satisfaction) AS etablissements_avec_score,
       SUM(nb_repondants) AS repondants,
       ROUND(SUM(score_satisfaction * nb_repondants) / SUM(nb_repondants), 2) AS score_pondere
FROM hive.gold.f_satisfaction
GROUP BY annee, region
ORDER BY annee, score_pondere DESC NULLS LAST, region
