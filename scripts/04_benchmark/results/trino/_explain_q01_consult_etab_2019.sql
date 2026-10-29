EXPLAIN ANALYZE
-- Q01 (métier 1) : taux de consultation par établissement et par période - top 20 établissements, 2019
SELECT e.raison_sociale_site, e.code_commune, e.region,
       SUM(f.nbre_consultation) AS nbre_consultations
FROM f_mesures f
JOIN d_etablissement e ON f.id_etablissement = e.id_etablissement
WHERE f.annee = 2019 AND f.nbre_consultation > 0
GROUP BY 1,2,3
ORDER BY nbre_consultations DESC
LIMIT 20;
