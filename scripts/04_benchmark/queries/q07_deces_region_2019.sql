-- Q07 (métier 7) : nombre de décès par région sur 2019
SELECT l.region AS region,
       SUM(f.nbre_deces) AS nbre_deces
FROM f_deces_agg f
JOIN d_localisation l ON f.id_localisation = l.id_localisation
WHERE f.annee = 2019
GROUP BY 1
ORDER BY nbre_deces DESC;
