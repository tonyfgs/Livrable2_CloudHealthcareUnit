-- Q04 (métier 4) : hospitalisation par diagnostic - top 10, 2015-2021
SELECT d.libelle_diagnostic,
       COUNT(*) AS nbre_hospitalisations,
       ROUND(AVG(f.duree_jours),1) AS duree_moyenne_jours
FROM f_hospitalisation f
JOIN d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
WHERE f.annee BETWEEN 2015 AND 2021
GROUP BY 1
ORDER BY nbre_hospitalisations DESC
LIMIT 10;
