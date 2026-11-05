-- D05 (besoin 4) : hospitalisations et duree moyenne par chapitre CIM-10, 2015-2021
SELECT SUBSTR(d.code_diag, 1, 1) AS lettre_cim10, COUNT(*) AS hospitalisations,
       ROUND(AVG(f.duree_jours), 1) AS duree_moyenne_jours
FROM hive.gold.f_hospitalisation f
JOIN hive.gold.d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
WHERE f.annee BETWEEN 2015 AND 2021
GROUP BY 1
ORDER BY 2 DESC, 1
