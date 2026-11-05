SELECT 'hospitalisations_2015_2021' AS indicateur, d.code_diag, d.libelle_diagnostic, COUNT(*) AS valeur
FROM hive.gold.f_hospitalisation f JOIN hive.gold.d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
WHERE f.annee BETWEEN 2015 AND 2021
GROUP BY 1, 2, 3
ORDER BY valeur DESC, d.code_diag
LIMIT 10
