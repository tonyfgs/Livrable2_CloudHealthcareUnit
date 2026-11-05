-- D04 (besoin 2) : consultations par chapitre CIM-10 (1ere lettre du code), 2019-2021
SELECT SUBSTR(d.code_diag, 1, 1) AS lettre_cim10, f.annee, COUNT(*) AS consultations
FROM hive.silver.f_consultation_detail f
JOIN hive.gold.d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
WHERE f.annee BETWEEN 2019 AND 2021
GROUP BY 1, 2
ORDER BY 1, 2
