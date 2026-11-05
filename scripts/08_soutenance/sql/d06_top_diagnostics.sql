-- D06 (besoins 2 et 4) : top 10 diagnostics, departage DETERMINISTE (ex aequo -> code_diag)
SELECT 'consultations_2019_2021' AS indicateur, d.code_diag, d.libelle_diagnostic, SUM(f.nbre_consultation) AS valeur
FROM hive.gold.f_consult_diag_mois f JOIN hive.gold.d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
WHERE f.annee BETWEEN 2019 AND 2021
GROUP BY 1, 2, 3
ORDER BY valeur DESC, d.code_diag
LIMIT 10
