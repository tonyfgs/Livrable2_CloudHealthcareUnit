-- Q02 (métier 2) : taux de consultation par diagnostic - top 10, 2019-2021
-- s'appuie sur l'agrégat pré-calculé f_consult_diag_mois (couche Gold)
SELECT d.libelle_diagnostic,
       SUM(f.nbre_consultation) AS nbre_consultations
FROM f_consult_diag_mois f
JOIN d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
WHERE f.annee BETWEEN 2019 AND 2021
GROUP BY 1
ORDER BY nbre_consultations DESC
LIMIT 10;
