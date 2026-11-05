-- D01 (besoin 1, tendance) : consultations par mois (toutes sources S1), source f_mesures
SELECT annee_mois, SUM(nbre_consultation) AS consultations
FROM hive.gold.f_mesures
WHERE annee_mois NOT LIKE '%-00'
GROUP BY annee_mois
HAVING SUM(nbre_consultation) > 0
ORDER BY annee_mois
