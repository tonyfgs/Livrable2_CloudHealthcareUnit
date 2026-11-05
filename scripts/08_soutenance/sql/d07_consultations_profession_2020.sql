-- D07 (besoin 6) : consultations 2020 par profession + top 10 professionnels (departage par identifiant)
SELECT p.profession, SUM(f.nbre_consultation) AS consultations, COUNT(DISTINCT f.id_professionnel) AS professionnels
FROM hive.gold.f_consult_prof_mois f JOIN hive.gold.d_professionnel p ON f.id_professionnel = p.identifiant
WHERE f.annee = 2020
GROUP BY p.profession
ORDER BY consultations DESC, p.profession
