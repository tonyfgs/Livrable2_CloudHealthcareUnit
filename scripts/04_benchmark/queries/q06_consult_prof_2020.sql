-- Q06 (métier 6) : consultation par professionnel - top 10, 2020
-- s'appuie sur l'agrégat pré-calculé f_consult_prof_mois (couche Gold)
SELECT p.identifiant, p.civilite, p.nom, p.prenom, p.profession,
       SUM(f.nbre_consultation) AS nbre_consultations
FROM f_consult_prof_mois f
JOIN d_professionnel p ON f.id_professionnel = p.identifiant
WHERE f.annee = 2020
GROUP BY 1,2,3,4,5
ORDER BY nbre_consultations DESC
LIMIT 10;
