EXPLAIN ANALYZE
-- Q10 (technique) : jointure étoile complète (fait + 4 dimensions), filtre croisé
-- annee x sexe, agrégation région x tranche d'âge, tri + limite
SELECT l.region AS region, p.tranche_age_ref AS tranche_age, t.annee,
       SUM(f.nbre_consultation) AS nbre_consultations
FROM f_mesures f
JOIN d_temps t ON f.id_temps = t.id_temps
JOIN d_patient p ON f.id_patient = p.id_patient
JOIN d_etablissement e ON f.id_etablissement = e.id_etablissement
JOIN d_localisation l ON f.id_localisation = l.id_localisation
WHERE f.annee = 2019 AND f.sexe = 'M' AND f.nbre_consultation > 0
GROUP BY 1,2,3
ORDER BY nbre_consultations DESC
LIMIT 20;
