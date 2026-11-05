-- Q03 (métier 3) : taux global d'hospitalisation sur la période 2015-2021
SELECT f.annee,
       SUM(f.nbre_hospitalisation) AS nbre_hospitalisations,
       SUM(f.nbre_consultation) AS nbre_consultations,
       ROUND(100.0 * SUM(f.nbre_hospitalisation) / NULLIF(SUM(f.nbre_consultation),0), 2) AS taux_hospitalisation_pour_100_consultations
FROM f_mesures f
-- fenetre d'observation commune : les hospitalisations couvrent 2015-06 a 2021-06 ;
-- diviser par les consultations de 2021 entier donnerait un taux sous-estime (0,13 au lieu de 0,21 a la maille mois, 0,22 a la maille jour)
WHERE f.annee_mois BETWEEN '2015-06' AND '2021-06'
GROUP BY f.annee
ORDER BY f.annee;
