-- Q09 (technique) : requête sur vue métier v_taux_consultation_mensuel
-- (agrégation + fonction fenêtrée moyenne mobile 3 mois + tri), période 2019-2021
-- Le filtre porte sur `annee` (colonne de partition en scénario "après") -> pruning.
SELECT * FROM v_taux_consultation_mensuel
WHERE annee >= 2019
ORDER BY annee_mois;
