-- Q09 (technique) côté Trino : requête sur vue métier v_taux_consultation_mensuel
-- Version Trino : le connecteur Hive de Trino ne lit pas les vues Hive, la vue
-- est recréée en vue NATIVE Trino dans le schéma `vues_t`
-- (01_ddl/04_vues_metier_trino.sql, exécuté par le DAG lakehouse_init et par
-- ce harnais). Agrégation + fonction fenêtrée moyenne mobile 3 mois + tri ;
-- le filtre porte sur `annee` (colonne de partition) -> pruning.
SELECT * FROM vues_t.v_taux_consultation_mensuel
WHERE annee >= 2019
ORDER BY annee_mois;
