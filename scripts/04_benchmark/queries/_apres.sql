-- ============================================================
-- Scénario APRES : tables partitionnées (style Hive) + tri sur les clés de
-- jointure (le bucketing a été retiré du schéma déployé : Trino 484 refuse de
-- lire les tables bucketées écrites par Spark — voir livrable2.md §4.2).
-- Lecture par glob avec hive_partitioning -> pruning des partitions.
-- ============================================================
CREATE OR REPLACE VIEW f_mesures AS SELECT * FROM read_parquet('poc/apres/f_mesures/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW f_consultation_detail AS SELECT * FROM read_parquet('poc/apres/f_consultation_detail/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW f_hospitalisation AS SELECT * FROM read_parquet('poc/apres/f_hospitalisation/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW f_deces_agg AS SELECT * FROM read_parquet('poc/apres/f_deces_agg/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW f_satisfaction AS SELECT * FROM read_parquet('poc/apres/f_satisfaction/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW f_consult_diag_mois AS SELECT * FROM read_parquet('poc/apres/f_consult_diag_mois/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW f_consult_prof_mois AS SELECT * FROM read_parquet('poc/apres/f_consult_prof_mois/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW d_temps AS SELECT * FROM read_parquet('poc/apres/d_temps/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW d_patient AS SELECT * FROM read_parquet('poc/apres/d_patient/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW d_etablissement AS SELECT * FROM read_parquet('poc/apres/d_etablissement/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW d_localisation AS SELECT * FROM read_parquet('poc/apres/d_localisation/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW d_diagnostic AS SELECT * FROM read_parquet('poc/apres/d_diagnostic/**/*.parquet', hive_partitioning=true);
CREATE OR REPLACE VIEW d_professionnel AS SELECT * FROM read_parquet('poc/apres/d_professionnel/**/*.parquet', hive_partitioning=true);
-- vue métier : taux de consultation mensuel national + moyenne mobile 3 mois (fonction fenêtrée)
CREATE OR REPLACE VIEW v_taux_consultation_mensuel AS
SELECT annee, annee_mois,
       SUM(nbre_consultation) AS nbre_consultations,
       ROUND(AVG(SUM(nbre_consultation)) OVER (ORDER BY annee_mois ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),0) AS moyenne_mobile_3mois
FROM f_mesures
WHERE nbre_consultation > 0
GROUP BY annee, annee_mois;
.timer on
