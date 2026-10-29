-- ============================================================
-- Scénario AVANT : tables plates (Parquet non partitionné, non trié)
-- = schéma physique sans partitionnement ni index (référence)
-- ============================================================
CREATE OR REPLACE VIEW f_mesures AS SELECT * FROM read_parquet('poc/avant/f_mesures.parquet');
CREATE OR REPLACE VIEW f_consultation_detail AS SELECT * FROM read_parquet('poc/avant/f_consultation_detail.parquet');
CREATE OR REPLACE VIEW f_hospitalisation AS SELECT * FROM read_parquet('poc/avant/f_hospitalisation.parquet');
CREATE OR REPLACE VIEW f_deces_agg AS SELECT * FROM read_parquet('poc/avant/f_deces_agg.parquet');
CREATE OR REPLACE VIEW f_satisfaction AS SELECT * FROM read_parquet('poc/avant/f_satisfaction.parquet');
CREATE OR REPLACE VIEW f_consult_diag_mois AS SELECT * FROM read_parquet('poc/avant/f_consult_diag_mois.parquet');
CREATE OR REPLACE VIEW f_consult_prof_mois AS SELECT * FROM read_parquet('poc/avant/f_consult_prof_mois.parquet');
CREATE OR REPLACE VIEW d_temps AS SELECT * FROM read_parquet('poc/avant/d_temps.parquet');
CREATE OR REPLACE VIEW d_patient AS SELECT * FROM read_parquet('poc/avant/d_patient.parquet');
CREATE OR REPLACE VIEW d_etablissement AS SELECT * FROM read_parquet('poc/avant/d_etablissement.parquet');
CREATE OR REPLACE VIEW d_localisation AS SELECT * FROM read_parquet('poc/avant/d_localisation.parquet');
CREATE OR REPLACE VIEW d_diagnostic AS SELECT * FROM read_parquet('poc/avant/d_diagnostic.parquet');
CREATE OR REPLACE VIEW d_professionnel AS SELECT * FROM read_parquet('poc/avant/d_professionnel.parquet');
-- vue métier : taux de consultation mensuel national + moyenne mobile 3 mois (fonction fenêtrée)
CREATE OR REPLACE VIEW v_taux_consultation_mensuel AS
SELECT annee, annee_mois,
       SUM(nbre_consultation) AS nbre_consultations,
       ROUND(AVG(SUM(nbre_consultation)) OVER (ORDER BY annee_mois ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),0) AS moyenne_mobile_3mois
FROM f_mesures
WHERE nbre_consultation > 0
GROUP BY annee, annee_mois;
.timer on
