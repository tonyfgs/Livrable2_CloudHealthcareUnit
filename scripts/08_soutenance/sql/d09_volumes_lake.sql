-- D09 : inventaire du lake (preuve "tous les repertoires visibles") : tables et volumes
SELECT 'gold' AS schema_hive, 'd_temps' AS table_name, COUNT(*) AS lignes FROM hive.gold.d_temps UNION ALL
SELECT 'gold','d_patient',COUNT(*) FROM hive.gold.d_patient UNION ALL
SELECT 'gold','d_etablissement',COUNT(*) FROM hive.gold.d_etablissement UNION ALL
SELECT 'gold','d_localisation',COUNT(*) FROM hive.gold.d_localisation UNION ALL
SELECT 'gold','d_diagnostic',COUNT(*) FROM hive.gold.d_diagnostic UNION ALL
SELECT 'gold','d_professionnel',COUNT(*) FROM hive.gold.d_professionnel UNION ALL
SELECT 'gold','f_mesures',COUNT(*) FROM hive.gold.f_mesures UNION ALL
SELECT 'silver','f_consultation_detail',COUNT(*) FROM hive.silver.f_consultation_detail UNION ALL
SELECT 'gold','f_hospitalisation',COUNT(*) FROM hive.gold.f_hospitalisation UNION ALL
SELECT 'gold','f_deces_agg',COUNT(*) FROM hive.gold.f_deces_agg UNION ALL
SELECT 'gold','f_satisfaction',COUNT(*) FROM hive.gold.f_satisfaction UNION ALL
SELECT 'gold','f_consult_diag_mois',COUNT(*) FROM hive.gold.f_consult_diag_mois UNION ALL
SELECT 'gold','f_consult_prof_mois',COUNT(*) FROM hive.gold.f_consult_prof_mois
