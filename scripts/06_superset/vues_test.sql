-- ============================================================================
-- vues_test.sql — vues Trino du BAC A SABLE dashboard "TEST — Parcours CHU"
-- Appliquer avec le CLI trino (catalog hive) :
--   docker exec chu-trino trino --server http://localhost:8080 --catalog hive -f <ce fichier>
-- (ou requete par requete). A virer avec cleanup_test.sh (DROP SCHEMA vues_test CASCADE).
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS hive.vues_test;

-- Table region -> code ISO 3166-2. ATTENTION : le geojson france_regions embarque
-- dans Superset 4.1.1 contient 'FR-IDF\t' (tabulation parasite) et 'FR-LRE'
-- pour La Reunion ; le plugin country_map matche strictement
-- country_id === feature.properties.ISO, donc il faut ces valeurs exactes,
-- sinon la region reste blanche et le hover ne repond pas.
CREATE OR REPLACE VIEW hive.vues_test.ref_region_iso_test AS
SELECT * FROM (VALUES
 ('Auvergne-Rhône-Alpes','FR-ARA'),('Bourgogne-Franche-Comté','FR-BFC'),('Bretagne','FR-BRE'),
 ('Centre-Val de Loire','FR-CVL'),('Corse','FR-COR'),('Grand Est','FR-GES'),('Guadeloupe','FR-GUA'),
 ('Guyane','FR-GUF'),('Hauts-de-France','FR-HDF'),
 ('Île-de-France', 'FR-IDF' || chr(9)),
 ('La Réunion','FR-LRE'),('Mayotte','FR-MAY'),('Martinique','FR-MTQ'),
 ('Normandie','FR-NOR'),('Nouvelle-Aquitaine','FR-NAQ'),('Occitanie','FR-OCC'),
 ('Pays de la Loire','FR-PDL'),('Provence-Alpes-Côte d''Azur','FR-PAC')
) AS t(region, iso_region);

-- Vue des KPIs du bandeau : une ligne par fait f_mesures, avec les colonnes
-- annee/region/sexe pour que les filtres natifs du dashboard (barre horizontale
-- Année / Région / Sexe) s'appliquent aussi aux 4 grands chiffres.
-- Les agrégats sont calculés par les métriques des charts (SUM, COUNT DISTINCT)
-- sur les lignes filtrées : aucun double comptage.
-- nbre_deces de f_mesures = décès INSEE 2015-2021 (cohérent avec la carte décès).
CREATE OR REPLACE VIEW hive.vues_test.v_test_kpi AS
SELECT CAST(annee AS VARCHAR) AS annee, region, sexe,
       nbre_consultation, nbre_hospitalisation, nbre_deces, id_patient
FROM hive.gold.f_mesures;

-- Par annee (fenetre commune 2015-06..2021-06, cf. charts.sql chart 3)
CREATE OR REPLACE VIEW hive.vues_test.v_test_annee AS
SELECT CAST(annee AS VARCHAR) AS annee, SUM(nbre_consultation) AS consultations, SUM(nbre_hospitalisation) AS hospitalisations,
       ROUND(100.0*SUM(nbre_hospitalisation)/NULLIF(SUM(nbre_consultation),0),2) AS taux_hosp_pour_100
FROM hive.gold.f_mesures WHERE annee_mois BETWEEN '2015-06' AND '2021-06'
GROUP BY annee;

-- Funnel consultations -> hospitalisations
CREATE OR REPLACE VIEW hive.vues_test.v_test_funnel AS
SELECT 'consultations' AS etape, SUM(nbre_consultation) AS valeur FROM hive.gold.f_mesures
UNION ALL SELECT 'hospitalisations', SUM(nbre_hospitalisation) FROM hive.gold.f_mesures;

-- Hospitalisations par chapitre CIM-10 (sunburst : chapitre -> diagnostic)
-- + colonnes annee/region/sexe : présentes dans le dataset pour que les filtres
-- natifs du dashboard (Année / Région / Sexe) s'appliquent à ce graphique.
CREATE OR REPLACE VIEW hive.vues_test.v_test_cim10_chapitre AS
SELECT f.annee, f.region, f.sexe,
       substr(d.code_diag,1,3) AS code_chapitre,
       COALESCE(ch.libelle, substr(d.code_diag,1,3)) AS chapitre,
       d.libelle_diagnostic AS diagnostic,
       COUNT(*) AS nb_hospitalisations
FROM hive.gold.f_hospitalisation f
JOIN hive.gold.d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
-- Jointure des libellés CIM-10 par LETTRE de chapitre (le code exact 'T87' ne
-- matche jamais 'T00' : la jointure sur 3 caractères ne donnait que des codes).
LEFT JOIN (VALUES
 ('A','Maladies infectieuses et parasitaires'),('B','Maladies infectieuses et parasitaires'),
 ('C','Tumeurs malignes'),('D','Tumeurs et maladies du sang'),
 ('E','Maladies endocriniennes, nutritionnelles et métaboliques'),
 ('F','Troubles mentaux et du comportement'),
 ('G','Maladies du système nerveux'),
 ('H','Maladies de l oeil, de l oreille'),
 ('I','Maladies de l appareil circulatoire'),
 ('J','Maladies de l appareil respiratoire'),
 ('K','Maladies de l appareil digestif'),
 ('L','Maladies de la peau et du tissu cellulaire sous-cutané'),
 ('M','Maladies du système ostéo-articulaire'),
 ('N','Maladies de l appareil génito-urinaire'),
 ('O','Grossesse, accouchement et puerpéralité'),
 ('P','Affections de la période périnatale'),
 ('Q','Malformations congénitales et anomalies chromosomiques'),
 ('R','Symptômes, signes et résultats anormaux'),
 ('S','Lésions traumatiques, empoisonnements'),
 ('T','Lésions traumatiques, empoisonnements'),
 ('V','Causes externes de morbidité et de mortalité'),
 ('Z','Facteurs influençant l état de santé')
) AS ch(code, libelle) ON substr(d.code_diag,1,1) = ch.code
WHERE f.annee BETWEEN 2015 AND 2021
GROUP BY 1,2,3,4,5,6;

-- Satisfaction pondérée par annee et region
CREATE OR REPLACE VIEW hive.vues_test.v_test_satisfaction_annee_region AS
SELECT annee, region,
       ROUND(SUM(score_satisfaction*nb_repondants)/NULLIF(SUM(nb_repondants),0),2) AS score_moyen_pondere,
       SUM(nb_repondants) AS total_repondants
FROM hive.gold.f_satisfaction GROUP BY annee, region;

-- Carte des décès par région (joint la table ISO ci-dessus)
CREATE OR REPLACE VIEW hive.vues_test.v_test_carte_deces AS
SELECT d.annee, d.region, r.iso_region, SUM(d.nbre_deces) AS nbre_deces
FROM hive.gold.f_deces_agg d
LEFT JOIN hive.vues_test.ref_region_iso_test r ON d.region = r.region
WHERE d.annee >= 2015  -- fenetre projet 2015-2023
GROUP BY 1,2,3;

-- Durée de séjour par diagnostic (box plot / durée moyenne)
CREATE OR REPLACE VIEW hive.vues_test.v_test_box_duree_diag AS
SELECT d.code_diag, d.libelle_diagnostic, f.duree_jours, f.annee
FROM hive.gold.f_hospitalisation f JOIN hive.gold.d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
WHERE f.annee BETWEEN 2015 AND 2021;

-- Consultations par chapitre CIM-10 et annee (2019-2021), pour le visuel V03
CREATE OR REPLACE VIEW hive.vues_test.v_test_consult_chapitre_annee AS
SELECT f.annee, substr(d.code_diag,1,1) AS lettre,
       COALESCE(ch.libelle, substr(d.code_diag,1,1)) AS chapitre,
       SUM(f.nbre_consultation) AS consultations
FROM hive.gold.f_consult_diag_mois f
JOIN hive.gold.d_diagnostic d ON f.id_diagnostic = d.id_diagnostic
LEFT JOIN (VALUES ('A','Maladies infectieuses et parasitaires'),('B','Maladies infectieuses et parasitaires'),
 ('C','Tumeurs'),('D','Tumeurs et maladies du sang'),('E','Maladies endocriniennes, métaboliques'),
 ('F','Troubles mentaux'),('G','Maladies du système nerveux'),('H','Maladies de l oeil et de l oreille'),
 ('I','Maladies de l appareil circulatoire'),('J','Maladies de l appareil respiratoire'),
 ('K','Maladies de l appareil digestif'),('L','Maladies de la peau'),('M','Maladies ostéo-articulaires'),
 ('N','Maladies génito-urinaires'),('O','Grossesse, accouchement'),('P','Période périnatale'),
 ('Q','Malformations congénitales'),('R','Symptômes et signes anormaux'),('S','Lésions traumatiques'),
 ('T','Lésions traumatiques (suite)'),('U','Codes spéciaux (U00-U99)'),
 ('V','Causes externes de morbidité'),('W','Causes externes de morbidité'),('X','Causes externes de morbidité'),('Y','Causes externes de morbidité'),('Z','Facteurs de santé')
) AS ch(lettre, libelle) ON substr(d.code_diag,1,1) = ch.lettre
WHERE f.annee BETWEEN 2019 AND 2021
GROUP BY 1,2,3;

-- Distribution du nombre de consultations par professionnel (histogramme V07)
-- + colonne annee pour que le filtre natif "Année" s'applique au graphique.
-- (f_consult_prof_mois n'a pas de région/sexe : pas de filtre Région/Sexe ici.)
CREATE OR REPLACE VIEW hive.vues_test.v_test_distrib_pro AS
SELECT CAST(annee AS VARCHAR) AS annee, nb_consultations AS consultations_par_pro, COUNT(*) AS nb_professionnels
FROM (SELECT id_professionnel, annee, SUM(nbre_consultation) AS nb_consultations
      FROM hive.gold.f_consult_prof_mois GROUP BY 1, 2) t
GROUP BY 1, 2 ORDER BY 1, 2;

-- Carte de la satisfaction 2020 par région (choroplèthe, ISO pour country_map)
-- Carte satisfaction : derniere annee dispo par region (2020 si present, sinon 2019)
-- car ESATIS 2020 n'a pas de lignes pour Corse / La Reunion / Mayotte dans ce jeu
CREATE OR REPLACE VIEW hive.vues_test.v_test_carte_sat_2020 AS
WITH dispo AS (
  SELECT region, MAX(annee) AS annee_dispo FROM hive.gold.f_satisfaction WHERE annee IN (2019, 2020) GROUP BY 1
)
SELECT CAST(d.annee_dispo AS VARCHAR) AS annee, s.region, r.iso_region,
       ROUND(SUM(s.score_satisfaction*s.nb_repondants)/NULLIF(SUM(s.nb_repondants),0),2) AS score_moyen_pondere
FROM hive.gold.f_satisfaction s
JOIN dispo d ON s.region = d.region AND s.annee = d.annee_dispo
LEFT JOIN hive.vues_test.ref_region_iso_test r ON s.region = r.region
GROUP BY 1,2,3;

-- Fiabilité des données (barres de contrôle qualité, visuel V11), calculée live
CREATE OR REPLACE VIEW hive.vues_test.v_test_fiabilite AS
SELECT 'Hospitalisations rattachées à un établissement' AS controle, ROUND(100.0*COUNT(CASE WHEN id_etablissement<>-1 THEN 1 END)/COUNT(*),1) AS pct FROM hive.gold.f_hospitalisation
UNION ALL SELECT 'Décès avec département identifié', ROUND(100.0*COUNT(CASE WHEN id_localisation<>'INCONNU' THEN 1 END)/COUNT(*),1) FROM hive.gold.f_deces_agg
UNION ALL SELECT 'Satisfaction 2019 avec un score', ROUND(100.0*COUNT(CASE WHEN annee=2019 AND score_satisfaction IS NOT NULL THEN 1 END)/NULLIF(COUNT(CASE WHEN annee=2019 THEN 1 END),0),1) FROM hive.gold.f_satisfaction
UNION ALL SELECT 'Satisfaction 2020 avec un score', ROUND(100.0*COUNT(CASE WHEN annee=2020 AND score_satisfaction IS NOT NULL THEN 1 END)/NULLIF(COUNT(CASE WHEN annee=2020 THEN 1 END),0),1) FROM hive.gold.f_satisfaction
UNION ALL SELECT 'Consultations avec professionnel connu', ROUND(100.0*COUNT(CASE WHEN id_professionnel IS NOT NULL THEN 1 END)/COUNT(*),1) FROM hive.silver.f_consultation_detail
UNION ALL SELECT 'Consultations rattachées à un établissement', ROUND(100.0*COUNT(CASE WHEN id_etablissement<>-1 THEN 1 END)/COUNT(*),1) FROM hive.silver.f_consultation_detail;

-- Graphe réseau : top 12 groupes juridiques (finess) par volume de consultations
-- 2019 -> region d'appartenance, poids = consultations (chart Graph echarts)
CREATE OR REPLACE VIEW hive.vues_test.v_test_graph_groupes AS
WITH top_groupes AS (
  SELECT e.finess_etablissement_juridique AS groupe, SUM(f.nbre_consultation) AS vol
  FROM hive.gold.f_mesures f JOIN hive.gold.d_etablissement e ON f.id_etablissement = e.id_etablissement
  WHERE f.annee = 2019 AND f.nbre_consultation > 0
    AND e.finess_etablissement_juridique IS NOT NULL AND e.finess_etablissement_juridique <> ''
  GROUP BY 1 ORDER BY 2 DESC LIMIT 12
)
SELECT g.groupe AS source, e.region AS target, e.region AS region, SUM(f.nbre_consultation) AS value
FROM hive.gold.f_mesures f
JOIN hive.gold.d_etablissement e ON f.id_etablissement = e.id_etablissement
JOIN top_groupes g ON e.finess_etablissement_juridique = g.groupe
WHERE f.annee = 2019 AND f.nbre_consultation > 0
GROUP BY 1, 2, 3;

-- MAJ : carte satisfaction = derniere annee dispo par region (2020 si present,
-- sinon 2019) car ESATIS 2020 n'a pas de lignes pour Corse/DOM dans ce jeu.
-- La Reunion et Mayotte n'existent pas du tout dans f_satisfaction (ESATIS).
CREATE OR REPLACE VIEW hive.vues_test.v_test_carte_sat_2020 AS
WITH dispo AS (
  SELECT region, MAX(annee) AS annee_dispo FROM hive.gold.f_satisfaction WHERE annee IN (2019, 2020) GROUP BY 1
)
SELECT CAST(d.annee_dispo AS VARCHAR) AS annee, s.region, r.iso_region,
       ROUND(SUM(s.score_satisfaction*s.nb_repondants)/NULLIF(SUM(s.nb_repondants),0),2) AS score_moyen_pondere
FROM hive.gold.f_satisfaction s
JOIN dispo d ON s.region = d.region AND s.annee = d.annee_dispo
LEFT JOIN hive.vues_test.ref_region_iso_test r ON s.region = r.region
GROUP BY 1,2,3;

-- Graphe réseau hospitalisations : memes top 12 groupes juridiques (par consult 2019),
-- poids = nombre d'hospitalisations (chart Graph echarts, 2e courbe du module)
CREATE OR REPLACE VIEW hive.vues_test.v_test_graph_hosp AS
WITH top_groupes AS (
  SELECT e.finess_etablissement_juridique AS groupe, SUM(f.nbre_consultation) AS vol
  FROM hive.gold.f_mesures f JOIN hive.gold.d_etablissement e ON f.id_etablissement = e.id_etablissement
  WHERE f.annee = 2019 AND f.nbre_consultation > 0
    AND e.finess_etablissement_juridique IS NOT NULL AND e.finess_etablissement_juridique <> ''
  GROUP BY 1 ORDER BY 2 DESC LIMIT 12
)
SELECT g.groupe AS source, e.region AS target, e.region AS region, COUNT(*) AS value
FROM hive.gold.f_hospitalisation h
JOIN hive.gold.d_etablissement e ON h.id_etablissement = e.id_etablissement
JOIN top_groupes g ON e.finess_etablissement_juridique = g.groupe
GROUP BY 1, 2, 3;

-- Mensuel consultations + hospitalisations (2 lignes sur le même line chart)
-- + colonne annee (varchar) pour que le filtre natif "Année" s'applique à ce graphique.
CREATE OR REPLACE VIEW hive.vues_test.v_test_mensuel AS
SELECT annee_mois,
       CAST(substr(annee_mois,1,4) AS VARCHAR) AS annee,
       CAST(annee_mois || '-01' AS DATE) AS mois_date,
       SUM(nbre_consultation) AS consultations,
       SUM(nbre_hospitalisation) AS hospitalisations
FROM hive.gold.f_mesures
WHERE regexp_like(annee_mois, '^[0-9]{4}-(0[1-9]|1[0-2])$')  -- exclut les sentinelles '-00'
GROUP BY annee_mois;

-- Valeurs distinctes pour les FILTRES NATIFS du dashboard (barre horizontale en haut).
-- Choix lus dans des vues agrégées minuscules -> chargement instantané (pas de
-- requête lourde sur les 25M de décès INSEE qui faisait expirer la requête Trino).
CREATE OR REPLACE VIEW hive.vues_test.v_test_f_annee AS
SELECT annee, SUM(consultations) AS nb FROM hive.vues_test.v_test_annee GROUP BY 1 ORDER BY 1;

CREATE OR REPLACE VIEW hive.vues_test.v_test_f_region AS
SELECT region, SUM(nbre_deces) AS nb FROM hive.vues_test.v_test_carte_deces GROUP BY 1 ORDER BY 2 DESC;

CREATE OR REPLACE VIEW hive.vues_test.v_test_f_sexe AS
SELECT sexe, COUNT(*) AS nb FROM hive.gold.d_patient WHERE sexe IN ('F','M') GROUP BY 1;

-- Consultations par établissement et par année (top établissements), pour le graphique "Top établissements"
CREATE OR REPLACE VIEW hive.vues_test.v_test_etab_annee AS
SELECT f.annee, CAST(CAST(f.annee AS VARCHAR) || '-01-01' AS DATE) AS date_annee,
       f.region, f.sexe,
       e.raison_sociale_site, e.code_commune,
       SUM(f.nbre_consultation) AS nb_consultations,
       SUM(f.nbre_hospitalisation) AS nb_hospitalisations
FROM hive.gold.f_mesures f
JOIN hive.gold.d_etablissement e ON f.id_etablissement = e.id_etablissement
WHERE f.nbre_consultation > 0
GROUP BY 1, 2, 3, 4, 5, 6;

-- Top 20 établissements par (annee, region, sexe) : pré-calculé pour que le LIMIT du chart
-- tombe sur les bons établissements. Le chart "Top établissements" pointe dessus.
CREATE OR REPLACE VIEW hive.vues_test.v_test_etab_top20 AS
SELECT annee, date_annee, region, sexe, raison_sociale_site, code_commune, nb_consultations, nb_hospitalisations
FROM (
  SELECT f.annee,
         CAST(CAST(f.annee AS VARCHAR) || '-01-01' AS DATE) AS date_annee,
         f.region, f.sexe,
         e.raison_sociale_site, e.code_commune,
         SUM(f.nbre_consultation) AS nb_consultations,
         SUM(f.nbre_hospitalisation) AS nb_hospitalisations,
         row_number() OVER (PARTITION BY f.annee, f.region, f.sexe ORDER BY SUM(f.nbre_consultation) DESC) AS rn
  FROM hive.gold.f_mesures f
  JOIN hive.gold.d_etablissement e ON f.id_etablissement = e.id_etablissement
  WHERE f.nbre_consultation > 0
  GROUP BY 1, 2, 3, 4, 5, 6
) WHERE rn <= 20;
