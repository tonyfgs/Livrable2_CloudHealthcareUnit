-- Profilage de la source S1 (dump DATA2023 restauré dans la base PostgreSQL « chu »)
-- Chaque requête produit un chiffre cité dans le livrable 1 (chapitre 2 et catalogue RQ).

-- Volumétries (tableau 2.2)
SELECT 'Consultation' AS t, count(*) FROM "Consultation"
UNION ALL SELECT 'Patient', count(*) FROM "Patient"
UNION ALL SELECT 'Professionnel_de_sante', count(*) FROM "Professionnel_de_sante"
UNION ALL SELECT 'Adher', count(*) FROM "Adher";

-- Période couverte par les consultations
SELECT min("Date"), max("Date") FROM "Consultation";

-- Identifiants à 8 caractères (ADELI ayant perdu leur zéro initial) : 36 172 au total
SELECT count(*) FROM "Professionnel_de_sante" WHERE length("Identifiant") = 8;
-- dont identifiants corrompus en notation scientifique (RQ-14) : 9,00E+34 et 9,00E+42
SELECT "Identifiant" FROM "Professionnel_de_sante" WHERE "Identifiant" ~ 'E\+';

-- RPPS maximal de la table et des consultations (troncature sans effet sur les consultations)
SELECT max("Identifiant") FROM "Professionnel_de_sante" WHERE length("Identifiant") = 11;
SELECT max("Id_prof_sante") FROM "Consultation" WHERE length("Id_prof_sante") = 11;

-- Orphelins consultation -> référentiels (0 attendu)
SELECT count(*) FROM "Consultation" c LEFT JOIN "Patient" p USING ("Id_patient") WHERE p."Id_patient" IS NULL;
SELECT count(*) FROM "Consultation" c LEFT JOIN "Professionnel_de_sante" s ON s."Identifiant" = c."Id_prof_sante" WHERE s."Identifiant" IS NULL;

-- Consultations antérieures à la naissance (RQ-09 : 24 145, 2,35 %)
SELECT count(*) FROM "Consultation" c JOIN "Patient" p USING ("Id_patient")
WHERE c."Date" < to_date(p."Date", 'MM/DD/YYYY');

-- Mutuelle de l'acte absente d'Adher (RQ-12 : 44 155, 4,3 %)
SELECT count(*) FROM "Consultation" c
WHERE NOT EXISTS (SELECT 1 FROM "Adher" a WHERE a."Id_patient" = c."Id_patient" AND a."Id_mut" = c."Id_mut");

-- Répartition par sexe
SELECT "Sexe", count(*) FROM "Patient" GROUP BY 1;
