# -*- coding: utf-8 -*-
"""Source unique de vérité du modèle décisionnel (couche Gold).
Génère : diagrammes PlantUML (MLD/étoiles), dictionnaire LaTeX, MLD textuel, matrice des relations.
Colonnes : (nom, type logique, nullabilité, clé, référence, description / règle de dérivation)
"""
TECH_DIM = [
 ("id_lot", "VARCHAR(36)", "NN", "", "", "Identifiant (UUID) du lot de chargement ayant créé/modifié la ligne ; traçabilité (lignage)."),
 ("dt_chargement", "TIMESTAMP", "NN", "", "", "Horodatage technique d'insertion/mise à jour dans Gold."),
]
SCD2 = [
 ("date_debut_validite", "DATE", "NN", "", "", "SCD type 2 : date d'effet de la version ; 1900-01-01 pour la première version (historique réel à partir du premier chargement)."),
 ("date_fin_validite", "DATE", "NN", "", "", "SCD type 2 : fin d'effet (9999-12-31 pour la version courante)."),
 ("est_version_courante", "BOOLEAN", "NN", "", "", "SCD type 2 : vrai pour la seule version en vigueur."),
]
T = {}
def table(name, kind, label, grain, scd, source, cols, rows=""):
    T[name] = dict(name=name, kind=kind, label=label, grain=grain, scd=scd, source=source, cols=cols, rows=rows)


table("dim_date", "dim", "Dimension Temps (calendrier)",
 "un jour calendaire", "Statique (générée)", "Générée par le job Talend J_DIM_DATE (une fois, puis extension annuelle) ; jours fériés : calendrier légal français.",
 [("date_sk","INTEGER","NN","PK","","Clé « intelligente » AAAAMMJJ ; -1 = date inconnue, -2 = non applicable."),
  ("date_complete","DATE","N","UK","","Date calendaire (NULL pour les membres -1/-2)."),
  ("annee","SMALLINT","N","","","Année civile."),
  ("semestre","SMALLINT","N","","","1 ou 2."),
  ("trimestre","SMALLINT","N","","","1 à 4."),
  ("mois","SMALLINT","N","","","1 à 12."),
  ("libelle_mois","VARCHAR(12)","N","","","Libellé français (janvier…décembre)."),
  ("annee_mois","CHAR(7)","N","","","Clé de regroupement AAAA-MM."),
  ("semaine_iso","SMALLINT","N","","","Numéro de semaine ISO 8601."),
  ("annee_iso","SMALLINT","N","","","Année ISO 8601 de la semaine (peut différer de l'année civile : le 1er janvier 2021 est en semaine 53 de 2020)."),
  ("semaine_epidemio","INTEGER","N","","","Semaine épidémiologique au format AAAASS (annee_iso × 100 + semaine_iso), convention des bulletins de surveillance (ISO 8601, lundi au dimanche)."),
  ("jour_mois","SMALLINT","N","","","1 à 31."),
  ("jour_semaine_iso","SMALLINT","N","","","1 = lundi … 7 = dimanche (ISO 8601)."),
  ("libelle_jour","VARCHAR(10)","N","","","Libellé français du jour."),
  ("est_week_end","BOOLEAN","NN","","","Samedi ou dimanche."),
  ("est_jour_ferie","BOOLEAN","NN","","","Jour férié légal (métropole)."),
  ("est_jour_ouvre","BOOLEAN","NN","","","Lundi à vendredi hors jour férié (activité programmée, délais en jours ouvrés)."),
 ], "≈ 24 100 (1970-2035)")

table("dim_patient", "dim", "Dimension Patient pseudonymisé",
 "un patient (Id_patient source)", "Type 1", "slv_patient ← Patient (Source 1).",
 [("patient_sk","BIGINT","NN","PK","","Clé de substitution ; -1 = patient inconnu."),
  ("patient_pseudo","CHAR(64)","NN","UK","","HMAC-SHA-256(clé KMS, « CHU|PATIENT|<Id_patient> ») en hexadécimal. Ni nom, ni Num_Secu, ni adresse."),
  ("code_departement_residence","VARCHAR(3)","N","","","Généralisation de Code_postal (2 premiers caractères ; 97x/98x ; Corse 20 → 2A/2B via table de correspondance La Poste et COG) ; attribut conforme à dim_geographie.code_departement (rôle résidence)."),
  ("code_region_residence","CHAR(2)","N","","","Région COG déduite du département."),
 ] + TECH_DIM, "100 000")

table("dim_sexe", "dim", "Dimension Sexe (conforme)",
 "une modalité de sexe", "Statique", "Transcodage : Patient.Sexe (male/female) et décès.sexe (1/2) → codes INSEE.",
 [("sexe_sk","SMALLINT","NN","PK","","Clé ; -1 = inconnu."),
  ("sexe_code","CHAR(1)","NN","UK","","Code INSEE : 1 = masculin, 2 = féminin, 9 = inconnu."),
  ("sexe_libelle","VARCHAR(20)","NN","","","Masculin, Féminin, Inconnu."),
 ], "3")

table("dim_tranche_age", "dim", "Dimension Tranche d'âge (conforme)",
 "une tranche quinquennale d'âge révolu", "Statique", "Générée (bornes quinquennales de la statistique publique).",
 [("tranche_age_sk","SMALLINT","NN","PK","","Clé ; -1 = âge inconnu ou incohérent."),
  ("age_min","SMALLINT","N","","","Borne inférieure incluse."),
  ("age_max","SMALLINT","N","","","Borne supérieure incluse ; NULL pour « 95 ans et + »."),
  ("libelle_tranche","VARCHAR(16)","NN","UK","","Ex. « 00-04 ans »."),
  ("classe_age","VARCHAR(20)","NN","","","0-17, 18-64, 65 ans et +."),
  ("ordre_tri","SMALLINT","NN","","","Ordre d'affichage."),
 ], "21")

table("dim_geographie", "dim", "Dimension Géographie (COG INSEE)",
 "une commune du COG courant, ou un pays pour l'étranger", "Type 1 (re-projection sur le COG courant)", "Référentiel COG INSEE + table de passage des communes (référentiel complémentaire, H-07).",
 [("geographie_sk","INTEGER","NN","PK","","Clé ; -1 = lieu inconnu, -3 = code absent du COG après reprojection."),
  ("code_commune","CHAR(5)","NN","UK","","Code commune COG, ou 99CCC (pays étranger)."),
  ("libelle_commune","VARCHAR(100)","NN","","","Libellé officiel."),
  ("code_departement","VARCHAR(3)","N","","","Code département (2A/2B, 971…)."),
  ("libelle_departement","VARCHAR(60)","N","","","Libellé département."),
  ("code_region","CHAR(2)","NN","","","Code région COG ; « 99 » = étranger."),
  ("libelle_region","VARCHAR(60)","NN","","","Libellé officiel de la région."),
  ("est_etranger","BOOLEAN","NN","","","Vrai si code 99CCC."),
  ("millesime_cog","SMALLINT","NN","","","Millésime COG de référence."),
 ] + TECH_DIM, "≈ 35 000")

table("dim_etablissement", "dim", "Dimension Établissement / organisation",
 "une version d'une organisation de l'annuaire santé (FINESS géographique ou cabinet SIRET)", "Type 2 (raison sociale, commune, entité juridique, catégorie) ; type 1 (contacts exclus)", "slv_etablissement ← etablissement_sante.csv (Source 2) ; catégorie enrichie par les fichiers IQSS (Source 3).",
 [("etablissement_sk","INTEGER","NN","PK","","Clé (une par version) ; -1 = inconnu, -3 = absent de l'annuaire, -4 = non déterminable (RG-C04)."),
  ("identifiant_organisation","VARCHAR(20)","NN","","","Clé naturelle et clé durable, stable entre versions (« F » + FINESS, ou « C » + SIRET) : clé des regroupements (IND-01)."),
  ("type_organisation","CHAR(1)","NN","","","F = établissement FINESS ; C = cabinet/structure SIRET."),
  ("finess_geographique","CHAR(9)","N","","","identifiant_organisation sans le préfixe F (clé de jointure e-Satis finess_geo)."),
  ("finess_juridique","CHAR(9)","N","","","finess_etablissement_juridique (rattachement e-Satis finess)."),
  ("siret","CHAR(14)","N","","","siret_site."),
  ("raison_sociale","VARCHAR(150)","NN","","","raison_sociale_site (type 2)."),
  ("categorie_libelle","VARCHAR(100)","N","","","Catégorie d'établissement (IQSS : type / cat / lib_cat) ; NULL si absente."),
  ("code_postal","CHAR(5)","N","","","code_postal."),
  ("code_commune","CHAR(5)","N","","","code_commune (COG) ; 30 % vides dans l'échantillon → déduit du code postal si univoque."),
  ("libelle_commune","VARCHAR(100)","N","","","Attribut conforme issu de dim_geographie."),
  ("code_departement","VARCHAR(3)","N","","","Attribut conforme issu de dim_geographie."),
  ("code_region","CHAR(2)","N","","","Attribut conforme issu de dim_geographie."),
  ("libelle_region","VARCHAR(60)","N","","","Attribut conforme issu de dim_geographie."),
  ("est_groupe_chu","BOOLEAN","NN","","","Vrai si l'organisation appartient au périmètre du groupe CHU (liste fournie au cadrage, H-09)."),
 ] + SCD2 + TECH_DIM, "≈ 416 700 organisations")

table("dim_diagnostic", "dim", "Dimension Diagnostic (CIM-10)",
 "un code diagnostic", "Type 1", "slv_diagnostic ← union dédoublonnée sur le code : Diagnostic (Source 1, libellé de référence), Consultation.Code_diag et Hospitalisations.csv Code_diagnostic (code CIM-10 valide absent du référentiel : libellé de la source ; code hors CIM-10 : -3 côté fait) ; hiérarchie CIM-10 (chapitres) de l'ATIH. Le fichier des établissements (Source 2) ne contient aucun diagnostic.",
 [("diagnostic_sk","INTEGER","NN","PK","","Clé ; -1 = non renseigné, -3 = code hors référentiel."),
  ("code_diag","VARCHAR(6)","NN","UK","","Code_diag (3 à 6 caractères, extensions CIM-10 FR)."),
  ("libelle_diagnostic","VARCHAR(255)","NN","","","Diagnostic."),
  ("code_categorie","CHAR(3)","NN","","","3 premiers caractères (ex. S92)."),
  ("code_chapitre","VARCHAR(5)","N","","","Chapitre CIM-10 (I à XXII) par intervalle de catégories."),
  ("libelle_chapitre","VARCHAR(255)","N","","","Libellé du chapitre."),
 ] + TECH_DIM, "15 490")

table("dim_professionnel", "dim", "Dimension Professionnel de santé",
 "une version d'un professionnel", "Type 2 (profession, spécialité, mode d'exercice)", "slv_professionnel ← Professionnel_de_sante, Specialites (Source 1) + activite_professionnel_sante.csv (Source 2).",
 [("professionnel_sk","INTEGER","NN","PK","","Clé (une par version) ; -1 = inconnu."),
  ("professionnel_pseudo","CHAR(64)","NN","","","HMAC-SHA-256 de l'identifiant normalisé (ADELI complété à 9 chiffres, RPPS 11 chiffres) ; clé durable, stable entre versions."),
  ("type_identifiant","VARCHAR(5)","NN","","","RPPS ou ADELI."),
  ("profession","VARCHAR(60)","N","","","Profession (Médecin, Infirmier…)."),
  ("categorie_professionnelle","VARCHAR(20)","N","","","Civil, Militaire, Étudiant."),
  ("specialite_code","VARCHAR(10)","N","","","Code_specialite."),
  ("specialite_libelle","VARCHAR(100)","N","","","Specialites.Specialite."),
  ("fonction","VARCHAR(60)","N","","","Specialites.Fonction."),
  ("mode_exercice","VARCHAR(20)","N","","","Libéral / Salarié (activité principale, après correction d'encodage)."),
 ] + SCD2 + TECH_DIM, "≈ 1,05·10^6 (fichier plafonné à la limite d'Excel, R-02)")

table("dim_mutuelle", "dim", "Dimension Mutuelle (organisme complémentaire)",
 "un organisme complémentaire", "Type 1", "slv_mutuelle ← Mutuelle (Source 1).",
 [("mutuelle_sk","SMALLINT","NN","PK","","Clé ; -1 = inconnue."),
  ("id_mutuelle","INTEGER","NN","UK","","Id_Mut source."),
  ("nom_mutuelle","VARCHAR(150)","NN","","","Nom."),
 ] + TECH_DIM, "254")

table("dim_acte", "dim", "Dimension Acte de consultation (junk)",
 "une combinaison motif × plage horaire", "Statique (alimentée par les valeurs observées)", "Consultation.Motif, Heure_debut, Heure_fin (Source 1).",
 [("acte_sk","SMALLINT","NN","PK","","Clé ; -1 = inconnu."),
  ("motif","VARCHAR(60)","NN","","","Motif source (ex. Consultation, Soins dentaires, Radiologie generale)."),
  ("famille_motif","VARCHAR(30)","NN","","","Regroupement : Consultation, Dentaire, Imagerie, Biologie, Soins (table de transcodage H-10)."),
  ("plage_horaire","VARCHAR(12)","NN","","","MATIN (08-12), APRES_MIDI (13-18), SOIR (20-23), NUIT (20-02)."),
  ("heure_debut","TIME","NN","","","Heure_debut de la plage (HH:mm:ss ; texte dans Hive, qui n'a pas de type heure)."),
  ("heure_fin","TIME","NN","","","Heure_fin de la plage."),
 ], "≈ 100")

table("dim_campagne", "dim", "Dimension Campagne de mesure (HAS)",
 "une campagne d'un dispositif de mesure (millésime de fichier)", "Statique (une ligne par fichier/millésime)", "Métadonnées des fichiers de Source 3 (nom de fichier, lexique).",
 [("campagne_sk","SMALLINT","NN","PK","","Clé ; -1 = inconnue (membre défensif, aucun cas prévu : RG-S04, RQ-02)."),
  ("dispositif","VARCHAR(40)","NN","","","e-Satis 48h MCO, e-Satis MCO CA, IQSS DPA SSR, IQSS RCP MCO…"),
  ("annee_publication","SMALLINT","NN","","","Année de recueil / publication (ex. 2020)."),
  ("annee_donnees","SMALLINT","N","","","Année des données (ex. recueil 2016 → données 2015) ; attribut conforme à dim_date.annee."),
  ("fichier_source","VARCHAR(120)","NN","UK","","Nom du fichier source (lignage)."),
 ], "≈ 25")

table("dim_indicateur", "dim", "Dimension Indicateur (canonique)",
 "un indicateur canonique, indépendant des noms de colonnes des millésimes", "Type 1", "Table de correspondance versionnée (fichier de référence versionné) : (millésime, colonne source) → indicateur canonique.",
 [("indicateur_sk","SMALLINT","NN","PK","","Clé ; -1 = inconnu (membre défensif, aucun cas prévu : RQ-02)."),
  ("code_indicateur","VARCHAR(40)","NN","UK","","Ex. ESATIS48H_SCORE_GLOBAL, ESATISCA_SCORE_GLOBAL, ESATIS48H_SCORE_REPAS."),
  ("libelle_indicateur","VARCHAR(200)","NN","","","Libellé issu des lexiques HAS."),
  ("famille","VARCHAR(30)","NN","","","SATISFACTION, QUALITE_PRISE_EN_CHARGE, SECURITE…"),
  ("secteur","VARCHAR(10)","NN","","","MCO, MCO-CA, SSR, HAD."),
  ("unite","VARCHAR(20)","NN","","","Score /100, %, ratio O/A."),
  ("sens_favorable","CHAR(1)","NN","","","« + » si une valeur élevée est favorable, « - » sinon."),
 ], "≈ 60")

table("fait_consultation", "fait", "Fait Consultation (transactionnel)",
 "une consultation (Num_consultation)", "aucune", "slv_consultation ← Consultation (Source 1), enrichie par Patient, activite_professionnel_sante.",
 [("consultation_id","BIGINT","NN","PK","","Dimension dégénérée : Num_consultation ; clé de dédoublonnage (RQ-08)."),
  ("date_consultation_sk","INTEGER","NN","FK","dim_date","Consultation.Date."),
  ("patient_sk","BIGINT","NN","FK","dim_patient","Via Id_patient → patient_pseudo."),
  ("professionnel_sk","INTEGER","NN","FK","dim_professionnel","Id_prof_sante ; version en vigueur à la date de l'acte (la première version débute au 1900-01-01)."),
  ("etablissement_sk","INTEGER","NN","FK","dim_etablissement","Organisation d'exercice du professionnel, version en vigueur à la date de l'acte (RG-C04) ; -4 si non déterminable."),
  ("geographie_etablissement_sk","INTEGER","NN","FK","dim_geographie","Rôle « lieu de l'établissement » : code_commune de la version d'établissement retenue → dim_geographie ; -1 si établissement -1/-3/-4 ou commune inconnue."),
  ("diagnostic_sk","INTEGER","NN","FK","dim_diagnostic","Code_diag."),
  ("mutuelle_sk","SMALLINT","NN","FK","dim_mutuelle","Id_mut de la consultation (organisme complémentaire de l'acte)."),
  ("acte_sk","SMALLINT","NN","FK","dim_acte","Motif × plage horaire."),
  ("sexe_sk","SMALLINT","NN","FK","dim_sexe","Patient.Sexe transcodé."),
  ("tranche_age_sk","SMALLINT","NN","FK","dim_tranche_age","Âge révolu à la date de l'acte ; -1 si date de naissance postérieure (RG-C06)."),
  ("age_patient","SMALLINT","N","","","Âge révolu à la date de l'acte, recalculé depuis la date de naissance (Patient.Age, instantané de mi-2021, est ignoré)."),
  ("duree_plage_minutes","SMALLINT","N","","","Heure_fin - Heure_debut (+24 h si passage de minuit)."),
  ("nb_consultation","SMALLINT","NN","","","Mesure additive = 1."),
  ("id_lot","VARCHAR(36)","NN","","","Lot de chargement."),
  ("dt_chargement","TIMESTAMP","NN","","","Horodatage technique."),
 ], "1 027 157 (06/2015-03/2023)")

table("fait_hospitalisation", "fait", "Fait Hospitalisation (séjour)",
 "un séjour hospitalier (Num_Hospitalisation)", "Rechargement complet à chaque livraison", "slv_hospitalisation ← Hospitalisations.csv (Source 1, extrait fourni).",
 [("sejour_id","BIGINT","NN","PK","","Dimension dégénérée : Num_Hospitalisation ; clé de dédoublonnage."),
  ("date_entree_sk","INTEGER","NN","FK","dim_date","Date_Entree (JJ/MM/AAAA)."),
  ("date_sortie_sk","INTEGER","NN","FK","dim_date","Date_Entree + Jour_Hospitalisation (H-04)."),
  ("patient_sk","BIGINT","NN","FK","dim_patient","Id_patient."),
  ("etablissement_sk","INTEGER","NN","FK","dim_etablissement","identifiant_organisation ; version en vigueur à l'entrée ; absent de l'annuaire → -3 (RG-H04)."),
  ("geographie_etablissement_sk","INTEGER","NN","FK","dim_geographie","Rôle « lieu de l'établissement » : code_commune de la version d'établissement retenue → dim_geographie ; -1 si établissement -1/-3/-4 ou commune inconnue."),
  ("diagnostic_sk","INTEGER","NN","FK","dim_diagnostic","Code_diagnostic."),
  ("sexe_sk","SMALLINT","NN","FK","dim_sexe","Sexe du patient."),
  ("tranche_age_sk","SMALLINT","NN","FK","dim_tranche_age","Tranche d'âge à l'entrée."),
  ("age_entree","SMALLINT","N","","","Âge révolu à l'entrée (non additif)."),
  ("duree_sejour_jours","SMALLINT","NN","","","Jour_Hospitalisation (1 à 30 observé)."),
  ("nb_sejour","SMALLINT","NN","","","Mesure additive = 1."),
  ("id_lot","VARCHAR(36)","NN","","","Lot de chargement."),
  ("dt_chargement","TIMESTAMP","NN","","","Horodatage technique."),
 ], "2 479 (06/2015-06/2021)")

table("fait_deces", "fait", "Fait Décès (open data INSEE)",
 "un acte de décès (après dédoublonnage)", "Ajout mensuel (une partition par fichier INSEE), dédoublonné sur deces_id", "slv_deces ← deces.csv (Source 4) ; nom et prénoms écartés dès l'atterrissage ; lieu et date exacte de naissance supprimés en Silver.",
 [("deces_id","CHAR(64)","NN","PK","","SHA-256(numero_acte_deces | code_lieu_deces | date_deces) : clé de dédoublonnage."),
  ("date_deces_sk","INTEGER","NN","FK","dim_date","date_deces ; -1 si partielle ou invalide."),
  ("geographie_deces_sk","INTEGER","NN","FK","dim_geographie","code_lieu_deces → commune COG courante (ou pays 99CCC)."),
  ("sexe_sk","SMALLINT","NN","FK","dim_sexe","sexe (1/2)."),
  ("tranche_age_sk","SMALLINT","NN","FK","dim_tranche_age","Tranche de l'âge au décès."),
  ("annee_deces","SMALLINT","N","","","Année de décès, conservée même si la date est partielle."),
  ("age_deces","SMALLINT","N","","","Âge révolu au décès ; NULL si date de naissance inconnue."),
  ("nb_deces","SMALLINT","NN","","","Mesure additive = 1."),
  ("id_lot","VARCHAR(36)","NN","","","Lot de chargement."),
  ("dt_chargement","TIMESTAMP","NN","","","Horodatage technique."),
 ], "≈ 25,1·10^6 (décès depuis 1970 ; échantillon : 1983)")

table("fait_indicateur_qualite", "fait", "Fait Indicateur qualité & satisfaction (instantané périodique)",
 "un résultat d'indicateur pour un établissement et une campagne ; unicité aussi testée sur (identifiant_organisation, campagne, indicateur)", "Ajout par campagne (une partition par chargement) ; republication = réécriture de la partition", "slv_indicateur_qualite ← fichiers HAS e-Satis / IQSS 2014-2020 (Source 3), normalisés au format long.",
 [("etablissement_sk","INTEGER","NN","PK,FK","dim_etablissement","finess_geo (à défaut finess) → version en vigueur au 1er janvier de annee_donnees (date de référence fixe) ; -3 si non résolu (RG-S02)."),
  ("campagne_sk","SMALLINT","NN","PK,FK","dim_campagne","Millésime du fichier."),
  ("indicateur_sk","SMALLINT","NN","PK,FK","dim_indicateur","Indicateur canonique."),
  ("date_reference_sk","INTEGER","NN","FK","dim_date","1er janvier de dim_campagne.annee_donnees (AAAA0101), date de référence de la version d'établissement ; -1 si annee_donnees inconnue."),
  ("geographie_etablissement_sk","INTEGER","NN","FK","dim_geographie","Rôle « lieu de l'établissement » : code_commune de la version d'établissement retenue → dim_geographie ; -1 si établissement -1/-3/-4 ou commune inconnue."),
  ("valeur","DECIMAL(9,4)","N","","","Score / taux (virgule ou point décimal normalisés) ; NULL si non diffusé (DI, NR, NV)."),
  ("effectif_base","INTEGER","N","","","Effectif de base (nb_rep_*, *_den_etbt) : répondants ou dossiers selon l'indicateur ; pondère les moyennes."),
  ("ic_bas","DECIMAL(9,4)","N","","","Borne basse de l'intervalle de confiance (*_icbas_etbt)."),
  ("ic_haut","DECIMAL(9,4)","N","","","Borne haute (*_ichaut_etbt)."),
  ("classe","VARCHAR(4)","N","","","A, B, C, D, DI, NR, NV (classement HAS)."),
  ("evolution","VARCHAR(30)","N","","","Évolution de classe déclarée par la HAS."),
  ("est_participation_obligatoire","BOOLEAN","N","","","participation = « 1- Obligatoire »."),
  ("est_diffusable","BOOLEAN","NN","","","Faux si classe DI/NR/NV ou valeur absente."),
  ("id_lot","VARCHAR(36)","NN","","","Lot de chargement."),
  ("dt_chargement","TIMESTAMP","NN","","","Horodatage technique."),
 ], "≈ 10^5 (format long)")

FACTS = ["fait_consultation","fait_hospitalisation","fait_deces","fait_indicateur_qualite"]
DIMS = [k for k,v in T.items() if v["kind"]=="dim"]

# Job Talend (un par dimension et par fait) et fréquence : préfixés à l'alimentation dans le dictionnaire.
FREQ = {"dim_date": "mensuelle (contrôle de l'horizon ; extension annuelle)",
        "dim_sexe": "mensuelle (liste fixe, réécrite à l'identique)",
        "dim_tranche_age": "mensuelle (liste fixe, réécrite à l'identique)",
        "dim_geographie": "mensuelle (nouveau millésime COG une fois par an)",
        "dim_campagne": "mensuelle (nouvelle campagne à la publication HAS)",
        "dim_indicateur": "mensuelle (nouvelle campagne à la publication HAS)",
        "fait_indicateur_qualite": "mensuelle (nouvelle partition à la publication HAS)",
        "fait_deces": "mensuelle (fichier mensuel INSEE)"}
for _n, _v in T.items():
    _v["job"] = "J_" + _n.upper().replace("FAIT_", "FAIT_")
    _v["freq"] = FREQ.get(_n, "mensuelle")
    _v["source"] = f"job Talend {_v['job']}, fréquence {_v['freq']} ; " + _v["source"]
