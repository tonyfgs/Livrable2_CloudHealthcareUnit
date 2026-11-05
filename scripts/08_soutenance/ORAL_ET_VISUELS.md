# Soutenance : visuels (pourquoi, comment, où), déroulé oral et réponses aux questions

Tous les chiffres viennent du lakehouse (Trino, couches silver et gold) ou des contrôles du 30/10/2026 sur les données complètes. Les visuels se régénèrent avec `python3 build_visuels.py` (les assertions de réconciliation bloquent la génération si un total ne correspond pas aux sources : 1 027 157 consultations, 2 479 hospitalisations, par année égal au dump S1).

## 1. Inventaire des visuels

Légende des colonnes : **Pourquoi** = la question du CHU et le choix du type de graphique ; **Comment** = requête et mesure ; **Où** = fichier, diapositive, onglet et étape de l'histoire dans Superset (liens dans `superset/PERMALIENS.md`).

| N° | Fichier | Pourquoi (question et graphique) | Comment (données) | Où (diapo ; onglet Superset ; étape) |
|---|---|---|---|---|
| V01 | V01_consultations_mensuelles.png | Besoin 1 : comment évolue l'activité ? Une **courbe** montre une série temporelle continue, le pic et les ruptures. | `sql/d01` : `silver.f_consultation_detail` joint à `d_temps`, nombre de lignes par `annee_mois`. Somme = 1 027 157 (égale à S1). | Diapo 5 ; onglet 2 Activité ; étape 2 |
| V02 | V02_top20_etablissements_2019.png | Besoin 1 : quels établissements consultent le plus ? Un **classement en barres horizontales** lit les libellés longs et compare des quantités. Réponse : aucun ne domine (146 à 371 consultations). | Requête q01 (archive de la campagne sur données complètes, 30/10/2026) : `f_mesures` joint à `d_etablissement`, année 2019, top 20. | Diapo 6 ; onglet 2 Activité ; étape 2 |
| V03 | V03_consultations_chapitre_cim10.png | Besoin 2 : quels motifs de consultation ? Les 20 chapitres CIM-10 en **barres empilées par année** : on lit le total et la stabilité par année. | `sql/d04` : lettre CIM-10 du diagnostic regroupée en chapitres. Total 2019 à 2021 = 396 158 (= 87 497 + 162 778 + 145 883). | Diapo 5 ; onglet 2 Activité ; étape 2 |
| V04 | V04_taux_hospitalisation.png | Besoin 3 : quel est le taux d'hospitalisation ? Des **colonnes** annuelles comparent des taux, la couleur signale les années partielles. | `sql/d02` : hospitalisations ÷ consultations **sur la même fenêtre de dates** (21/06/2015 au 19/06/2021). | Diapo 7 ; onglet 3 Hospitalisations ; étape 4 |
| V05 | V05_hospitalisations_chapitre_cim10.png | Besoin 4 : pour quels diagnostics hospitalise-t-on ? **Barres** triées avec la durée moyenne en étiquette. | `sql/d05` : `f_hospitalisation` par chapitre, durée moyenne pondérée. Total 2 479. | Diapo 7 ; onglet 3 Hospitalisations ; étape 4 |
| V06 | V06_hospitalisation_sexe_age.png | Besoin 5 : qui est hospitalisé ? Un **anneau** (part de chaque sexe), des **colonnes groupées** (volumes par âge) et des **courbes** (taux par âge). | `sql/d03` : `f_hospitalisation` par sexe et tranche d'âge (âge à la date de l'événement). F = 1 425, H = 1 054. Réserve affichée : 62 hospitalisations datées avant la naissance du patient, classées 0-14 ans. | Diapo 7 ; onglet 3 Hospitalisations ; étape 4 |
| V07 | V07_activite_par_professionnel_2020.png | Besoin 6 : l'activité est-elle concentrée sur quelques professionnels ? Un **histogramme de distribution** répond mieux qu'un top 10 (écarts de 7 à 9 consultations seulement). | `sql/d07c` : nombre de consultations par professionnel en 2020, puis nombre de professionnels par valeur. 116 705 médecins, 162 778 consultations. | Diapo 6 ; onglet 2 Activité ; étape 2 |
| V08 | V08_carte_deces_2019.png | Besoin 7 : où meurt-on ? Une **carte choroplèthe** (information géographique) et des barres pour les territoires non cartographiés. | Requête q07 (archive, données complètes) : `f_deces_agg` par région de lieu de décès, 2019, 620 606 décès. | Diapo 8 ; onglet 4 Mortalité et qualité ; étape 6 |
| V09 | V09_carte_satisfaction_2020.png | Besoin 8 : la satisfaction diffère-t-elle selon les régions ? **Carte** pour la géographie et **barres** pour chiffrer les écarts fins (72,6 à 75,0). | `sql/d08` : score ESATIS pondéré par les répondants, par région, 2019 et 2020. | Diapo 8 ; onglet 4 Mortalité et qualité ; étape 6 |
| V10 | V10_schema_en_etoile.png | Montrer le modèle cible repris dans Superset : faits au centre, 6 dimensions autour (vue simplifiée ; les jointures sont écrites dans le SQL des datasets virtuels `etoile_*`). | DDL `01_schema_gold_hive.sql` du Livrable 2. | Diapo 3 ; onglet 1 Contexte (tableau des volumes) |
| V11 | V11_fiabilite_des_donnees.png | Quelle confiance accorder aux chiffres ? **Barres horizontales** colorées par seuil (vert au-dessus de 95 %, orange de 75 à 95 %, rouge sous 75 %). | Contrôles qualité du 30/10/2026 : rattachement des clés et complétude. | Diapo 9 ; onglet 4 Mortalité et qualité ; étape 6 |
| V12 | V12_benchmark_partitionnement.png | Le partitionnement apporte-t-il quelque chose ? Deux **panneaux** : volume lu et temps de réponse. Réponse honnête : moins de volume lu, peu de gain en temps. | `bench_synthese.csv` du Livrable 2 (médiane de 5 répétitions, DuckDB). | Diapo 10 ; onglet 6 Annexe technique ; étape 9 |
| V13 | V13_architecture_lakehouse.png | Situer le travail : du fichier brut au tableau de bord. | `docker-compose.yml` (17 services). | Diapo 2 |
| V14 | V14_top10_diagnostics.png | Besoin 2 : quels diagnostics dominent ? Réponse : aucun (le premier pèse 0,018 % des consultations). | `sql/d06` : top 10 par **code CIM-10** (départage par code). | Diapo 6 ; onglet 2 Activité ; étape 2 |
| Tableau de bord HTML | dashboard_chu.html, dashboard_bureau.png, dashboard_mobile.png | Même histoire en une page lisible sur téléphone (colonne unique en dessous de 760 px). | `build_dashboard.py` : mêmes CSV que les visuels, mêmes assertions. | Plan B hors connexion (avec les captures `visuels/superset/`) |

Règles appliquées à chaque visuel : un titre qui énonce la conclusion, la source et la limite en pied de page, un axe qui démarre à zéro (sauf V09 droite, signalé sur le visuel), une couleur orange réservée aux avertissements ou aux années partielles, aucun nom de professionnel de santé.

## 2. Déroulé oral (20 minutes d'exposé, puis 10 minutes de questions, 4 intervenants)

Le plan suit les cinq aspects attendus : méthodologie, modélisation et implémentation, restitution analytique et storytelling, démonstration technique en direct, recommandations.

| Diapo | Durée | Contenu | Visuel | Intervenant conseillé |
|---|---|---|---|---|
| 1 | 0:45 | Contexte : le CHU veut piloter activité, hospitalisations, décès et satisfaction ; les 8 besoins des décideurs. | Titre | Tous |
| 2 | 2:00 | Méthodologie et architecture : lakehouse en trois couches (raw, silver, gold), choix des outils (MinIO, Spark, Airflow, Hive Metastore, Trino, Superset) et pourquoi. **MinIO (fichiers) + Hive Metastore (catalogue) + Trino (SQL) jouent le rôle de HDFS + Hive d'une pile Hadoop**, sans machine virtuelle Cloudera. Conformité RGPD : aucun nom de patient ni de professionnel affiché, les datasets étoile ne sélectionnent aucune colonne nominative. | V13 | Technique |
| 3 | 2:15 | Modélisation et implémentation : schéma en étoile de Kimball (6 dimensions, faits de détail et fait consolidé), règles ETL (sexe normalisé, âge révolu à la date de l'événement, membre inconnu conservé, jamais supprimé), stratégie d'optimisation (partitionnement par année, sexe, tranche d'âge), tests qualité à chaque étape. | V10 | Technique |
| 4 | 1:30 | Connexion : console MinIO (dossiers `raw`, `silver`, `gold` du bucket `lake`), puis Superset > SQL Lab sur la base « Trino CHU » : la liste des schémas montre `raw`, `silver`, `gold` ; liste des datasets (18 physiques, 12 virtuels) ; schéma en étoile décrit par les datasets `etoile_*`. | Captures `sqllab_couches_du_lac.png`, `liste_des_datasets.png` | Technique |
| 5 | 2:00 | Activité : 1 027 157 consultations, mensuelles très irrégulières, ostéo-articulaire 17 % et traumatismes 15 %. | V01, V03 | Analyse |
| 6 | 2:00 | Établissements, professionnels, diagnostics : pas de concentration (20 premiers établissements de 146 à 371 consultations), 68 % des médecins à 1 consultation. | V02, V07, V14 | Analyse |
| 7 | 2:00 | Hospitalisations : 0,2 à 0,4 pour 100 consultations de 2016 à 2021 (0,76 en 2015, début d'observation), 57 % de femmes, 15 jours de séjour moyen. | V04, V06, V05 | Analyse |
| 8 | 1:30 | Territoires : 24 % des décès en Île-de-France et Auvergne-Rhône-Alpes (valeurs brutes), satisfaction entre 72,6 et 75,0. | V08, V09 | Analyse |
| 9 | 1:30 | Fiabilité : 63,87 % des consultations rattachées à un établissement, limites dites spontanément (section 4). | V11 | Qualité |
| 10 | 3:00 | **Démonstration technique en direct** (voir l'encadré ci-dessous) : navigation dans le tableau de bord, une requête Trino exécutée en direct, performances. | Écran partagé, V12 en secours | Technique |
| 11 | 1:30 | Recommandations et perspectives (section 3) et conclusion. | Texte | Tous |

Total : 20:00. Les questions (10 minutes) s'appuient sur la section 5.

Phrase d'ouverture de chaque visuel : « Ce que l'on voit, pourquoi c'est important pour le CHU, ce que l'on propose. »

### Démonstration technique en direct (diapo 10, 3 minutes)

1. **Navigation (1 min)** : dans Superset, ouvrir `chu-histoire`, suivre les liens permanents de l'étape 2 (Activité), 3 (zoom 2019, filtre Année), 6 (Mortalité et qualité) ; montrer le même parcours sur téléphone avec `chu-histoire-mobile` (largeur 390 px) ou les captures `telephone_*`.
2. **Requête en direct (1 min)** : dans un terminal, lancer sur le lakehouse la requête q03 (taux d'hospitalisation par année) :
   `docker exec -i chu-trino trino --catalog hive --schema gold < 04_benchmark/queries/q03_taux_hosp_periode.sql`
   Résultat attendu en environ une seconde : 7 lignes (2015 à 2021), taux de 0,75 ; 0,21 ; 0,34 ; 0,25 ; 0,43 ; 0,25 ; 0,21 pour 100 consultations. Les tableaux de bord affichent 0,76 pour 2015 et 0,22 pour 2021 : la requête q03 compte à la maille du mois, les visuels à la maille du jour (écart d'arrondi, voir question 4 de la section 5).
3. **Performances (1 min)** : onglet 6 Annexe technique (ou V12) : le partitionnement lit moins de données sur 8 requêtes sur 10, mais le temps de réponse ne baisse que sur 3 requêtes (avec le pruning façon metastore) à cette échelle.
Plan de secours si le réseau ou Docker échoue : les captures `visuels/superset/` (bureau, téléphone, étapes), les visuels PNG et `dashboard_chu.html`, qui ne demandent aucune connexion.

### Histoire en 9 étapes dans Superset (liens dans `superset/PERMALIENS.md`)

Chaque lien permanent restaure l'onglet et l'état des filtres. Version bureau : tableau de bord `chu-histoire` ; version téléphone : `chu-histoire-mobile`. Les clés des liens changent à chaque provisionnement : relire `PERMALIENS.md` avant de présenter.

| Étape | Onglet et filtre | Phrase à dire | Diapo |
|---|---|---|---|
| 1 | Contexte | « Le CHU veut piloter avec ses données : 1 027 157 consultations, 100 000 patients, 2 479 hospitalisations, trois couches lisibles. » | 1 à 4 |
| 2 | Activité | « Activité irrégulière, sans tendance de fond : de 4 682 à 18 759 consultations par mois. » | 5 |
| 3 | Activité, Année = 2019 | « Aucun établissement ne domine : de 146 à 371 consultations pour les 20 premiers. » | 6 |
| 4 | Hospitalisations | « Hospitalisations rares, séjour moyen de 15,4 jours. » | 7 |
| 5 | Hospitalisations, Sexe = F | « 57 % de femmes ; le taux pour 1 000 consultations reste plat selon l'âge. » | 7 |
| 6 | Mortalité et qualité | « Les décès suivent la taille des régions, la satisfaction est homogène, 63,87 % de rattachement. » | 8 et 9 |
| 7 | Mortalité et qualité, Région = Île-de-France | « Zoom sur la région où les décès sont les plus nombreux (76 988) : valeurs brutes, pas de taux. » | 8 |
| 8 | Recommandations | « Cinq recommandations, chacune appuyée par un chiffre. » | 11 |
| 9 | Annexe technique | « Le partitionnement lit moins de données, il ne gagne du temps que sur 3 requêtes. » | 10 |

## 3. Recommandations et perspectives à présenter (chacune appuyée sur un chiffre vérifié)

1. **Fiabiliser le rattachement consultation vers établissement** : seulement 63,87 % des consultations sont rattachées via le professionnel puis son activité ; compléter la source (identifiant d'établissement dans la consultation).
2. **Ne pas piloter par professionnel** à partir de ces données : 68 % des médecins n'ont qu'une consultation en 2020, le top 10 se joue à 7 à 9 consultations.
3. **Rapporter les décès à la population régionale** avant toute comparaison : la carte actuelle suit la taille des régions.
4. **Compléter les scores de satisfaction** : 25 à 27 % des lignes source sans score, et un score au niveau de l'établissement juridique, pas du site.
5. **Garder le partitionnement par année** (volume lu divisé jusqu'à 13,7 sur les décès) mais ne pas en attendre un gain de temps à cette échelle.

Perspectives d'évolution (à dire en conclusion) :

* **Nouvelles sources** : population régionale (taux de mortalité), identifiant d'établissement dans la consultation, référentiel FINESS complet.
* **Modèles prédictifs** : prévision des consultations mensuelles et du risque d'hospitalisation, à envisager seulement une fois la fiabilité des données améliorée (les distributions actuelles sont plates).
* **Mise en conformité continue** : contrôles qualité rejoués à chaque exécution du DAG quotidien, suppression des identités nominatives du périmètre analytique, journalisation des accès (RGPD et hébergement de données de santé).

## 4. Limites à dire spontanément

* Début d'observation partiel (consultations à partir du 20 juin 2015, hospitalisations du 21 juin 2015 au 19 juin 2021) et fin au 31 mars 2023.
* D'août à décembre 2022, les volumes mensuels reproduisent ceux de 2021 : artefact des données sources, ne pas l'interpréter.
* Données de formation : les distributions plates (professionnels, âges, diagnostics) ne portent pas de signal clinique réel.
* Données incohérentes dans les sources : 62 hospitalisations (2,5 %) et 24 145 consultations (2,4 %) sont datées avant la naissance du patient ; elles sont comptées dans la tranche 0-14 ans (V06). Le champ Age du patient ne correspond à la date de naissance (écart de 2 ans au plus) que dans 46 % des cas, c'est pourquoi l'âge est recalculé à la date de l'événement.
* Le fichier des décès contient 0,70 % de doublons (174 794 lignes) non retirés : les décès par région sont des valeurs brutes.
* Un écart de 2 décès sur 620 000 entre deux calculs de 2019 (620 606 avec Trino, 620 608 avec le PoC DuckDB) : écart non expliqué à ce jour, sans effet sur les conclusions.

## 5. Questions probables et réponses

| Question | Réponse |
|---|---|
| Pourquoi Superset et pas Power BI ? | Superset est déjà dans la stack Docker (service `superset`), lit Trino directement (aucun export ni passerelle), est libre et reproductible par script ; tout le tableau de bord se reconstruit avec `provision.py` et se vérifie avec `verify.py`. |
| Où sont HDFS et Hive dans votre architecture ? | Le stockage des fichiers est assuré par MinIO (compatible S3), le catalogue des tables par Hive Metastore, l'exécution des requêtes SQL par Trino : MinIO + Hive Metastore + Trino jouent le rôle de HDFS + Hive. Les scripts Cloudera restent dans le dépôt comme annexe optionnelle, non utilisés pour la démonstration. |
| Les requêtes sont-elles lentes à chaque clic ? | Superset interroge Trino en direct. Les gros comptages passent par les agrégats gold (`f_mesures`, `f_consult_diag_mois`, `f_consult_prof_mois`) et Trino est réglé pour 2 requêtes simultanées (groupes de ressources). En cas de lenteur le jour de la soutenance, les captures `visuels/superset/` servent de secours. |
| Pourquoi le taux de 2021 est-il de 0,22 et non 0,13 ? | Les hospitalisations s'arrêtent le 19 juin 2021 : on divise par les consultations de la même fenêtre. Diviser par l'année entière (145 883) donne 0,13, comparaison fausse. |
| Pourquoi 63,87 % de rattachement seulement ? | Le lien passe par le professionnel de santé puis son activité ; 71,87 % des consultations ont un professionnel connu de l'activité et un établissement n'est trouvé que pour une partie. Les 36 % restants sont regroupés sous « inconnu », jamais supprimés. |
| Pourquoi la requête q03 donne-t-elle 0,75 pour 2015 et 0,21 pour 2021 alors que les visuels affichent 0,76 et 0,22 ? | q03 compte les consultations à la maille du mois (juin 2015 entier, juin 2021 entier) ; les visuels les comptent du 21/06/2015 au 19/06/2021, exactement la fenêtre des hospitalisations. Les années 2016 à 2020 sont identiques. |
| Que se passe-t-il pour les hospitalisations datées avant la naissance du patient ? | 62 cas sur 2 479, repérés par comparaison de la date d'entrée et de la date de naissance ; ils sont rangés dans 0-14 ans par la règle d'âge révolu. Sans eux, la tranche 0-14 passerait de 276 à 237 femmes et de 181 à 158 hommes ; le taux reste plat autour de 3 pour 1 000. |
| Comment montrez-vous que tout le datalake est visible ? | Dans Superset, la connexion « Trino CHU » expose les schémas `raw` (7 tables brutes lues sur MinIO), `silver` (1 table) et `gold` (12 tables et 9 vues métier) ; le tableau « Volume de chaque table du lac » de l'onglet 1 compte les lignes de chaque table en direct. |
| Pourquoi pas de top 10 des médecins nommés ? | Données nominatives inutiles à la décision ; la distribution répond mieux à la question de concentration. |
| Pourquoi la carte des décès ne montre-t-elle pas de taux ? | Nous n'avons pas la population régionale dans le lac : nous le disons sur le visuel et le recommandons. |
| Pourquoi des barres en plus de la carte pour la satisfaction ? | La carte montre où, les barres montrent de combien : l'écart est de 2,4 points seulement. |
| Comment le schéma en étoile existe-t-il dans Superset ? | Les faits et dimensions sont des datasets physiques ; les jointures sont écrites une fois dans les datasets virtuels `etoile_consultations`, `etoile_hospitalisations`, `etoile_mesures`, avec des métriques nommées (consultations, patients distincts, durée moyenne, taux). Superset ne dessine pas de relations : la jointure est le SQL du dataset. |
| Comment racontez-vous une histoire sans les signets de Power BI ? | Un lien permanent Superset mémorise l'onglet et l'état des filtres : 9 étapes sont enregistrées (liste dans `superset/PERMALIENS.md`), précédées d'un bloc de texte qui dit le message de l'étape. |
| Comment prouvez-vous que les chiffres du tableau de bord sont justes ? | `verify.py` compare chaque nombre affiché à une requête Trino indépendante (118 contrôles) ; `concordance_q01_q10.py` rejoue les requêtes q02 à q06, q09 et q10 du benchmark contre les datasets Superset (9 concordances). |
| La stack est-elle reproductible ? | Oui : `docker compose up` construit 17 services, les DAG Airflow chargent raw vers gold, le tout a été rejoué sur Linux avec contrôle des volumes à chaque étape. |
