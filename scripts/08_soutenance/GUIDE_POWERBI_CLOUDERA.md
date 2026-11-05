# Annexe optionnelle : Cloudera, Power BI, modèle en étoile, tableau de bord mobile et signets

> **Annexe optionnelle.** La démonstration de la soutenance utilise **Apache Superset** (voir `GUIDE_SUPERSET.md`) : MinIO + Hive Metastore + Trino jouent le rôle de HDFS + Hive, sans machine virtuelle Cloudera. Ce guide est conservé pour mémoire, pour qui voudrait rejouer le parcours Cloudera et Power BI du cours.

Ce guide part du lakehouse Docker du Livrable 2 (déjà construit) et mène jusqu'à un rapport Power BI.
Deux routes de connexion existent. La **route A (Cloudera Quickstart)** est celle du cours. La **route B (HiveServer2 de la stack Docker)** sert de plan de secours et pour répéter.

Temps estimé : route A 45 min, modèle et mesures 60 min, pages, mobile et signets 90 min.

---------------------------------------------------------------------

## 0. Prérequis

| Élément | Détail |
|---|---|
| Machine | Windows 10 ou 11, 8 Go de RAM minimum, 10 Go de disque libres (Power BI Desktop ne fonctionne pas sous Linux ni macOS) |
| Power BI Desktop | Microsoft Store ou https://powerbi.microsoft.com/fr-fr/desktop/ (version de l'année en cours) |
| Pilote ODBC | **Cloudera ODBC Driver for Apache Hive 2.6.x, 64 bits** (téléchargement gratuit sur le site Cloudera, section Downloads, ODBC drivers) |
| VM Cloudera | cloudera-quickstart-vm-5.12.0-0-virtualbox (déjà installée avec le Manuel d'installation), 8 Go de RAM alloués si possible |
| Stack Docker | `docker compose up -d` dans le dossier `scripts/`, tous les services en état healthy |
| Données | Les données complètes du projet (dossier DATA) chargées dans la stack, sinon les chiffres affichés différeront de ceux de ce guide |

Contrôle avant de commencer : `docker ps` doit montrer `chu-hive-hs2`, `chu-trino`, `chu-minio` et `chu-airflow-scheduler` ; dans Airflow, les DAG `lakehouse_init` et `lakehouse_daily` doivent être verts.

---------------------------------------------------------------------

## 1. Route A : Cloudera Quickstart (VM) vers Power BI

### 1.1 Pourquoi un export intermédiaire

La VM Cloudera embarque Hive 1.1 (CDH 5.12). Trois obstacles par rapport au lac MinIO :

1. Spark écrit les Parquet en **zstd**, que Hive 1.1 ne sait pas lire : il faut du **snappy**.
2. Les colonnes de partition (`annee`, `sexe`, `tranche_age`, `code_region`) sont dans les chemins de dossiers, pas dans les fichiers : l'export les réintègre.
3. Le type `DATE` n'est pas lu en Parquet par Hive 1.1 : les dates sont exportées en texte `AAAA-MM-JJ` (Power BI les retypera en Date).

Le script `cloudera/export_snappy.py` fait ce travail pour les 13 tables du modèle (12 gold + `silver.f_consultation_detail`) et génère le DDL Hive 1.1 (`ddl_hive_cloudera.sql`) ainsi qu'un fichier de volumes de contrôle (`volumes_export.csv`).

Le lac comporte trois couches : **raw** (fichiers sources bruts), **silver** et **gold**. Pour que *tous* les répertoires du datalake soient visibles dans Cloudera puis dans Power BI, le script `cloudera/export_raw.sh` exporte aussi la couche raw (7 fichiers CSV sources) et `cloudera/ddl_raw_cloudera.sql` les expose dans une base Hive `raw` (colonnes en texte, en-têtes ignorés).

### 1.2 Exporter depuis le lakehouse (sur la machine qui héberge Docker)

Dans le dossier `scripts/` :

```bash
docker cp 08_soutenance/cloudera/export_snappy.py chu-airflow-scheduler:/tmp/export_snappy.py
docker exec chu-airflow-scheduler bash -c "rm -rf /tmp/export_cloudera && cd /tmp && python3 export_snappy.py"
docker cp chu-airflow-scheduler:/tmp/export_cloudera ./export_cloudera
./08_soutenance/cloudera/export_raw.sh        # couche raw -> ./export_cloudera/raw (RAW_COMPLET=1 pour les fichiers de plus de 500 Mo en entier)
```

Sous Linux, préfixer `docker` par `sudo` si votre utilisateur n'est pas dans le groupe docker.
Attendu : 13 lignes `OK schema.table : N lignes` pour gold et silver, puis 7 lignes `OK raw/...` pour la couche raw (`export_raw.sh` tronque par défaut à 200 000 lignes un fichier de plus de 500 Mo, par exemple `deces.csv` : table raw.deces alors marquée comme extrait ; la VM n'a pas la place pour plusieurs Go). Les volumes affichés doivent être égaux à ceux de `gold.*` et `silver.*` vus par Trino (`SELECT COUNT(*) ...`). La durée est de quelques minutes.

### 1.3 Charger dans la VM Cloudera

1. Démarrer la VM, attendre que Cloudera Manager montre HDFS, Hive et YARN en vert (voir Manuel d'installation, chapitre Cloudera).
2. Relever l'adresse IP de la VM, l'ajouter au fichier hosts de la machine : `192.168.x.y quickstart.cloudera` (la même ligne que celle du Manuel d'installation).
3. Lancer, depuis le dossier qui contient `export_cloudera/` :

```bash
./08_soutenance/cloudera/charger_cloudera.sh quickstart.cloudera
```

Le script copie les fichiers, les range dans HDFS sous `/user/cloudera/lake/gold/<table>`, `/user/cloudera/lake/silver/<table>` et `/user/cloudera/lake/raw/<source>/<fichier>`, crée les tables Hive (bases `gold`, `silver`, `raw`) et compare les `COUNT(*)` avec `volumes_export.csv` et `volumes_raw.csv`. Sortie attendue : `OK` pour les 13 tables gold et silver et les 7 tables raw, aucun `ECART`.
Si le mot de passe SSH est demandé : `cloudera` (compte par défaut de la VM).

4. Contrôle visuel : dans Hue (`http://quickstart.cloudera:8888`, utilisateur cloudera, mot de passe cloudera), onglet Navigateur de fichiers, ouvrir `/user/cloudera/lake` : les trois dossiers `raw`, `silver` et `gold` et leurs contenus doivent être visibles. **Faire une capture d'écran** : c'est la preuve que les répertoires du datalake sont dans Cloudera.

### 1.4 Créer la source de données ODBC (Windows)

1. Installer le pilote Cloudera ODBC for Hive (64 bits).
2. Ouvrir **Sources de données ODBC (64 bits)**, onglet **DSN système**, bouton **Ajouter**, choisir **Cloudera ODBC Driver for Apache Hive**.
3. Renseigner exactement :

| Champ | Valeur |
|---|---|
| Data Source Name | `CHU_Cloudera` |
| Host(s) | `quickstart.cloudera` |
| Port | `10000` |
| Database | `default` |
| Mechanism | `User Name` |
| User Name | `cloudera` |
| Password | laisser vide |
| Thrift Transport | `SASL` (valeur par défaut) ou `Binary` si le test échoue |
| Hive Server Type | `Hive Server 2` |

4. Cliquer sur **Test** : le message attendu est `Success`. En cas d'échec : vérifier le ping vers `quickstart.cloudera`, que HiveServer2 est démarré dans Cloudera Manager, et que le pare-feu de Windows ne bloque pas le port 10000.

### 1.5 Connecter Power BI

1. Power BI Desktop : **Accueil > Obtenir les données > Plus... > Autre > ODBC > Connecter**.
2. Dans la liste de DSN, choisir `CHU_Cloudera`, laisser le champ SQL vide, **OK**.
3. Identifiants : onglet **Par défaut ou Personnalisé**, laisser vide (ou nom `cloudera`), **Se connecter**.
4. Le **Navigateur** montre l'arborescence : `raw` (7 tables), `silver` (`f_consultation_detail`) et `gold` (12 tables), plus `default`. C'est cette arborescence complète qui prouve que tous les répertoires du datalake sont visibles.
5. **Cocher les 13 tables gold et silver** pour le modèle (tableau de la section 3), puis **Transformer les données** (et non Charger) pour passer par Power Query. Les tables `raw` servent à la démonstration d'arborescence : ne pas les charger dans le modèle (colonnes en texte, fichiers bruts).
6. Mode de connectivité : **Importer**. Ne pas utiliser DirectQuery : Hive 1.1 est lent (MapReduce) et les agrégats seraient recalculés à chaque clic.

Capture à faire pour l'oral : la fenêtre **Navigateur** avec les bases `raw`, `silver` et `gold` dépliées (preuve que tous les répertoires du datalake sont visibles dans Power BI).

---------------------------------------------------------------------

## 2. Route B : HiveServer2 de la stack Docker (secours et répétition)

Le conteneur `chu-hive-hs2` publie le port 10000 de la machine hôte. Avec la même procédure ODBC :

| Champ | Valeur |
|---|---|
| Host | adresse de la machine Docker (`localhost` si Power BI est sur la même machine) |
| Port | `10000` |
| Mechanism | `User Name`, nom `hive`, mot de passe vide |
| Database | `default` |

Bases `gold` et `silver`, mêmes noms de tables (aucun export nécessaire, mais les fichiers sont en zstd : cela marche car HiveServer2 4.2 les lit). Attention : la base `gold` de la stack contient aussi 9 vues Hive `v_*` en plus des 12 tables : **ne pas les cocher** (leur lecture échoue côté HiveServer2, voir plus bas). La couche raw n'est pas déclarée dans le Metastore de la stack : elle n'apparaît qu'avec la route A.

Limites vérifiées lors de l'exécution réelle de la stack sur la machine d'audit :

* `SHOW DATABASES`, `SHOW TABLES` et `SELECT * FROM table` fonctionnent : ils suffisent pour le mode **Importer**.
* Les requêtes d'agrégation (`COUNT`, `SUM`, `GROUP BY`) et les vues Hive échouent côté HiveServer2 de la stack (tâches Tez sans accès aux jars et aux identifiants S3). **Conséquence : toujours importer les tables brutes et calculer les agrégats en DAX.** C'est de toute façon la bonne pratique pour un modèle en étoile.
* Pour les requêtes SQL agrégées, utiliser Trino (port 8080) ou Superset.

---------------------------------------------------------------------

## 3. Power Query : préparation des tables

Appliquer dans l'éditeur Power Query (une étape par ligne dans le volet Étapes appliquées).

| Table | Transformations |
|---|---|
| `d_temps` | `date_jour` : type **Date**. `annee`, `mois`, `trimestre`, `id_temps` : nombre entier. Trier `annee_mois` par `id_temps` (étape de modélisation). |
| `d_patient` | `id_patient` entier, `date_naissance` Date. Supprimer `code_postal` et `ville` (inutiles, données personnelles). |
| `d_etablissement` | `id_etablissement` entier, `id_localisation` texte. |
| `d_localisation` | `id_localisation` texte (ne pas convertir : `2A`, `2B`, `INCONNU`). |
| `d_diagnostic` | Aucune. |
| `d_professionnel` | **Supprimer `nom`, `prenom`, `civilite`** (données nominatives, inutiles pour l'analyse, cf. protection des données). |
| `f_mesures` | `id_temps`, `id_patient`, `id_etablissement` entiers. Ne pas charger si la mémoire est limitée : les analyses de la présentation reposent sur les faits de détail ; on l'utilise pour la page schéma. |
| `f_hospitalisation` | `duree_jours` entier, `id_*` selon le schéma. |
| `f_deces_agg`, `f_satisfaction` | `nbre_deces`, `nb_repondants` entiers ; `score_satisfaction` nombre décimal. |
| `f_consultation_detail` | `id_professionnel` texte, autres clés entières. |
| `f_consult_diag_mois`, `f_consult_prof_mois` | Optionnelles (agrégats). Charger seulement si l'on veut montrer les agrégats Gold. |

Cliquer sur **Fermer et appliquer**. Mémoire : `d_professionnel` et `f_consultation_detail` pèsent chacune environ 1 million de lignes, c'est normal pour Power BI.

Tables calculées à ajouter (Modélisation > Nouvelle table) :

```DAX
d_region =
ADDCOLUMNS (
    DISTINCT ( SELECTCOLUMNS ( d_localisation, "region", d_localisation[region] ) ),
    "pays", "France",
    "lieu", [region] & ", France"
)
```

Sur `d_region[region]` et `d_region[lieu]`, définir **Catégorie de données = Région/État** (carte).

---------------------------------------------------------------------

## 4. Reproduire le schéma en étoile dans le moteur de modélisation

Ouvrir la **Vue Modèle** (icône de gauche, troisième icône). Si Power BI a créé des relations automatiques, les supprimer toutes, puis créer à la main (glisser la clé de la dimension vers la clé du fait).

### 4.1 Relations

Toutes les relations : cardinalité **un à plusieurs (1:*)**, direction du filtre croisé **unique** (de la dimension vers le fait).

| # | Dimension (côté 1) | Fait (côté *) | Active |
|---|---|---|---|
| 1 | `d_temps[id_temps]` | `f_consultation_detail[id_temps]` | oui |
| 2 | `d_temps[id_temps]` | `f_hospitalisation[id_temps]` | oui |
| 3 | `d_temps[id_temps]` | `f_deces_agg[id_temps]` | oui |
| 4 | `d_temps[id_temps]` | `f_mesures[id_temps]` | oui |
| 5 | `d_patient[id_patient]` | `f_consultation_detail[id_patient]` | oui |
| 6 | `d_patient[id_patient]` | `f_hospitalisation[id_patient]` | oui |
| 7 | `d_patient[id_patient]` | `f_mesures[id_patient]` | oui |
| 8 | `d_etablissement[id_etablissement]` | `f_consultation_detail[id_etablissement]` | oui |
| 9 | `d_etablissement[id_etablissement]` | `f_hospitalisation[id_etablissement]` | oui |
| 10 | `d_etablissement[id_etablissement]` | `f_mesures[id_etablissement]` | oui |
| 11 | `d_localisation[id_localisation]` | `f_consultation_detail[id_localisation]` | oui |
| 12 | `d_localisation[id_localisation]` | `f_hospitalisation[id_localisation]` | oui |
| 13 | `d_localisation[id_localisation]` | `f_deces_agg[id_localisation]` | oui |
| 14 | `d_localisation[id_localisation]` | `f_mesures[id_localisation]` | oui |
| 15 | `d_diagnostic[id_diagnostic]` | `f_consultation_detail[id_diagnostic]` | oui |
| 16 | `d_diagnostic[id_diagnostic]` | `f_hospitalisation[id_diagnostic]` | oui |
| 17 | `d_professionnel[identifiant]` | `f_consultation_detail[id_professionnel]` | oui |
| 18 | `d_region[region]` | `f_satisfaction[region]` | oui |
| 19 | `d_localisation[id_localisation]` | `d_etablissement[id_localisation]` | **non (inactive)** |

Points d'attention :

* La relation 19 (flocon) est **inactive** pour éviter deux chemins entre `d_localisation` et les faits (ambiguïté). Pour une mesure qui doit filtrer par la région de l'établissement : `CALCULATE ( [Consultations], USERELATIONSHIP ( d_etablissement[id_localisation], d_localisation[id_localisation] ) )`.
* `f_satisfaction` n'a pas de clé d'établissement (le score ESATIS est rattaché à un établissement juridique puis à la région) : c'est pourquoi elle se relie à `d_region`, pas à `d_etablissement`.
* Les clés « membre inconnu » (`-1` pour les établissements, `INCONNU` pour les localisations) n'existent pas dans les dimensions : Power BI les regroupe dans le libellé **(Vide)**. C'est attendu : 36 % des consultations n'ont pas d'établissement rattaché. **Ne pas masquer cette ligne**, c'est une information de fiabilité.
* Marquer `d_temps` comme **table de dates** (clic droit sur la table > Marquer comme table de dates > colonne `date_jour`).
* Masquer dans la vue Rapport toutes les colonnes de clés (`id_*`) et les tables de faits non utilisées en axe.

### 4.2 Contrôle de conformité du modèle

Dans la Vue Modèle, la disposition attendue : les dimensions en haut, les faits en bas, les flèches pointant vers les faits. **Faire une capture d'écran** et la comparer au visuel V10_schema_en_etoile.png. Chaque relation doit afficher `1` côté dimension et `*` côté fait.

---------------------------------------------------------------------

## 5. Mesures DAX

Créer une table vide `_Mesures` (Saisir des données, nom `_Mesures`), puis y ajouter les mesures ci-dessous (Modélisation > Nouvelle mesure).

```DAX
Consultations = COUNTROWS ( f_consultation_detail )

Hospitalisations = COUNTROWS ( f_hospitalisation )

Patients hospitalisés = DISTINCTCOUNT ( f_hospitalisation[id_patient] )

Durée moyenne de séjour (jours) = AVERAGE ( f_hospitalisation[duree_jours] )

Décès = SUM ( f_deces_agg[nbre_deces] )

Consultations sur la fenêtre des hospitalisations =
CALCULATE (
    [Consultations],
    KEEPFILTERS ( DATESBETWEEN ( d_temps[date_jour], DATE ( 2015, 6, 21 ), DATE ( 2021, 6, 19 ) ) )
)

Hospitalisations pour 100 consultations =
DIVIDE ( [Hospitalisations], [Consultations sur la fenêtre des hospitalisations] ) * 100

Hospitalisations pour 1000 consultations =
DIVIDE ( [Hospitalisations], [Consultations sur la fenêtre des hospitalisations] ) * 1000

Satisfaction pondérée =
VAR T = FILTER ( f_satisfaction, NOT ISBLANK ( f_satisfaction[score_satisfaction] ) )
RETURN
    DIVIDE (
        SUMX ( T, f_satisfaction[score_satisfaction] * f_satisfaction[nb_repondants] ),
        SUMX ( T, f_satisfaction[nb_repondants] )
    )

Consultations avec établissement =
CALCULATE ( [Consultations], f_consultation_detail[id_etablissement] <> -1 )

Part rattachée à un établissement =
DIVIDE ( [Consultations avec établissement], [Consultations] )

Part de femmes hospitalisées =
DIVIDE ( CALCULATE ( [Hospitalisations], d_patient[sexe] = "F" ), [Hospitalisations] )
```

Pourquoi la fenêtre de dates : les hospitalisations n'existent que du **21/06/2015 au 19/06/2021**. Diviser par toutes les consultations d'une année (2015 et 2021 incomplètes pour les hospitalisations) fausserait le taux (2021 : 0,13 faux, 0,22 correct).

Contrôles chiffrés à reproduire après chargement des données complètes (carte Power BI > chiffres) :

| Contrôle | Valeur attendue |
|---|---|
| `[Consultations]` total | 1 027 157 |
| `[Hospitalisations]` total | 2 479 |
| Patients (`d_patient`, nombre de lignes) | 100 000 |
| `[Consultations]` par année 2015, 2016, ..., 2023 | 33 896 ; 184 308 ; 133 403 ; 160 373 ; 87 497 ; 162 778 ; 145 883 ; 101 991 ; 17 028 |
| `[Part rattachée à un établissement]` | 63,87 % |
| `[Hospitalisations pour 100 consultations]` 2015 à 2021 | 0,76 ; 0,21 ; 0,34 ; 0,25 ; 0,43 ; 0,25 ; 0,22 (arrondis à 2 décimales) |
| `[Hospitalisations]` par année 2015 à 2021 | 255 ; 395 ; 458 ; 395 ; 374 ; 410 ; 192 |

Si un seul de ces nombres diffère, **ne pas présenter** : il y a un défaut de chargement ou de relation.

---------------------------------------------------------------------

## 6. Pages du rapport (DataViz)

Thème : Affichage > Thèmes > Personnaliser le thème : couleurs `#0B4F8A` (bleu), `#1B998B` (vert d'eau), `#E07A1F` (orange), `#6B7280` (gris). Police Segoe UI. Format de page : 16:9. Chaque page porte un **titre qui énonce la conclusion** (pas seulement le sujet) et une zone « Source et limites » en bas.

Cinq pages (équivalents des visuels V01 à V14, voir le fichier ORAL_ET_VISUELS.md).

### Page 1 : Vue d'ensemble

* 4 cartes : `[Consultations]`, `[Hospitalisations]`, `COUNTROWS(d_patient)` (Nombre de patients), `[Décès]` (filtre année 2019).
* Segmentation (slicer) : `d_temps[annee]`, style Mosaïque.
* Zone texte : question posée par le CHU et périmètre des données (2015 à 2023).

### Page 2 : Activité (besoins 1, 2 et 6)

| Visuel | Type | Champs | Réglages |
|---|---|---|---|
| Consultations par mois | **Courbe** | Axe : `d_temps[annee_mois]` (trié par `id_temps`) ; Valeurs : `[Consultations]` | Titre : « Activité très irrégulière, de 4 700 à 18 800 consultations par mois » |
| Top 20 établissements 2019 | **Barres horizontales** | Axe : `d_etablissement[raison_sociale_site]` ; Valeurs : `[Consultations]` ; Légende : `d_etablissement[region]` | Filtre visuel : année = 2019, **Top N 20** par `[Consultations]`, exclure (Vide) ; note : « 63,87 % des consultations seulement sont rattachables » |
| Consultations par chapitre CIM-10 | **Barres empilées par année** | Axe : colonne calculée `chapitre` dans `d_diagnostic` (voir 6.1) ; Valeurs : `[Consultations]` ; Légende : `d_temps[annee]` | Filtre année 2019 à 2021 |
| Top 10 diagnostics | **Barres** | Axe : `d_diagnostic[code_diag]` + `libelle_diagnostic` ; Valeurs : `[Consultations]` | **Top N 10**, départage par code ; afficher aussi le pourcentage du total |
| Activité par professionnel 2020 | **Histogramme de distribution** | Axe : nombre de consultations par professionnel (voir 6.2) ; Valeurs : nombre de professionnels | Ne **pas** afficher les noms |

### Page 3 : Hospitalisations (besoins 3, 4 et 5)

| Visuel | Type | Champs |
|---|---|---|
| Taux d'hospitalisation par année | **Colonnes** | Axe : `d_temps[annee]` ; Valeurs : `[Hospitalisations pour 100 consultations]` ; couleur orange pour 2015 et 2021 (années partielles) |
| Volume d'hospitalisations | **Colonnes** | `[Hospitalisations]` par `d_temps[annee]` |
| Répartition par sexe | **Anneau** | Légende : `d_patient[sexe]` ; Valeurs : `[Hospitalisations]` |
| Par tranche d'âge et sexe | **Colonnes groupées** | Axe : `f_hospitalisation[tranche_age]` ; Légende : `d_patient[sexe]` ; trier les tranches `0-14, 15-29, 30-44, 45-59, 60-74, 75+` |
| Diagnostics | **Barres** | Axe : chapitre CIM-10 ; Valeurs : `[Hospitalisations]` et `[Durée moyenne de séjour (jours)]` en info-bulle |

### Page 4 : Territoires (besoins 7 et 8)

| Visuel | Type | Champs | Réglages |
|---|---|---|---|
| Décès 2019 par région | **Carte choroplèthe (Carte remplie)** | Emplacement : `d_localisation[region]` (ou `d_region[lieu]`) ; Valeurs : `[Décès]` | Filtre visuel `d_temps[annee] = 2019`. Avertir : valeurs brutes, non rapportées à la population |
| Satisfaction 2020 par région | **Carte choroplèthe** | Emplacement : `d_region[lieu]` ; Valeurs : `[Satisfaction pondérée]` | Filtre visuel `f_satisfaction[annee] = 2020`. Échelle de couleurs de 72 à 75 (min et max fixés) |
| Comparaison 2019 et 2020 | **Barres groupées** | Axe : `d_region[region]` ; Légende : `f_satisfaction[annee]` | Axe démarrant à 60, **le dire sur le visuel** |

Pourquoi la carte : l'information est géographique (région), la carte localise d'un coup d'œil ; pourquoi le barre en complément : la carte rend mal les écarts fins (72,6 à 75,0), le barre les chiffre.

### Page 5 : Fiabilité et limites

* Barres horizontales : part de consultations rattachées à un établissement (63,87 %), part avec professionnel connu (71,87 %), lignes de satisfaction avec un score (74,48 % en 2019, 72,79 % en 2020), hospitalisations rattachées (100 %).
* Zone texte : limites connues (début et fin de période partiels, août à décembre 2022 qui reproduit 2021, scores de satisfaction sur des établissements juridiques, 62 hospitalisations et environ 24 000 consultations datées avant la naissance du patient dans les sources, doublons de décès de 0,70 % non retirés).

### 6.1 Chapitre CIM-10 (colonne calculée dans `d_diagnostic`)

```DAX
chapitre =
SWITCH (
    TRUE (),
    LEFT ( d_diagnostic[code_diag], 1 ) IN { "A", "B" }, "Infectieuses et parasitaires",
    LEFT ( d_diagnostic[code_diag], 1 ) = "C", "Tumeurs malignes",
    LEFT ( d_diagnostic[code_diag], 1 ) = "D", "Tumeurs bénignes, sang",
    LEFT ( d_diagnostic[code_diag], 1 ) = "E", "Endocriniennes, métaboliques",
    LEFT ( d_diagnostic[code_diag], 1 ) = "F", "Troubles mentaux",
    LEFT ( d_diagnostic[code_diag], 1 ) = "G", "Système nerveux",
    LEFT ( d_diagnostic[code_diag], 1 ) = "H", "Oeil et oreille",
    LEFT ( d_diagnostic[code_diag], 1 ) = "I", "Appareil circulatoire",
    LEFT ( d_diagnostic[code_diag], 1 ) = "J", "Appareil respiratoire",
    LEFT ( d_diagnostic[code_diag], 1 ) = "K", "Appareil digestif",
    LEFT ( d_diagnostic[code_diag], 1 ) = "L", "Peau",
    LEFT ( d_diagnostic[code_diag], 1 ) = "M", "Ostéo-articulaire",
    LEFT ( d_diagnostic[code_diag], 1 ) = "N", "Génito-urinaire",
    LEFT ( d_diagnostic[code_diag], 1 ) = "O", "Grossesse, accouchement",
    LEFT ( d_diagnostic[code_diag], 1 ) = "P", "Période périnatale",
    LEFT ( d_diagnostic[code_diag], 1 ) = "Q", "Malformations",
    LEFT ( d_diagnostic[code_diag], 1 ) = "R", "Symptômes et signes",
    LEFT ( d_diagnostic[code_diag], 1 ) IN { "S", "T" }, "Traumatismes, empoisonnements",
    LEFT ( d_diagnostic[code_diag], 1 ) = "U", "Codes spéciaux",
    LEFT ( d_diagnostic[code_diag], 1 ) IN { "V", "W", "X", "Y" }, "Causes externes",
    LEFT ( d_diagnostic[code_diag], 1 ) = "Z", "Facteurs de santé",
    "Autre"
)
```

Contrôle : somme des consultations par chapitre 2019 à 2021 = 396 158 ; Ostéo-articulaire = 67 093 ; Traumatismes = 60 349.

### 6.2 Distribution des consultations par professionnel

Colonne calculée dans `d_professionnel` (pour l'année choisie dans un paramètre, ici 2020) :

```DAX
consultations 2020 =
CALCULATE ( COUNTROWS ( f_consultation_detail ), d_temps[annee] = 2020 )
```

Placer `d_professionnel[consultations 2020]` en axe (grouper en cases de 1) et `COUNTROWS(d_professionnel)` en valeurs, filtre `consultations 2020 >= 1`. Attendu : 79 246 médecins à 1 consultation, 30 164 à 2, 6 282 à 3, 798 à 4, 147 à 5, 51 à 6, 13 à 7, 2 à 8, 2 à 9 (116 705 médecins, 162 778 consultations).

---------------------------------------------------------------------

## 7. Disposition mobile (lisibilité sur téléphone)

1. Pour chaque page : **Affichage > Disposition mobile**. Le canevas vertical 9:16 apparaît.
2. Glisser les visuels depuis le volet « Visuels de la page » vers le canevas, dans l'ordre de lecture : titre, 2 cartes, graphique principal, graphique secondaire. **3 à 4 visuels maximum par page mobile.**
3. Règles : barres plutôt que carte pour les comparaisons fines ; étiquettes de données activées ; taille de police de 12 pt minimum ; pas de tableaux à plus de 4 colonnes.
4. Publier : **Accueil > Publier** vers l'espace de travail (compte Power BI gratuit avec adresse e-mail de l'école).
5. Sur le téléphone : installer l'application **Power BI Mobile**, se connecter avec le même compte, ouvrir le rapport : la **disposition mobile** s'affiche en portrait. Faire une capture d'écran du téléphone (c'est la preuve de visibilité mobile).
6. Alternative sans publication : le fichier `visuels/dashboard_chu.html` de ce dossier offre le même récit en HTML responsive (ouvrir sur le téléphone, voir captures `dashboard_mobile.png`).

---------------------------------------------------------------------

## 8. Storytelling avec les signets

Principe : une histoire en six étapes, chaque étape est un signet qui affiche une page et masque ou montre des éléments.

### 8.1 Préparer

1. Affichage > cocher **Volet Sélection** et **Signets**.
2. Sur la page 1, ajouter un bouton **Navigateur de signets** (Insérer > Boutons > Navigateur > Navigateur de signets) : il liste automatiquement les signets.
3. Pour les étapes qui découvrent des éléments (annotations), créer pour chaque zone de texte d'annotation un nom clair dans le Volet Sélection (par exemple `note_pic_2016`).

### 8.2 Créer les six signets

Pour chaque étape : régler la page, les filtres et les éléments visibles, puis **Signets > Ajouter**, renommer, clic droit > **Options** : garder **Données**, **Affichage**, **Page actuelle** cochés.

| N° | Nom du signet | Page | Message (titre parlé) | Éléments affichés |
|---|---|---|---|---|
| 1 | `01 Contexte` | Page 1 | Le CHU veut comprendre son activité et sa fiabilité | Cartes, schéma en étoile (image) |
| 2 | `02 Activité` | Page 2 | Une activité très irrégulière, un tiers de motifs ostéo-articulaires et traumatismes | Courbe, chapitres, annotation du pic de mars 2016 |
| 3 | `03 Hospitalisations` | Page 3 | Environ 0,2 à 0,4 hospitalisation pour 100 consultations, 57 % de femmes, 15 jours de séjour | Taux, sexe et âge |
| 4 | `04 Territoires` | Page 4 | L'Île-de-France et Auvergne-Rhône-Alpes concentrent 24 % des décès, la satisfaction varie peu | Deux cartes |
| 5 | `05 Fiabilité` | Page 5 | Chiffres par établissement : 64 % des consultations seulement | Barres de fiabilité, limites |
| 6 | `06 Recommandations` | Page 5 (zone texte) | Fiabiliser le rattachement établissement, enrichir les données, normaliser par la population | Zone de recommandations visible |

### 8.3 Raconter

* Navigation par boutons : sur chaque page, ajouter un bouton fléché (Insérer > Boutons > Flèche) avec **Action = Signet** (suivant ou précédent).
* Mode présentation : **Signets > Afficher** (ou icône d'écran) déroule l'histoire en plein écran, signet par signet.
* Durée : 45 secondes par signet, soit 4 à 5 minutes de narration.
* Chaque étape suit le schéma : **le constat (chiffre), pourquoi c'est important pour le CHU, ce que l'on propose.**

---------------------------------------------------------------------

## 9. Check-list avant la soutenance

Cocher chaque ligne la veille :

- [ ] VM Cloudera démarrée, `hdfs dfs -ls /user/cloudera/lake` liste `raw`, `silver` et `gold`.
- [ ] Capture Hue des répertoires et capture du Navigateur Power BI avec les bases `raw`, `silver`, `gold` dépliées (13 tables du modèle plus 7 tables raw).
- [ ] Vue Modèle : 18 relations actives et 1 inactive, conformes à la section 4.1.
- [ ] Les contrôles chiffrés de la section 5 sont identiques à l'écran.
- [ ] Cinq pages avec titre conclusif, source et limites ; aucune étiquette coupée.
- [ ] Disposition mobile faite sur les 5 pages, test sur téléphone.
- [ ] Six signets créés, navigateur de signets fonctionnel, mode présentation testé.
- [ ] Plan B : stack Docker démarrée et route B testée, `dashboard_chu.html` ouvert dans un navigateur, visuels PNG prêts.
- [ ] Aucun nom de professionnel de santé affiché.

---------------------------------------------------------------------

## 10. Dépannage rapide

| Symptôme | Cause probable | Solution |
|---|---|---|
| Test ODBC : « could not connect » | VM non joignable ou HiveServer2 arrêté | `ping quickstart.cloudera` ; redémarrer HiveServer2 dans Cloudera Manager |
| Hive : « Failed with exception java.io.IOException ... ParquetDecodingException » | Fichiers écrits en zstd | Refaire l'export avec `export_snappy.py` (snappy) |
| Colonnes décalées ou valeurs nulles | DDL différent du fichier exporté | Recharger `ddl_hive_cloudera.sql` généré avec le même export |
| Power BI : « type DATE non pris en charge » | Dates exportées en texte | Retyper en Date dans Power Query |
| Relation impossible 1:* (« valeurs en double ») | Clé de dimension dupliquée | Vérifier que la table est la bonne ; `d_localisation[id_localisation]` est unique (102 valeurs) |
| Carte vide | Catégorie de données non définie | Définir Région/État sur `d_region[lieu]` |
| Chargement très long | `f_mesures` et `f_consultation_detail` volumineuses | Charger `f_mesures` plus tard, fermer Chrome, passer le DSN en mode `Binary` |
| HiveServer2 de la stack : erreur sur `COUNT(*)` | Limite connue (Tez) | Importer les tables brutes, agréger en DAX |
