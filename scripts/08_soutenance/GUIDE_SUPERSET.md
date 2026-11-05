# Guide Superset : du lac au tableau de bord, reproductible de zéro

Ce guide remplace le parcours Power BI. Le tableau de bord de la soutenance est **Apache Superset 4.1.1**, déjà présent dans la stack Docker (service `superset`, http://localhost:8088, compte `admin` / `admin`). Il interroge le lac **directement** : MinIO (fichiers) + Hive Metastore (catalogue) + Trino (moteur SQL). Dans l'architecture du projet, MinIO + Hive Metastore + Trino jouent le rôle que HDFS + Hive jouent dans une pile Hadoop classique. Cloudera n'est plus utilisé pour la démonstration (voir l'annexe en fin de document).

Tout ce qui suit est du code versionné dans `scripts/08_soutenance/superset/` : rien n'est à cliquer pour reconstruire le tableau de bord, mais chaque étape est aussi décrite dans l'interface pour pouvoir l'expliquer à l'oral.

## 1. Vue d'ensemble

| Besoin | Réponse dans Superset | Fichier |
|---|---|---|
| Voir raw, silver et gold | Connexion « Trino CHU » + SQL Lab + 18 datasets physiques | `provision_lake.py`, `provision.py` |
| Modèle en étoile | Datasets virtuels qui joignent faits et dimensions + métriques | `datasets.py` |
| Répondre aux 8 besoins du CHU | 2 tableaux de bord (bureau et téléphone), 6 onglets, 27 graphiques | `provision.py`, `charts.py` |
| Filtrer | 6 filtres natifs : année, sexe, tranche d'âge, région, établissement, chapitre CIM-10 | `provision.py` |
| Raconter | Onglets ordonnés, blocs de texte, 9 liens permanents (une étape = un onglet + des filtres) | `histoire.py`, `PERMALIENS.md` |
| Prouver les chiffres | Chaque nombre affiché est comparé à une requête Trino indépendante | `verify.py`, `concordance_q01_q10.py` |
| Lire sur téléphone | Tableau de bord dédié en colonne unique, vérifié en 390 px | `capture.py` |

## 2. Prérequis

* Docker avec Compose, environ 8 Go de RAM disponibles pour la stack (Trino est plafonné à 3 Go, voir la section 9).
* Python 3.10 ou plus avec `requests` et `pyyaml` ; `playwright` et Chrome (ou Chromium) seulement pour les captures d'écran.
* Les données sources dans le lac (étapes 1 à 3). Sans les fichiers complets des décès et de FINESS, utiliser le mode `archive` (section 8).

```bash
python3 -m venv .venv && .venv/bin/pip install requests pyyaml playwright
```

## 3. Démarrer la stack et charger le lac

Depuis `scripts/` :

```bash
cp .env.example .env                       # DOCKER_GID : voir le commentaire du fichier (stat -c %g /var/run/docker.sock)
docker compose up -d --build               # 17 services
docker compose ps                          # tout doit être Up / healthy
docker exec chu-airflow-scheduler airflow dags unpause lakehouse_init
docker exec chu-airflow-scheduler airflow dags trigger lakehouse_init
# attendre 5/5 tâches, puis le DAG quotidien
docker exec chu-airflow-scheduler airflow dags unpause lakehouse_daily
docker exec chu-airflow-scheduler airflow dags trigger lakehouse_daily
```

Ne jamais déclencher un DAG encore en pause. Les paramètres de Trino (`config/trino/jvm.config`, `resource-groups.json`) plafonnent la mémoire et le nombre de requêtes simultanées : ils sont montés automatiquement par `docker-compose.yml`.

## 4. Rendre les trois couches visibles dans Trino

```bash
cd 08_soutenance/superset
python3 provision_lake.py
```

Le script est rejouable. Il crée :

* le schéma **raw** : tables externes en lecture seule sur les fichiers bruts du bucket `lake/raw` (décès, FINESS, hospitalisations, satisfaction 2019 et 2020), toutes colonnes en texte ; les colonnes sont lues dans `cloudera/ddl_raw_cloudera.sql`, qui reste donc dans le dépôt ;
* le schéma **archive** : résultats de la campagne sur données complètes du 30/10/2026 (q01, q07, contrôles qualité, benchmark), utilisés seulement quand les fichiers complets sont absents de la machine ;
* le schéma **vues_t** : référentiel des codes ISO des régions (pour les cartes) et diagnostics enrichis par chapitre CIM-10 ;
* il affiche les tables de **raw**, **silver**, **gold** et **archive** à la fin : c'est la preuve que toutes les couches sont lisibles.

Contrôle : `docker exec -i chu-trino trino --execute "SHOW SCHEMAS FROM hive"` doit lister `raw`, `silver`, `gold`, `archive`, `vues_t`.

## 5. Connexion Superset vers Trino (raw, silver, gold)

Interface : **Settings > Database Connections > + Database > Trino**, URI SQLAlchemy `trino://trino@trino:8080/hive` (le nom d'hôte `trino` est celui du réseau Docker, pas `localhost`). Cocher **Expose database in SQL Lab**. Nom : `Trino CHU`.

Dans SQL Lab, choisir la base `Trino CHU` : la liste déroulante des schémas montre `raw`, `silver`, `gold`, `archive`. Une requête comme `SELECT COUNT(*) FROM silver.f_consultation_detail` renvoie 1 027 157.

Le script fait la même chose par l'API : `python3 provision.py --only datasets` (connexion, datasets, métriques). Capture : `visuels/superset/sqllab_couches_du_lac.png`.

## 6. Modèle en étoile dans Superset

Superset ne dessine pas de relations comme la Vue Modèle de Power BI : une jointure est écrite dans un **dataset virtuel** (une requête SQL enregistrée, réutilisée par les graphiques). Le script crée :

* **18 datasets physiques** : 6 dimensions (`d_temps`, `d_patient`, `d_etablissement`, `d_localisation`, `d_diagnostic`, `d_professionnel`), 6 faits ou agrégats gold (`f_mesures`, `f_hospitalisation`, `f_deces_agg`, `f_satisfaction`, `f_consult_diag_mois`, `f_consult_prof_mois`), le fait silver `f_consultation_detail` et 5 tables raw. Chacun a des métriques de base (nombre de lignes, sommes).
* **12 datasets virtuels** qui servent de « vues étoile » :

| Dataset virtuel | Contenu | Métriques principales |
|---|---|---|
| `etoile_consultations` | `f_consultation_detail` joint à temps, patient, diagnostic, établissement, localisation | consultations, patients distincts, professionnels distincts, part rattachée (%), consultations avant la naissance |
| `etoile_hospitalisations` | `f_hospitalisation` joint aux mêmes dimensions | hospitalisations, durée moyenne (jours), patients hospitalisés, hospitalisations avant la naissance |
| `etoile_mesures` | `f_mesures` joint à établissement et localisation | consultations, hospitalisations, décès |
| `taux_hospitalisation_annuel`, `taux_hosp_sexe_age` | hospitalisations ÷ consultations sur la même fenêtre de dates (21/06/2015 au 19/06/2021) | taux pour 100 et pour 1 000 consultations |
| `etoile_satisfaction` | `f_satisfaction` et codes ISO des régions | score pondéré par les répondants, nombre de répondants |
| `deces_par_region`, `top_etablissements`, `fiabilite_donnees` | résultats de la campagne complète (mode `archive`) ou tables du lac (mode `live`) | décès, consultations, taux de fiabilité (%) |
| `distribution_activite_professionnels` | nombre de professionnels par nombre de consultations | professionnels |
| `volumes_du_lac` | nombre de lignes de chaque table des trois couches | lignes |
| `benchmark_partitionnement` | benchmark avant et après partitionnement | temps (ms) et facteur de volume lu |

Les 10 requêtes du benchmark (`04_benchmark/queries/q01` à `q10`) ont chacune leur équivalent : q01 `top_etablissements` ; q02 `etoile_consultations` par code diagnostic ; q03 `etoile_mesures` par année ; q04 `etoile_hospitalisations` par code diagnostic ; q05 `etoile_mesures` par sexe et tranche d'âge ; q06 `distribution_activite_professionnels` ; q07 `deces_par_region` ; q08 `etoile_satisfaction` ; q09 `etoile_mesures` par mois ; q10 `etoile_mesures` par région. `concordance_q01_q10.py` exécute chaque requête sur Trino et la même question sur Superset (API `chart/data`) et compare les résultats. Pour q10, la tranche d'âge de la requête est celle de référence du patient (`d_patient.tranche_age_ref`) alors que les datasets portent l'âge à la date de l'événement : la comparaison porte donc sur le total par région.

## 7. Tableaux de bord, filtres, histoire

```bash
python3 provision.py --mode archive        # ou --mode live (section 8)
```

Le script crée deux tableaux de bord à six onglets :

* **`chu-histoire`** (bureau) : http://localhost:8088/superset/dashboard/chu-histoire/
* **`chu-histoire-mobile`** (téléphone, colonne unique) : http://localhost:8088/superset/dashboard/chu-histoire-mobile/

| Onglet | Besoin du CHU traité | Graphiques |
|---|---|---|
| 1. Contexte | Cadre, volumes, preuve des trois couches | 4 grands chiffres (consultations, patients, hospitalisations, décès), tableau des volumes par couche |
| 2. Activité | Besoins 1, 2 et 6 | courbe mensuelle, chapitres CIM-10 par année, 10 diagnostics, distribution des professionnels, 20 établissements |
| 3. Hospitalisations | Besoins 3, 4 et 5 | taux annuel, volumes par année, sexe, tranche d'âge, taux par âge et sexe, chapitres, durée de séjour |
| 4. Mortalité et qualité | Besoins 7 et 8 | carte et barres des décès, carte et barres de la satisfaction, fiabilité, mises en garde |
| 5. Recommandations | Décisions | texte, chaque recommandation appuyée par un chiffre |
| 6. Annexe technique | Performances | benchmark (volume lu et temps de réponse) |

**Filtres natifs** (panneau de gauche) : Année, Sexe, Tranche d'âge, Région, Établissement, Chapitre CIM-10. Chaque filtre ne s'applique qu'aux graphiques pour lesquels il a un sens (par exemple l'année est exclue des graphiques déjà fixés sur 2019 ou 2020, et les contrôles qualité ne sont jamais filtrés).

**Choix de lecture** : titres avec l'unité entre parenthèses, axes nommés, axes des barres démarrant à zéro, échelle de couleur des cartes monotone du clair au foncé (« plus la teinte est foncée, plus la valeur est élevée »), mise en garde sur les dates antérieures à la naissance (62 hospitalisations, 24 145 consultations), mois partiels signalés, libellés raccourcis pour tenir sur téléphone.

### Storytelling : l'équivalent des signets

Superset n'a pas de signets Power BI ; l'équivalent est le **lien permanent** (permalink) : il enregistre l'onglet affiché **et** l'état des filtres. `provision.py` en crée 9 par tableau de bord et les écrit dans `PERMALIENS.md` (les clés changent à chaque provisionnement : relire le fichier avant de présenter).

| Étape | Message à dire | Onglet et filtre |
|---|---|---|
| 1 | Le CHU veut piloter avec ses données ; 1 027 157 consultations, trois couches visibles | Contexte |
| 2 | Activité irrégulière, sans concentration | Activité |
| 3 | Zoom sur 2019 : les 20 premiers établissements, 146 à 371 consultations | Activité, Année = 2019 |
| 4 | Hospitalisations rares, plutôt féminines, séjour d'environ 15 jours | Hospitalisations |
| 5 | Zoom sur les femmes (57 % des hospitalisations) | Hospitalisations, Sexe = F |
| 6 | Décès liés à la taille des régions, satisfaction homogène, fiabilité limitée | Mortalité et qualité |
| 7 | Zoom sur l'Île-de-France | Mortalité et qualité, Région = Île-de-France |
| 8 | Recommandations chiffrées | Recommandations |
| 9 | Annexe : partitionnement et benchmark | Annexe technique |

Pendant l'oral : ouvrir le lien de l'étape, parler, passer au suivant. Aucune manipulation de filtre en direct n'est nécessaire (elle reste possible pour répondre à une question).

## 8. Modes de données : `live` et `archive`

| Mode | Source des décès, établissements et fiabilité | Quand l'utiliser |
|---|---|---|
| `live` | tables du lac (`gold.f_deces_agg`, `gold.d_etablissement`, contrôles qualité calculés) | machine qui dispose des fichiers complets (décès INSEE, FINESS) |
| `archive` | schéma `archive`, rempli depuis `ref/archive_donnees_completes/` (campagne du 30/10/2026) | machine avec des extraits seulement |

Les graphiques concernés portent la mention « campagne complète du 30/10/2026 » en mode `archive`. Les consultations, hospitalisations, patients et la satisfaction sont toujours lus dans le lac réel. Changer de mode : `python3 provision.py --mode live` puis `python3 verify.py`. Le script `verify.py` lit le mode dans `registry.json`.

## 9. Vérifier

```bash
python3 verify.py                 # chaque nombre affiché comparé à une requête Trino ou aux CSV de la campagne complète
python3 concordance_q01_q10.py    # q02 à q06, q09 et q10 : requête Trino contre dataset Superset
```

`verify.py` s'arrête au premier écart et écrit `verification.txt` (118 contrôles en mode `archive`). `concordance_q01_q10.py` écrit `concordance_q01_q10.txt`.

Sur une machine modeste, Trino peut être arrêté par le noyau faute de mémoire (code 137). Les réglages fournis évitent cela : tas de 1 280 Mo, plafond de conteneur de 3 Go, 2 requêtes simultanées. Si Superset affiche « Bad request » ou « Trino server is still initializing », attendre que `docker inspect -f '{{.State.Health.Status}}' chu-trino` soit `healthy` et recharger.

## 10. Captures d'écran (bureau 1440 px, téléphone 390 px)

```bash
.venv/bin/python capture.py ../visuels/superset      # CAPTURE_ONLY=mobile ou bureau pour une seule série
```

Fichiers produits dans `visuels/superset/` : `bureau_*` (un par onglet), `telephone_*` (un par onglet, largeur 390 px, aucun défilement horizontal), `etape_*` (une capture par étape de l'histoire, filtres appliqués), `bureau_filtres_natifs.png`, `sqllab_couches_du_lac.png`, `liste_des_datasets.png`. Le script contrôle qu'aucun graphique n'affiche d'erreur (compte écrit dans `resume_capture.json`).

## 11. Exporter et importer (pour reconstruire sans rejouer le script)

```bash
python3 export_bundle.py                         # écrit export/chu_superset_<mode>.zip
python3 import_bundle.py export/chu_superset_archive.zip   # sur un Superset vierge (connexion Trino CHU déjà créée)
python3 provision.py --only permalinks           # recréer les liens permanents après un import
```

## 12. Pièges rencontrés (et leur correction)

| Symptôme | Cause | Correction (dans le code) |
|---|---|---|
| Carte avec des couleurs de catégories | le thème de couleurs du tableau de bord s'applique à la carte | `color_scheme` vide sur le tableau de bord et sur la carte, échelle `dark_blue` |
| Île-de-France reste sans couleur sur la carte | dans le fichier de contours de Superset, le code ISO de l'Île-de-France se termine par une tabulation (`FR-IDF` suivi d'une tabulation) | la table `vues_t.ref_region_iso` reproduit ce code à l'identique |
| Graphiques trop larges sur téléphone | Superset calcule la largeur en JavaScript, pas en CSS | tableau de bord mobile séparé en une colonne |
| Onglet non changé en test automatique sur téléphone | le clic simple n'est pas pris en compte | `dispatch_event("click")` dans `capture.py` |
| Barres horizontales : maximum en bas | la première catégorie est en bas | tri croissant |
| Valeurs coupées à droite des barres | l'axe s'arrête à la plus grande valeur | borne maximale fixée sur l'axe |
| Filtres et liens permanents perdus au redémarrage | cache mémoire par défaut | cache persistant dans `superset_config.py` |
| Trino arrêté sous charge | `MaxRAMPercentage=80` dans l'image, `JAVA_OPTS` ignoré | `config/trino/jvm.config`, `mem_limit`, groupes de ressources |

## Annexe (optionnelle) : parcours Cloudera et Power BI

Le dossier `cloudera/` et le guide `GUIDE_POWERBI_CLOUDERA.md` restent dans le dépôt pour mémoire : export snappy des couches, DDL Hive 1.1, chargement de la VM Cloudera Quickstart, connexion ODBC et modèle Power BI. **Ils ne servent pas à la démonstration.** Seul `cloudera/ddl_raw_cloudera.sql` est lu par `provision_lake.py` (liste des colonnes de la couche raw).
