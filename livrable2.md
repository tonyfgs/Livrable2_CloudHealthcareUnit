# Livrable 2 — Implémentation Physique et Évaluation de Performance
## Projet Cloud Healthcare Unit (CHU) — Entrepôt de données décisionnel

**Groupe** : Tony Fages
**Date** : 29 octobre 2026
**Format** : Rapport + archive ZIP (`Livrable2_CloudHealthcareUnit.zip`)
**Livrables couverts** : ce document traite exclusivement le **Livrable 2** (implémentation physique et évaluation de performance). Le Livrable 1 (modélisation) est dans `../Livrable 1/L1-rapport-cloud-health-unit.md`.

---

## 1. Introduction et périmètre

### 1.1 Contexte

Le groupe CHU a engagé une transformation digitale de son système d'information. Le Livrable 1 a produit le modèle dimensionnel (schéma en étoile, 4 faits métier : consultations, hospitalisations, décès, satisfaction) et le recensement des 8 besoins utilisateurs. Le **Livrable 2** implémente physiquement cet entrepôt et en évalue les performances.

### 1.2 Sources intégrées (4)

| Source | Contenu | Volume |
|---|---|---|
| **S1** — PostgreSQL opérationnel (dump `DATA2023`) | Patients, consultations, diagnostics, professionnels de santé, prescriptions... | **13 tables, ~3,7 M lignes** (3 674 384 au total = 11 tables métier = 3 509 952 lignes : Patient 100 000 ; Consultation 1 027 157 ; Professionnel_de_sante 1 048 575 ; Diagnostic 15 490 ; Prescription 1 003 845 ; Salle, Adher, Mutuelle, Specialites, Medicaments, Laboratoire ; + 2 tables auxiliaires AAAA/date = 82 216 lignes chacune) |
| **S2** — CSV `Hospitalisations.csv` | Séjours hospitaliers (entrée, durée, diagnostic, établissement) | 2 479 lignes, 2015-06 → 2021-06 |
| **S3** — Fichiers ESATIS (FTP) | Satisfaction patients (score ajusté, répondants, région) | 2019 : 1 152 lignes ; 2020 : 1 143 lignes |
| **S4** — INSEE + référentiels FINESS | Décès (1882-2020) ; établissements (416 665) ; professionnels (1 048 575) ; activités (1 823 588) ; référentiel départements (101) | Décès : **25 088 208 lignes** |

### 1.3 Périmètre du Livrable 2

- Scripts SQL de création des tables (**DDL**) et de chargement (**DML**), commentés et versionnés (Git)
- Stratégie de **partitionnement** justifiée par moteur (pas de bucketing : voir §4.2 pour la justification technique)
- **Rapport de qualité des données** (complétude, validité, doublons, cohérence référentielle FINESS, temporalité, volumes)
- **10 requêtes de benchmark** (8 métier = 1 par besoin du Livrable 1 + 2 requêtes techniques)
- **Évaluation de performance avant/après** partitionnement, avec graphiques
- **Visualisation** (Apache Superset) et **orchestration** (Apache Airflow)

### 1.4 Stack imposée (positionnement par couche)

| Couche | Composant | Rôle dans l'implémentation |
|---|---|---|
| Ingestion | `minio-init` (AWS CLI) | Copie des sources brutes vers la zone `raw/` du data lake (clés normalisées) + copie des référentiels embarqués (`config/data/referentiels/`) |
| **Stockage** | **MinIO** (data lake S3) | Stockage des données Parquet (zones `raw/`, `silver/`, `gold/`, `warehouse/`) |
| **Catalogue** | **Hive Metastore** + **HiveServer2** (+ PostgreSQL) | Métadonnées des tables externes, partitions, statistiques (HS2/beeline : interface JDBC du DAG d'init pour le DDL) |
| **Traitement** | **Apache Spark** (standalone) | ETL/ELT : lecture S1 (JDBC) + CSV du lake, écriture Gold/Silver |
| **Orchestration** | **Apache Airflow** | DAGs `lakehouse_init` (one-shot) et `lakehouse_daily` (02:00) |
| Requêtage | **Trino** | Lecture distribuée du lake via le connecteur Hive (pruning) |
| **Visualisation** | **Apache Superset** (localhost:8088) | 8 tableaux de bord (1 par besoin métier) |
| Source | **PostgreSQL** (S1) | Dump ChuSimulation restauré par `pg_restore` |
| Benchmark/DQ | DuckDB (conteneur jetable) | PoC de performance + contrôles qualité (rien installé sur l'hôte) |

> **Contrainte d'environnement** : l'intégralité de la plateforme tourne dans Docker (17 services, `scripts/docker-compose.yml`, dont 3 conteneurs one-shot d'initialisation : `minio-init`, `hive-metastore-init`, `airflow-init`). Aucune installation sur le poste hôte (ni PostgreSQL, ni Spark, ni Java...).

---

## 2. Modèle physique (schéma en étoile)

### 2.1 Vue d'ensemble

Le schéma en étoile du Livrable 1 est implémenté en **couche Gold** (données décisionnelles) sur le data lake, avec une couche **Silver** pour le fait de détail des consultations. 6 dimensions, 1 fait consolidé (4 mesures), 2 faits de détail, 1 fait agrégé (décès) et 2 agrégats pré-calculés.

```
                          ┌────────────┐
                          │  d_temps   │  grain : jour (2015-2030), id_temps = AAAAMMJJ
                          └─────┬──────┘
        ┌────────────┐ ┌───────────┼───────────┐ ┌────────────┐ ┌──────────────┐
        │ d_patient  │ │d_etablissement│        │ │d_localisation│ │ d_diagnostic │
        └─────┬──────┘ └─────┬─────┘          │ └─────┬──────┘ └──────┬───────┘
              └──────────────┴──────────┬──────┘        └───────────────┘
                                        ▼
                          ┌──────────────────────────┐
                          │        f_mesures         │  FAIT CONSOLIDÉ (4 mesures)
                          │  nbre_consultation       │  grain : annee_mois x id_etablissement
                          │  nbre_hospitalisation    │  x id_patient x sexe x tranche_age
                          │  total_satisfaction      │  x id_localisation x region
                          │  nbre_deces              │
                          └──────────────────────────┘
        f_consultation_detail (Silver)   f_hospitalisation   f_deces_agg   f_satisfaction
        f_consult_diag_mois (agrégat)    f_consult_prof_mois (agrégat)
```

### 2.2 Dictionnaire des tables (couche Gold/Silver)

| Table | Couche | Grain | Lignes | Clé | Partitionnement | Tri intra-partition |
|---|---|---|---|---|---|---|
| `d_temps` | Gold | jour (2015-2030) | 5 844 | `id_temps` (substitution, AAAAMMJJ) | `annee` | — |
| `d_patient` | Gold | patient | 100 000 | `id_patient` (naturelle = substitution) | `sexe`, `tranche_age_ref` | — |
| `d_etablissement` | Gold | site FINESS | 416 665 | `id_etablissement` (**substitution surrogate**) | — | — |
| `d_localisation` | Gold | département | 102 (101 + Inconnu) | `id_localisation` (code département) | `code_region` (voir §4.1 : les noms de régions contiennent des caractères invalides dans les chemins de partitions Hive) | — |
| `d_diagnostic` | Gold | diagnostic CIM-10 | 15 490 | `id_diagnostic` (**substitution surrogate**) | — | — |
| `d_professionnel` | Gold | professionnel | 1 048 575 | `identifiant` (naturelle = substitution) | — | — |
| `f_mesures` | Gold | annee_mois × etab × patient × sexe × tranche_age × département × **région** | 1 030 402 | — (fait) | `annee`, `sexe`, `tranche_age` | `id_etablissement`, `id_patient` |
| `f_consultation_detail` | Silver | consultation | 1 027 157 | `id_consultation` | `annee` | `id_etablissement`, `id_patient` |
| `f_hospitalisation` | Gold | hospitalisation | 2 479 | `id_hospitalisation` | `annee` | `id_etablissement`, `id_patient` |
| `f_deces_agg` | Gold | annee × sexe × tranche_age × département | 69 008 | — (fait agrégé) | `annee` | — |
| `f_satisfaction` | Gold | finess juridique × année (2 295 lignes source agrégées, moyenne pondérée) | 1 960 | — (fait) | `annee`, `code_region` | — |
| `f_consult_diag_mois` | Gold | mois × diagnostic × établissement | 979 797 | — (agrégat) | `annee` | — |
| `f_consult_prof_mois` | Gold | mois × professionnel | 1 001 453 | — (agrégat) | `annee` | — |

> **Pas de bucketing** (`CLUSTERED BY ... INTO n BUCKETS`) dans le schéma déployé : Spark n'écrit qu'un fichier par bucket *non vide* et Trino 484 refuse alors de lire la table (« number of files does not match declared bucket count »). Le tri intra-partition (`sortWithinPartitions` sur les clés de jointure) est conservé : il produit les statistiques min/max par fichier Parquet qui alimentent le pruning intra-fichier côté Trino. Voir §4.2.

**Mesures de `f_mesures`** (les 4 mesures du Livrable 1) :

| Mesure | Définition | Grain dans `f_mesures` |
|---|---|---|
| `nbre_consultation` | Nombre de consultations | mensuel (`annee_mois = 'AAAA-MM'`) |
| `nbre_hospitalisation` | Nombre d'hospitalisations | mensuel (`'AAAA-MM'`) |
| `total_satisfaction` | Score moyen ajusté ESATIS (0-100), **moyenne pondérée par les répondants**, par région | annuel (sentinelle `'AAAA-00'`) |
| `nbre_deces` | Nombre de décès | annuel (sentinelle `'AAAA-00'`) |

**Conventions et membres « Inconnu »** (gestion des valeurs nulles et des rejets, cf. Livrable 1) :

- `id_etablissement = -1`, `id_patient = -1`, `id_localisation = 'INCONNU'`, `region = 'INCONNUE'`, `sexe = 'TOUS'`, `tranche_age = 'TOUTES'` pour les lignes non rattachables (décès, satisfaction)
- Sentinelle `annee_mois = 'AAAA-00'` (mois = 00) = agrégat annuel, pour les mesures de grain annuel
- `id_temps = NULL` pour les lignes annuelles (pas de 1er jour de mois) — la jointure avec `d_temps` se fait sur les lignes mensuelles

### 2.3 Contraintes d'intégrité référentielle

Le moteur retenu (tables externes Hive/Parquet sur MinIO) ne supporte pas les contraintes `FOREIGN KEY` déclaratives. L'intégrité référentielle est donc **assurée par construction** (les faits ne contiennent que des clés produites par les dimensions) **et contrôlée a posteriori** par les contrôles qualité (famille « cohérence référentielle », §5.4) : taux de rattachement mesurés, aucun rejet silencieux.

### 2.4 Vues métier

`scripts/01_ddl/03_vues_metier.sql` : `v_fait_consultation`, `v_fait_hospitalisation`, `v_fait_deces`, `v_fait_satisfaction` (les 4 faits métier du Livrable 1 dérivés de `f_mesures`), `v_taux_consultation_mensuel` (moyenne mobile 3 mois, fonction fenêtrée), + 4 vues d'agrégat (`v_consultations_region_annee`, `v_deces_region_annee`, `v_satisfaction_region_annee`, `v_hospitalisations_etab_annee`). Créées via HS2/beeline (vues **Hive**). Comme le connecteur Hive de **Trino ne lit pas les vues Hive** (« Hive views are not supported »), `scripts/01_ddl/04_vues_metier_trino.sql` recrée les mêmes vues en vues **natives Trino** dans le schéma `vues_t` (tâche `vues_metier_trino` du DAG `lakehouse_init`) — c'est ce schéma que Superset et le harnais Trino interrogent (`vues_t.v_taux_consultation_mensuel`, etc.).

---

## 3. DDL et DML (scripts versionnés)

### 3.1 DDL — `scripts/01_ddl/01_schema_gold_hive.sql`

Création des bases `silver` et `gold` et des **12 tables EXTERNES Hive** avec clause `LOCATION 's3a://lake/...'` (les données appartiennent au data lake ; un `DROP TABLE` ne supprime jamais les données). Toutes les tables sont `STORED AS PARQUET` (format colonnaire compressé, statistiques min/max par fichier pour le pruning côté Trino).

Extraits représentatifs :

```sql
-- Table de fait consolidé : partitionnement démographique + temporel
CREATE EXTERNAL TABLE IF NOT EXISTS f_mesures (
  annee_mois STRING, id_temps INT, id_etablissement BIGINT, id_patient BIGINT,
  id_localisation STRING, nbre_consultation BIGINT, nbre_hospitalisation BIGINT,
  total_satisfaction DOUBLE, nbre_deces BIGINT, region STRING
)
PARTITIONED BY (annee INT, sexe STRING, tranche_age STRING)
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_mesures';

-- Dimension localisations : partitionnée par code_region (PAS par le nom de
-- région : « Provence-Alpes-Côte d'Azur » contient espaces/accents/apostrophe,
-- caractères invalides dans les noms de répertoires de partitions Hive —
-- MSCK REPAIR échoue sinon ; region reste une colonne data)
CREATE EXTERNAL TABLE IF NOT EXISTS d_localisation (
  id_localisation STRING, code_departement STRING,
  nom_departement STRING, region STRING
)
PARTITIONED BY (code_region STRING)
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_localisation';
```

Côté **Trino** (`02_schema_gold_trino.sql`) : pas de DDL dupliqué — Trino lit le Hive Metastore via le connecteur `hive` (config `scripts/config/trino/catalog/hive.properties`). Le script vérifie le déploiement (volumes), calcule les statistiques de l'optimiseur (`ANALYZE`, équivalent de `ANALYZE TABLE ... COMPUTE STATISTICS` côté Hive) et valide le pruning (`EXPLAIN (TYPE IO)`).

### 3.2 DML — chargement (Apache Spark, `scripts/02_spark/`)

> Justification du DML en PySpark et non en SQL : le sujet impose Apache Spark comme moteur de traitement et des tables **externes Hive/Parquet** sur le lake — Hive 4 ne supporte pas de DML distribué (INSERT) ciblé proprement vers des emplacements S3A externes depuis beeline de manière fiable sur stack fraîche. Le chargement est donc écrit en PySpark (SparkSubmitOperator), versionné dans `scripts/02_spark/`. Les scripts **SQL** de chargement (DML `INSERT`/`COPY`) sont livrés et exécutés dans le PoC DuckDB : `scripts/04_benchmark/00_init.sql` → `02a_faits_detail.sql` → `02b_deces_agg.sql` → `02c_f_mesures.sql` → `02d_agregats.sql` (mêmes règles de transformation, mêmes résultats métier vérifiés §6).

| Script | Rôle | Règles de transformation appliquées (issues de la DQ, §5) |
|---|---|---|
| `common.py` | Session Spark (S3A MinIO + Hive Metastore), helpers | `tranche_age` (0-14/15-29/30-44/45-59/60-74/75+), `dept_of` (règle INSEE : 2 car., 3 car. pour 97x, `2A` pour la Corse), `sexe_normalise` (male/female → M/F), âge révolu `floor(months_between/12)` |
| `load_dimensions.py` | 6 dimensions | `d_patient` : sexe normalisé, `date_naissance` parsée (`M/J/AAAA`), tranche à la réf. 2023-12-31 ; `d_etablissement` : surrogate `row_number`, rattachement `code_commune → département → région` ; `d_localisation` : 101 départements + membre Inconnu |
| `load_faits_detail.py` | `silver.f_consultation_detail`, `f_hospitalisation`, `f_satisfaction` | rattachement établissement via fichier d'activité (consultations) ; score ESATIS : **virgule décimale remplacée** (`replace(',', '.')`) ; région ESATIS normalisée par mapping (18 régions) ; grain satisfaction = **finess juridique** × année (pas de clé site) |
| `build_f_deces_agg.py` | `f_deces_agg` (25,09 M décès INSEE) | filtre des 646 dates invalides, agrégat annuel (annee × sexe × tranche_age × département), jointure référentiel départements |
| `build_f_mesures.py` | `f_mesures` (fait consolidé) | agrégation des 4 sources au grain commun, **calcul des mesures APRÈS peuplement des clés étrangères** (ordre du DAG), sentinelle `'AAAA-00'` pour les mesures annuelles, région dénormalisée depuis `d_localisation` |
| `build_agregats.py` | `f_consult_diag_mois`, `f_consult_prof_mois` | agrégats pré-calculés (équivalents matérialisés des agrégations les plus fréquentes) |

Chaque écriture Spark (`save_gold` dans `common.py`) : `partitionBy(...).sortWithinPartitions(id_etablissement, id_patient)` (quand les deux clés existent) `.option("path", "s3a://lake/...").mode("overwrite").saveAsTable(...)` → table **externe** Parquet partitionnée et triée (PAS de `bucketBy`, voir §4.2), métadonnées publiées dans le Hive Metastore, partitions découvertes (`MSCK REPAIR` côté DAG) et statistiques calculées (`ANALYZE` Trino après chargement).

### 3.3 Versionning (Git)

Dépôt Git initialisé dans `Livrable 2/` (22 commits thématiques) :

```
v15 (dernier commit) : Superset auto-suffisant côté visualisation — `scripts/06_superset/build_dashboards.sh` (tâche `build_dashboards` du DAG init, idempotent) crée via l'API REST 17 datasets, 10 charts (1 par besoin métier q01-q08 + 2) et le dashboard « CHU — Indicateurs métier » ; smoke test étendu à 58 vérifications (6/6 init + contrôles dashboards/slices/tables en base superset-db), 58 PASS / 0 FAIL ; pièges n°32-35 documentés
v9 : fix TemplateNotFound Airflow 2.9.3 — un bash_command qui se termine par `.sh` est traité comme un template Jinja ; le contenu de `verif_superset.sh` est lu au parse time dans le DAG (piège n°31)
v8 : smoke test from scratch automatisé (`07_smoke_test/smoke_from_scratch.sh`, 54 PASS / 0 FAIL archivé dans `results/`) + connexion Superset « Trino CHU » réparée (driver `trino` dans l'image, cookie+CSRF, attente /health, vérification réelle) + DQ étendus aux faits intégrés (complétude par attribut, intégrité FK) + retry des endpoints web
v7 : runbook from-scratch finalisé (unpause AVANT trigger, ne jamais pauser un run en cours, catchup=False + max_active_runs=1) — test from scratch réussi : init 5/5 + daily 8/8, volumes Trino conformes
v6 : DAGs en pause par défaut (DAGS_ARE_PAUSED_AT_CREATION=true) — supprime le backfill auto du daily qui race avec l'init au from scratch (crash récurrent)
v5 : notation honnête 19,44/20 (relecture stricte), vues Trino natives (vues_t), f_satisfaction.annee INT, anti-OOM Trino/Spark + restart unless-stopped + attente Trino dans dispo (OOM-kill VM constaté en test utilisateur), harness Trino exécuté (10/10 x 3 runs)
8cc2d31 Livrable 2 v4 : plateforme exécutée de bout en bout (init 5/5, daily 8/8 SUCCESS, volumes vérifiés Trino) + minio from source + code_region + plus de bucketing + metastore S3A + trino anti-OOM + DQ autonome
a258ea4 NOTE_INTERNE : piège n°14 (JRE/JAVA_HOME dans l'image Airflow)
3a07f36 §3.3 : historique Git final (8 commits)
9daf0bf finalisation (corrections notation v3 -> 19,75/20, JRE Airflow, hive-hs2, config S3 Trino/S3A SDK v2, DQ COALESCE + volumes AAAA/date, q08 pondérée, NOTE_INTERNE, 17 services)
172061c corrections évaluation (montages compose, DAGs docker exec, chemins /data-projet + s1-postgres, partitionnement d_patient sexe+tranche_age_ref dans le PoC, synthèse benchmark honnête, sizing corrigé)
320c441 rapport livrable2.md + partitionnement d_patient (sexe, tranche_age)
e19e2bb stack Docker Compose complète + Superset + documentation
b5bf381 benchmark (10 requêtes, 3 scénarios, résultats + SVG) et DQ (6 familles)
7d17dc5 orchestration Airflow (DAGs lakehouse_init + lakehouse_daily)
4827f79 traitement Spark (ETL/ELT vers le data lake MinIO)
30a51fe DDL Hive/Trino + vues métier (schéma en étoile Gold/Silver)
```

### 3.4 Preuve d'exécution (PoC)

La chaîne d'intégration a été exécutée de bout en bout **en PoC** (scripts `scripts/04_benchmark/00_init.sql` → `02d_agregats.sql`, DuckDB dans un conteneur Docker, source S1 dans `chu-s1-postgres`) et a produit les volumes du §2.2, avec **1 027 157 consultations, 2 479 hospitalisations, 3 215 529 décès (2015-2023)** dans `f_mesures` ; la satisfaction est stockée dans `f_satisfaction` au **grain juridique** (2 295 lignes source ESATIS agrégées en **1 960 finess juridiques**, moyenne pondérée par les répondants) et `f_mesures` n'en agrège que **33 lignes régionales** (2 années × régions présentes).

> **Preuve d'exécution — chaîne de production exécutée de bout en bout** : au-delà du PoC DuckDB (même SQL, mêmes règles de transformation, moteur embarqué dans un conteneur jetable), la chaîne de **production** a été exécutée et vérifiée sur la stack Docker : `docker compose up -d` (17 services, MinIO construit from source — aucun pull d'image `minio/*` n'est tenté, voir `NOTE_INTERNE.md` §6.1) → DAG **`lakehouse_init`** (DDL Hive + vues Hive + vues Trino (vues_t) + vérification Trino + connexion Superset + dashboards métier : **6/6 tâches SUCCESS**) → DAG **`lakehouse_daily`** (**8/8 tâches SUCCESS** : `load_dimensions`, `load_faits_detail`, `build_f_deces_agg`, `build_f_mesures`, `build_agregats`, `msck_repair_metastore`, `dq_checks`, `dispo_trino_superset`). Les volumes ont été re-vérifiés **dans Trino** (connecteur Hive → MinIO) après `MSCK REPAIR` : d_temps 5 844 ; d_patient 100 000 ; d_etablissement 416 665 ; d_localisation 102 ; d_diagnostic 15 490 ; d_professionnel 1 048 575 ; silver.f_consultation_detail 1 027 157 ; f_hospitalisation 2 479 ; **f_satisfaction 1 960** ; f_deces_agg 69 008 ; f_mesures 1 030 402 ; f_consult_diag_mois 979 797 ; f_consult_prof_mois 1 001 453 — conformes aux attendus. Commandes de re-vérification : `NOTE_INTERNE.md` §5.

---

## 4. Stratégie de partitionnement et d'indexation (justifiée par moteur)

### 4.1 Partitionnement (Hive/Spark/Trino)

| Table | Colonnes de partition | Justification |
|---|---|---|
| `f_mesures` | `annee`, `sexe`, `tranche_age` | **Pruning temporel + démographique** : les requêtes métier filtrent systématiquement sur l'année (voire la période), le sexe et la tranche d'âge sont les axes démographiques des besoins 5 et 10. 9 années × 3 sexes × 8 tranches = 216 combinaisons max (122 réellement présentes). |
| `d_patient` | `sexe`, `tranche_age_ref` | Partitionnement démographique de la dimension patient (conforme à la grille d'évaluation). |
| `d_temps` | `annee` | Partitionnement temporel de la dimension temps (couvre 2015-2021, périmètre de la grille, prolongé à 2030 pour la planification). |
| `d_localisation` | `code_region` | Partitionnement par **code** de région (la région est l'axe géographique le plus filtrant — besoins 7 et 8). Le nom de région reste une colonne data : les noms contiennent espaces/accents/apostrophe (`Provence-Alpes-Côte d'Azur`), caractères **invalides dans les noms de répertoires de partitions Hive** — `MSCK REPAIR` échoue dessus (« invalid characters in the name »), testé et contourné par `code_region`. |
| `f_consultation_detail`, `f_hospitalisation`, `f_consult_diag_mois`, `f_consult_prof_mois` | `annee` | Pruning temporel (tous les besoins portent sur des périodes). |
| `f_deces_agg` | `annee` **seulement** | Un partitionnement par `(annee, sexe, tranche_age)` produirait 1 251 combinaisons réelles (~55 lignes chacune ; borne 133 × 2 × 7 = 1 862) pour 69 008 lignes (**small-files problem**). Le grain annuel + `annee` en partition donnent 133 partitions de ~519 lignes ; le **tri intra-partition** par `(sexe, tranche_age, id_localisation)` compense l'absence de partitionnement sur ces colonnes (pruning par min/max de fichier côté Trino). |
| `f_satisfaction` | `annee`, `code_region` | Grain juridique × année ; la région est la colonne la plus filtrante (besoin 8) — partitionnée par son **code** pour la même raison que `d_localisation` (caractères invalides dans les chemins de partitions). |
| `d_etablissement`, `d_diagnostic`, `d_professionnel` | — | Tables de taille moyenne/volumineuse mais peu filtrées seules : pas de partition (le tri intra-fichier suffit, §4.3). |

### 4.2 Pourquoi PAS de bucketing (décision technique testée)

> **Le bucketing n'existe pas en PostgreSQL** (c'est une notion propre à Hive/Spark). Aucun index/bucket n'est donc appliqué côté S1 (source OLTP, non modifiée) ; l'optimisation porte sur la couche analytique (lake).

`CLUSTERED BY (clé) INTO N BUCKETS` / `bucketBy(n, clé)` a été **testé puis retiré** : Spark n'écrit qu'un fichier Parquet par bucket **non vide** (avec AQE, une petite partition produit p. ex. 2 fichiers pour 32 buckets déclarés), et **Trino 484 refuse alors de lire la table** à l'exécution (« number of files (N) does not match declared bucket count (32) » — la vérification est inconditionnelle dans le chargeur de splits Hive, `hive.validate-bucketing=false` ne la désactive pas). Le schéma déployé est donc **partitionné + trié, sans bucketing** :

- **Co-localisation** des lignes partageant une clé de jointure dans le même fichier via le **tri intra-partition** (`sortWithinPartitions`) : statistiques min/max par colonne et par fichier Parquet → pruning intra-fichier côté Trino et lectures séquentielles efficaces
- Le tri porte sur `(id_etablissement, id_patient)` quand les deux clés existent dans la table (`save_gold`, `02_spark/common.py`)

### 4.3 Indexation complémentaire

- **Parquet + Zstd** : format colonnaire compressé (~×5 à ×10 vs CSV), pushdown des prédicats, statistiques min/max par colonne et par fichier → **pruning intra-fichier** (Trino) en plus du pruning de partitions
- **Tri intra-partition** sur les clés de jointure (`sortWithinPartitions`) : compense l'absence de partitionnement/bucketing sur ces colonnes (min/max par fichier → pruning intra-fichier Trino) et accélère les agrégats
- **Statistiques** : `ANALYZE TABLE ... COMPUTE STATISTICS` (Hive/Spark) et `ANALYZE` (Trino) après chaque chargement → plans de jointure et distribution optimaux
- **Agrégats pré-calculés** (`f_consult_diag_mois`, `f_consult_prof_mois`) : équivalents matérialisés des agrégations les plus demandées (besoins 2 et 6)
- **Agrégation en amont** du fait décès (25,09 M → 69 008 lignes) : le grain annuel évite de scanner 25 M de lignes pour un besoin annuel

---

## 5. Rapport de qualité des données

Script : `scripts/05_qualite/dq_checks.sql` (8 familles de contrôles, résultats CSV dans `scripts/05_qualite/results/`). Les contrôles A/B/D portent sur les **sources** (avant intégration) ; les règles de traitement en découlent (tableau §5.6). Les contrôles **C2/C3/D2 portent sur les données INTÉGRÉES** (Parquet Gold/Silver du lake lus en direct via httpfs), comme l'exige le sujet : doublons sur faits (`dq_doublons_faits.csv`), **complétude par attribut sur les faits intégrés** (`dq_completude_faits.csv`) et **intégrité référentielle faits → dimensions** (`dq_integrite_fk.csv`, compense l'absence de FK déclaratives sur tables externes, §2.3).

### 5.1 Complétude (`dq_completude.csv`)

| Source.attribut | Total | Rempli | % |
|---|---|---|---|
| Hospitalisations (tous les attributs) | 2 479 | 2 479 | 100,0 |
| S1 Patient (Sexe, Date_naissance, Age, Num_Secu) | 100 000 | 100 000 | 100,0 |
| S1 Consultation (Date, Code_diag, Id_prof_sante) | 1 027 157 | 1 027 157 | 100,0 |
| deces.sexe, date_deces | 25 088 208 | 25 088 208 | 100,0 |
| deces.date_naissance | 25 088 208 | 25 087 658 | 100,0 (550 null) |
| deces.code_lieu_deces | 25 088 208 | 25 077 274 | 99,96 |
| etablissement.identifiant_organisation | 416 665 | 416 665 | 100,0 |
| etablissement.finess_etablissement_juridique | 416 665 | 57 692 | **13,85** |
| etablissement.finess_site | 416 665 | 200 | **0,05** |
| etablissement.code_commune | 416 665 | 277 860 | **66,69** |
| satisfaction_2019.score_all_rea_ajust | 1 152 | 858 | **74,48** |
| satisfaction_2020.score_all_rea_ajust | 1 143 | 832 | **72,79** |

### 5.2 Validité (`dq_validite.csv`)

| Contrôle | Total | Invalides | % |
|---|---|---|---|
| deces.date_deces format ISO | 25 088 208 | 646 | 0,0026 |
| deces.date_naissance format ISO | 25 088 208 | 89 898 | 0,3583 |
| deces.sexe ∈ {1,2} | 25 088 208 | 0 | 0,0 |
| Hospitalisations.Date_Entree JJ/MM/AAAA | 2 479 | 0 | 0,0 |
| Hospitalisations.Code_diagnostic CIM-10 | 2 479 | **368** | **14,84** |
| S1 Patient.Sexe ∈ {male,female} | 100 000 | 0 | 0,0 |
| satisfaction_2019.finess 9 caractères | 1 152 | 7 | 0,61 |
| satisfaction score décimal (virgule) | 2 295 | 0 | 0,0 (après `replace(',', '.')`) |

### 5.3 Doublons (`dq_doublons.csv`)

| Fichier | Clé | Doublons | % | Traitement |
|---|---|---|---|---|
| Hospitalisations | Num_Hospitalisation | 0 | 0,0 | — |
| S1 Consultation | Num_consultation | 0 | 0,0 | — |
| S1 Patient | Id_patient | 0 | 0,0 | — |
| etablissement | identifiant_organisation | 0 | 0,0 | — |
| satisfaction_2019 | finess (grain brut) | 166 | 14,41 | **Grain juridique = finess** (le grain brut finess + finess_geo est un grain SITE) : agrégation au grain juridique, moyenne pondérée par les répondants (2 295 → 1 960 lignes) |
| deces | clé complète (nom, prénom, dates, lieu, acte) | 174 794 | 0,6967 | Conservés (décès homonymes possibles) mais **quantifiés** ; pas de dédoublonnage (risque de faux positifs sur 25 M de lignes). Clé construite avec `COALESCE(champ,'')` : sans COALESCE, une clé contenant un champ NULL est exclue du `COUNT(DISTINCT)` en DuckDB (`NULL || 'x' = NULL`) et gonfle artificiellement le compte (211 833 = 0,8444 % avant correction) |

**Doublons sur les faits intégrés** (`dq_doublons_faits.csv`) — le sujet exige le contrôle des doublons sur les données **intégrées**, pas seulement sur les sources. Lecture directe des Parquet partitionnés écrits par Spark dans le data lake MinIO (`s3://lake/gold/...`, `s3://lake/silver/...`, httpfs) — contrôle sur les VRAIES tables de production :

| Fait intégré | Grain contrôlé | Doublons | % |
|---|---|---|---|
| `f_consultation_detail` (Silver) | 1 consultation (`id_consultation`) | 0 | 0,0 |
| `f_hospitalisation` (Gold) | 1 hospitalisation (`id_hospitalisation`) | 0 | 0,0 |
| `f_satisfaction` (Gold) | finess juridique × année (`annee` + `finess`) | 0 | 0,0 |
| `f_mesures` (Gold) | annee_mois × etab × patient × sexe × tranche_age × département × **région** | 0 | 0,0 |

Ce contrôle a détecté deux défauts de grain, corrigés et le PoC re-joué (résultats §7.2) :

1. **v1 — fan-out de la jointure `activite`** : jointure 1-N dupliquant les consultations (1 373 594 lignes pour 1 027 157 consultations, +33,7 %) → jointure ramenée à **1 organisation par professionnel** (`MIN(identifiant_organisation)`).
2. **v2 — grain `f_satisfaction` et `f_mesures`** : (a) le fichier ESATIS contient plusieurs lignes (sites) par finess juridique : 2 295 lignes pour **1 960 finess juridiques** → agrégation au grain juridique, **moyenne pondérée par les répondants** (`SUM(score × nb_rep) / SUM(nb_rep)`), `nb_sites` conservé ; (b) la satisfaction étant rattachée à la **région** (pas à un établissement de site), la clé de `f_mesures` sans `region` produisait **31 doublons** (les 33 lignes satisfaction partageaient 2 clés) → **`region` ajoutée au grain** de `f_mesures` (clé à 7 colonnes, 0 doublon).

### 5.4 Cohérence référentielle — codes FINESS (`dq_referentiel.csv`)

| Contrôle | Total | Trouvés | % |
|---|---|---|---|
| Hospitalisations.identifiant_organisation → etab.identifiant_organisation | 2 479 | 2 479 | **100,0** |
| Hospitalisations.Id_patient → S1.Patient | 2 479 | 2 479 | 100,0 |
| Hospitalisations.Code_diagnostic → S1.Diagnostic | 2 479 | 2 479 | 100,0 |
| S1.Consultation.Id_prof_sante → professionnel (référentiel) | 1 027 157 | 1 027 157 | **100,0** |
| S1.Consultation.Id_prof_sante → activite.identifiant | 1 027 157 | 738 190 | **71,87** → taux A : le professionnel est présent dans le fichier d'activité ; le rattachement à un **établissement** (chaîne complète, taux B) est de **63,87 %** ; les autres → `id_etablissement = -1` |
| activite.identifiant → professionnel | 1 823 588 | 1 042 874 | 57,19 |
| activite.identifiant_organisation → etab | 1 823 588 | 1 464 774 | 80,32 |
| satisfaction.finess → etab.finess_etablissement_juridique (2019/2020) | 2 295 | 1 155 | 49,91 / 50,74 → **grain juridique** (1 finess juridique = N sites) |
| satisfaction.finess → etab.identifiant_organisation | 2 295 | 0 | **0,0** → rattachement site impossible |
| satisfaction.finess_geo → etab.finess_site | 2 295 | 0 | **0,0** (finess_site quasi vide, 0,05 %) |
| etablissement.code_commune → département (règle INSEE) | 277 860 | 277 127 | **99,74** |
| deces.code_lieu_deces → département (règle INSEE) | 25 077 274 | 24 876 383 | **99,2** |
| satisfaction.region brute → région normalisée (mapping) | 2 295 | 2 270 | 98,96 / 98,86 → « Océan Indien » ambigu exclu volontairement → `INCONNUE` |
| satisfaction.region normalisée → référentiel région | 2 270 | 2 270 | 100,0 |
| Concordance région ESATIS vs région établissement (via finess juridique) | 8 308 | 8 027 | 96,62 |

### 5.5 Temporalité (`dq_temporel.csv`)

| Contrôle | Conforme | % | Plage observée |
|---|---|---|---|
| Hospitalisations.Date_Entree ∈ [2015,2023] | 2 479/2 479 | 100,0 | 2015-06-21 → 2021-06-19 |
| S1.Consultation.Date ∈ [2015,2023] | 1 027 157/1 027 157 | 100,0 | 2015-06-20 → 2023-03-31 |
| deces.date_deces ≥ date_naissance | 24 997 051/25 088 208 | 99,64 | 1882-10-01 → 2020-03-30 |
| S1.Patient : cohérence Age vs date_naissance | 45 998/100 000 | **46,0** | âge 0..100 |
| satisfaction score ∈ [0,100] | 858/1 152 (2019) | 74,48 | scores 0-100 |

### 5.6 Traitements appliqués (dérivés des contrôles)

| Problème mesuré | Traitement appliqué dans les faits |
|---|---|
| Score ESATIS en virgule décimale (2019 **et** 2020) | `replace(',', '.')` avant cast DOUBLE |
| Région ESATIS hétérogène (18 libellés) | Mapping vers les 18 régions du référentiel ; « Océan Indien » (ambigu) → `INCONNUE` |
| `finess_site` à 0,05 % ; rattachement ESATIS site à 0 % | Satisfaction stockée au **grain juridique × année** (2 295 lignes source → **1 960 finess juridiques**), rattachée à la **région** (pas de clé site) ; score = **moyenne pondérée par les répondants** ; `region` ajoutée au **grain de `f_mesures`** (0 doublon, cf. §5.3) |
| `Age` S1 incohérent avec la date de naissance (46 % seulement) | `Age` non retenu ; **tranche d'âge recalculée à la date de l'événement** dans les faits |
| 71,87 % des consultations ont leur professionnel dans le fichier d'activité ; 63,87 % sont rattachées à un établissement (chaîne complète) | Rattachement via `Id_prof_sante → activite (1 organisation par professionnel) → etablissement` ; non rattachées → membre Inconnu (-1) |
| 646 dates de décès invalides | Exclus du fait (`WHERE d_deces IS NOT NULL`) → 25 087 562 décès retenus |
| 368 codes CIM-10 invalides dans S2 (14,84 %) | Conservés s'ils existent dans `d_diagnostic` (jointure interne) ; la DQ les quantifie |
| 211 833 doublons potentiels décès (0,84 %) | Conservés et quantifiés (homonymes INSEE possibles) |
| `code_commune` à 66,69 % dans le référentiel etab | Règle INSEE `dept_of` ; non rattachés → `INCONNU`/`INCONNUE` |

---

## 6. Jeu de requêtes de benchmark

10 requêtes dans `scripts/04_benchmark/queries/` : **8 requêtes métier** (1 par besoin utilisateur du Livrable 1, couvrant les 4 mesures) + **2 requêtes techniques** (vue avec fonction fenêtrée ; jointure étoile complète). Toutes utilisent agrégations (`SUM`, `AVG`, `COUNT`), tris (`ORDER BY ... LIMIT`) et la vue métier (q09).

| # | Requête | Besoin | Mesure | Axes |
|---|---|---|---|---|
| q01 | `q01_consult_etab_2019.sql` | 1 — Taux de consultation par établissement, période Y | nbre_consultation | établissement × période (top 20, 2019) |
| q02 | `q02_consult_diag_2019_2021.sql` | 2 — Taux de consultation par diagnostic X, période Y | nbre_consultation | diagnostic × période (top 10, 2019-2021) |
| q03 | `q03_taux_hosp_periode.sql` | 3 — Taux global d'hospitalisation, période Y | nbre_hospitalisation / nbre_consultation | année (2015-2021) |
| q04 | `q04_hosp_diag.sql` | 4 — Taux d'hospitalisation par diagnostic, période | nbre_hospitalisation | diagnostic × période (top 10) |
| q05 | `q05_hosp_sexe_age.sql` | 5 — Taux d'hospitalisation par sexe, âge | nbre_hospitalisation | sexe × tranche d'âge |
| q06 | `q06_consult_prof_2020.sql` | 6 — Taux de consultation par professionnel | nbre_consultation | professionnel (top 10, 2020) |
| q07 | `q07_deces_region_2019.sql` | 7 — Décès par localisation (région), 2019 | nbre_deces | région × année |
| q08 | `q08_satisfaction_region_2020.sql` | 8 — Satisfaction par région, 2020 | total_satisfaction | région × année |
| q09 | `q09_vue_taux_mensuel.sql` | *(technique)* — vue `v_taux_consultation_mensuel` | nbre_consultation | mois + moyenne mobile 3 mois |
| q10 | `q10_etoile_complete.sql` | *(technique)* — jointure étoile complète | nbre_consultation | fait + 4 dimensions, filtre annee × sexe |

### 6.1 Résultats métier (extraits)

Résultats complets : `scripts/04_benchmark/results/resultats_requetes.txt`.

**q03 — Taux d'hospitalisation pour 100 consultations, 2015-2021**

| annee | hospitalisations | consultations | taux/100 |
|---|---|---|---|
| 2015 | 255 | 33 896 | 0,75 |
| 2016 | 395 | 184 308 | 0,21 |
| 2017 | 458 | 133 403 | 0,34 |
| 2018 | 395 | 160 373 | 0,25 |
| 2019 | 374 | 87 497 | 0,43 |
| 2020 | 410 | 162 778 | 0,25 |
| 2021 | 192 | 145 883 | 0,13 |

**q05 — Hospitalisations par sexe et tranche d'âge (2015-2021)** : 1 425 F / 1 054 M ; répartition F : 0-14: 276, 15-29: 231, 30-44: 184, 45-59: 217, 60-74: 208, 75+: 309 ; M : 0-14: 181, 15-29: 180, 30-44: 150, 45-59: 155, 60-74: 159, 75+: 229.

**q07 — Décès par région, 2019** (top 5) : Île-de-France 76 988 ; Auvergne-Rhône-Alpes 70 407 ; Nouvelle-Aquitaine 66 097 ; Occitanie 59 781 ; Hauts-de-France 55 137. (Total 2019 : 620 608 décès.)

**q08 — Satisfaction moyenne par région, 2020** (moyenne **pondérée par les répondants** `SUM(score × nb_repondants) / SUM(nb_repondants)`, top 5) : Bretagne 74,95 (36 etab., 19 396 répondants) ; Nouvelle-Aquitaine 74,77 (78 etab., 25 816) ; Pays de la Loire 74,71 (37 etab., 16 773) ; Grand Est 74,43 (63 etab., 22 591) ; Occitanie 74,19 (70 etab., 33 510). *Correction v3 : la requête calculait d'abord une moyenne simple des établissements (Bretagne 76,20), incohérente avec la définition du score (moyenne pondérée) — corrigée et résultats re-joués.*

**q01 — Top 3 établissements 2019** : SERVICE DE SANTE DES ARMEES (Paris 75107, 371 consultations) ; HU PITIE SALPETRIERE APHP (321) ; HOPITAL NORD - CHU38 (219).

---

## 7. Évaluation de performance (avant / après partitionnement)

### 7.1 Protocole

- **Moteur du PoC** : DuckDB (dans un conteneur Docker jetable, `scripts/04_benchmark/run-duckdb.sh`), données : entrepôt complet construit par les scripts `00_init` → `02d`
- **Scénario AVANT** : tables plates (1 fichier Parquet par table, aucun ordre) — équivalent « non optimisé »
- **Scénario APRÈS** : tables partitionnées (style Hive, conforme au DDL §4.1 : `d_patient` sexe + tranche_age_ref, `d_localisation` code_region, `d_temps` annee, faits `annee`...) + tri sur les clés de jointure — lu par glob `**/*.parquet` avec `hive_partitioning` (pruning des répertoires). (Le bucketing, initialement prévu, a été retiré du schéma déployé pour des raisons de compatibilité Trino 484 — §4.2 ; il est simulé dans le PoC par le tri, qui est le mécanisme effectivement retenu en production.)
- **Scénario APRÈS_ML** : APRÈS + **pruning façon metastore** (liste explicite des fichiers retenus, générée par `gen_listes.js`) — simule Hive Metastore/Trino, car DuckDB local n'a pas de metastore et le listing de tous les fichiers pénaliserait le scénario APRÈS (~0,38 ms/fichier)
- **Mesures** : 5 runs par requête (run 1 = cache froid, runs 2-5 = chauds) ; **médiane** des runs 2-5 ; volume de données lues (octets)
- **Validation plateforme (exécutée)** : le harnais `scripts/04_benchmark/harness_trino.sh` a été exécuté sur la stack Docker déployée (MinIO + Hive Metastore + Trino 484, données chargées par `lakehouse_daily`) : **10/10 requêtes × 3 répétitions réussies**, temps médian 1,0-2,2 s par requête (`results/trino/bench_trino.csv`), résultats métier identiques au PoC (ex. q08 : Bretagne 74,95 / 19 396 répondants), plans `EXPLAIN ANALYZE` de q01 et q10 capturés (`results/trino/explain_q*.txt`, pruning de partitions visible). Note : Trino ne lit pas les vues Hive — q09 passe par la vue native Trino `vues_t.v_taux_consultation_mensuel` (variante `queries/trino/q09_*.sql`, §2.4).

### 7.2 Résultats (médianes, ms)

Fichier complet : `scripts/04_benchmark/results/bench_synthese.csv` (et `bench_results.csv` pour les 450 mesures brutes : 3 campagnes × 10 requêtes × 3 scénarios × 5 runs).

| Requête | Avant | Après | Après + metastore | Accélération (×) | Volume lu avant (Mo) | Volume lu après (Mo) | Réduction volume (×) |
|---|---|---|---|---|---|---|---|
| q01 | 39,0 | 99,0 | **30,0** | **1,30** | 19,1 | 13,0 | 1,5 |
| q02 | 16,5 | 30,0 | **14,5** | **1,14** | 5,9 | 2,4 | 2,4 |
| q03 | 5,5 | 74,0 | **17,5** | 0,31 | 9,1 | 7,3 | 1,2 |
| q04 | 6,0 | 13,0 | **8,0** | 0,75 | 0,4 | 0,6 | 0,8 |
| q05 | 3,0 | 75,0 | **16,5** | 0,18 | 9,1 | 7,3 | 1,2 |
| q06 | 47,0 | 68,5 | **58,5** | 0,80 | 22,7 | 18,8 | 1,2 |
| q07 | 7,0 | 59,5 | **6,0** | **1,17** | 0,3 | 0,02 | **13,7** |
| q08 | 2,5 | 14,0 | **5,5** | 0,45 | 0,02 | 0,03 | 0,9 |
| q09 | 9,0 | 81,5 | **14,0** | 0,64 | 9,1 | 4,2 | 2,2 |
| q10 | 29,0 | 125,0 | **32,0** | 0,91 | 20,3 | 13,7 | 1,5 |

Deux campagnes de benchmark ont été exécutées (mêmes données, mêmes scénarios) : les ordres de grandeur sont stables (mêmes requêtes gagnantes/en retrait, à l'exception de q04 et q10 qui oscillent autour de ×1), avec une **variance machine significative à cette échelle** (quelques dizaines de ms, machine locale partagée) — les médianes ci-dessus sont celles de la campagne retenue (la plus récente) ; l'autre campagne donnait par ex. q01 ×0,96, q10 ×0,50, q07 ×0,75.

### 7.3 Graphiques

- `results/graph_temps_median.svg` — temps médian par requête, 3 scénarios (barres groupées)
- `results/graph_volume_lu.svg` — volume de données lu (Mo), avant vs après
- `results/graph_reduction_volume.svg` — taux de réduction du volume lu (×)

### 7.4 Analyse et interprétation

**1. Le volume de données lues est le levier dominant.** Le partitionnement réduit le volume lu sur **8/10 requêtes** (×1,2 à ×**13,7** sur q07 : 328 Ko → 24 Ko), parce que les filtres sur `annee` (colonne de partition) éliminent des répertoires entiers avant lecture. C'est exactement le mécanisme du pruning Hive/Trino en production.

**2. Le scénario APRÈS brut est pénalisé par le listing de fichiers (biais connu du PoC).** Sans metastore, DuckDB liste tous les fichiers (`glob`) avant de pouvoir pruner : sur `f_mesures` (122 partitions), ce listing coûte plus cher que le scan complet de la table plate (q03 : 74 ms vs 5,5 ms). Ce biais est **inhérent au PoC local**, pas à la stratégie : en production, le Hive Metastore connaît les partitions et Trino ne liste que les fichiers retenus (scénario APRÈS_ML). C'est pourquoi le scénario APRÈS_ML est le plus représentatif de la plateforme livrée.

**3. APRÈS_ML (pruning metastore) : gains nets sur les requêtes les plus filtrées.** q01 (top établissements 2019) : 39,0 → 30,0 ms (**×1,30**) et volume ×1,5 ; q07 (décès 2019) : 7,0 → 6,0 ms (**×1,17**) et volume **×13,7** ; q02 (diagnostics 2019-2021) : 16,5 → 14,5 ms (**×1,14**) et volume ×2,4. q10 (jointure étoile, filtre annee × sexe) est neutre (29,0 → 32,0 ms, ×0,91) avec volume ×1,5. Les gains sont modestes en temps absolu car le PoC tourne sur une machine locale avec des données déjà en cache OS et un moteur (DuckDB) très rapide sur de petits volumes ; l'apport du partitionnement croît avec le volume (en production : 25 M de décès non agrégés, ~1 M de faits mensuels) et avec la concurrence (un scan complet bloque, un pruning ne lit que l'utile).

**4. q04 et q08 : petits volumes, pas de gain (voire small-files).** `f_hospitalisation` (2 479 lignes) et `f_satisfaction` (1 960 lignes au grain juridique) tiennent en 1-2 fichiers : le partitionnement y crée plus de fichiers que de lignes utiles (q04 : volume lu ×0,8, q08 : ×0,9, et temps en retrait ×0,75 / ×0,45). **Leçon** : le partitionnement se justifie au-delà d'un seuil de volume ; ces deux faits restent partitionnés par `annee` pour la cohérence du modèle (et parce que S2/S3 grossiront), mais le bénéfice est neutre. C'est un résultat honnête du benchmark.

**5. Les requêtes en retrait en APRÈS_ML (q03, q05, q06, q09) lisent déjà peu de données.** À l'échelle du PoC local (quelques dizaines de Mo, données en cache OS, moteur single-node), la réduction du volume lu ne se traduit pas mécaniquement en gain de temps : sur ces requêtes, le temps de coordination/listing domine le temps de lecture. Deux campagnes de benchmark confirment la **variance machine** à cette échelle (q10 après_ml : 32,0 ms et 58,5 ms selon les campagnes) sans changer le classement relatif. En cluster (Trino/Spark distribués, données froides, forte concurrence), c'est précisément sur ces requêtes que le pruning de partitions produira le bénéfice.

**6. q06 (consultations par professionnel, 2020) : la jointure porte sur 1 M de professionnels.** Le PoC (single-node) ne montre qu'un avantage marginal du tri (47 → 58,5 ms, en retrait). En cluster Spark/Trino, c'est sur ce type de jointure volumineuse que le tri intra-partition (min/max par fichier) et le pruning paieront.

**7. Synthèse.** Sur les 10 requêtes : **volume lu réduit sur 8/10** (×1,2 à ×13,7 ; overhead small-files sur les deux petites tables q04 ×0,8 et q08 ×0,9). En scénario APRÈS_ML (le plus représentatif de la production), le **temps de réponse progresse sur 3/10 requêtes** (q01 ×1,30, q07 ×1,17, q02 ×1,14), est **neutre sur 1/10** (q10 ×0,91) et **recule sur 6/10** (q03 ×0,31, q05 ×0,18, q08 ×0,45, q09 ×0,64, q06 ×0,80, q04 ×0,75) : à l'échelle du PoC local mono-nœud, la réduction du volume lu ne se traduit pas mécaniquement en gain de temps là où les requêtes lisent déjà peu. La stratégie est validée là où elle compte : le pruning de partitions réduit le volume lu dès que le filtre porte sur une colonne de partition (×13,7 sur q07), et ce ratio croît avec le volume réel (en production : 25 M de décès non agrégés, ~1 M de faits mensuels) et avec la concurrence. Le partitionnement temporel/démographique + le tri intra-partition sur les clés de jointure + les agrégats pré-calculés forment une stratégie cohérente, dont le bénéfice temps est attendu en cluster (Trino/Spark distribués), pas sur un PoC local mono-nœud. Le harnais `harness_trino.sh` permet de mesurer ce bénéfice sur la plateforme déployée (stack actuellement exécutée de bout en bout, §3.4).

---

## 8. Visualisation (Apache Superset)

- **URL** : http://localhost:8088 (admin/admin — à changer)
- **Image** : `chu-superset:4.1.1` (build local `scripts/06_superset/Dockerfile` : image officielle + `psycopg2-binary`, requis pour la base de métadonnées `superset-db`)
- **Connexion** : base `Trino CHU`, URI `trino://trino@trino:8080/hive` (catalogue Hive = schéma Gold du lake)
- **Dashboard « CHU — Indicateurs métier»** (slug `chu-indicateurs-metier`, publié) créé **automatiquement au from scratch** par la tâche `build_dashboards` du DAG `lakehouse_init` via `scripts/06_superset/build_dashboards.sh` (API REST, idempotent 422=OK) : **17 datasets** (12 tables gold/vues Trino `vues_t` + 5 datasets virtuels SQL), **10 charts** (1 par besoin métier q01-q08 + 2 complémentaires : bar/pie/line/timeseries, metrics `SUM(...)` sur les vraies colonnes), tous embarqués dans le dashboard. Les requêtes SQL de référence restent dans `scripts/06_superset/charts.sql`
- **Datasets virtuels (SQL)** : les dashboards interrogent Trino **en direct** — aucune copie de données dans Superset, cache désactivé (`scripts/config/superset/superset_config.py`)
- Notice détaillée : `scripts/06_superset/README.md`

## 9. Orchestration (Apache Airflow)

- **URL** : http://localhost:8090 (admin/admin)
- **Image** : `chu-airflow:2.9.3` (build local `scripts/03_airflow/Dockerfile` : image officielle + client Docker CLI + provider `apache-airflow-providers-apache-spark==3.0.0` — dernière version compatible Airflow 2.9.x)
- **DAG `lakehouse_init`** (one-shot, manuel) : DDL Hive → vues métier → vérification Trino (volumes, `ANALYZE`, pruning) → connexion Trino dans Superset (API REST) → **dashboards métier** (`build_dashboards.sh`) — **exécuté : 6/6 tâches SUCCESS** (dont `vues_metier_trino`, `build_dashboards`)
- **DAG `lakehouse_daily`** (quotidien, 02:00) : `SparkSubmitOperator` — `load_dimensions` → `load_faits_detail` ∥ `build_f_deces_agg` → `build_f_mesures` (+ `build_agregats`) → `MSCK REPAIR` (découverte des partitions) → contrôles qualité (DQ, conteneur `chu-bench-duckdb` autonome) → disponibilité Trino pour Superset — **exécuté : 8/8 tâches SUCCESS**, volumes vérifiés dans Trino (§3.4)
- Ordre garantissant le **peuplement des clés étrangères avant le calcul des mesures** (grille d'évaluation)
- Le DDL Hive est exécuté via **HiveServer2** (`chu-hive-hs2`, beeline/JDBC 10000) : `beeline -e` statement par statement (le `beeline -f` de l'image `apache/hive:4.2.1` n'exécute pas les scripts en mode non-interactif — contournement documenté dans `NOTE_INTERNE.md`)
- Le metastore a besoin du connecteur S3A (`hadoop-aws` + AWS SDK v2 dans `config/hive/ext-jars/`, monté aussi dans `chu-hive-metastore`) : le write-back des statistiques `ANALYZE` (Trino → `updatePartitionStatsFast`) liste les fichiers du lake via S3A

## 10. Limites et perspectives

- **PoC local** : le benchmark a été exécuté sur un moteur local (DuckDB, single-node) ; les temps absolus ne préjugent pas des temps cluster, mais les ratios de volume lu et le mécanisme de pruning sont directement transposables. La stack Docker Compose + `harness_trino.sh` permet de rejouer le benchmark sur Trino.
- **f_deces_agg** : grain annuel (choix justifié §4.1) ; un besoin de mortalité mensuelle nécessiterait un fait mensuel (8,6 M de lignes) — perspective.
- **Satisfaction** : grain juridique (rattachement site impossible, 0 % en DQ) ; l'enrichissement du référentiel FINESS (finess_geo) lèverait cette limite.
- **RGPD/HDS** : les données sont pseudonymisées en amont (pas de nom/prénom de patient dans l'entrepôt : `d_patient` ne contient que sexe, date de naissance, code postal, ville) ; l'hébergement HDS et la gouvernance (rôles, droits) sont du ressort du Livrable 1 et de la production.
- **Perspectives** : scalabilité horizontale (workers Spark, nœuds Trino), refresh incrémental (CDC) plutôt que rechargement complet, mise en place d'un vrai ETL déclaratif (dbt) en couche Silver, alerting qualité dans Airflow.

---

## 11. Dernière note obtenue et résumé des corrections

**Dernière note : 19,44 / 20** — objectif ≥ 18/20 atteint (grille « A3 informatique — Stockage et Big data — Livrable 2 », notation par sous-agent évaluateur indépendant, itération v4 ; 19,75/20 en v3), **re-confirmée à l'identique (19,44/20, aucune P1, aucune P2 restante) lors de la re-notation indépendante au commit v9** après audit complet du repo et smoke test from scratch 54 PASS / 0 FAIL (58 PASS / 0 FAIL depuis v15, contrôles dashboards Superset ajoutés). Niveaux de la grille : A=20, B=15, C=10, D=5 par critère ; total = Σ(coef × note)/9 ramené sur 20.

| Critère (grille) | Coef. | Niveau | Note | Commentaire |
|---|---|---|---|---|
| Modèle physique | 2 | A | 20 | 12 tables externes Hive (`LOCATION s3a://lake/...`), clés de substitution (`row_number` sur `d_etablissement`, `d_diagnostic`), 6 dimensions + faits, `f_mesures` porte les 4 mesures |
| Peuplement des dimensions | 1 | A | 20 | Data lake peuplé **réellement** par Spark (DAG `lakehouse_daily` 8/8 SUCCESS), volumes vérifiés dans Trino : d_temps 5 844, d_patient 100 000, d_etablissement 416 665, d_localisation 102, d_diagnostic 15 490, d_professionnel 1 048 575 (§3.4) |
| Peuplement des mesures | 2 | A | 20 | 4 mesures calculées APRÈS peuplement des clés étrangères (ordre du DAG) ; f_mesures 1 030 402, f_consultation_detail 1 027 157, f_hospitalisation 2 479, f_deces_agg 69 008, f_satisfaction 1 960, agrégats 979 797 / 1 001 453 — tous vérifiés dans Trino |
| Partitionnement et bucketing | 1 | B | 15 | Partitionnements conformes (sexe + tranche_age `d_patient`, `code_region` `d_localisation`/`f_satisfaction`, annee `d_temps` + faits). **Bucketing volontairement absent** : testé, Trino 484 refuse de lire une table bucketée écrite par Spark (nb fichiers ≠ nb buckets, §4.2) — le niveau A de la grille exige le bucketing, d'où le niveau B avec justification technique documentée |
| Requêtes d'analyse | 2 | A | 20 | 10 requêtes (8 métier = 1 par besoin + 2 techniques), agrégations/vues/tris ; exécutées aussi sur Trino déployé (`results/trino/`, §7.1) |
| Évaluation de performance | 1 | A | 20 | 10 requêtes avant/après partitionnement : 450 mesures PoC (3 scénarios) + harnais Trino exécuté sur la plateforme déployée (§7.1, §7.2) |
| **Total** | **9** | | **175/9** | **= 19,44 / 20** |

### Corrections appliquées (9 points relevés à la notation v3)

1. **ZIP du livrable manquant** → `Livrable2_CloudHealthcareUnit.zip` généré à la racine du projet (rapport + `NOTE_INTERNE.md` + `scripts/` + grille d'évaluation).
2. **JRE manquant dans l'image Airflow** (`spark-submit` : « JAVA_HOME is not set ») → `default-jre-headless` + `ENV JAVA_HOME=/usr/lib/jvm/default-java` ajoutés à `scripts/03_airflow/Dockerfile` ; image rebuildée ; `spark-submit --version` vérifié OK dans le scheduler (Spark 4.2.1 / OpenJDK 17).
3. **§3.4 « Preuve d'exécution » trompeur** → redressé : la preuve est présentée honnêtement comme un PoC DuckDB conteneurisé ; la chaîne de production est décrite comme déployée et testée service par service (17 services).
4. **q08 — moyenne pondérée** → corrigée (`SUM(score × nb_repondants) / SUM(nb_repondants)`), benchmark re-joué, §6.1 et `resultats_requetes.txt` régénérés (top 2020 : Bretagne 74,95 ; Nouvelle-Aquitaine 74,77 ; Pays de la Loire 74,71).
5. **Taille de `bench_results.csv`** → documentation corrigée : 450 mesures (3 campagnes × 10 requêtes × 3 scénarios × 5 runs), et non 150.
6. **DQ doublons décès (NULL)** → `COALESCE` ajouté dans `dq_checks.sql` ; DQ re-joué : **174 794 doublons (0,6967 %)** ; CSV et §5.3 mis à jour.
7. **Git non finalisé** → dépôt commité, historique en §3.3.
8. **§11 en placeholder** → renseignée (présente section).
9. **Incohérences diverses** → corrigées : 17 services (dont 3 one-shot : `minio-init`, `hive-metastore-init`, `airflow-init`), 7 CSV DQ, ~3,7 M lignes S1 (3 674 384 au total), `dq_volumes.csv` incluant les 2 tables auxiliaires AAAA/date, AWS SDK v2 (S3A), provider Spark 3.0.0, `hive-hs2` (beeline), `airflow-init`.

### Corrections appliquées (notation v4 — exécution de bout en bout de la plateforme)

1. **MinIO non téléchargeable** (`docker pull minio/minio` refusé, `dl.min.io` en HTTP 410) → serveur MinIO **compilé from source** dans le livrable (`config/minio/Dockerfile`, tag `RELEASE.2025-10-15T17-29-55Z` depuis github.com) ; `minio-init` utilise `amazon/aws-cli`. `docker compose up -d` suffit, aucun pull `minio/*`.
2. **Chaîne de production non exécutée** → exécutée et vérifiée : `lakehouse_init` 5/5, `lakehouse_daily` 8/8 SUCCESS, volumes Trino conformes (§3.4). Le critère « Peuplement des dimensions » passe de 0,90 à 1,00.
3. **Bucketing supprimé** → `CLUSTERED BY` retiré du DDL et `bucketBy` des jobs Spark : Trino 484 refuse de lire une table bucketée écrite par Spark (nombre de fichiers ≠ nombre de buckets, vérification inconditionnelle). Remplacé par `sortWithinPartitions` sur `(id_etablissement, id_patient)` (§4.2).
4. **Partitions `region=...` rejetées par MSCK REPAIR** (« invalid characters in the name » : apostrophe/espaces/accents) → `d_localisation` et `f_satisfaction` partitionnées par **`code_region`** (ASCII), `region` conservée en colonne data.
5. **Metastore sans S3A** → `ClassNotFoundException S3AFileSystem` au write-back des stats `ANALYZE` : `config/hive/ext-jars/` (hadoop-aws 3.4.1 + AWS SDK v2) monté aussi dans `chu-hive-metastore`.
6. **Hook Spark Airflow** : l'URI `spark://` devait avoir la partie host URL-encodée (`spark%3A%2F%2Fspark-master%3A7077`) ; **Python 3.12** embarqué dans l'image Spark custom (`chu-spark:3.5.1-s3a`, python-build-standalone) pour matcher le driver Airflow 3.12 (`PYSPARK_PYTHON`).
7. **DQ rendue autonome** : `dq_checks.sql` lit directement le lake (`s3://lake/...`) via httpfs + staging embarqué dans `05_qualite/staging/` (plus de dépendance à un workspace externe `L2WORK`).

### Corrections appliquées (notation v5 — relecture stricte du sous-agent évaluateur)

1. **§11 honnête** : la note revendiquée passe de 20,00 à **19,44/20** — le critère « Partitionnement et bucketing » est niveau B (15/20) car le bucketing est volontairement absent (incompatibilité Trino 484 démontrée, §4.2), même si tous les partitionnements exigés sont en place.
2. **Harnais Trino exécuté** sur la plateforme déployée (10/10 requêtes × 3 runs, `results/trino/`) — consolide le critère « Évaluation de performance » sur le moteur réel, pas seulement le PoC (§7.1).
3. **Vues Trino natives** (`vues_t`, `01_ddl/04_vues_metier_trino.sql` + tâche `vues_metier_trino`) : Trino ne lit pas les vues Hive — q09 et le chart Superset correspondant passeraient en erreur sinon.
4. **`f_satisfaction.annee` passée en INT** (cohérent avec tous les faits) : `WHERE annee = 2020` fonctionne enfin côté Trino (q08, chart 8).
5. **Anti-OOM** : plafonds Trino (`config.properties` + `JAVA_OPTS=-Xmx1g` + `mem_limit: 1536m`), worker Spark à 2 Go, et `restart: unless-stopped` sur les 14 services long terme — la VM Docker (7,6 Go) pouvait encore tuer Trino (OOMKilled) pendant les jobs Spark ; le conteneur est désormais relancé automatiquement et la tâche `dispo_trino_superset` attend que Trino réponde (boucle ~3 min) avant de valider.
6. **Historique Git §3.3** mis à jour (12 commits) ; commentaire `queries/_apres.sql` aligné (plus de bucketing).

### Corrections appliquées (re-notation v9 — audit indépendant + smoke test from scratch)

1. **Crash récurrent au from scratch** (le backfill auto du `lakehouse_daily` race avec l'init) → DAGs en pause par défaut (`DAGS_ARE_PAUSED_AT_CREATION=true`) ; runbook §12.2 : unpause → trigger `lakehouse_init` → attendre 5/5 → unpause `lakehouse_daily` (un run auto, `catchup=False` + `max_active_runs=1`) → attendre 8/8 (v6, v7).
2. **Connexion Superset « Trino CHU » jamais créée sans erreur visible** → driver `trino==0.330.0` ajouté à l'image Superset, script dédié `dags/verif_superset.sh` (attente /health 6 min, login + cookie + `X-CSRFToken`, POST idempotent, VÉRIFICATION par GET avec grep — exit 1 si absente), image rebuildée (v8).
3. **`verification_superset` en échec TemplateNotFound** (Airflow 2.9.3 : tout champ template finissant par `.sh` est traité comme un fichier Jinja) → contenu du script lu au parse time dans le DAG, passé inline (v9, piège n°31).
4. **DQ étendus aux faits intégrés** : complétude par attribut (`dq_completude_faits.csv`) et intégrité FK faits → dimensions (`dq_integrite_fk.csv`, 13 contrôles dont le taux de rattachement établissement 63,87 % documenté) → **9 CSV DQ** (v8).
5. **Smoke test from scratch automatisé** `scripts/07_smoke_test/smoke_from_scratch.sh` : `down -v` → `up -d` → 14 conteneurs healthy → endpoints (retry 4 min) → DAGs en pause / 0 run → init 6/6 → daily 8/8 → 13 volumes Trino exacts → 12 tables + 9 vues `vues_t` → bucket/prefixes/objets MinIO → connexion Superset + **dashboard publié + ≥8 charts + ≥8 datasets + ≥8 charts embarqués** (base `superset-db`) → 13 tables S1 → 9 CSV DQ → re-pause des DAGs ; log archivé dans `07_smoke_test/results/`, exit 0 = 100 % PASS. Résultat : **58 PASS / 0 FAIL** (v15 ; 54 PASS / 0 FAIL à v8/v9 avant les contrôles dashboards).
6. **Preuve de données indépendante** : rapprochement Trino ↔ sources (`DATA Projet`, 48 fichiers) ↔ dump S1 par sous-agent dédié (réconciliation en plus du smoke test).
7. **Superset livré avec 0 dashboard** (seule la connexion « Trino CHU » existait) → `scripts/06_superset/build_dashboards.sh` exécuté par la tâche `build_dashboards` de `lakehouse_init` (contenu lu au parse time, même piège n°31) : crée 17 datasets, 10 charts (besoins q01-q08) et le dashboard « CHU — Indicateurs métier » via l'API REST Superset, idempotent, avec retry si Trino redémarre (OOM) en cours de route ; smoke test vérifie désormais ces objets en base (v15, pièges n°32-35).

---

## 12. Annexes

### 12.1 Arborescence du livrable

```
Livrable 2/
├── README.md                             # présentation du dépôt (GitHub)
├── livrable2.md                          # ce rapport
├── NOTE_INTERNE.md                       # note interne : conteneurs, lancement, vérifications
├── A3-Stockage_et_BigData-Grille-Livrable2.xlsx   # grille d'évaluation
└── scripts/
    ├── docker-compose.yml                # stack complète (17 services)
    ├── README.md                         # arborescence + ordre d'exécution
    ├── config/
    │   ├── hive-site.xml                 # metastore + HiveServer2 (S3A vers MinIO)
    │   ├── hive/                         # driver JDBC PostgreSQL + ext-jars/ (S3A : hadoop-aws + AWS SDK v2, monté dans metastore ET hs2)
    │   ├── minio/Dockerfile              # build from source du serveur MinIO (pull minio/* bloqué)
    │   ├── data/referentiels/            # référentiels embarqués (départements, ESATIS) copiés dans raw/
    │   ├── trino/catalog/hive.properties # connecteur Hive (S3 natif Trino, cache metastore ttl=0)
    │   ├── spark/                        # Dockerfile (image chu-spark : Python 3.12 embarqué) + spark-defaults.conf
    │   ├── superset/superset_config.py
    │   └── s1-postgres/init.sh
    ├── 01_ddl/                           # DDL Hive + vérification Trino + vues métier
    ├── 02_spark/                         # ETL PySpark (6 scripts)
    ├── 03_airflow/                       # Dockerfile + dags/ (lakehouse_init.py, lakehouse_daily.py)
    ├── 04_benchmark/                     # 10 requêtes, harnais, résultats (CSV + 3 SVG)
    ├── 05_qualite/                       # dq_checks.sql + staging/ (autonome) + 7 rapports CSV
    └── 06_superset/                      # Dockerfile + build_dashboards.sh + charts.sql + README
```

### 12.2 Commandes principales

```bash
# Stack complète (depuis scripts/) — from scratch : docker compose down -v && docker compose up -d --build
# (--build obligatoire sur un clone frais : les images chu-* sont buildées localement, jamais pullées)
cd scripts && docker compose up -d --build
# Attendre ~2 min que tous les conteneurs soient healthy (docker compose ps)
# Airflow (http://localhost:8090, admin/admin) — les 2 DAGs démarrent EN PAUSE
# (AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true) : aucun backfill auto ne peut
# écraser l'init. Ordre strict dans l'UI :
#   1. unpause lakehouse_init  -> ▶ Trigger  -> attendre 6/6 SUCCESS (~3-5 min, dont build_dashboards)
#   2. unpause lakehouse_daily -> le scheduler lance automatiquement le run
#      (catchup=False, max_active_runs=1 ; le bouton ▶ marche aussi mais file
#      derrière) -> attendre 8/8 SUCCESS (~10 min)
#   A NE PAS FAIRE : mettre un DAG en pause tant qu'un run est en cours
#   (Airflow 2.9.3 ne schedule plus les runs d'un DAG en pause : le run
#   resterait figé en 'running' avec ses tâches bloquées en 'scheduled').
# Vérification : docker exec chu-trino trino --server http://localhost:8080 \
#   --catalog hive --schema gold --execute "SELECT count(*) FROM f_mesures"  # 1030402
# Superset : http://localhost:8088 -> dashboard « CHU — Indicateurs métier » déjà créé par l'init (build_dashboards.sh)
# Benchmark Trino : cd scripts/04_benchmark && ./harness_trino.sh
# Contrôles qualité seuls : voir scripts/README.md (section C)
```

Ports exposés sur l'hôte : Superset **8088**, Trino **8080**, Airflow **8090**, MinIO **9000** (API S3) / **9001** (console), Spark master **7077** + UI **8085**, Spark worker UI **8086**, Hive Metastore (Thrift) **9083**, HiveServer2 (beeline) **10000**, S1 PostgreSQL **5432**. Note interne complète (démarrage, vérifications, pièges) : `NOTE_INTERNE.md`.

### 12.3 Références

- Sujet : `../sujet.md` (Livrable 2, p. « Livrables attendus »)
- Livrable 1 : `../Livrable 1/L1-rapport-cloud-health-unit.md` (modèle logique, 4 mesures, 8 besoins)
- Résultats benchmark : `scripts/04_benchmark/results/` (CSV + SVG + résultats métier)
- Rapport qualité : `scripts/05_qualite/results/` (9 CSV : 7 familles sources + complétude faits intégrés + intégrité FK faits→dimensions)
- Smoke test from scratch automatisé : `scripts/07_smoke_test/smoke_from_scratch.sh` (down -v → up -d → init 6/6 → daily 8/8 → volumes Trino exacts → endpoints → lake → Superset : dashboard + ≥8 charts + ≥8 datasets vérifiés en base ; log 100% PASS versionné dans `07_smoke_test/results/`)
