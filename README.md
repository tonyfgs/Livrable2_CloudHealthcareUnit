# Livrable 2 — CloudHealthcareUnit (CHU)

Plateforme **lakehouse** complète pour le projet CESI 4A Big Data (sujet « A3 — Stockage et Big Data », livrable 2) : ingestion, stockage, catalogue, traitement, orchestration et visualisation du data lake d'un CHU. **100 % conteneurisée en Docker Compose** — rien n'est installé sur le poste hôte.

Rapport du livrable : `rapport_latex/rapport.pdf` (source LaTeX dans `rapport_latex/rapport.tex`) + `livrable2.md`.

## Sommaire

- [Architecture](#architecture)
- [Prérequis (machine vierge)](#prérequis-machine-vierge)
- [Lancement from scratch](#lancement-from-scratch)
- [Interfaces et identifiants](#interfaces-et-identifiants)
- [Lancer le pipeline (Airflow)](#lancer-le-pipeline-airflow)
- [Vérifier les données](#vérifier-les-données)
- [Smoke test (vérification automatisée)](#smoke-test-vérification-automatisée)
- [Arrêt et nettoyage](#arrêt-et-nettoyage)
- [Structure du dépôt](#structure-du-dépôt)

## Architecture

```
        ┌────────────── sources ──────────────┐
        │  DATA Projet/ (48 fichiers CSV + dump PostgreSQL S1, 2,1 Go)
        └──────┬──────────────────┬───────────┘
               │ minio-init (aws-cli)│ JDBC (Spark)
               ▼                     ▼
      ┌─────────────────┐   ┌──────────────────┐
      │ MinIO lake/raw/ │   │ PostgreSQL S1    │ :5432 (dump restauré)
      └────────┬────────┘   └──────────────────┘
               │ Spark (ETL/ELT → Parquet silver/gold, partitionné + trié)
               ▼
      ┌──────────────────────────────┐     ┌──────────────────────────┐
      │ lake/silver/ + lake/gold/    │◄───►│ Hive Metastore (+HS2)    │ :9083 / :10000
      │ tables externes Hive         │     │ métadonnées + partitions │
      └────────┬─────────────────────┘     └──────────────────────────┘
               │ Trino (connecteur Hive, pruning de partitions) :8080
               ▼
      ┌──────────────────┐       ┌──────────────────────────────────┐
      │ Superset :8088   │       │ Airflow :8090                    │
      │ dashboard CHU    │       │ DAGs lakehouse_init / _daily     │── SparkSubmit
      └──────────────────┘       └──────────────────────────────────┘
```

| Couche | Service | Image | Port hôte | Rôle |
|---|---|---|---|---|
| Ingestion | `minio-init` (one-shot) | `amazon/aws-cli` (digest épinglé) | — | copie `DATA Projet/` → `lake/raw/` |
| Stockage | MinIO (data lake S3) | `chu-minio` (**build from source**) | **9000** API / **9001** console | zones `raw/ silver/ gold/ warehouse/` |
| Catalogue | Hive Metastore + HS2 + PG | `apache/hive:4.2.1`, `postgres:15` | **9083** / **10000** | métadonnées, partitions, stats |
| Traitement | Spark master + worker | `chu-spark:3.5.1-s3a` (**build**) | **7077**, UI **8085** / **8086** | ETL/ELT S1 + CSV → lake |
| Orchestration | Airflow 2.9.3 (webserver + scheduler + PG) | `chu-airflow:2.9.3` (**build**) | **8090** | DAGs `lakehouse_init`, `lakehouse_daily` |
| Requêtage | Trino | `trinodb/trino` (digest épinglé = **484**) | **8080** | lecture distribuée du lake |
| Visualisation | Superset | `chu-superset:4.1.1` (**build**) | **8088** | dashboard « CHU — Indicateurs métier » (10 charts, 17 datasets, créé automatiquement par l'init) |
| Source | PostgreSQL S1 | `postgres:15` | **5432** | dump `DATA2023` restauré |
| Benchmark/DQ | DuckDB (conteneur jetable) | `chu-bench-duckdb` (**build**) | — | PoC perf + 9 contrôles qualité |

17 services au total : 14 long-running + 3 one-shot (`minio-init`, `hive-metastore-init`, `airflow-init`).

## Prérequis (machine vierge)

1. **Docker Desktop** installé, VM avec **≥ 8 Go de RAM** alloués (Settings → Resources) — sinon OOM-kill de Trino/Spark.
2. **Un compte Docker Hub gratuit + `docker login`** — obligatoire sur une machine neuve : les pulls anonymes sont limités (rate limit), et les images de base (`postgres:15`, `apache/hive:4.2.1`, `trinodb/trino`, `amazon/aws-cli`) viennent de Docker Hub.
3. **Le dossier `DATA Projet/`** (sources brutes : 48 fichiers, **2,1 Go**, hors git car trop gros) placé **en frère du dépôt** — le compose le monte via `../../DATA Projet` en lecture seule :

```
mon-projet/
├── Livrable2_CloudHealthcareUnit/     <- le clone (ce dépôt)
└── DATA Projet/                       <- 2,1 Go, 48 fichiers sources (à récupérer à part)
```

Rien d'autre : pas de Python, Java, PostgreSQL, LaTeX... tout est dans les conteneurs.

## Lancement from scratch

```bash
git clone git@github.com:tonyfgs/Livrable2_CloudHealthcareUnit.git
# récupérer DATA Projet/ (2,1 Go) et le placer à côté du clone, comme ci-dessus
cd Livrable2_CloudHealthcareUnit/scripts

docker compose up -d --build     # 1re fois : build les 6 images custom chu-* from source, pull les images de base, démarre les 17 services (~10-20 min)
docker compose ps                # tout doit être Up / healthy
```

Pourquoi `--build` est indispensable sur une machine vierge : les 6 images custom (`chu-minio`, `chu-spark`, `chu-airflow`, `chu-superset`, `chu-bench-duckdb` + le build MinIO) **n'existent sur aucun registry** (les pulls `minio/*` sont bloqués par Docker Hub) — elles sont compilées from source depuis les Dockerfiles du dépôt (`scripts/config/`) ; le compose a `pull_policy: build` sur ces services, donc `up -d --build` les construit localement. Les images de base sont épinglées (tags fixés, Trino et aws-cli épinglés **par digest**) pour la reproductibilité.

Une fois healthy, lancer l'initialisation (voir section suivante), puis vérifier.

## Interfaces et identifiants

| Interface | URL | Identifiants |
|---|---|---|
| Airflow (orchestration) | http://localhost:8090 | admin / admin |
| Superset (dashboards) | http://localhost:8088 | admin / admin |
| Trino (UI + SQL) | http://localhost:8080 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Spark UI | http://localhost:8085 | — |
| PostgreSQL S1 | localhost:5432 | chu / chu (base `chu_s1`) |

## Lancer le pipeline (Airflow)

Les DAGs démarrent **en pause** (pas de backfill auto). Toujours **unpause AVANT trigger** :

```bash
# 1. init one-shot : ingère les sources, crée le schéma, charge silver/gold, crée le dashboard Superset
docker exec chu-airflow-scheduler airflow dags unpause lakehouse_init
docker exec chu-airflow-scheduler airflow dags trigger lakehouse_init
# attendre ~2 min : run = success, 6/6 tâches

# 2. daily : pipeline planifié (02:00) — un run suffit pour la démo
docker exec chu-airflow-scheduler airflow dags unpause lakehouse_daily
# attendre ~4-5 min : run = success, 8/8 tâches

# vérifier l'état
docker exec chu-airflow-db psql -U airflow -d airflow -c "SELECT dag_id, run_id, state FROM dag_run ORDER BY start_date DESC LIMIT 3;"
```

Règles d'or (Airflow 2.9.3) : ne jamais trigger un DAG encore en pause (le run reste `queued`) ; ne jamais pauser un DAG dont un run est en cours.

## Vérifier les données

```bash
docker exec chu-trino trino --server http://localhost:8080 --catalog hive --schema gold \
  --execute "SELECT count(*) FROM f_mesures"     # 1030402
```

Volumes exacts attendus (reproduits à l'identique depuis les sources brutes) :

| Table | Lignes | | Table | Lignes |
|---|---:|---|---|---:|
| `gold.d_temps` | 5 844 | | `gold.f_mesures` | 1 030 402 |
| `gold.d_patient` | 100 000 | | `gold.f_consult_diag_mois` | 979 797 |
| `gold.d_etablissement` | 416 665 | | `gold.f_consult_prof_mois` | 1 001 453 |
| `gold.d_localisation` | 102 | | `gold.f_deces_agg` | 69 008 |
| `gold.d_diagnostic` | 15 490 | | `gold.f_satisfaction` | 1 960 |
| `gold.d_professionnel` | 1 048 575 | | `gold.f_hospitalisation` | 2 479 |
| `silver.f_consultation_detail` | 1 027 157 | | | |

Le dashboard Superset « CHU — Indicateurs métier » est créé automatiquement par la tâche `build_dashboards` du DAG d'init (API REST Superset, 10 charts couvrant les 8 besoins métier).

## Smoke test (vérification automatisée)

```bash
cd scripts
./07_smoke_test/smoke_from_scratch.sh; echo $?    # 0 = 58/58 PASS
```

From scratch complet (~10 min) : `down -v` → `up -d --build` → conteneurs → endpoints → DAGs → init 6/6 → daily 8/8 → 13 volumes Trino exacts → 13 tables externes + 9 vues → bucket/prefixes MinIO → dashboard Superset publié (≥8 charts, ≥8 datasets) → 13 tables S1 → 9 CSV DQ. Logs archivés dans `scripts/07_smoke_test/results/`.

## Arrêt et nettoyage

```bash
cd scripts
docker compose down        # arrête et supprime les conteneurs (volumes conservés)
docker compose down -v     # purge totale, volumes compris (retour à l'état initial)
```

## Structure du dépôt

```
├── README.md                             # ce fichier
├── livrable2.md                          # rapport complet du livrable
├── rapport_latex/                        # rapport LaTeX + PDF compilé + figures + captures
│   ├── rapport.tex / rapport.pdf
│   ├── figures/                          # graphiques benchmark (pgfplots) + tables DQ générées
│   └── ImagesRapport/                    # captures d'écran du rapport
└── scripts/
    ├── docker-compose.yml                # stack complète (17 services, build from source pour les images custom)
    ├── README.md                         # arborescence détaillée + ordre d'exécution
    ├── config/                           # Dockerfiles + configs (hive-site, trino, spark, superset, minio, référentiels)
    ├── 01_ddl/                           # schéma Gold Hive + vues métier (Hive + Trino)
    ├── 02_ingestion/                     # init sources (minio-init, restore S1)
    ├── 03_airflow/                       # image + DAGs lakehouse_init / lakehouse_daily + connexions
    ├── 04_spark/                         # jobs ETL/ELT (dimensions, faits, agrégats, mesures)
    ├── 05_qualite/                       # dq_checks.sql (9 familles de contrôles) + résultats CSV
    ├── 06_superset/                      # image (driver Trino) + build_dashboards.sh + config
    ├── 07_smoke_test/                    # smoke_from_scratch.sh + results/ (logs 58/58 PASS)
    └── queries/                          # requêtes q01–q10 + résultats + harnais Trino
```

Contenu métier du livrable (voir le rapport) : modèle physique en schéma en étoile (13 tables externes Hive `LOCATION s3a://lake/...`, 6 dimensions + faits, §2-3), DDL/DML commentés (§3), partitionnement et indexation justifiés (§5), rapport de qualité des données — complétude, doublons (174 794 décès), cohérence FINESS (§6), 10 requêtes de benchmark couvrant les 8 besoins (§7), évaluation de performance avant/après partitionnement avec graphiques comparatifs et analyse (§8), captures de la plateforme déployée (§3).

> L'archive prête à rendre est `Livrable2_CloudHealthcareUnit.zip` (racine du dossier projet, hors dépôt git). Pour rejouer la plateforme depuis l'archive : dézipper, placer `DATA Projet/` en frère de `Livrable2_CloudHealthcareUnit/`, puis suivre [Lancement from scratch](#lancement-from-scratch).
