# Livrable 2 — CloudHealthcareUnit (CHU)

Plateforme **lakehouse** complète pour le projet CESI 4A Big Data (sujet « A3 — Stockage et Big Data », livrable 2) : ingestion, stockage, catalogue, traitement, orchestration et visualisation du data lake d'un CHU, **100 % conteneurisée en Docker Compose** (rien n'est installé sur le poste hôte).

![note](https://img.shields.io/badge/évaluation-19%2C44%2F20-brightgreen) ![smoke](https://img.shields.io/badge/smoke%20test-54%2F54%20PASS-brightgreen) ![docker](https://img.shields.io/badge/docker--compose-17%20services-2496ED) ![Trino](https://img.shields.io/badge/Trino-484-DD00A1) ![Spark](https://img.shields.io/badge/Spark-3.5.1-E25A1C) ![Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE) ![Superset](https://img.shields.io/badge/Superset-4.1.1-20A6C9) ![MinIO](https://img.shields.io/badge/MinIO-lake-C72E49)

## Sommaire

- [Contexte](#contexte)
- [Architecture](#architecture)
- [Prérequis](#prérequis)
- [Démarrage from scratch](#démarrage-from-scratch)
- [Runbook Airflow (DAGs)](#runbook-airflow-dags)
- [Modèle de données](#modèle-de-données)
- [Données et volumes](#données-et-volumes)
- [Qualité des données (DQ)](#qualité-des-données-dq)
- [Performance (benchmark)](#performance-benchmark)
- [Vérification automatisée (smoke test)](#vérification-automatisée-smoke-test)
- [Résultat de l'évaluation](#résultat-de-lévaluation)
- [Arborescence du dépôt](#arborescence-du-dépôt)
- [Documentation](#documentation)

## Contexte

Le livrable 2 du sujet CHU demande de concevoir et déployer un **entrepôt de données en étoile** (couche Gold/Silver) à partir de 4 sources (dump PostgreSQL S1, CSV établissements FINESS, décès, hospitalisations, satisfaction ESATIS), avec la stack imposée positionnée couche par couche, un benchmark avant/après partitionnement, des requêtes d'analyse et un rapport de qualité.

Tout est livré ici : DDL/DML versionnés, plateforme Docker Compose exécutée de bout en bout, résultats de benchmark et DQ archivés, rapport complet (`livrable2.md`), note interne (`NOTE_INTERNE.md`, 31 pièges testés) et grille d'évaluation.

## Architecture

```
              ┌───────────────── sources ─────────────────┐
              │  DATA Projet (48 fichiers CSV) + dump S1 PG │
              └──────┬──────────────────────────┬───────────┘
                     │ minio-init (AWS CLI)       │ JDBC (Spark)
                     ▼                            ▼
           ┌──────────────────┐          ┌───────────────────┐
           │  MinIO lake/raw/ │          │  PostgreSQL S1    │
           └────────┬─────────┘          └───────────────────┘
                    │ Spark (ETL/ELT : silver + gold Parquet)
                    ▼
   ┌──────────────────────────────────────┐
   │  lake/silver/  +  lake/gold/         │◄──► Hive Metastore (+ HS2)
   │  (tables externes Hive, partitionnées)│    métadonnées & partitions
   └────────┬─────────────────────────────┘
            │ Trino (connecteur Hive, pruning de partitions)
            ▼
   ┌──────────────────┐        ┌───────────────────────────┐
   │  Superset :8088  │        │  Airflow :8090            │
   │  dashboards      │        │  lakehouse_init (5 tâches)│── SparkSubmit
   └──────────────────┘        │  lakehouse_daily (8 tâches, 02:00)
                               └───────────────────────────┘
```

| Couche | Composant | Image | Port hôte | Rôle |
|---|---|---|---|---|
| Ingestion | `minio-init` | `amazon/aws-cli` | — | copie des sources vers `lake/raw/` (clés normalisées) |
| Stockage | MinIO (data lake S3) | build from source `chu-minio` | **9000** API / **9001** console | zones `raw/ silver/ gold/ warehouse/` |
| Catalogue | Hive Metastore + HiveServer2 + PG | `apache/hive:4.2.1`, `postgres:15` | **9083** / **10000** | métadonnées, partitions, stats ; beeline |
| Traitement | Spark standalone (master + worker) | `chu-spark:3.5.1-s3a` | **7077**, UI **8085** / worker **8086** | ETL/ELT S1 + CSV → lake |
| Orchestration | Airflow 2.9.3 (webserver + scheduler + PG) | `chu-airflow:2.9.3` | **8090** (admin/admin) | DAGs `lakehouse_init`, `lakehouse_daily` |
| Requêtage | Trino | `trinodb/trino:latest` (484) | **8080** | lecture distribuée du lake |
| Visualisation | Superset | `chu-superset:4.1.1` | **8088** (admin/admin) | connexion « Trino CHU » + dashboards |
| Source | PostgreSQL S1 | `postgres:15` | **5432** | dump ChuSimulation restauré |
| Benchmark/DQ | DuckDB (conteneur jetable) | `chu-bench-duckdb` | — | PoC perf + 9 contrôles qualité |

17 services au total (14 long-running + 3 one-shot : `minio-init`, `hive-metastore-init`, `airflow-init`).

## Prérequis

- Docker Desktop (VM ≥ 8 Go recommandés)
- Le dossier **`DATA Projet/`** (sources brutes, 48 fichiers) en **frère** de ce dépôt — le compose le monte via `../../DATA Projet`
- Aucune installation locale : tout passe par Docker

## Démarrage from scratch

```bash
cd scripts
docker compose down -v          # purge totale (volumes compris)
docker compose up -d            # 14 conteneurs + 3 one-shots, ~1-2 min
docker compose ps               # tout doit être Up / healthy
```

Interfaces : Airflow http://localhost:8090 (admin/admin) · Superset http://localhost:8088 (admin/admin) · Trino http://localhost:8080 · MinIO console http://localhost:9001 (minioadmin/minioadmin).

## Runbook Airflow (DAGs)

```bash
# 1. init (one-shot) — TOUJOURS unpause AVANT trigger
 docker exec chu-airflow-scheduler airflow dags unpause lakehouse_init
 docker exec chu-airflow-scheduler airflow dags trigger lakehouse_init
# attendre ~2 min : run = success, 5/5 tâches

# 2. daily — un seul run auto (catchup=False, max_active_runs=1)
 docker exec chu-airflow-scheduler airflow dags unpause lakehouse_daily
# attendre ~4-5 min : run = success, 8/8 tâches

# vérification d'état
 docker exec chu-airflow-db psql -U airflow -d airflow -c "SELECT dag_id, run_id, state FROM dag_run ORDER BY start_date DESC LIMIT 3;"
```

> Règles d'or (Airflow 2.9.3) : ne **jamais** trigger un DAG encore en pause (le run reste bloqué en `queued`) ; ne **jamais** pauser un DAG dont le run est en cours. Les DAGs démarrent **en pause** (`DAGS_ARE_PAUSED_AT_CREATION=true`) pour éviter tout backfill auto.

Vérification des données :

```bash
docker exec chu-trino trino --server http://localhost:8080 --catalog hive --schema gold \
  --execute "SELECT count(*) FROM f_mesures"     # 1030402
```

## Modèle de données

Schéma en étoile, 12 tables externes Hive (`LOCATION s3a://lake/...`) :

- **Dimensions (6)** : `d_temps` (5 844 j, 2015-2030), `d_patient`, `d_etablissement`, `d_localisation`, `d_diagnostic`, `d_professionnel` — clés de substitution (`id_temps` = AAAAMMJJ, `row_number` sur établissement/diagnostic)
- **Faits (6)** : `f_mesures` (grain mois × établissement × patient, 4 mesures : consultations, hospitalisations, satisfaction, décès), `f_consult_diag_mois`, `f_consult_prof_mois`, `f_deces_agg` (grain annuel), `f_hospitalisation`, `f_satisfaction` (grain juridique) + silver `f_consultation_detail`
- **Partitionnement** : `d_patient` (sexe, tranche_age), `d_localisation` (code_region), `d_temps` (annee), faits par `annee` (+ sexe/tranche_age pour `f_mesures`). Bucketing volontairement absent : Trino 484 refuse les tables bucketées écrites par Spark (démontré, rapport §4.2)
- **9 vues métier** exposées à Trino dans le schéma `vues_t` (Trino ne lit pas les vues Hive)

## Données et volumes

Volumes exacts vérifiés dans Trino et **reproduits à l'identique depuis les sources brutes** (réconciliation indépendante dump S1 via `pg_restore` + CSV) :

| Table | Lignes | | Table | Lignes |
|---|---:|---|---|---:|
| `gold.d_temps` | 5 844 | | `gold.f_mesures` | 1 030 402 |
| `gold.d_patient` | 100 000 | | `gold.f_consult_diag_mois` | 979 797 |
| `gold.d_etablissement` | 416 665 | | `gold.f_consult_prof_mois` | 1 001 453 |
| `gold.d_localisation` | 102 | | `gold.f_deces_agg` | 69 008 |
| `gold.d_diagnostic` | 15 490 | | `gold.f_satisfaction` | 1 960 |
| `gold.d_professionnel` | 1 048 575 | | `gold.f_hospitalisation` | 2 479 |
| `silver.f_consultation_detail` | 1 027 157 | | | |

## Qualité des données (DQ)

9 contrôles exécutés en autonome sur le lake (DuckDB conteneurisé, `scripts/05_qualite/dq_checks.sql`), résultats CSV archivés : volumes, doublons (dont 174 794 doublons décès corrigés via `COALESCE`), complétude par attribut **sur les faits intégrés**, intégrité FK faits → dimensions (13 contrôles ; taux de rattachement établissement 63,87 % documenté), cohérence FINESS, dates invalides, etc.

## Performance (benchmark)

- PoC conteneurisé (DuckDB) : 10 requêtes × 3 scénarios × 5 runs = **450 mesures**, 3 graphiques SVG avant/après partitionnement (rapport §7)
- Harnais rejoué sur la plateforme déployée : **Trino 10/10 requêtes × 3 runs** sans erreur (`scripts/…/results/trino/`), plans `EXPLAIN ANALYZE`

## Vérification automatisée (smoke test)

```bash
cd scripts
./07_smoke_test/smoke_from_scratch.sh; echo $?    # 0 = 100 % PASS
```

From scratch complet (~10 min) : `down -v` → `up -d` → conteneurs → endpoints → DAGs en pause/0 run → init 5/5 → daily 8/8 → 13 volumes Trino exacts → 12 tables + 9 vues → bucket/prefixes/objets MinIO → connexion Superset « Trino CHU » → 13 tables S1 → 9 CSV DQ → re-pause. Résultat : **54 PASS / 0 FAIL** (logs archivés dans `scripts/07_smoke_test/results/`, rejoués 3× dont 2 le 30/10 matin).

## Résultat de l'évaluation

**Note : 19,44 / 20** (grille « A3 — Stockage et Big Data — Livrable 2 », notation par sous-agent indépendant ; A=20, B=15, C=10, D=5, total = Σ(coef × note)/9) :

| Critère | Coef | Niveau | Note |
|---|---:|---|---:|
| Modèle physique | 2 | A | 20 |
| Peuplement des dimensions | 1 | A | 20 |
| Peuplement des mesures | 2 | A | 20 |
| Partitionnement et bucketing | 1 | B | 15 |
| Requêtes d'analyse | 2 | A | 20 |
| Évaluation de performance | 1 | A | 20 |

Seule déduction : bucketing absent (niveau B), justifié techniquement (incompatibilité Trino 484 démontrée).

## Arborescence du dépôt

```
├── README.md                             # ce fichier
├── livrable2.md                          # rapport complet du livrable (12 sections)
├── NOTE_INTERNE.md                       # note interne : conteneurs, lancement, vérifications, 31 pièges
├── A3-Stockage_et_BigData-Grille-Livrable2.xlsx   # grille d'évaluation
└── scripts/
    ├── docker-compose.yml                # stack complète (17 services)
    ├── README.md                         # arborescence détaillée + ordre d'exécution
    ├── config/                           # hive-site, trino, spark, superset, minio (build), référentiels
    ├── 01_ddl/                           # schéma Gold Hive + vues métier (Hive + Trino)
    ├── 02_ingestion/                     # init sources (minio-init, restore S1)
    ├── 03_airflow/                       # image + DAGs lakehouse_init / lakehouse_daily + connexions
    ├── 04_spark/                         # jobs ETL/ELT (dimensions, faits, agrégats, mesures)
    ├── 05_qualite/                       # dq_checks.sql (9 familles) + staging + résultats CSV
    ├── 06_superset/                      # image (driver trino) + config
    ├── 07_smoke_test/                    # smoke_from_scratch.sh + results/ (logs 54/54 PASS)
    └── queries/                          # q01–q10 + résultats + harness Trino
```

## Documentation

- **`livrable2.md`** — rapport complet : modèle physique, DDL/DML commentés, partitionnement justifié, 10 requêtes d'analyse, benchmark avant/après, DQ, preuve d'exécution, limites, note et corrections
- **`NOTE_INTERNE.md`** — comment on lance chaque conteneur, comment on vérifie les résultats, et les **31 pièges** réellement rencontrés et leur contournement (MinIO pull bloqué → build from source, Trino 484, metastore S3A, OOM, DAGs en pause, TemplateNotFound `.sh`, etc.)
- **`A3-Stockage_et_BigData-Grille-Livrable2.xlsx`** — grille d'évaluation du sujet

> L'archive livrable prête à rendre est `Livrable2_CloudHealthcareUnit.zip` (racine du dossier projet, hors dépôt git) : elle embarque ce dépôt tel quel. Pour rejouer la plateforme depuis l'archive : dézipper, placer `DATA Projet/` en frère de `Livrable2_CloudHealthcareUnit/`, puis suivre [Démarrage from scratch](#démarrage-from-scratch).
