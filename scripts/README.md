# Scripts - Livrable 2 (Cloud Healthcare Unit)

Tout le projet s'exécute **dans Docker** : aucune installation (PostgreSQL,
Spark, Trino, Airflow, Superset...) sur le poste hôte.

## Arborescence

```
scripts/
├── docker-compose.yml            # stack complète (toutes les couches)
├── config/
│   ├── hive-site.xml             # Hive Metastore + HS2 (S3A vers MinIO)
│   ├── hive/                     # driver JDBC PostgreSQL + ext-jars/ (S3A : hadoop-aws + AWS SDK v2)
│   ├── minio/Dockerfile          # MinIO compilé from source (pulls minio/* bloqués)
│   ├── data/referentiels/        # référentiels embarqués, copiés dans lake/raw/ par minio-init
│   ├── trino/catalog/hive.properties   # Trino : connecteur Hive + S3 natif + MinIO
│   ├── spark/                    # Dockerfile (image chu-spark : S3A + Python 3.12 embarqué) + spark-defaults.conf
│   ├── superset/superset_config.py     # Superset : connexion Trino, cache off
│   └── s1-postgres/init.sh       # restauration du dump S1 (pg_restore)
├── 01_ddl/
│   ├── 01_schema_gold_hive.sql   # DDL Hive : schéma en étoile (Silver/Gold)
│   ├── 02_schema_gold_trino.sql  # Trino : vérification + statistiques (ANALYZE)
│   └── 03_vues_metier.sql        # vues métier (portable Hive/Trino)
├── 02_spark/
│   ├── common.py                 # session Spark (S3A, Hive Metastore, helpers)
│   ├── load_dimensions.py        # 6 dimensions (d_temps, d_patient, ...)
│   ├── load_faits_detail.py      # silver.f_consultation_detail, f_hospitalisation, f_satisfaction
│   ├── build_f_deces_agg.py      # gold.f_deces_agg (25,09 M décès INSEE)
│   ├── build_f_mesures.py        # gold.f_mesures (fait consolidé, 4 mesures)
│   └── build_agregats.py         # agrégats pré-calculés (diag/prof x mois)
├── 03_airflow/
│   └── dags/
│       ├── lakehouse_init.py     # one-shot : DDL + vues + vérifications + Superset (connexion + dashboards)
│       └── lakehouse_daily.py    # quotidien 02:00 : Spark -> DQ -> disponibilité Trino
├── 04_benchmark/
│   ├── 00_init.sql ... 02d_agregats.sql  # build PoC (DuckDB, exécuté)
│   ├── queries/                  # q01..q10 (+ _avant/_apres/_timer, ml/)
│   ├── results/                  # bench_results.csv, bench_synthese.csv, 3 SVG,
│   │                             #   resultats_requetes.txt
│   ├── run-duckdb.sh             # DuckDB dans un conteneur (rien sur l'hôte)
│   ├── run_bench.sh / run_bench_ml.sh    # harnais avant/après (PoC)
│   ├── gen_listes.js / synthese.js       # pruning simulé + synthèse/SVG
│   ├── harness_trino.sh          # benchmark contre la stack Docker (Trino)
│   ├── Dockerfile.duckdb         # image du conteneur benchmark/DQ
│   └── README.md
├── 05_qualite/
│   ├── dq_checks.sql             # 6 familles de contrôles qualité (autonome : lit le lake via httpfs)
│   ├── staging/                  # CSV staging embarqués (montés dans chu-bench-duckdb)
│   └── results/                  # dq_*.csv (7 rapports, régénérés par le DAG daily)
├── 06_superset/
│   ├── Dockerfile                # image chu-superset:4.1.1 (driver trino + psycopg2)
│   ├── build_dashboards.sh       # API REST : 17 datasets + 10 charts + dashboard (tâche init, idempotent)
│   ├── charts.sql                # requêtes de référence des besoins métier (SQL Lab)
│   └── README.md                 # connexion Trino + dashboards
└── 07_smoke_test/                # smoke test from scratch automatisé (smoke_from_scratch.sh + results/)
```

## Ordre d'exécution

### A. Stack complète (production)

```bash
cd scripts
docker compose up -d          # démarre les 17 services (MinIO, Hive Metastore,
                              # Spark, Trino, Superset, Airflow, S1, benchmark)
                              # dont 3 one-shot : minio-init, hive-metastore-init,
                              # airflow-init
# 1. Ingestion : minio-init copie "DATA Projet" -> s3a://lake/raw/ (one-shot)
# 2. Airflow (http://localhost:8090, admin/admin) :
#    - déclencher le DAG lakehouse_init      (DDL + vues + vérifications)
#    - déclencher le DAG lakehouse_daily     (charges Spark + DQ)
# 3. Superset (http://localhost:8088, admin/admin) : la connexion Trino
#    « Trino CHU » ET le dashboard « CHU — Indicateurs métier » (10 charts,
#    17 datasets) sont créés automatiquement par la tâche build_dashboards
#    du DAG init (06_superset/build_dashboards.sh, API REST idempotente)
# 4. Benchmark Trino : cd 04_benchmark && ./harness_trino.sh
```

### B. PoC benchmark (exécuté pendant le projet, hôte uniquement)

Voir `04_benchmark/README.md` : DuckDB tourne dans un conteneur jetable
(`run-duckdb.sh`), la source S1 dans le conteneur `chu-s1-postgres`
(service `s1-postgres`). Les résultats (CSV + SVG) sont déjà dans
`04_benchmark/results/`.

### C. Contrôles qualité seuls

La DQ s'exécute automatiquement dans le DAG `lakehouse_daily` (tâche
`dq_checks`, conteneur `chu-bench-duckdb`, autonome : httpfs + MinIO + staging
embarqué). Pour la rejouer seule :

```bash
docker exec chu-bench-duckdb sh -c 'cd /opt/l2work && duckdb /tmp/dq.duckdb < /opt/l2work/dq/dq_checks.sql'
# résultats écrits dans scripts/05_qualite/results/ (7 CSV)
```

## Couches de la stack (positionnement)

| Couche | Composant | Rôle dans le livrable |
|---|---|---|
| Ingestion | `minio-init` (AWS CLI) | copie des sources brutes vers `s3://lake/raw/` (clés normalisées) + référentiels embarqués (`config/data/referentiels/`) |
| Stockage | **MinIO** (build from source, `config/minio/Dockerfile`) | data lake (Parquet partitionné + tri intra-partition, zones raw/silver/gold) |
| Catalogue | **Hive Metastore** (+ PostgreSQL) | métadonnées des tables externes, statistiques, partitions |
| Traitement | **Apache Spark** | ETL/ELT : S1 (JDBC) + CSV -> Gold/Silver (02_spark/) |
| Orchestration | **Apache Airflow** | DAGs `lakehouse_init` / `lakehouse_daily` (03_airflow/) |
| Requêtage | **Trino** | lecture distribuée du lake, pruning, EXPLAIN ANALYZE |
| Visualisation | **Apache Superset** (localhost:8088) | dashboard « CHU — Indicateurs métier » : 10 charts (1/besoin q01-q08) + 17 datasets, créés auto par `build_dashboards.sh` (06_superset/) |
| Source | **PostgreSQL** (S1) | dump ChuSimulation (13 tables, ~3,7 M lignes = 3 674 384 au total : 11 tables métier = 3 509 952 lignes + 2 tables auxiliaires AAAA/date = 82 216 lignes chacune) |
