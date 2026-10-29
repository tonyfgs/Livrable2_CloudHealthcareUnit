# NOTE INTERNE — Livrable 2 CHU : la stack Docker en pratique

> Note interne (hors grille) : inventaire des conteneurs, comment on les lance,
> comment on vérifie les résultats, et les pièges rencontrés/testés.
> Public visé : la personne qui récupère le livrable et veut **arriver et lancer**.

---

## 1. Vue d'ensemble

Tout tourne dans Docker Compose : **17 services** sur un réseau bridge dédié
`chu-net` (pas d'installation sur le poste hôte — ni PostgreSQL, ni Spark, ni
Java, ni Python). Les sources brutes (`../DATA Projet`) sont montées en lecture
seule dans les conteneurs qui en ont besoin.

```
scripts/docker-compose.yml
├── Stockage      minio (9000 API S3 / 9001 console) + minio-init (one-shot : bucket lake + copie raw/)
├── Ingestion     (réalisé par minio-init : copie des CSV bruts vers s3a://lake/raw/)
├── Catalogue     metastore-db (PostgreSQL 15) + hive-metastore-init (one-shot : schematool)
│                 + hive-metastore (Thrift 9083) + hive-hs2 (HiveServer2, beeline 10000)
├── Source        s1-postgres (PostgreSQL 15, dump ChuSimulation restauré au 1er boot)
├── Traitement    spark-master (7077 + UI 8085) + spark-worker (UI 8086)
├── Requêtage     trino (8080)
├── Visualisation superset-db (PostgreSQL 15) + superset (8088)
├── Orchestration airflow-db (PostgreSQL 15) + airflow-init (one-shot : db migrate + user admin)
│                 + airflow-scheduler + airflow-webserver (8090)
└── Benchmark/DQ  bench-duckdb (DuckDB, conteneur « tooling », reste ouvert)
```

Quatre images sont **construites localement** (build automatique au `up`) :
- `chu-minio:RELEASE.2025-10-15T17-29-55Z` (`config/minio/Dockerfile` : compile le serveur MinIO depuis github.com — les pulls `minio/*` étant bloqués, voir §6.1)
- `chu-spark:3.5.1-s3a` (`config/spark/Dockerfile` : image officielle Spark 3.5.1 + connecteur S3A (hadoop-aws + AWS SDK v2) + **Python 3.12 embarqué** (python-build-standalone) pour matcher le driver Airflow (Python 3.12) — sinon `PYTHON_VERSION_MISMATCH` côté worker Python 3.8)
- `chu-superset:4.1.1` (`scripts/06_superset/Dockerfile` : image officielle + `psycopg2-binary`)
- `chu-airflow:2.9.3` (`scripts/03_airflow/Dockerfile` : image officielle + client Docker CLI + provider Spark 3.0.0)

---

## 2. Lancement

```bash
cd "Livrable 2/scripts"
docker compose up -d          # build les 4 images custom puis démarre les 17 services
```

Temps de démarrage : compter **2-3 minutes** au premier lancement
(téléchargement des images + `airflow db migrate` + initialisation du schéma
Hive Metastore par `hive-metastore-init` + démarrage de HiveServer2 ~60-90 s).

Vérification rapide que tout est levé :

```bash
docker compose ps             # 17 services, colonne STATUS = Up / healthy (ou Exited (0) pour les 3 one-shot)
```

Arrêt :

```bash
docker compose down           # arrêt (les volumes nommés conservent les données)
docker compose down -v        # arrêt + purge des volumes ( repart de zéro)
```

> **Piège (résolu) — pulls des repos `minio/*` bloqués** : sur certains
> daemons/proxys (dont celui du poste de développement), `docker pull
> minio/minio`, `quay.io/minio/minio` et `bitnami/minio` sont refusés
> (« pull access denied ... repository does not exist »), et `dl.min.io` ne
> sert plus les binaires en direct (HTTP 410). `docker compose up -d` aurait
> donc échoué en bloc au pull. **Solution livrée** : le service `minio` est
> construit **from source** (`config/minio/Dockerfile` compile le serveur
> MinIO officiel depuis `github.com/minio/minio`, tag
> `RELEASE.2025-10-15T17-29-55Z`) et `minio-init` utilise
> `amazon/aws-cli` (pull OK) au lieu de `minio/mc`. Résultat : un simple
> `docker compose up -d` suffit, **aucun pull d'image "minio" n'est tenté**.
> Premier build : compter ~5-10 min (compilation Go).

---

## 3. Inventaire des services (rôle, ports, healthcheck)

| Service | Image | Rôle | Ports (hôte) | Healthcheck |
|---|---|---|---|---|
| `minio` | `chu-minio:RELEASE.2025-10-15T17-29-55Z` (build local from source, `config/minio/Dockerfile`) | Data lake S3 (zones raw/silver/gold/warehouse) | 9000 (API S3), 9001 (console) | `wget /minio/health/live` |
| `minio-init` | `amazon/aws-cli:latest` | One-shot : `aws s3 mb s3://lake` + copie `../DATA Projet` vers `lake/raw/` | — | `depends_on: minio healthy` |
| `metastore-db` | `postgres:15` | SGBD du catalogue Hive (métadonnées) | — (interne) | `pg_isready` |
| `hive-metastore-init` | `apache/hive:4.2.1` | One-shot : `schematool -initSchema` (PostgreSQL) | — | `depends_on: metastore-db healthy` |
| `hive-metastore` | `apache/hive:4.2.1` (+ `ext-jars/` S3A copié dans `/opt/hive/lib` au boot) | Hive Metastore (Thrift) — catalogue des tables externes ; S3A requis pour le write-back des stats `ANALYZE` (voir §6.15) | 9083 | — |
| `hive-hs2` | `apache/hive:4.2.1` | HiveServer2 (beeline/JDBC) — interface du DAG `lakehouse_init` pour le DDL | 10000 | TCP 10000 |
| `s1-postgres` | `postgres:15` | Source S1 (dump ChuSimulation, restauré au 1er boot via `init.sh`) | 5432 | `pg_isready` |
| `bench-duckdb` | build `04_benchmark/Dockerfile.duckdb` | PoC/benchmark DuckDB + contrôles qualité (DQ) | — | (conteneur outillage, `sleep infinity`) |
| `spark-master` | `chu-spark:3.5.1-s3a` (build local, `config/spark/Dockerfile`) | Master Spark standalone (soumission Airflow) + S3A + Python 3.12 embarqué | 7077, 8085 (UI) | — |
| `spark-worker` | `chu-spark:3.5.1-s3a` (build local) | Worker Spark (2 cœurs, 4 Go) | 8086 (UI) | — |
| `trino` | `trinodb/trino:latest` (484) | Moteur de requêtage distribué (connecteur Hive → MinIO) | 8080 | — |
| `superset-db` | `postgres:15` | Métadonnées Superset | — (interne) | `pg_isready` |
| `superset` | `chu-superset:4.1.1` (build local) | Dashboards (8 charts, 1 par besoin métier) | 8088 | `/health` (via `curl`) |
| `airflow-db` | `postgres:15` | Métadonnées Airflow | — (interne) | `pg_isready` |
| `airflow-scheduler` | `chu-airflow:2.9.3` (build local) | Ordonnancement (LocalExecutor) + exécution des DAGs | — | — |
| `airflow-webserver` | `chu-airflow:2.9.3` | UI Airflow | 8090 | — |

Identifiants par défaut : MinIO `minioadmin/minioadmin` · Superset `admin/admin` ·
Airflow `admin/admin` · PostgreSQL internes : `hive/hive`, `chu/chu`,
`superset/superset`, `airflow/airflow`.

---

## 4. Ordre de mise en route (orchestration)

1. `docker compose up -d` (tout démarre ; `hive-metastore-init` et `minio-init`
   sont one-shot).
2. Ouvrir **Airflow** : http://localhost:8090 (admin/admin).
3. Déclencher **`lakehouse_init`** (one-shot, manuel) :
   DDL Hive (schéma silver/gold, tables externes) → vues métier →
   vérification Trino (volumes, `ANALYZE`, pruning) → connexion Trino dans
   Superset (API REST). **Testé : 5/5 tâches SUCCESS** (dont
   `vues_metier_trino`, voir piège n°23).
4. Déclencher **`lakehouse_daily`** (quotidien 02:00, déclenchement manuel
   possible) : `SparkSubmitOperator` — `load_dimensions` → `load_faits_detail`
   ∥ `build_f_deces_agg` → `build_f_mesures` (+ `build_agregats`) →
   `MSCK REPAIR` (découverte des partitions) → contrôles qualité (DQ dans
   `chu-bench-duckdb`, autonome) → disponibilité Trino pour Superset.
   **Testé : 8/8 tâches SUCCESS**, volumes vérifiés dans Trino (§5.4).

Les deux DAGs se testent aussi en CLI (sans l'UI) :

```bash
docker exec chu-airflow-scheduler airflow dags test lakehouse_init  2026-11-19
docker exec chu-airflow-scheduler airflow dags test lakehouse_daily 2026-11-19
# log : chercher "Marking task as SUCCESS" (8 tâches pour daily) et "state: success"
```
5. Ouvrir **Superset** : http://localhost:8088 (admin/admin) → connexion
   `Trino CHU` (créée par `lakehouse_init`, URI `trino://trino@trino:8080/hive`)
   → 8 tableaux de bord (requêtes prêtes dans `scripts/06_superset/charts.sql`).

---

## 5. Comment vérifier les résultats

### 5.1 Résultats du benchmark (perf avant/après partitionnement)

- Fichiers : `scripts/04_benchmark/results/` — `bench_results.csv` (450 lignes =
  3 campagnes × 10 requêtes × 3 scénarios × 5 runs), `bench_synthese.csv`
  (médianes), 3 graphiques SVG, `resultats_requetes.txt` (résultats métier
  commentés).
- Rejouer le benchmark sur Trino (stack levée) :
  `cd scripts/04_benchmark && ./harness_trino.sh`

### 5.2 Rapports qualité (DQ)

- Fichiers : `scripts/05_qualite/results/` — 7 CSV (`dq_completude.csv`,
  `dq_validite.csv`, `dq_doublons.csv`, `dq_doublons_faits.csv`,
  `dq_referentiel.csv`, `dq_temporel.csv`, `dq_volumes.csv`).
- Rejouer les contrôles seuls (la DQ tourne sinon automatiquement dans le DAG
  `lakehouse_daily`, tâche `dq_checks`) :
  ```bash
  docker exec chu-bench-duckdb sh -c 'cd /opt/l2work && duckdb /tmp/dq.duckdb < /opt/l2work/dq/dq_checks.sql'
  # résultats : scripts/05_qualite/results/ (monté dans le conteneur)
  ```
  Le script est **autonome** : `INSTALL httpfs` + endpoint MinIO en dur, lecture
  directe des Parquet du lake (`s3://lake/...`), staging embarqué
  (`05_qualite/staging/` monté en `:ro`).

### 5.3 Commandes de vérification, service par service

```bash
# État global
docker compose ps

# PostgreSQL (x4 : metastore-db, s1-postgres, superset-db, airflow-db)
docker exec chu-metastore-db  pg_isready -U hive    -d metastore
docker exec chu-s1-postgres    pg_isready -U chu     -d chu_s1
docker exec chu-s1-postgres    psql -U chu -d chu_s1 -tAc 'SELECT COUNT(*) FROM "Consultation"'   # attendu : 1027157

# Hive Metastore (Thrift 9083) — via Trino (qui joint le metastore)
docker exec chu-trino trino --execute 'SHOW CATALOGS'                 # attendu : hive, system
docker exec chu-trino trino --execute 'SHOW SCHEMAS FROM hive'        # attendu : default, gold, silver, ...
docker exec chu-trino trino --execute 'SHOW TABLES FROM hive.gold'    # 12 tables (6 dims + 6 faits/agrégats) + 8 vues métier

# HiveServer2 (beeline 10000)
docker exec -e TERM=dumb chu-hive-hs2 beeline -u jdbc:hive2://localhost:10000/gold -e 'SHOW TABLES'

# Spark
docker logs chu-spark-worker | grep "Successfully registered with master"
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8085   # UI master  -> 200
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8086   # UI worker  -> 200

# Trino
docker exec chu-trino trino --execute 'SELECT version()'           # 484
docker exec chu-trino trino --server http://localhost:8080 --catalog hive --schema gold \
  --file /opt/scripts/01_ddl/02_schema_gold_trino.sql             # volumes + ANALYZE + pruning

# Superset
curl -s http://localhost:8088/health                              # OK

# Airflow
docker exec chu-airflow-scheduler airflow version                 # 2.9.3
docker exec chu-airflow-scheduler airflow dags list               # lakehouse_init, lakehouse_daily
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:8090/health   # 200

# MinIO (data lake)
curl -s -o /dev/null -w '%{http_code}\n' http://localhost:9000/minio/health/live   # 200
docker compose run --rm --entrypoint aws minio-init \
  --endpoint-url http://minio:9000 s3 ls s3://lake/raw/ --recursive | head   # sources copiées
docker compose run --rm --entrypoint aws minio-init \
  --endpoint-url http://minio:9000 s3 ls s3://lake/gold/ --recursive | head  # tables Gold (Spark)
```

### 5.4 Volumes attendus (schéma en étoile, couche Gold/Silver) — vérifiés dans Trino après `lakehouse_daily`

| Table | Lignes attendues |
|---|---|
| `gold.d_temps` | 5 844 |
| `gold.d_patient` | 100 000 |
| `gold.d_etablissement` | 416 665 |
| `gold.d_localisation` | 102 (101 départements + Inconnu) |
| `gold.d_diagnostic` | 15 490 |
| `gold.d_professionnel` | 1 048 575 |
| `silver.f_consultation_detail` | 1 027 157 |
| `gold.f_mesures` | 1 030 402 |
| `gold.f_consult_diag_mois` | 979 797 |
| `gold.f_consult_prof_mois` | 1 001 453 |
| `gold.f_deces_agg` | 69 008 |
| `gold.f_satisfaction` | 1 960 |
| `gold.f_hospitalisation` | 2 479 |

Requête de contrôle (depuis le conteneur Trino) :

```bash
docker exec chu-trino trino --server http://localhost:8080 --catalog hive --schema gold \
  --output-format CSV --execute "
  SELECT 'd_temps', COUNT(*) FROM d_temps UNION ALL SELECT 'd_patient', COUNT(*) FROM d_patient
  UNION ALL SELECT 'd_etablissement', COUNT(*) FROM d_etablissement UNION ALL SELECT 'd_localisation', COUNT(*) FROM d_localisation
  UNION ALL SELECT 'd_diagnostic', COUNT(*) FROM d_diagnostic UNION ALL SELECT 'd_professionnel', COUNT(*) FROM d_professionnel
  UNION ALL SELECT 'f_consultation_detail', COUNT(*) FROM silver.f_consultation_detail
  UNION ALL SELECT 'f_mesures', COUNT(*) FROM f_mesures UNION ALL SELECT 'f_consult_diag_mois', COUNT(*) FROM f_consult_diag_mois
  UNION ALL SELECT 'f_consult_prof_mois', COUNT(*) FROM f_consult_prof_mois UNION ALL SELECT 'f_deces_agg', COUNT(*) FROM f_deces_agg
  UNION ALL SELECT 'f_satisfaction', COUNT(*) FROM f_satisfaction UNION ALL SELECT 'f_hospitalisation', COUNT(*) FROM f_hospitalisation"
```

---

## 6. Pièges connus et contournements (tous testés)

1. **MinIO — pulls des repos `minio/*` bloqués** (docker.io/minio,
   quay.io/minio, bitnami/minio : « pull access denied ... repository does
   not exist » ; dl.min.io ne sert plus les binaires : HTTP 410) : résolu par
   un **build from source** (`config/minio/Dockerfile`, tag
   `RELEASE.2025-10-15T17-29-55Z` cloné depuis github.com/minio/minio) et
   `amazon/aws-cli` pour `minio-init` — voir §2. Sans MinIO up, le DDL Hive
   échoue à l'appel S3 (`Failed to create external path s3a://lake/...`) :
   normal, MinIO est requis pour `lakehouse_init`.
2. **Trino 484 — propriétés refusées** : l'ancien filesystem S3A legacy
   (`hive.s3.*`, `hive.allow-drop-table`, `hive.parquet.batch-read-optimization.enabled`,
   `time-zone` dans le catalogue...) est supprimé. `config/trino/catalog/hive.properties`
   utilise le **S3 natif** : `fs.s3.enabled=true` + `s3.endpoint`,
   `s3.aws-access-key`, `s3.aws-secret-key`, `s3.path-style-access=true`,
   `s3.region=us-east-1`. Trino refuse de démarrer sur une propriété inconnue
   ("Configuration property ... was not used") : en cas de doute, démarrer avec
   `docker compose up -d --no-deps trino` et lire `docker logs chu-trino`.
3. **HiveServer2 séparé du metastore (`hive-hs2`)** : l'entrypoint de l'image
   `apache/hive:4.2.1` ne démarre HS2 que si `SERVICE_NAME=hiveserver2` (avec
   le classpath Tez). Le DAG exécute le DDL via `docker exec chu-hive-hs2
   beeline ...` (port 10000). Ne pas confondre avec `chu-hive-metastore`
   (Thrift 9083 uniquement).
4. **`beeline -f` inutilisable** (image `apache/hive:4.2.1`) : en mode
   non-interactif, le JLine embarqué soumet les lignes du script **sans les
   exécuter**. Le DAG `lakehouse_init` découpe donc le DDL en statements et
   exécute chacun via `beeline -e` (fonction `run_hive_sql_file`).
5. **`beeline` plante sans TTY** (`Unable to create a terminal` : le provider
   JLine FFM est compilé Java 21 *preview*, incompatible de la JVM du
   conteneur) : toujours appeler beeline avec `docker exec -e TERM=dumb ...`
   (provider « dumb »).
6. **S3A absent de l'image Hive** : `ClassNotFoundException
   org.apache.hadoop.fs.s3a.S3AFileSystem` au DDL. Fix : les jars
   `hadoop-aws-3.4.1` + AWS SDK **v2** 2.24.6 (modules individuels, ~10 Mo) sont
   embarqués dans `config/hive/ext-jars/` et copiés automatiquement dans
   `/opt/hive/lib` au démarrage par le *dynamic jar loader* de l'image
   (montage `/tmp/ext-jars`). Hadoop 3.4.x utilise AWS SDK **v2**
   (`software.amazon.awssdk`), pas v1. Régénérer : `ext-jars/download-jars.sh`.
   Credentials S3A (MinIO) : dans `config/hive-site.xml` (`fs.s3a.endpoint`,
   `fs.s3a.access.key`, `fs.s3a.secret.key`, `fs.s3a.path.style.access=true`,
   `fs.s3a.connection.ssl.enabled=false`).
7. **Driver JDBC PostgreSQL absent de l'image Hive** : `config/hive/postgresql-42.7.3.jar`
   est versionné dans le livrable et monté en `:ro` dans `hive-metastore-init`,
   `hive-metastore` et `hive-hs2` (pas de téléchargement au démarrage).
8. **`schematool` non idempotent** : `hive-metastore` démarre avec
   `IS_RESUME=true` (l'initialisation est faite une seule fois par
   `hive-metastore-init`, one-shot). Si le schéma est corrompu :
   `docker volume rm chu-l2_metastore-db-data` puis `docker compose up -d`.
9. **`init.sh` de `s1-postgres` doit être exécutable** (`chmod +x` — fait sur le
   dépôt ; un `git clone` frais préserve le bit via `git update-index
   --chmod=+x` si nécessaire). Sinon : « bad interpreter: Permission denied »
   et le dump n'est jamais restauré.
10. **Provider Spark Airflow** : `apache-airflow-providers-apache-spark==3.0.0`
    (dernière version compatible Airflow **2.9.x**). Les providers ≥ 4.x sont
    pour Airflow 3.x et feraient `pip` installer `apache-airflow 3.x` (qui
    casse le CLI `airflow` de l'image 2.9.3). Le `pip install` du Dockerfile
    passe par `python -m pip` (le wrapper `pip` de l'image officielle refuse
    de tourner en root).
11. **Ports Spark UI** : 8080 (Trino) est déjà pris → UI master Spark sur
    **8085** (hôte) et UI worker sur **8086**.
12. **Bind mount de fichier** : éditer un fichier monté **en place** (pas de
    remplacement d'inode) ou **recréer le conteneur** après édition
    (`docker compose up -d --force-recreate <service>`), sinon le conteneur
    garde l'ancien inode (symptôme : `ls` voit le fichier, `head` dit
    « No such file or directory »).
13. **Conteneur étranger** : un conteneur `hive-workshop` (image
    `apache/hive:4.2.1`, projet externe) peut tourner sur la même machine sans
    conflit (noms/ports/réseaux distincts).
14. **JRE absent de l'image Airflow** : `SparkSubmitOperator` exige
    `JAVA_HOME` — sans JRE, `spark-submit` échoue (« JAVA_HOME is not set »).
    Le Dockerfile installe `default-jre-headless` et fixe
    `ENV JAVA_HOME=/usr/lib/jvm/default-java` (symlink portable amd64/arm64 :
    ne PAS hardcoder `java-17-openjdk-amd64`, absent sur une machine arm64).
    Vérifié : `spark-submit --version` OK dans `chu-airflow-scheduler`
    (Spark 4.2.1 / OpenJDK 17.0.21).
15. **Metastore sans connecteur S3A** : `ANALYZE` (Trino) écrit les statistiques
    de partitions dans le metastore, qui liste alors les fichiers du lake via
    S3A (`updatePartitionStatsFast` → `Warehouse.getFileStatusesForLocation`)
    → `ClassNotFoundException: org.apache.hadoop.fs.s3a.S3AFileSystem` si les
    jars `hadoop-aws` + AWS SDK v2 sont absents. Fix : `config/hive/ext-jars/`
    monté sur `/tmp/ext-jars` **aussi** dans `chu-hive-metastore` (copié dans
    `/opt/hive/lib` au boot par le dynamic jar loader de l'image), comme déjà
    le cas pour `chu-hive-hs2`. Symptôme si absent : `ANALYZE` = `FAILED
    (HIVE_METASTORE_ERROR)` dans les logs Trino.
16. **Noms de partitions avec caractères invalides** : `MSCK REPAIR TABLE`
    échoue sur `region=Provence-Alpes-Côte d%27Azur` (« invalid characters in
    the name » — espaces/accents/apostrophe interdits dans les chemins de
    partitions Hive). Fix : `d_localisation` et `f_satisfaction` sont
    partitionnées par **`code_region`** (ASCII, ex. `93`), le nom de région
    restant une colonne data. Ne jamais partitionner sur un libellé texte
    accentué.
17. **Hook Spark Airflow** : l'URI de connexion doit avoir la partie host
    **URL-encodée** — `AIRFLOW_CONN_SPARK_DEFAULT=spark://spark%3A%2F%2Fspark-master%3A7077`
    (le hook reconstruit `master = host:port`). Et le driver Airflow tourne en
    Python 3.12 alors que l'image Spark embarque Python 3.8 →
    `PYTHON_VERSION_MISMATCH` : l'image custom `chu-spark:3.5.1-s3a`
    embarque un **Python 3.12** (python-build-standalone, asset selon
    `TARGETARCH`) en `/opt/python3.12`, exposé via `PYSPARK_PYTHON` et
    `spark.pyspark.python` dans la conf du DAG. Ne PAS télécharger de binaire
    x86_64 sur une machine arm64 (Apple Silicon).
18. **Cache du metastore côté Trino** : après un `MSCK REPAIR` ou un
    rechargement Spark, Trino peut garder les anciennes métadonnées en cache.
    `config/trino/catalog/hive.properties` fixe `hive.metastore-cache-ttl=0s`
    (+ `hive.metastore-refresh-interval=1m`). Après édition d'un fichier de
    config monté en bind : **recréer le conteneur** (`docker compose up -d
    --force-recreate <svc>` ou `docker restart chu-trino`) — un bind mount ne
    suffit pas si le conteneur a déjà démarré.
19. **Clés raw normalisées** : `minio-init` copie `../DATA Projet` vers
    `s3://lake/raw/` (arborescence d'origine) puis **normalise** les chemins
    lus par les jobs Spark (`raw/deces/deces.csv`, `raw/etablissement/etablissement_sante.csv`,
    `raw/hospitalisation/Hospitalisations.csv`, `raw/satisfaction/ESATIS_2019.csv`,
    `raw/satisfaction/ESATIS_2020.csv`, `raw/referentiels/...`) par copie
    server-side S3, et embarque les référentiels du dépôt
    (`config/data/referentiels/` monté en `:ro`). Les jobs Spark lisent
    exclusivement ces clés normalisées (`02_spark/common.py::read_raw_csv`).
20. **DQ autonome** : `05_qualite/dq_checks.sql` s'exécute dans
    `chu-bench-duckdb` avec `INSTALL httpfs` + endpoint MinIO en dur, lit les
    Parquet du lake (`s3://lake/{silver,gold}/.../**/*.parquet`) et embarque
    son staging (`05_qualite/staging/` monté en `:ro`) — plus de dépendance à
    un workspace externe (`L2WORK`). 7 CSV régénérés dans
    `05_qualite/results/` à chaque run du DAG daily.
21. **Trino tué par OOM (exit 137)** : sans plafond, le launcher Trino prend
    ~80 % de la RAM de la VM Docker Desktop (~6 Go sur une VM de 7,6 Go) ;
    pendant les gros jobs Spark (f_deces_agg, CSV 2 Go), la VM manque de
    mémoire et le noyau tue Trino (`Exited (137)`), ce qui fait échouer la
    tâche `dispo_trino_superset` (« container ... is not running »). Fix :
    `config/trino/config.properties` (plafonds `query.max-memory*`) +
    `JAVA_OPTS=-Xmx1500m` + `mem_limit: 2g` sur le service `trino`.
    Propriété supprimée en Trino 484 : `query.max-total-memory-per-node`
    (ne pas la remettre — Trino refuse de démarrer, « was not used »).
23. **Trino ne lit pas les vues Hive** : `SELECT ... FROM gold.v_xxx` échoue
    côté Trino (« Hive views are not supported ») — les vues de
    `01_ddl/03_vues_metier.sql` sont des vues HIVE (créées via beeline/HS2).
    Pour Superset/Trino, `01_ddl/04_vues_metier_trino.sql` recrée les mêmes
    vues en vues NATIVES Trino dans le schéma **`vues_t`** (tâche
    `vues_metier_trino` du DAG init) : interroger `vues_t.v_taux_consultation_mensuel`,
    pas `gold.v_taux_consultation_mensuel`. Le harnais Trino a une variante
    `04_benchmark/queries/trino/q09_*.sql` pour cette raison.
24. **Type de la partition `annee` de `f_satisfaction`** : déclarée `INT`
    (comme tous les faits) — ne pas écrire `annee = 2020` en dur sur une
    colonne STRING de partition, Trino refuse (`varchar = integer`).
25. **ZIP livrable** : `Livrable2_CloudHealthcareUnit.zip` (racine du projet)
    = dossier `Livrable2_CloudHealthcareUnit/` (rapport + `NOTE_INTERNE.md` +
    grille xlsx + `scripts/`, staging DQ embarqué, sans `.git`). **Extraire
    le ZIP à côté du dossier `DATA Projet`** (les sources brutes sont montées
    en relatif `../../DATA Projet` depuis `scripts/`) : poser `DATA Projet`
    en sibling du dossier extrait, puis `cd Livrable2_CloudHealthcareUnit/scripts
    && docker compose up -d`.
26. **OOM-killer de la VM Docker pendant `lakehouse_daily`** : la VM Docker
    Desktop fait 7,65 Go. Pendant les jobs Spark (worker à 3g) + Trino
    (Xmx1g) + Airflow/Superset/Hive/3×Postgres, la mémoire est saturée et le
    noyau tue le plus gros consommateur : **Trino mourait (OOMKilled=true,
    exit 137) au milieu du DAG**, et la tâche `dispo_trino_superset` (qui fait
    `docker exec chu-trino` depuis le scheduler via le socket Docker) échouait
    avec « container … is not running ». Correctifs appliqués :
    `restart: unless-stopped` sur les 14 services long terme (un conteneur
    tué est automatiquement relancé), `SPARK_WORKER_MEMORY: 2g` (suffisant pour
    ~1M de lignes), et la tâche `dispo_trino_superset` boucle jusqu'à ~3 min
    en attendant que Trino réponde avant de valider. Pour relancer une tâche
    plantée sans rejouer tout le DAG : `airflow tasks clear lakehouse_daily
    -t '^dispo_trino_superset$' -s <exec_date> -e <exec_date+1s> -y` (ou clic
    droit sur la tâche rouge dans la Grid view → Clear).

---

## 7. Où trouver quoi

| Besoin | Fichier |
|---|---|
| Rapport (grille d'évaluation) | `livrable2.md` |
| Cette note | `NOTE_INTERNE.md` |
| Stack Docker | `scripts/docker-compose.yml` |
| DDL Hive (schéma étoile) | `scripts/01_ddl/01_schema_gold_hive.sql` |
| Vérification Trino | `scripts/01_ddl/02_schema_gold_trino.sql` |
| Vues métier | `scripts/01_ddl/03_vues_metier.sql` |
| ETL PySpark | `scripts/02_spark/*.py` |
| DAGs Airflow | `scripts/03_airflow/dags/*.py` |
| Benchmark (10 requêtes) | `scripts/04_benchmark/` (+ `results/`) |
| Contrôles qualité | `scripts/05_qualite/` (+ `results/`) |
| Dashboards Superset | `scripts/06_superset/charts.sql` |
| Configs (Hive, Trino, Spark, Superset, S1) | `scripts/config/` |
27. **Splitter SQL du DAG init (`_split_sql_statements`)** : les statements
    étaient joints avec des retours à la ligne puis passés à `beeline -e` —
    beeline coupe à chaque newline, donc `CREATE DATABASE gold` perdait son
    `LOCATION 's3a://lake/gold'` (mkdir par défaut `s3a://lake/warehouse/…`,
    unreliable sur stack fraîche), et les commentaires inline `--` dans les
    listes de colonnes mangeaient la fin du statement (ParseException <EOF>).
    Fix : supprimer les commentaires `--` ligne par ligne PUIS normaliser
    chaque statement sur une seule ligne (`" ".join(stmt.split())`).
28. **CREATE DATABASE flaky sur stack fraîche** : juste après `docker compose
    up -d`, le metastore échoue par intermittence à créer le répertoire managé
    `s3a://lake/warehouse/<db>.db` (MetaException « failed to create database »)
    tant que `minio-init` n'a pas fini de recréer le bucket et de copier le raw.
    Fix dans le DAG init : `_wait_minio_init_done()` attend la fin du conteneur
    one-shot chu-minio-init, et le script DDL est retenté 3 fois (30 s) en cas
    d'échec transitoire. Ne PAS pré-créer de markers `warehouse/x.db/` à la main :
    un marker existant fait échouer le mkdir S3A (testé).
