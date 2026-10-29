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

Deux images sont **construites localement** (build automatique au `up`) :
- `chu-superset:4.1.1` (`scripts/06_superset/Dockerfile` : image officielle + `psycopg2-binary`)
- `chu-airflow:2.9.3` (`scripts/03_airflow/Dockerfile` : image officielle + client Docker CLI + provider Spark 3.0.0)

---

## 2. Lancement

```bash
cd "Livrable 2/scripts"
docker compose up -d          # build les 2 images custom puis démarre les 17 services
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

> **Piège (constaté sur la machine de développement)** : `docker pull
> minio/minio` était refusé par le daemon (« pull access denied ») sur un
> poste. `docker compose up -d` échoue alors en bloc (compose pull toutes les
> images avant de démarrer). Contournement : démarrer service par service,
> MinIO excepté, puis les services qui en dépendent avec `--no-deps` :
> ```bash
> docker compose up -d metastore-db hive-metastore-init hive-metastore hive-hs2 \
>   s1-postgres bench-duckdb spark-master spark-worker airflow-db \
>   airflow-scheduler airflow-webserver superset-db
> docker compose up -d --no-deps trino superset
> ```
> Sur une machine avec un accès Docker Hub normal, un simple
> `docker compose up -d` suffit.

---

## 3. Inventaire des services (rôle, ports, healthcheck)

| Service | Image | Rôle | Ports (hôte) | Healthcheck |
|---|---|---|---|---|
| `minio` | `minio/minio:latest` | Data lake S3 (zones raw/silver/gold/warehouse) | 9000 (API S3), 9001 (console) | `mc ready local` |
| `minio-init` | `minio/mc:latest` | One-shot : crée le bucket `lake`, copie `../DATA Projet` vers `lake/raw/` | — | `depends_on: minio healthy` |
| `metastore-db` | `postgres:15` | SGBD du catalogue Hive (métadonnées) | — (interne) | `pg_isready` |
| `hive-metastore-init` | `apache/hive:4.2.1` | One-shot : `schematool -initSchema` (PostgreSQL) | — | `depends_on: metastore-db healthy` |
| `hive-metastore` | `apache/hive:4.2.1` | Hive Metastore (Thrift) — catalogue des tables externes | 9083 | — |
| `hive-hs2` | `apache/hive:4.2.1` | HiveServer2 (beeline/JDBC) — interface du DAG `lakehouse_init` pour le DDL | 10000 | TCP 10000 |
| `s1-postgres` | `postgres:15` | Source S1 (dump ChuSimulation, restauré au 1er boot via `init.sh`) | 5432 | `pg_isready` |
| `bench-duckdb` | build `04_benchmark/Dockerfile.duckdb` | PoC/benchmark DuckDB + contrôles qualité (DQ) | — | (conteneur outillage, `sleep infinity`) |
| `spark-master` | `apache/spark:3.5.1` | Master Spark standalone (soumission Airflow) | 7077, 8085 (UI) | — |
| `spark-worker` | `apache/spark:3.5.1` | Worker Spark (2 cœurs, 4 Go) | 8086 (UI) | — |
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
   Superset (API REST).
4. Déclencher **`lakehouse_daily`** (quotidien 02:00, déclenchement manuel
   possible) : `SparkSubmitOperator` — `load_dimensions` → `load_faits_detail`
   ∥ `build_f_deces_agg` → `build_f_mesures` (+ `build_agregats`) → contrôles
   qualité (DQ dans `chu-bench-duckdb`) → disponibilité Trino pour Superset.
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
- Rejouer les contrôles seuls : `cd scripts/04_benchmark && ./run-duckdb.sh`
  (le script choisit automatiquement le réseau `chu-l2_chu-net` si `chu-s1-postgres`
  tourne, sinon un réseau jetable).

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
docker exec chu-trino trino --execute 'SHOW TABLES FROM hive.gold'    # 10 tables (4 faits + 4 dims + vues)

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
docker exec chu-minio-init mc ls chu/lake/raw/ --recursive | head  # sources copiées
docker exec chu-minio-init mc ls chu/lake/gold/  --recursive | head # tables Gold écrites par Spark
```

### 5.4 Volumes attendus (schéma en étoile, couche Gold/Silver)

| Table | Lignes attendues |
|---|---|
| `gold.f_mesures` | 1 030 402 |
| `silver.f_consultation_detail` | 1 027 157 |
| `gold.f_consult_diag_mois` | 979 797 |
| `gold.f_consult_prof_mois` | 1 001 453 |
| `gold.f_deces_agg` | 69 008 |
| `gold.f_satisfaction` | 1 960 |
| `gold.f_hospitalisation` | 2 479 |

---

## 6. Pièges connus et contournements (tous testés)

1. **MinIO — pull bloqué sur la machine de dev** : voir §2 (démarrage avec
   `--no-deps`). Sans MinIO, le DDL Hive échoue à l'appel S3
   (`Failed to create external path s3a://lake/warehouse/gold.db`) : normal,
   MinIO est requis pour `lakehouse_init`.
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
