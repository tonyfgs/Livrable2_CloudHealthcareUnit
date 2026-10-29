# Benchmark - Livrable 2 (CHU)

Deux harnais complémentaires :

| Harnais | Où | Rôle |
|---|---|---|
| `run_bench.sh` / `run_bench_ml.sh` + `synthese.js` | hôte (DuckDB) | **PoC exécuté** : évalue l'apport du partitionnement + bucketing + tri sur les 10 requêtes, scénarios `avant` / `apres` / `apres_ml` (pruning façon metastore), 5 répétitions, médianes + volumes lus |
| `harness_trino.sh` | stack Docker Compose | **Validation plateforme** : rejoue les 10 requêtes contre Trino (Hive Metastore + MinIO) après déploiement, avec `EXPLAIN ANALYZE` sur q01/q10 |

## Résultats du PoC (exécuté)

- `results/bench_results.csv` : 10 requêtes x 3 scénarios x 5 runs (ms + volume lu)
- `results/bench_synthese.csv` : médianes, accélérations, ratio de volume
- `results/graph_temps_median.svg` : temps médian avant / après / après+metastore
- `results/graph_volume_lu.svg` : volume de données lu (Mo)
- `results/graph_reduction_volume.svg` : taux de réduction du volume lu
- `results/resultats_requetes.txt` : résultats métier des 8 requêtes (métier)

## Rejouer le PoC (hôte)

Le PoC s'exécute **100 % dans Docker** (aucune installation sur le poste) :
DuckDB tourne dans un conteneur `postgres:15` jetable (voir `run-duckdb.sh`),
la source S1 est le conteneur PostgreSQL `chu-s1-postgres`.

### 1. Préparer le workspace (une fois)

```bash
export L2WORK=/tmp/l2work                 # surchargeable
mkdir -p $L2WORK
ln -s "$(pwd)/scripts/04_benchmark" $L2WORK/poc   # ce répertoire = poc/
cp scripts/04_benchmark/run-duckdb.sh $L2WORK/     # runner DuckDB-in-Docker
# staging/ : CSV nettoyés (encodage UTF-8, séparateur ';') dérivés de
# "DATA Projet" : satisfaction 2019/2020, hospitalisations, référentiels
# (départements, professionnels, activités) - cf. scripts/05_qualite.
# Le binaire DuckDB est téléchargé automatiquement par run-duckdb.sh.
```

### 2. Conteneur source S1 (PostgreSQL ChuSimulation)

```bash
# Either via la stack complète :
docker compose -f scripts/docker-compose.yml up -d s1-postgres
# or standalone (identique) :
docker run -d --name chu-s1-postgres --network chu-net \
  -e POSTGRES_USER=chu -e POSTGRES_PASSWORD=chu -e POSTGRES_DB=chu_s1 \
  -v s1-data:/var/lib/postgresql/data \
  -v "$PWD/DATA Projet/BDD PostgreSQL/DATA2023:/docker-entrypoint-initdb.d/DATA2023.dump:ro" \
  -v "$PWD/scripts/config/s1-postgres/init.sh:/docker-entrypoint-initdb.d/init.sh:ro" \
  postgres:15
```

### 3. Construire l'entrepôt PoC (scripts 00 -> 02d, ordre impératif)

```bash
cd $L2WORK
./run-duckdb.sh poc/warehouse.duckdb < poc/00_init.sql        # sources + matérialisation
./run-duckdb.sh poc/warehouse.duckdb < poc/01_dims.sql         # 6 dimensions
./run-duckdb.sh poc/warehouse.duckdb < poc/02a_faits_detail.sql # faits détail + satisfaction
./run-duckdb.sh poc/warehouse.duckdb < poc/02b_deces_agg.sql   # décès agrégés (25 M lignes)
./run-duckdb.sh poc/warehouse.duckdb < poc/02c_f_mesures.sql   # fait consolidé f_mesures
./run-duckdb.sh poc/warehouse.duckdb < poc/02d_agregats.sql    # agrégats pré-calculés
# -> exports avant/ (plat) et apres/ (partitionné + bucketé + trié)
```

### 4. Benchmark + synthèse

```bash
cd $L2WORK
./poc/run_bench.sh avant        # scénario AVANT  (Parquet plat, 1 fichier/table)
./poc/run_bench.sh apres        # scénario APRES  (partitionné + bucketé + trié)
./poc/run_bench_ml.sh           # scénario APRES_ML (pruning façon Hive Metastore)
node poc/synthese.js            # synthèse CSV + 3 graphiques SVG
```

Le scénario `apres_ml` simule le pruning de partitions d'un vrai metastore
(Hive/Trino) par liste explicite de fichiers (générée par `node poc/gen_listes.js`) :
DuckDB local n'a pas de metastore, le glob `**/*.parquet` pénaliserait sinon
le scénario "après" (~0,38 ms de listing par fichier).

## Rejouer le benchmark Trino (stack Docker)

```bash
docker compose -f scripts/docker-compose.yml up -d
# ... exécuter les DAGs Airflow lakehouse_init puis lakehouse_daily ...
cd scripts/04_benchmark
./harness_trino.sh          # résultats dans results/trino/
```

## Les 10 requêtes

| # | Fichier | Type | Besoin |
|---|---|---|---|
| q01 | `queries/q01_consult_etab_2019.sql` | métier 1 | Consultations par établissement, 2019 (top 20) |
| q02 | `queries/q02_consult_diag_2019_2021.sql` | métier 2 | Consultations par diagnostic, 2019-2021 (top 10) |
| q03 | `queries/q03_taux_hosp_periode.sql` | métier 3 | Taux d'hospitalisation, 2015-2021 |
| q04 | `queries/q04_hosp_diag.sql` | métier 4 | Hospitalisations par diagnostic (top 10) |
| q05 | `queries/q05_hosp_sexe_age.sql` | métier 5 | Hospitalisations par sexe x tranche d'âge |
| q06 | `queries/q06_consult_prof_2020.sql` | métier 6 | Consultations par professionnel, 2020 (top 10) |
| q07 | `queries/q07_deces_region_2019.sql` | métier 7 | Décès par région, 2019 |
| q08 | `queries/q08_satisfaction_region_2020.sql` | métier 8 | Satisfaction par région, 2020 |
| q09 | `queries/q09_vue_taux_mensuel.sql` | technique | Vue métier (moyenne mobile 3 mois) |
| q10 | `queries/q10_etoile_complete.sql` | technique | Jointure étoile complète (fait + 4 dims) |

Les 8 requêtes métier correspondent 1:1 aux 8 besoins du Livrable 1.
