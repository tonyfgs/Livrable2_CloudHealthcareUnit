# Superset - Livrable 2 (CHU)

Apache Superset est la couche **visualisation** de la plateforme (dashboards),
connectée à **Trino** (requêtage) qui lit le schéma en étoile Gold du data
lake MinIO via le Hive Metastore.

## Connexion

1. Démarrer la stack : `docker compose -f scripts/docker-compose.yml up -d`
2. Ouvrir **http://localhost:8088** (admin / admin — à changer en production)
3. Créer la connexion base de données : **Settings > Database Connections > + Database**
   - Display name : `Trino CHU`
   - SQLAlchemy URI : `trino://trino@trino:8080/hive`
   - Extra (JSON) : `{"engine_params": {"connect_args": {"source": "chu-superset"}}}` (optionnel)
4. Vérifier : **SQL Lab** > base `Trino CHU` > schéma `gold` >
   `SELECT COUNT(*) FROM gold.f_mesures;`

> Les datasets sont des datasets **virtuels** (SQL) : les dashboards interrogent
> Trino en direct, aucune donnée n'est copiée dans Superset (cache désactivé
> dans `config/superset/superset_config.py`).

## Tableaux de bord (automatique)

Au from scratch, le DAG `lakehouse_init` exécute la tâche **`build_dashboards`**
(script `build_dashboards.sh`, API REST idempotente) qui crée directement :

- **17 datasets** : 12 tables physiques (`gold.*`, vues Trino `vues_t.*`) +
  5 datasets virtuels SQL (jointures agrégées, ex. mesures × établissement) ;
- **10 charts** : 1 par besoin métier q01-q08 (+ 2 complémentaires), types
  `dist_bar` / `pie` / `echarts_timeseries_line`, metrics `SUM(...)` sur les
  vraies colonnes du schéma gold ;
- **1 dashboard publié « CHU — Indicateurs métier »** (slug
  `chu-indicateurs-metier`) contenant les 10 charts (association via
  `json_metadata.positions`, piège n°35).

Vérification : `docker exec chu-superset-db psql -U superset -d superset -c
"SELECT (SELECT count(*) FROM dashboards), (SELECT count(*) FROM slices),
(SELECT count(*) FROM tables);"` → 1 / 10 / 17. Le smoke test vérifie ces
seuils (≥1 dashboard, ≥8 charts, ≥8 datasets, ≥8 charts embarqués).

## Tableaux de bord (manuel, alternative)

Le fichier `charts.sql` contient les **8 requêtes des 8 besoins métier** du
Livrable 1, prêtes à copier-coller dans SQL Lab. Pour chaque requête :

1. **SQL > SQL Lab** > base `Trino CHU`, schéma `gold`
2. Coller la requête, exécuter (Run)
3. **Visualize** > choisir le type de graphique recommandé (indiqué en
   commentaire de chaque requête) > **Save** > créer le dataset virtuel
   (nom suggéré : `chart_<n>_<besoin>`) > ajouter au dashboard
4. Assembler les 8 charts dans un dashboard **"CHU - Cloud Healthcare Unit"**
   (Dashboard > + Dashboard)

## Rafraîchissement

Aucun rafraîchissement de données : les charts sont live. Après chaque
exécution du DAG Airflow `lakehouse_daily` (02:00), les nouveaux données
sont visibles immédiatement (le cache du metastore Trino est rafraîchi au
TTL de 1 h, cf. `config/trino/catalog/hive.properties` ; forcer avec
`ANALYZE` / `MSCK REPAIR` côté Spark, déjà fait par les jobs).
