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

## Tableaux de bord

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
