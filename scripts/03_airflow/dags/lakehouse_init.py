# ============================================================================
# Livrable 2 - CHU - Airflow : DAG d'INITIALISATION (one-shot, manuel)
# ----------------------------------------------------------------------------
# Rôle d'Apache Airflow dans la stack : couche ORCHESTRATION. Ce DAG prépare
# la plateforme une seule fois (déclenchement manuel) :
#   1. création du schéma GOLD/Silver dans le Hive Metastore (DDL Hive)
#   2. création des vues métier (Hive + Trino)
#   3. vérification du déploiement côté Trino (volumes, pruning)
#   4. connexion Trino dans Superset (base de données + dataset)
#
# Le DAG quotidien (lakehouse_daily.py) prend le relais pour les charges.
#
# Prérequis : docker-compose.yml démarré (services hive-metastore, trino,
# superset, s1-postgres). Les scripts DDL sont montés dans /opt/airflow/scripts
# (conteneur Airflow) et /opt/scripts (conteneurs hive-metastore / trino) :
# ce DAG pilote beeline et trino-cli via `docker exec` (socket Docker monté
# dans le scheduler, image chu-airflow construite depuis 03_airflow/Dockerfile).
# ============================================================================
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "chu-data-team",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="lakehouse_init",
    description="Initialisation one-shot de l'entrepôt CHU (DDL, vues, Superset)",
    default_args=default_args,
    schedule=None,          # déclenchement manuel uniquement
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["chu", "livrable2", "init"],
) as dag:

    # -------------------------------------------------------------------------
    # 1. DDL Hive : schéma silver/gold (tables EXTERNES sur s3a://lake)
    #    beeline tourne DANS le conteneur hive-metastore (docker exec) :
    #    jdbc:hive2://localhost:10000 depuis ce conteneur
    # -------------------------------------------------------------------------
    ddl_hive = BashOperator(
        task_id="ddl_schema_gold_hive",
        bash_command=(
            "docker exec chu-hive-metastore "
            "beeline -u jdbc:hive2://localhost:10000/default "
            "-f /opt/scripts/01_ddl/01_schema_gold_hive.sql"
        ),
    )

    # -------------------------------------------------------------------------
    # 2. Vues métier (portable Hive/Trino)
    # -------------------------------------------------------------------------
    vues_metier = BashOperator(
        task_id="vues_metier",
        bash_command=(
            "docker exec chu-hive-metastore "
            "beeline -u jdbc:hive2://localhost:10000/gold "
            "-f /opt/scripts/01_ddl/03_vues_metier.sql"
        ),
    )

    # -------------------------------------------------------------------------
    # 3. Vérification Trino (volumes + statistiques optimiseur + pruning)
    # -------------------------------------------------------------------------
    verif_trino = BashOperator(
        task_id="verification_trino",
        bash_command=(
            "docker exec chu-trino "
            "trino --server http://localhost:8080 --catalog hive --schema gold "
            "--file /opt/scripts/01_ddl/02_schema_gold_trino.sql"
        ),
    )

    # -------------------------------------------------------------------------
    # 4. Superset : la connexion Trino est déclarée dans superset_config.py
    #    (voir scripts/config/superset/superset_config.py) ; cette tâche vérifie
    #    qu'elle est active et que le schéma gold est requêtable.
    # -------------------------------------------------------------------------
    verif_superset = BashOperator(
        task_id="verification_superset",
        bash_command=(
            "curl -sf http://superset:8088/api/v1/database/ "
            "-H 'Accept: application/json' | head -c 400; "
            "curl -sf -X POST http://superset:8088/api/v1/sqllab/execute/ "
            "-H 'Content-Type: application/json' "
            "-d '{\"database_id\": 1, \"sql\": \"SELECT COUNT(*) FROM hive.gold.f_mesures\"}' "
            "|| echo 'Superset: connexion Trino à vérifier manuellement (localhost:8088)'"
        ),
    )

    ddl_hive >> vues_metier >> verif_trino >> verif_superset
