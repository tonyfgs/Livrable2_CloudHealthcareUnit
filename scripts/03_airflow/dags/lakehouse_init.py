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
# Prérequis : docker-compose.yml démarré (services hive-metastore, hive-hs2,
# trino, superset, s1-postgres). Les scripts DDL sont montés dans
# /opt/airflow/scripts (conteneur Airflow) et /opt/scripts (conteneurs
# hive-hs2 / trino) : ce DAG pilote beeline et trino-cli via `docker exec`
# (socket Docker monté dans le scheduler, image chu-airflow construite depuis
# 03_airflow/Dockerfile).
#
# Note technique (beeline) : le DDL est exécuté statement par statement via
# `beeline -e` et NON via `beeline -f` :
#   - `beeline -f` est inutilisable avec l'image apache/hive:4.2.1 en mode
#     non-interactif (le JLine embarqué soumet les lignes du script sans
#     jamais les exécuter) ;
#   - sans TTY, beeline tente d'ouvrir le provider JLine FFM (compilé Java 21
#     preview, incompatible de la JVM 17 du conteneur) et plante ; la variable
#     TERM=dumb force le provider 'dumb' et beeline démarre correctement.
# ============================================================================
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "chu-data-team",
    "depends_on_past": False,
    "retries": 1,
}

# HiveServer2 (service chu-hive-hs2, port 10000) : interface JDBC/beeline
# utilisée pour le DDL. Le metastore (chu-hive-metastore) n'expose que le
# Thrift 9083 (Spark, Trino).
HS2_CONTAINER = "chu-hive-hs2"
BEELINE_URL = "jdbc:hive2://localhost:10000"


def _split_sql_statements(sql_text):
    """Découpe un script SQL en statements exécutables (commentaires -- ignorés)."""
    lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("--"):
            continue
        lines.append(line)
    statements = []
    for stmt in "\n".join(lines).split(";"):
        stmt = stmt.strip()
        if stmt:
            statements.append(stmt)
    return statements


def run_hive_sql_file(database, sql_path):
    """Exécute un script SQL Hive statement par statement via beeline (-e).

    Chaque statement est soumis avec `docker exec -e TERM=dumb chu-hive-hs2
    beeline -u jdbc:hive2://localhost:10000/<database> -e "<statement>"`.
    beeline renvoie 0 même en cas d'erreur SQL : la sortie est donc aussi
    inspectée (Error / FAILED / Exception) pour détecter les échecs.
    """
    with open(sql_path, "r", encoding="utf-8") as f:
        statements = _split_sql_statements(f.read())
    if not statements:
        raise AirflowException(f"Aucun statement trouvé dans {sql_path}")
    for i, stmt in enumerate(statements, 1):
        proc = subprocess.run(
            [
                "docker", "exec", "-e", "TERM=dumb", HS2_CONTAINER,
                "beeline", "-u", f"{BEELINE_URL}/{database}", "-e", stmt,
            ],
            capture_output=True,
            text=True,
            timeout=600,
        )
        output = (proc.stdout or "") + (proc.stderr or "")
        failed = (
            proc.returncode != 0
            or "Error" in output
            or "FAILED" in output
            or "Exception" in output
        )
        if failed:
            raise AirflowException(
                f"Échec du statement {i}/{len(statements)} de {sql_path} "
                f"(base '{database}') :\n{output[-3000:]}"
            )


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
    #    beeline tourne DANS le conteneur chu-hive-hs2 (docker exec) :
    #    jdbc:hive2://localhost:10000 depuis ce conteneur
    # -------------------------------------------------------------------------
    ddl_hive = PythonOperator(
        task_id="ddl_schema_gold_hive",
        python_callable=run_hive_sql_file,
        op_kwargs={
            "database": "default",
            "sql_path": "/opt/airflow/scripts/01_ddl/01_schema_gold_hive.sql",
        },
    )

    # -------------------------------------------------------------------------
    # 2. Vues métier (portable Hive/Trino)
    # -------------------------------------------------------------------------
    vues_metier = PythonOperator(
        task_id="vues_metier",
        python_callable=run_hive_sql_file,
        op_kwargs={
            "database": "gold",
            "sql_path": "/opt/airflow/scripts/01_ddl/03_vues_metier.sql",
        },
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
    #    (TRINO_CHU_URI, voir scripts/config/superset/superset_config.py) ;
    #    cette tâche vérifie que l'API répond (health) et tente de créer la
    #    connexion Trino via l'API REST (login admin/admin par défaut).
    # -------------------------------------------------------------------------
    verif_superset = BashOperator(
        task_id="verification_superset",
        bash_command=(
            "curl -sf http://superset:8088/health && echo ' <- health OK' && "
            "TOKEN=$(curl -sf -X POST http://superset:8088/api/v1/security/login "
            "-H 'Content-Type: application/json' "
            "-d '{\"username\":\"admin\",\"password\":\"admin\",\"provider\":\"db\",\"refresh\":true}' "
            "| python3 -c 'import sys,json;print(json.load(sys.stdin)[\"access_token\"])') && "
            "curl -sf -X POST http://superset:8088/api/v1/database/ "
            "-H \"Authorization: Bearer $TOKEN\" -H 'Content-Type: application/json' "
            "-d '{\"database_name\":\"Trino CHU\",\"sqlalchemy_uri\":\"trino://trino@trino:8080/hive\"}' "
            "| head -c 300; "
            "echo ' <- connexion Trino CHU (idempotente si déjà créée : 422 ignoré)'"
        ),
    )

    ddl_hive >> vues_metier >> verif_trino >> verif_superset
