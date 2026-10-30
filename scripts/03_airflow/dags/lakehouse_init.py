# ============================================================================
# Livrable 2 - CHU - Airflow : DAG d'INITIALISATION (one-shot, manuel)
# ----------------------------------------------------------------------------
# Rôle d'Apache Airflow dans la stack : couche ORCHESTRATION. Ce DAG prépare
# la plateforme une seule fois (déclenchement manuel) :
#   1. création du schéma GOLD/Silver dans le Hive Metastore (DDL Hive)
#   2. création des vues métier (Hive + Trino)
#   3. vérification du déploiement côté Trino (volumes, pruning)
#   4. connexion Trino dans Superset (base de données + dataset)
#   5. tableaux de bord Superset (datasets + charts q01-q08 + dashboard
#      "CHU — Indicateurs métier") via scripts/06_superset/build_dashboards.sh
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
import re
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
        # Supprimer les commentaires (-- ...) AVANT de joindre les lignes :
        # aplati sur une seule ligne, un commentaire inline mangerait la fin
        # du statement (ParseException <EOF>).
        line = re.sub(r"--.*$", "", line)
        if line.strip():
            lines.append(line)
    statements = []
    for stmt in "\n".join(lines).split(";"):
        # Normaliser sur UNE seule ligne : beeline -e "..." coupe à chaque
        # retour à la ligne, ce qui séparait par ex. "CREATE DATABASE gold"
        # de son "LOCATION 's3a://lake/gold'" (le LOCATION était perdu et Hive
        # créait le répertoire par défaut s3a://lake/warehouse/gold.db, dont
        # le mkdir S3A est unreliable sur une stack fraîche).
        stmt = " ".join(stmt.split())
        if stmt:
            statements.append(stmt)
    return statements


def _wait_minio_init_done(timeout_s=900):
    """Attend que le conteneur one-shot chu-minio-init soit terminé.

    Sur une stack fraîche, minio-init recrée le bucket `lake` et y copie les
    sources raw (gros fichiers). Pendant ce bulk copy, les mkdir S3A faits par
    le metastore (CREATE DATABASE -> répertoire managé s3a://lake/warehouse/...)
    échouent de façon intermittente ("failed to create database"). On attend
    donc que l'ingestion initiale soit finie avant de jouer le DDL.
    """
    import time

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        proc = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Status}}", "chu-minio-init"],
            capture_output=True, text=True,
        )
        status = (proc.stdout or "").strip()
        if status in ("exited", "dead"):
            return
        time.sleep(10)
    raise AirflowException("chu-minio-init n'est pas terminé après 15 min")


def run_hive_sql_file(database, sql_path):
    """Exécute un script SQL Hive statement par statement via beeline (-e).

    Chaque statement est soumis avec `docker exec -e TERM=dumb chu-hive-hs2
    beeline -u jdbc:hive2://localhost:10000/<database> -e "<statement>"`.
    beeline renvoie 0 même en cas d'erreur SQL : la sortie est donc aussi
    inspectée (Error / FAILED / Exception) pour détecter les échecs.

    Le script complet est retenté jusqu'à 3 fois (30 s entre les essais) :
    les premier mkdir S3A du metastore peuvent échouer de façon transitoire
    juste après le démarrage de la stack.
    """
    _wait_minio_init_done()
    with open(sql_path, "r", encoding="utf-8") as f:
        statements = _split_sql_statements(f.read())
    if not statements:
        raise AirflowException(f"Aucun statement trouvé dans {sql_path}")
    last_output = ""
    for attempt in range(1, 4):
        try:
            _run_statements(database, sql_path, statements)
            return
        except AirflowException as exc:
            last_output = str(exc)
            if attempt < 3:
                import time

                time.sleep(30)
    raise AirflowException(last_output)


def _run_statements(database, sql_path, statements):
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
    # 2b. Vues métier côté Trino (vues NATIVES Trino, schéma vues_t) : le
    #     connecteur Hive de Trino ne lit pas les vues Hive du metastore
    #     (« Hive views are not supported ») -> Superset/Trino utilisent vues_t
    # -------------------------------------------------------------------------
    vues_metier_trino = BashOperator(
        task_id="vues_metier_trino",
        bash_command=(
            "docker exec chu-trino "
            "trino --server http://localhost:8080 --catalog hive "
            "--file /opt/scripts/01_ddl/04_vues_metier_trino.sql"
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
    # 4. Superset : cree la connexion Trino via l'API REST (voir
    #    dags/verif_superset.sh : attente /health, cookie+CSRF, driver trino
    #    dans l'image, et ECHEC REEL si la connexion n'est pas creee).
    # -------------------------------------------------------------------------
    # Le contenu du script est lu au parse time : un bash_command qui SE TERMINE
    # par ".sh" est traité par Airflow comme un template Jinja (template_ext
    # contient ".sh" -> jinja_env.get_template -> TemplateNotFound avant même
    # l'exécution). En passant par le contenu du fichier, on évite ce piège.
    from pathlib import Path as _Path
    _verif_sh = (_Path(__file__).parent / "verif_superset.sh").read_text()
    verif_superset = BashOperator(
        task_id="verification_superset",
        bash_command=_verif_sh,
    )

    # -------------------------------------------------------------------------
    # 5. Superset : dashboards métier (datasets + charts + dashboard) via
    #    scripts/06_superset/build_dashboards.sh, idempotent (422 = OK).
    #    Même piège Airflow 2.9.3 que ci-dessus : le contenu du script est lu
    #    au parse time (jamais de bash_command qui se termine par ".sh").
    #    Chemins candidats : conteneur Airflow (scripts montes sur
    #    /opt/airflow/scripts) puis repo local (scripts/06_superset/...).
    # -------------------------------------------------------------------------
    _bd_candidates = [
        _Path("/opt/airflow/scripts/06_superset/build_dashboards.sh"),
        _Path(__file__).resolve().parents[2] / "06_superset" / "build_dashboards.sh",
    ]
    _bd_path = next((p for p in _bd_candidates if p.is_file()), None)
    if _bd_path is None:
        raise FileNotFoundError(
            "build_dashboards.sh introuvable (candidats: "
            + ", ".join(str(p) for p in _bd_candidates) + ")"
        )
    _bd_sh = _bd_path.read_text()
    build_dashboards = BashOperator(
        task_id="build_dashboards",
        bash_command=_bd_sh,
    )

    ddl_hive >> vues_metier >> vues_metier_trino >> verif_trino >> verif_superset >> build_dashboards
