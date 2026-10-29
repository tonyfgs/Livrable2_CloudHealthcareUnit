# ============================================================================
# Livrable 2 - CHU - Apache Superset : configuration (service `superset`)
# ----------------------------------------------------------------------------
# Superset se connecte à Trino (catalogue `hive`) pour interroger le schéma
# en étoile Gold. Les datasets sont des datasets VIRTUELS (SQL) : les
# tableaux de bord sont donc LIVE (pas de copie de données dans Superset).
# ============================================================================
import os

# ---------------------------------------------------------------------------
# Base de métadonnées de Superset (service superset-db)
# ---------------------------------------------------------------------------
SQLALCHEMY_DATABASE_URI = (
    os.environ.get(
        "SUPERSET_SQLALCHEMY_URI",
        "postgresql+psycopg2://superset:superset@superset-db:5432/superset",
    )
)

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me-in-production-0123456789abcdef")

# ---------------------------------------------------------------------------
# Connexion Trino : créée automatiquement au premier démarrage.
# Chaîne SQLAlchemy du driver Trino : trino://<user>@trino:8080/hive
# (catalogue `hive` = schéma Gold du lake ; l'utilisateur Trino est libre,
#  aucune authentification côté Trino en environnement local).
# ---------------------------------------------------------------------------
PREFERRED_DATABASES = ["Trino"]

# Connexion déclarée par défaut (Superset > Settings > Database Connections)
# -> Database name : Trino CHU  |  SQLAlchemy URI : trino://trino@trino:8080/hive
TRINO_CHU_URI = "trino://trino@trino:8080/hive"

# ---------------------------------------------------------------------------
# Caches : AUCUN cache de données (tableaux de bord live sur Trino).
# ---------------------------------------------------------------------------
CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
FILTER_STATE_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
EXPLORE_FORM_DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}

# ---------------------------------------------------------------------------
# Divers
# ---------------------------------------------------------------------------
ROW_LIMIT = 100000                 # limite de lignes par requête de chart
DEFAULT_SQLLAB_LIMIT = 10000
SQL_MAX_ROW = 1000000
SUPERSET_WEBSERVER_PORT = 8088
ENABLE_TEMPLATE_PROCESSING = True

# Feature flags utiles pour un entrepôt (drill, requêtes croisées)
FEATURE_FLAGS = {
    "DRILL_TO_DETAIL": True,
    "DYNAMIC_PLUGINS": False,
}

# Le connecteur Trino est fourni par le package `trino` (driver DBAPI)
# installé dans l'image apache/superset.
