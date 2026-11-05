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
# Etat des filtres natifs et des formulaires Explore : doit PERSISTER pour que les filtres natifs
# et les liens permanents (permalinks) du storytelling fonctionnent. Les caches de DONNEES restent
# desactives (dashboards toujours en direct).
FILTER_STATE_CACHE_CONFIG = {"CACHE_TYPE": "FileSystemCache", "CACHE_DIR": "/tmp/superset_filter_state",
                             "CACHE_DEFAULT_TIMEOUT": 86400, "CACHE_THRESHOLD": 1000}
EXPLORE_FORM_DATA_CACHE_CONFIG = {"CACHE_TYPE": "FileSystemCache", "CACHE_DIR": "/tmp/superset_explore_form",
                                  "CACHE_DEFAULT_TIMEOUT": 86400, "CACHE_THRESHOLD": 1000}

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
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_VIRTUALIZATION": False,   # rend tous les graphiques (captures d'ecran fiables)
}

# Le connecteur Trino est fourni par le package `trino` (driver DBAPI)
# installé dans l'image apache/superset.

# --- Livrable 3 : presentation en francais, nombres au format francais ---
BABEL_DEFAULT_LOCALE = "fr"
D3_FORMAT = {"decimal": ",", "thousands": "\u202f", "grouping": [3], "currency": ["", " \u20ac"]}
# Les dashboards sont affiches dans des iframes/captures : pas de restriction de contenu pour le Markdown de l'histoire
TALISMAN_ENABLED = False
