# ============================================================================
# Livrable 2 - CHU - Image Superset du projet
# ----------------------------------------------------------------------------
# Image officielle Apache Superset 4.1.1 + drivers Python absents de l'image
# officielle :
#   - psycopg2-binary : requis pour la base de métadonnées `superset-db`
#     (service PostgreSQL du docker-compose.yml) ;
#   - trino           : driver DBAPI du connecteur Trino (sqlalchemy_uri
#     trino://trino@trino:8080/hive). SANS lui, la création de la connexion
#     "Trino CHU" via l'API echoue en 422 "Connection failed" (teste).
# ============================================================================
FROM apache/superset:4.1.1

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.9 trino==0.330.0
USER superset
