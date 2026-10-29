#!/usr/bin/env bash
# ============================================================================
# verification_superset (DAG lakehouse_init) : cree la connexion Trino dans
# Superset via l'API REST et ECHOUE VRAIMENT si elle n'est pas creee.
#
# Pièges testes et corrigés :
#   - attendre /health : Superset met 2-4 min à démarrer sur la VM Docker ;
#   - Superset 4.x exige, pour un POST authentifié : le bearer token MAIS
#     AUSSI le cookie de session + l'en-tête X-CSRFToken (bearer seul -> 400
#     "CSRF token is missing") ;
#   - le driver python `trino` doit être installé dans l'image
#     (06_superset/Dockerfile) sinon création -> 422 "Connection failed" ;
#   - ne JAMAIS terminer par un simple `echo` : un échec en amont serait
#     masqué et la tâche passerait au vert quand même.
# ============================================================================
set -u
BASE=http://superset:8088

echo "attente de /health (max 6 min)..."
for i in $(seq 1 36); do
  curl -sf "$BASE/health" >/dev/null 2>&1 && break
  echo "  superset pas encore pret ($i/36), attente 10s..."
  sleep 10
done
curl -sf "$BASE/health" >/dev/null || { echo 'ERREUR: superset /health KO apres 6 min'; exit 1; }
echo "health OK"

rm -f /tmp/sup_c.txt
TOKEN=$(curl -sf -c /tmp/sup_c.txt -X POST "$BASE/api/v1/security/login" \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin","provider":"db","refresh":true}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])') \
  || { echo 'ERREUR: login superset'; exit 1; }

CSRF=$(curl -sf -b /tmp/sup_c.txt -c /tmp/sup_c.txt "$BASE/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["result"])') \
  || { echo 'ERREUR: recuperation CSRF'; exit 1; }

code=$(curl -s -o /tmp/sup_resp.json -w '%{http_code}' -b /tmp/sup_c.txt \
  -X POST "$BASE/api/v1/database/" \
  -H "Authorization: Bearer $TOKEN" -H "X-CSRFToken: $CSRF" \
  -H 'Content-Type: application/json' -H 'Referer: http://superset:8088' \
  -d '{"database_name":"Trino CHU","sqlalchemy_uri":"trino://trino@trino:8080/hive"}')
echo "POST /api/v1/database/ -> HTTP $code : $(head -c 200 /tmp/sup_resp.json)"
# 201 = creee ; 422 = existe deja (idempotent) OU connexion invalide -> le GET
# final tranche dans tous les cas.
[ "$code" = "201" ] || [ "$code" = "422" ] || { echo 'ERREUR: creation connexion Trino CHU'; exit 1; }

# list simple (pas de filtre q=... : les espaces/caracteres speciaux de l'URL
# filtre cassent curl) puis grep sur le nom
curl -sf -b /tmp/sup_c.txt "$BASE/api/v1/database/" \
  -H "Authorization: Bearer $TOKEN" | grep -q '"database_name":"Trino CHU"' \
  || { echo 'ERREUR: connexion Trino CHU absente apres creation'; exit 1; }

echo ' <- connexion Trino CHU creee et verifiee via l API'
