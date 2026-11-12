# ============================================================================
# cleanup_test.sh — RETIRÉ.
#
# Le bac à sable "TEST — Parcours CHU" est devenu le dashboard OFFICIEL
# "Parcours CHU" (provisionné par build_dashboards_test.sh) : ce script ne
# doit PLUS être exécuté, il supprimerait le dashboard officiel, ses charts
# et le schéma Trino hive.vues_test dont il dépend.
#
# Le provisionnement du dashboard officiel se fait désormais avec :
#   docker exec -i chu-superset bash -s < scripts/06_superset/build_dashboards_test.sh
# ============================================================================
echo "cleanup_test.sh est retiré : le bac à sable est devenu le dashboard officiel 'Parcours CHU'."
echo "Ne rien exécuter d'autre ici. Pour tout reprovisionner : build_dashboards_test.sh."
exit 0
