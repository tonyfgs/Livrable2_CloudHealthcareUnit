# 08_soutenance : matériel de la présentation des résultats

| Élément | Rôle |
|---|---|
| `sql/d01..d09*.sql` | Requêtes décisionnelles exécutées sur Trino (couches silver et gold), départage des ex aequo déterministe |
| `export_donnees.sh` | Exécute les requêtes sur le conteneur `chu-trino` et écrit `data/*.csv` |
| `_common.py`, `build_visuels.py` | Génèrent les 14 visuels PNG ; les assertions de réconciliation s'arrêtent si un total diffère des sources |
| `build_dashboard.py` | Génère `visuels/dashboard_chu.html` (HTML responsive, lisible sur téléphone) |
| `ref/` | Contours des régions métropolitaines (GeoJSON) et résultats de la campagne sur données complètes (q01, q07, qualité, benchmark) |
| `superset/` | **Tableau de bord Apache Superset** (parcours de la démonstration) : provisionnement par l'API (`provision_lake.py`, `provision.py`), contrôles (`verify.py`, `concordance_q01_q10.py`), captures (`capture.py`), export et import (`export_bundle.py`, `import_bundle.py`), liens permanents (`PERMALIENS.md`) |
| `GUIDE_SUPERSET.md` | Pas à pas : du lac au tableau de bord, reproductible de zéro (connexion Trino, étoile, filtres, histoire, mobile, vérification) |
| `cloudera/` et `GUIDE_POWERBI_CLOUDERA.md` | **Annexe optionnelle**, non utilisée pour la démonstration : export snappy, DDL Hive 1.1, VM Cloudera Quickstart, ODBC et Power BI. `cloudera/ddl_raw_cloudera.sql` est lu par `superset/provision_lake.py` |
| `ORAL_ET_VISUELS.md` | Pourquoi, comment, où pour chaque visuel ; déroulé oral ; histoire en 9 étapes dans Superset ; questions probables |

Remarque : `data/*.csv` et `data/d09_volumes_lake.csv` ont été produits sur une machine qui ne disposait que d'extraits des fichiers FINESS et décès (d_etablissement 1 999 lignes, f_deces_agg 307 lignes) ; les visuels qui en dépendent (V02, V08, V11) utilisent les résultats de la campagne sur données complètes rangés dans `ref/archive_donnees_completes/`. Relancer `export_donnees.sh` avec les données complètes pour remplacer ces volumes.

Régénération (stack démarrée) :

```bash
cd scripts/08_soutenance
./export_donnees.sh                  # CSV depuis Trino
python3 -m venv .venv && .venv/bin/pip install pandas matplotlib plotly
.venv/bin/python build_visuels.py
.venv/bin/python build_dashboard.py
```

Tableau de bord Superset (stack démarrée, voir `GUIDE_SUPERSET.md`) :

```bash
cd scripts/08_soutenance/superset
python3 provision_lake.py && python3 provision.py --mode archive   # ou --mode live
python3 verify.py && python3 concordance_q01_q10.py
```
