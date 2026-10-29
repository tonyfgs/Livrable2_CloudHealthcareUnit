# 07_smoke_test — Test from scratch automatisé

`smoke_from_scratch.sh` prouve qu'un démarrage from scratch fonctionne sans bug :

```bash
cd scripts && ./07_smoke_test/smoke_from_scratch.sh
```

Enchaînement testé (exit 0 = 100% PASS, exit 1 = au moins un FAIL) :

1. `docker compose down -v` (purge totale) puis `up -d`
2. 14 conteneurs `running` + 3 one-shots `exited(0)` (minio-init, hive-metastore-init, airflow-init)
3. Endpoints HTTP : Airflow 8090, Superset 8088, Trino 8080, MinIO 9000/9001, Spark UI 8085
4. Les 2 DAGs sont enregistrés **en pause** avec **0 run auto** (le backfill ne peut plus
   rimer avec l'init — fix `DAGS_ARE_PAUSED_AT_CREATION=true`)
5. `lakehouse_init` : unpause + trigger → run `success`, **5/5 tâches success, 0 failed**
6. `lakehouse_daily` : unpause → run auto (`catchup=False`, `max_active_runs=1`) →
   run `success`, **8/8 tâches success, 0 failed**
7. **Volumes exacts vérifiés dans Trino** : d_temps 5 844 ; d_patient 100 000 ;
   d_etablissement 416 665 ; d_localisation 102 ; d_diagnostic 15 490 ;
   d_professionnel 1 048 575 ; silver.f_consultation_detail 1 027 157 ;
   f_mesures **1 030 402** ; f_consult_diag_mois 979 797 ; f_consult_prof_mois 1 001 453 ;
   f_deces_agg 69 008 ; f_satisfaction 1 960 ; f_hospitalisation 2 479 ;
   + 12 tables dans `gold`, 9 vues dans `vues_t`, vues métier non vides
8. Data lake MinIO : bucket `lake`, prefixes `raw/ silver/ gold/ warehouse/` non vides,
   clés normalisées présentes, `raw/` >= nb de fichiers sources de `DATA Projet`
9. Superset : connexion `Trino CHU` créée par l'init
10. Source S1 PostgreSQL peuplée + 9 fichiers de résultats DQ non vides (dont `dq_completude_faits.csv` et `dq_integrite_fk.csv` sur les données intégrées)
11. Les 2 DAGs sont remis en pause (état final = celui d'un démarrage neuf)

Logs : `07_smoke_test/results/smoke_<timestamp>.log`.
Durée ~20-30 min. Prérequis : `DATA Projet/` présent à côté de `Livrable 2/`.
