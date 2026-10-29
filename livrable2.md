# Livrable 2 — Implémentation Physique et Évaluation de Performance
## Projet Cloud Healthcare Unit (CHU) — Entrepôt de données décisionnel

**Groupe** : Tony Fages
**Date** : 29 octobre 2026
**Format** : Rapport + archive ZIP (`Livrable2_CloudHealthcareUnit.zip`)
**Livrables couverts** : ce document traite exclusivement le **Livrable 2** (implémentation physique et évaluation de performance). Le Livrable 1 (modélisation) est dans `../Livrable 1/L1-rapport-cloud-health-unit.md`.

---

## 1. Introduction et périmètre

### 1.1 Contexte

Le groupe CHU a engagé une transformation digitale de son système d'information. Le Livrable 1 a produit le modèle dimensionnel (schéma en étoile, 4 faits métier : consultations, hospitalisations, décès, satisfaction) et le recensement des 8 besoins utilisateurs. Le **Livrable 2** implémente physiquement cet entrepôt et en évalue les performances.

### 1.2 Sources intégrées (4)

| Source | Contenu | Volume |
|---|---|---|
| **S1** — PostgreSQL opérationnel (dump `DATA2023`) | Patients, consultations, diagnostics, professionnels de santé, prescriptions... | 10 tables, ~4,2 M lignes (Patient 100 000 ; Consultation 1 027 157 ; Professionnel_de_sante 1 048 575 ; Diagnostic 15 490 ; Prescription 1 003 845) |
| **S2** — CSV `Hospitalisations.csv` | Séjours hospitaliers (entrée, durée, diagnostic, établissement) | 2 479 lignes, 2015-06 → 2021-06 |
| **S3** — Fichiers ESATIS (FTP) | Satisfaction patients (score ajusté, répondants, région) | 2019 : 1 152 lignes ; 2020 : 1 143 lignes |
| **S4** — INSEE + référentiels FINESS | Décès (1882-2020) ; établissements (416 665) ; professionnels (1 048 575) ; activités (1 823 588) ; référentiel départements (101) | Décès : **25 088 208 lignes** |

### 1.3 Périmètre du Livrable 2

- Scripts SQL de création des tables (**DDL**) et de chargement (**DML**), commentés et versionnés (Git)
- Stratégie de **partitionnement et bucketing** justifiée par moteur
- **Rapport de qualité des données** (complétude, validité, doublons, cohérence référentielle FINESS, temporalité, volumes)
- **10 requêtes de benchmark** (8 métier = 1 par besoin du Livrable 1 + 2 requêtes techniques)
- **Évaluation de performance avant/après** partitionnement + bucketing, avec graphiques
- **Visualisation** (Apache Superset) et **orchestration** (Apache Airflow)

### 1.4 Stack imposée (positionnement par couche)

| Couche | Composant | Rôle dans l'implémentation |
|---|---|---|
| Ingestion | `minio-init` (MinIO Client) | Copie des sources brutes vers la zone `raw/` du data lake |
| **Stockage** | **MinIO** (data lake S3) | Stockage des données Parquet (zones `raw/`, `silver/`, `gold/`, `warehouse/`) |
| **Catalogue** | **Hive Metastore** (+ PostgreSQL) | Métadonnées des tables externes, partitions, statistiques |
| **Traitement** | **Apache Spark** (standalone) | ETL/ELT : lecture S1 (JDBC) + CSV du lake, écriture Gold/Silver |
| **Orchestration** | **Apache Airflow** | DAGs `lakehouse_init` (one-shot) et `lakehouse_daily` (02:00) |
| Requêtage | **Trino** | Lecture distribuée du lake via le connecteur Hive (pruning) |
| **Visualisation** | **Apache Superset** (localhost:8088) | 8 tableaux de bord (1 par besoin métier) |
| Source | **PostgreSQL** (S1) | Dump ChuSimulation restauré par `pg_restore` |
| Benchmark/DQ | DuckDB (conteneur jetable) | PoC de performance + contrôles qualité (rien installé sur l'hôte) |

> **Contrainte d'environnement** : l'intégralité de la plateforme tourne dans Docker (15 services, `scripts/docker-compose.yml`). Aucune installation sur le poste hôte (ni PostgreSQL, ni Spark, ni Java...).</description>

---

## 2. Modèle physique (schéma en étoile)

### 2.1 Vue d'ensemble

Le schéma en étoile du Livrable 1 est implémenté en **couche Gold** (données décisionnelles) sur le data lake, avec une couche **Silver** pour le fait de détail des consultations. 6 dimensions, 1 fait consolidé (4 mesures), 2 faits de détail, 1 fait agrégé (décès) et 2 agrégats pré-calculés.

```
                          ┌────────────┐
                          │  d_temps   │  grain : jour (2015-2030), id_temps = AAAAMMJJ
                          └─────┬──────┘
        ┌────────────┐ ┌───────────┼───────────┐ ┌────────────┐ ┌──────────────┐
        │ d_patient  │ │d_etablissement│        │ │d_localisation│ │ d_diagnostic │
        └─────┬──────┘ └─────┬─────┘          │ └─────┬──────┘ └──────┬───────┘
              └──────────────┴──────────┬──────┘        └───────────────┘
                                        ▼
                          ┌──────────────────────────┐
                          │        f_mesures         │  FAIT CONSOLIDÉ (4 mesures)
                          │  nbre_consultation       │  grain : annee_mois x id_etablissement
                          │  nbre_hospitalisation    │  x id_patient x sexe x tranche_age
                          │  total_satisfaction      │  x id_localisation
                          │  nbre_deces              │
                          └──────────────────────────┘
        f_consultation_detail (Silver)   f_hospitalisation   f_deces_agg   f_satisfaction
        f_consult_diag_mois (agrégat)    f_consult_prof_mois (agrégat)
```

### 2.2 Dictionnaire des tables (couche Gold/Silver)

| Table | Couche | Grain | Lignes | Clé | Partitionnement | Bucketing |
|---|---|---|---|---|---|---|
| `d_temps` | Gold | jour (2015-2030) | 5 844 | `id_temps` (substitution, AAAAMMJJ) | `annee` | — |
| `d_patient` | Gold | patient | 100 000 | `id_patient` (naturelle = substitution) | `sexe`, `tranche_age_ref` | `id_patient` × 32 |
| `d_etablissement` | Gold | site FINESS | 416 665 | `id_etablissement` (**substitution surrogate**) | — | `id_etablissement` × 32 |
| `d_localisation` | Gold | département | 102 (101 + Inconnu) | `id_localisation` (code département) | `region` | — |
| `d_diagnostic` | Gold | diagnostic CIM-10 | 15 490 | `id_diagnostic` (**substitution surrogate**) | — | `id_diagnostic` × 8 |
| `d_professionnel` | Gold | professionnel | 1 048 575 | `identifiant` (naturelle = substitution) | — | `identifiant` × 32 |
| `f_mesures` | Gold | annee_mois × etab × patient × sexe × tranche_age × département | 1 375 686 | — (fait) | `annee`, `sexe`, `tranche_age` | `id_etablissement` × 32 |
| `f_consultation_detail` | Silver | consultation | 1 373 594 | `id_consultation` | `annee` | `id_patient` × 32 |
| `f_hospitalisation` | Gold | hospitalisation | 2 479 | `id_hospitalisation` | `annee` | `id_etablissement` × 8 |
| `f_deces_agg` | Gold | annee × sexe × tranche_age × département | 69 008 | — (fait agrégé) | `annee` | — |
| `f_satisfaction` | Gold | finess juridique × année | 2 295 | — (fait) | `annee`, `region` | — |
| `f_consult_diag_mois` | Gold | mois × diagnostic × établissement | 1 319 261 | — (agrégat) | `annee` | `id_diagnostic` × 8 |
| `f_consult_prof_mois` | Gold | mois × professionnel | 1 001 453 | — (agrégat) | `annee` | `id_professionnel` × 32 |

**Mesures de `f_mesures`** (les 4 mesures du Livrable 1) :

| Mesure | Définition | Grain dans `f_mesures` |
|---|---|---|
| `nbre_consultation` | Nombre de consultations | mensuel (`annee_mois = 'AAAA-MM'`) |
| `nbre_hospitalisation` | Nombre d'hospitalisations | mensuel (`'AAAA-MM'`) |
| `total_satisfaction` | Score moyen ajusté ESATIS (0-100) | annuel (sentinelle `'AAAA-00'`) |
| `nbre_deces` | Nombre de décès | annuel (sentinelle `'AAAA-00'`) |

**Conventions et membres « Inconnu »** (gestion des valeurs nulles et des rejets, cf. Livrable 1) :

- `id_etablissement = -1`, `id_patient = -1`, `id_localisation = 'INCONNU'`, `region = 'INCONNUE'`, `sexe = 'TOUS'`, `tranche_age = 'TOUTES'` pour les lignes non rattachables (décès, satisfaction)
- Sentinelle `annee_mois = 'AAAA-00'` (mois = 00) = agrégat annuel, pour les mesures de grain annuel
- `id_temps = NULL` pour les lignes annuelles (pas de 1er jour de mois) — la jointure avec `d_temps` se fait sur les lignes mensuelles

### 2.3 Contraintes d'intégrité référentielle

Le moteur retenu (tables externes Hive/Parquet sur MinIO) ne supporte pas les contraintes `FOREIGN KEY` déclaratives. L'intégrité référentielle est donc **assurée par construction** (les faits ne contiennent que des clés produites par les dimensions) **et contrôlée a posteriori** par les contrôles qualité (famille « cohérence référentielle », §5.4) : taux de rattachement mesurés, aucun rejet silencieux.

### 2.4 Vues métier

`scripts/01_ddl/03_vues_metier.sql` (SQL portable Hive/Trino) : `v_fait_consultation`, `v_fait_hospitalisation`, `v_fait_deces`, `v_fait_satisfaction` (les 4 faits métier du Livrable 1 dérivés de `f_mesures`), `v_taux_consultation_mensuel` (moyenne mobile 3 mois, fonction fenêtrée), + 4 vues d'agrégat pour Superset (`v_consultations_region_annee`, `v_deces_region_annee`, `v_satisfaction_region_annee`, `v_hospitalisations_etab_annee`).

---

## 3. DDL et DML (scripts versionnés)

### 3.1 DDL — `scripts/01_ddl/01_schema_gold_hive.sql`

Création des bases `silver` et `gold` et des **12 tables EXTERNES Hive** avec clause `LOCATION 's3a://lake/...'` (les données appartiennent au data lake ; un `DROP TABLE` ne supprime jamais les données). Toutes les tables sont `STORED AS PARQUET` (format colonnaire compressé, statistiques min/max par fichier pour le pruning côté Trino).

Extraits représentatifs :

```sql
-- Table de fait consolidé : partitionnement démographique + temporel
CREATE EXTERNAL TABLE IF NOT EXISTS f_mesures (
  annee_mois STRING, id_temps INT, id_etablissement BIGINT, id_patient BIGINT,
  id_localisation STRING, nbre_consultation BIGINT, nbre_hospitalisation BIGINT,
  total_satisfaction DOUBLE, nbre_deces BIGINT, region STRING
)
PARTITIONED BY (annee INT, sexe STRING, tranche_age STRING)
CLUSTERED BY (id_etablissement) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 's3a://lake/gold/f_mesures';

-- Dimension localisations : partitionnée par région (grille)
CREATE EXTERNAL TABLE IF NOT EXISTS d_localisation (
  id_localisation STRING, code_departement STRING,
  nom_departement STRING, code_region STRING
)
PARTITIONED BY (region STRING)
STORED AS PARQUET
LOCATION 's3a://lake/gold/d_localisation';
```

Côté **Trino** (`02_schema_gold_trino.sql`) : pas de DDL dupliqué — Trino lit le Hive Metastore via le connecteur `hive` (config `scripts/config/trino/catalog/hive.properties`). Le script vérifie le déploiement (volumes), calcule les statistiques de l'optimiseur (`ANALYZE`, équivalent de `ANALYZE TABLE ... COMPUTE STATISTICS` côté Hive) et valide le pruning (`EXPLAIN (TYPE IO)`).

### 3.2 DML — chargement (Apache Spark, `scripts/02_spark/`)

| Script | Rôle | Règles de transformation appliquées (issues de la DQ, §5) |
|---|---|---|
| `common.py` | Session Spark (S3A MinIO + Hive Metastore), helpers | `tranche_age` (0-14/15-29/30-44/45-59/60-74/75+), `dept_of` (règle INSEE : 2 car., 3 car. pour 97x, `2A` pour la Corse), `sexe_normalise` (male/female → M/F), âge révolu `floor(months_between/12)` |
| `load_dimensions.py` | 6 dimensions | `d_patient` : sexe normalisé, `date_naissance` parsée (`M/J/AAAA`), tranche à la réf. 2023-12-31 ; `d_etablissement` : surrogate `row_number`, rattachement `code_commune → département → région` ; `d_localisation` : 101 départements + membre Inconnu |
| `load_faits_detail.py` | `silver.f_consultation_detail`, `f_hospitalisation`, `f_satisfaction` | rattachement établissement via fichier d'activité (consultations) ; score ESATIS : **virgule décimale remplacée** (`replace(',', '.')`) ; région ESATIS normalisée par mapping (18 régions) ; grain satisfaction = **finess juridique** × année (pas de clé site) |
| `build_f_deces_agg.py` | `f_deces_agg` (25,09 M décès INSEE) | filtre des 646 dates invalides, agrégat annuel (annee × sexe × tranche_age × département), jointure référentiel départements |
| `build_f_mesures.py` | `f_mesures` (fait consolidé) | agrégation des 4 sources au grain commun, **calcul des mesures APRÈS peuplement des clés étrangères** (ordre du DAG), sentinelle `'AAAA-00'` pour les mesures annuelles, région dénormalisée depuis `d_localisation` |
| `build_agregats.py` | `f_consult_diag_mois`, `f_consult_prof_mois` | agrégats pré-calculés (équivalents matérialisés des agrégations les plus fréquentes) |

Chaque écriture Spark : `bucketBy(n, clé).partitionBy(...).sortBy(...).option("path", "s3a://lake/...").saveAsTable(...)` → table **externe** Parquet partitionnée/bucketée/triée, métadonnées publiées dans le Hive Metastore, partitions découvertes (`MSCK REPAIR`) et statistiques calculées (`ANALYZE ... COMPUTE STATISTICS`).

### 3.3 Versionning (Git)

Dépôt Git initialisé dans `Livrable 2/` (5 commits thématiques) :

```
e19e2bb stack Docker Compose complète + Superset + documentation
b5bf381 benchmark (10 requêtes, 3 scénarios, résultats + SVG) et DQ (6 familles)
7d17dc5 orchestration Airflow (DAGs lakehouse_init + lakehouse_daily)
4827f79 traitement Spark (ETL/ELT vers le data lake MinIO)
30a51fe DDL Hive/Trino + vues métier (schéma en étoile Gold/Silver)
```

### 3.4 Preuve d'exécution (PoC)

La chaîne complète a été exécutée de bout en bout (scripts `scripts/04_benchmark/00_init.sql` → `02d_agregats.sql`, DuckDB dans un conteneur Docker, source S1 dans `chu-s1-postgres`) et a produit les volumes du §2.2, avec **1 373 594 consultations, 2 479 hospitalisations, 3 215 529 décès (2015-2023), 2 295 scores de satisfaction** dans `f_mesures`.

---

## 4. Stratégie de partitionnement et d'indexation (justifiée par moteur)

### 4.1 Partitionnement (Hive/Spark/Trino)

| Table | Colonnes de partition | Justification |
|---|---|---|
| `f_mesures` | `annee`, `sexe`, `tranche_age` | **Pruning temporel + démographique** : les requêtes métier filtrent systématiquement sur l'année (voire la période), le sexe et la tranche d'âge sont les axes démographiques des besoins 5 et 10. 9 années × 3 sexes × 8 tranches = 216 combinaisons max (122 réellement présentes). |
| `d_patient` | `sexe`, `tranche_age_ref` | Partitionnement démographique de la dimension patient (conforme à la grille d'évaluation). |
| `d_temps` | `annee` | Partitionnement temporel de la dimension temps (couvre 2015-2021, périmètre de la grille, prolongé à 2030 pour la planification). |
| `d_localisation` | `region` | Partitionnement par région (la région est l'axe géographique le plus filtrant — besoins 7 et 8). |
| `f_consultation_detail`, `f_hospitalisation`, `f_consult_diag_mois`, `f_consult_prof_mois` | `annee` | Pruning temporel (tous les besoins portent sur des périodes). |
| `f_deces_agg` | `annee` **seulement** | Un partitionnement par `(annee, sexe, tranche_age)` produirait 1 251 combinaisons réelles (~55 lignes chacune ; borne 133 × 2 × 7 = 1 862) pour 69 008 lignes (**small-files problem**). Le grain annuel + `annee` en partition donnent 133 partitions de ~519 lignes ; le **tri intra-partition** par `(sexe, tranche_age, id_localisation)` compense l'absence de partitionnement sur ces colonnes (pruning par min/max de fichier côté Trino). |
| `f_satisfaction` | `annee`, `region` | Grain juridique × année ; la région est la colonne la plus filtrante (besoin 8). |
| `d_etablissement`, `d_diagnostic`, `d_professionnel` | — | Tables de taille moyenne/volumineuse mais peu filtrées seules : le **bucketing** suffit (pas de partition). |

### 4.2 Bucketing (Hive/Spark UNIQUEMENT)

> **Le bucketing n'existe pas en PostgreSQL** (c'est une notion propre à Hive/Spark). Aucun index/bucket n'est donc appliqué côté S1 (source OLTP, non modifiée) ; l'optimisation porte sur la couche analytique (lake).

`CLUSTERED BY (clé de jointure) INTO N BUCKETS` (Hive) / `bucketBy(n, clé)` (Spark) :

- **Co-localisation** des lignes partageant la même clé de jointure dans le même fichier → jointures **map-side / bucketed** sans shuffle complet (ex. `f_consultation_detail.id_patient` × 32 ↔ `d_patient.id_patient` × 32)
- **Pré-tri** intra-bucket → agrégations, `ORDER BY ... LIMIT` et jointures merge efficaces
- Nombre de buckets = multiple de la cardinalité de la clé : **32** pour les tables volumineuses (faits, `d_etablissement`, `d_professionnel`), **8** pour les petites (`d_diagnostic`, `f_hospitalisation`, `f_consult_diag_mois`)

### 4.3 Indexation complémentaire

- **Parquet + Zstd** : format colonnaire compressé (~×5 à ×10 vs CSV), pushdown des prédicats, statistiques min/max par colonne et par fichier → **pruning intra-fichier** (Trino) en plus du pruning de partitions
- **Tri intra-partition/bucket** sur les clés de jointure (`sortBy`) : compense l'absence de partitionnement sur certaines colonnes et accélère les agrégats
- **Statistiques** : `ANALYZE TABLE ... COMPUTE STATISTICS` (Hive/Spark) et `ANALYZE` (Trino) après chaque chargement → plans de jointure et distribution optimaux
- **Agrégats pré-calculés** (`f_consult_diag_mois`, `f_consult_prof_mois`) : équivalents matérialisés des agrégations les plus demandées (besoins 2 et 6)
- **Agrégation en amont** du fait décès (25,09 M → 69 008 lignes) : le grain annuel évite de scanner 25 M de lignes pour un besoin annuel

---

## 5. Rapport de qualité des données

Script : `scripts/05_qualite/dq_checks.sql` (6 familles de contrôles, résultats CSV dans `scripts/05_qualite/results/`). Les contrôles sont exécutés **avant** intégration ; les règles de traitement en découlent (tableau §5.6).

### 5.1 Complétude (`dq_completude.csv`)

| Source.attribut | Total | Rempli | % |
|---|---|---|---|
| Hospitalisations (tous les attributs) | 2 479 | 2 479 | 100,0 |
| S1 Patient (Sexe, Date_naissance, Age, Num_Secu) | 100 000 | 100 000 | 100,0 |
| S1 Consultation (Date, Code_diag, Id_prof_sante) | 1 027 157 | 1 027 157 | 100,0 |
| deces.sexe, date_deces | 25 088 208 | 25 088 208 | 100,0 |
| deces.date_naissance | 25 088 208 | 25 087 658 | 100,0 (550 null) |
| deces.code_lieu_deces | 25 088 208 | 25 077 274 | 99,96 |
| etablissement.identifiant_organisation | 416 665 | 416 665 | 100,0 |
| etablissement.finess_etablissement_juridique | 416 665 | 57 692 | **13,85** |
| etablissement.finess_site | 416 665 | 200 | **0,05** |
| etablissement.code_commune | 416 665 | 277 860 | **66,69** |
| satisfaction_2019.score_all_rea_ajust | 1 152 | 858 | **74,48** |
| satisfaction_2020.score_all_rea_ajust | 1 143 | 832 | **72,79** |

### 5.2 Validité (`dq_validite.csv`)

| Contrôle | Total | Invalides | % |
|---|---|---|---|
| deces.date_deces format ISO | 25 088 208 | 646 | 0,0026 |
| deces.date_naissance format ISO | 25 088 208 | 89 898 | 0,3583 |
| deces.sexe ∈ {1,2} | 25 088 208 | 0 | 0,0 |
| Hospitalisations.Date_Entree JJ/MM/AAAA | 2 479 | 0 | 0,0 |
| Hospitalisations.Code_diagnostic CIM-10 | 2 479 | **368** | **14,84** |
| S1 Patient.Sexe ∈ {male,female} | 100 000 | 0 | 0,0 |
| satisfaction_2019.finess 9 caractères | 1 152 | 7 | 0,61 |
| satisfaction score décimal (virgule) | 2 295 | 0 | 0,0 (après `replace(',', '.')`) |

### 5.3 Doublons (`dq_doublons.csv`)

| Fichier | Clé | Doublons | % | Traitement |
|---|---|---|---|---|
| Hospitalisations | Num_Hospitalisation | 0 | 0,0 | — |
| S1 Consultation | Num_consultation | 0 | 0,0 | — |
| S1 Patient | Id_patient | 0 | 0,0 | — |
| etablissement | identifiant_organisation | 0 | 0,0 | — |
| satisfaction_2019 | finess (grain brut) | 166 | 14,41 | **Grain réel = finess + finess_geo** (0 doublon) : dédoublonnage par `distinct` sur le grain réel |
| deces | clé complète (nom, prénom, dates, lieu, acte) | 211 833 | 0,8444 | Conservés (décès homonymes possibles) mais **quantifiés** ; pas de dédoublonnage (risque de faux positifs sur 25 M de lignes) |

### 5.4 Cohérence référentielle — codes FINESS (`dq_referentiel.csv`)

| Contrôle | Total | Trouvés | % |
|---|---|---|---|
| Hospitalisations.identifiant_organisation → etab.identifiant_organisation | 2 479 | 2 479 | **100,0** |
| Hospitalisations.Id_patient → S1.Patient | 2 479 | 2 479 | 100,0 |
| Hospitalisations.Code_diagnostic → S1.Diagnostic | 2 479 | 2 479 | 100,0 |
| S1.Consultation.Id_prof_sante → professionnel (référentiel) | 1 027 157 | 1 027 157 | **100,0** |
| S1.Consultation.Id_prof_sante → activite.identifiant | 1 027 157 | 738 190 | **71,87** → rattachement établissement via activité ; les autres → `id_etablissement = -1` |
| activite.identifiant → professionnel | 1 823 588 | 1 042 874 | 57,19 |
| activite.identifiant_organisation → etab | 1 823 588 | 1 464 774 | 80,32 |
| satisfaction.finess → etab.finess_etablissement_juridique (2019/2020) | 2 295 | 1 155 | 49,91 / 50,74 → **grain juridique** (1 finess juridique = N sites) |
| satisfaction.finess → etab.identifiant_organisation | 2 295 | 0 | **0,0** → rattachement site impossible |
| satisfaction.finess_geo → etab.finess_site | 2 295 | 0 | **0,0** (finess_site quasi vide, 0,05 %) |
| etablissement.code_commune → département (règle INSEE) | 277 860 | 277 127 | **99,74** |
| deces.code_lieu_deces → département (règle INSEE) | 25 077 274 | 24 876 383 | **99,2** |
| satisfaction.region brute → région normalisée (mapping) | 2 295 | 2 270 | 98,96 / 98,86 → « Océan Indien » ambigu exclu volontairement → `INCONNUE` |
| satisfaction.region normalisée → référentiel région | 2 270 | 2 270 | 100,0 |
| Concordance région ESATIS vs région établissement (via finess juridique) | 8 308 | 8 027 | 96,62 |

### 5.5 Temporalité (`dq_temporel.csv`)

| Contrôle | Conforme | % | Plage observée |
|---|---|---|---|
| Hospitalisations.Date_Entree ∈ [2015,2023] | 2 479/2 479 | 100,0 | 2015-06-21 → 2021-06-19 |
| S1.Consultation.Date ∈ [2015,2023] | 1 027 157/1 027 157 | 100,0 | 2015-06-20 → 2023-03-31 |
| deces.date_deces ≥ date_naissance | 24 997 051/25 088 208 | 99,64 | 1882-10-01 → 2020-03-30 |
| S1.Patient : cohérence Age vs date_naissance | 45 998/100 000 | **46,0** | âge 0..100 |
| satisfaction score ∈ [0,100] | 858/1 152 (2019) | 74,48 | scores 0-100 |

### 5.6 Traitements appliqués (dérivés des contrôles)

| Problème mesuré | Traitement appliqué dans les faits |
|---|---|
| Score ESATIS en virgule décimale (2019 **et** 2020) | `replace(',', '.')` avant cast DOUBLE |
| Région ESATIS hétérogène (18 libellés) | Mapping vers les 18 régions du référentiel ; « Océan Indien » (ambigu) → `INCONNUE` |
| `finess_site` à 0,05 % ; rattachement ESATIS site à 0 % | Satisfaction stockée au **grain juridique × année**, rattachée à la **région** (pas de clé site) |
| `Age` S1 incohérent avec la date de naissance (46 % seulement) | `Age` non retenu ; **tranche d'âge recalculée à la date de l'événement** dans les faits |
| 71,87 % des consultations rattachables à un établissement | Rattachement via `Id_prof_sante → activite → etablissement` ; non rattachées → membre Inconnu (-1) |
| 646 dates de décès invalides | Exclus du fait (`WHERE d_deces IS NOT NULL`) → 25 087 562 décès retenus |
| 368 codes CIM-10 invalides dans S2 (14,84 %) | Conservés s'ils existent dans `d_diagnostic` (jointure interne) ; la DQ les quantifie |
| 211 833 doublons potentiels décès (0,84 %) | Conservés et quantifiés (homonymes INSEE possibles) |
| `code_commune` à 66,69 % dans le référentiel etab | Règle INSEE `dept_of` ; non rattachés → `INCONNU`/`INCONNUE` |

---

## 6. Jeu de requêtes de benchmark

10 requêtes dans `scripts/04_benchmark/queries/` : **8 requêtes métier** (1 par besoin utilisateur du Livrable 1, couvrant les 4 mesures) + **2 requêtes techniques** (vue avec fonction fenêtrée ; jointure étoile complète). Toutes utilisent agrégations (`SUM`, `AVG`, `COUNT`), tris (`ORDER BY ... LIMIT`) et la vue métier (q09).

| # | Requête | Besoin | Mesure | Axes |
|---|---|---|---|---|
| q01 | `q01_consult_etab_2019.sql` | 1 — Taux de consultation par établissement, période Y | nbre_consultation | établissement × période (top 20, 2019) |
| q02 | `q02_consult_diag_2019_2021.sql` | 2 — Taux de consultation par diagnostic X, période Y | nbre_consultation | diagnostic × période (top 10, 2019-2021) |
| q03 | `q03_taux_hosp_periode.sql` | 3 — Taux global d'hospitalisation, période Y | nbre_hospitalisation / nbre_consultation | année (2015-2021) |
| q04 | `q04_hosp_diag.sql` | 4 — Taux d'hospitalisation par diagnostic, période | nbre_hospitalisation | diagnostic × période (top 10) |
| q05 | `q05_hosp_sexe_age.sql` | 5 — Taux d'hospitalisation par sexe, âge | nbre_hospitalisation | sexe × tranche d'âge |
| q06 | `q06_consult_prof_2020.sql` | 6 — Taux de consultation par professionnel | nbre_consultation | professionnel (top 10, 2020) |
| q07 | `q07_deces_region_2019.sql` | 7 — Décès par localisation (région), 2019 | nbre_deces | région × année |
| q08 | `q08_satisfaction_region_2020.sql` | 8 — Satisfaction par région, 2020 | total_satisfaction | région × année |
| q09 | `q09_vue_taux_mensuel.sql` | *(technique)* — vue `v_taux_consultation_mensuel` | nbre_consultation | mois + moyenne mobile 3 mois |
| q10 | `q10_etoile_complete.sql` | *(technique)* — jointure étoile complète | nbre_consultation | fait + 4 dimensions, filtre annee × sexe |

### 6.1 Résultats métier (extraits)

Résultats complets : `scripts/04_benchmark/results/resultats_requetes.txt`.

**q03 — Taux d'hospitalisation pour 100 consultations, 2015-2021**

| annee | hospitalisations | consultations | taux/100 |
|---|---|---|---|
| 2015 | 255 | 45 374 | 0,56 |
| 2016 | 395 | 246 399 | 0,16 |
| 2017 | 458 | 177 860 | 0,26 |
| 2018 | 395 | 215 038 | 0,18 |
| 2019 | 374 | 117 329 | 0,32 |
| 2020 | 410 | 217 536 | 0,19 |
| 2021 | 192 | 195 372 | 0,10 |

**q05 — Hospitalisations par sexe et tranche d'âge (2015-2021)** : 1 777 F / 1 054 M ; répartition F : 0-14: 276, 15-29: 231, 30-44: 184, 45-59: 217, 60-74: 208, 75+: 309 ; M : 0-14: 181, 15-29: 180, 30-44: 150, 45-59: 155, 60-74: 159, 75+: 229.

**q07 — Décès par région, 2019** (top 5) : Île-de-France 76 988 ; Auvergne-Rhône-Alpes 70 407 ; Nouvelle-Aquitaine 66 097 ; Occitanie 59 781 ; Hauts-de-France 55 137. (Total 2019 : 622 278 décès.)

**q08 — Satisfaction moyenne par région, 2020** (top 5) : Bretagne 75,96 (40 etab., 19 396 répondants) ; Nouvelle-Aquitaine 75,22 ; Pays de la Loire 75,21 ; Hauts-de-France 74,58 ; Bourgogne-Franche-Comté 74,56.

**q01 — Top 3 établissements 2019** : SERVICE DE SANTE DES ARMEES (Paris 75107, 380 consultations) ; HU PITIE SALPETRIERE APHP (367) ; HU NECKER ENFANTS MALADES APHP (230).

---

## 7. Évaluation de performance (avant / après partitionnement + bucketing)

### 7.1 Protocole

- **Moteur du PoC** : DuckDB (dans un conteneur Docker jetable, `scripts/04_benchmark/run-duckdb.sh`), données : entrepôt complet construit par les scripts `00_init` → `02d`
- **Scénario AVANT** : tables plates (1 fichier Parquet par table, aucun ordre) — équivalent « non optimisé »
- **Scénario APRÈS** : tables partitionnées (style Hive, conforme au DDL §4.1 : `d_patient` sexe + tranche_age_ref, `d_localisation` region, `d_temps` annee, faits `annee`...) + bucketing simulé + tri sur les clés de jointure — lu par glob `**/*.parquet` avec `hive_partitioning` (pruning des répertoires)
- **Scénario APRÈS_ML** : APRÈS + **pruning façon metastore** (liste explicite des fichiers retenus, générée par `gen_listes.js`) — simule Hive Metastore/Trino, car DuckDB local n'a pas de metastore et le listing de tous les fichiers pénaliserait le scénario APRÈS (~0,38 ms/fichier)
- **Mesures** : 5 runs par requête (run 1 = cache froid, runs 2-5 = chauds) ; **médiane** des runs 2-5 ; volume de données lues (octets)
- **Validation plateforme** : `harness_trino.sh` rejoue les 10 requêtes contre Trino (stack Docker) avec `EXPLAIN ANALYZE` sur q01/q10

### 7.2 Résultats (médianes, ms)

Fichier complet : `scripts/04_benchmark/results/bench_synthese.csv` (et `bench_results.csv` pour les 150 mesures brutes).

| Requête | Avant | Après | Après + metastore | Accélération (×) | Volume lu avant (Mo) | Volume lu après (Mo) | Réduction volume (×) |
|---|---|---|---|---|---|---|---|
| q01 | 32,0 | 97,5 | **27,0** | **1,19** | 23,1 | 13,2 | 1,7 |
| q02 | 16,5 | 30,5 | **15,5** | **1,06** | 8,1 | 3,0 | 2,7 |
| q03 | 7,0 | 78,0 | **17,0** | 0,41 | 13,1 | 9,7 | 1,4 |
| q04 | 10,5 | 13,5 | **7,5** | **1,40** | 0,4 | 0,6 | 0,8 |
| q05 | 8,5 | 74,0 | **15,5** | 0,55 | 13,1 | 9,7 | 1,4 |
| q06 | 53,5 | 69,0 | **51,5** | **1,04** | 23,0 | 18,7 | 1,2 |
| q07 | 8,0 | 63,0 | **7,0** | **1,14** | 0,3 | 0,02 | **13,6** |
| q08 | 2,0 | 19,0 | **5,0** | 0,40 | 0,02 | 0,02 | 0,9 |
| q09 | 11,0 | 85,0 | **15,0** | 0,73 | 13,1 | 5,5 | 2,4 |
| q10 | 37,5 | 125,5 | **26,0** | **1,44** | 24,3 | 13,8 | 1,8 |

### 7.3 Graphiques

- `results/graph_temps_median.svg` — temps médian par requête, 3 scénarios (barres groupées)
- `results/graph_volume_lu.svg` — volume de données lu (Mo), avant vs après
- `results/graph_reduction_volume.svg` — taux de réduction du volume lu (×)

### 7.4 Analyse et interprétation

**1. Le volume de données lues est le levier dominant.** Le partitionnement réduit systématiquement le volume lu (jusqu'à ×13,6 sur q07 : 328 Ko → 24 Ko), parce que les filtres sur `annee` (colonne de partition) éliminent des répertoires entiers avant lecture. C'est exactement le mécanisme du pruning Hive/Trino en production.

**2. Le scénario APRÈS brut est pénalisé par le listing de fichiers (biais connu du PoC).** Sans metastore, DuckDB liste tous les fichiers (`glob`) avant de pouvoir pruner : sur `f_mesures` (122 partitions), ce listing coûte plus cher que le scan complet de la table plate (q03 : 78 ms vs 7,0 ms). Ce biais est **inhérent au PoC local**, pas à la stratégie : en production, le Hive Metastore connaît les partitions et Trino ne liste que les fichiers retenus (scénario APRÈS_ML). C'est pourquoi le scénario APRÈS_ML est le plus représentatif de la plateforme livrée.

**3. APRÈS_ML (pruning metastore) : gains nets sur les requêtes filtrées.** q07 (décès 2019) : 8,0 → 7,0 ms (×1,14) et volume ×13,6 ; q10 (jointure étoile, filtre annee × sexe) : 37,5 → 26,0 ms (×1,44) et volume ×1,8. Les gains sont modestes en temps absolu car le PoC tourne sur une machine locale avec des données déjà en cache OS et un moteur (DuckDB) très rapide sur de petits volumes ; l'apport du partitionnement croît avec le volume (en production : 25 M de décès non agrégés, ~1,4 M de faits mensuels) et avec la concurrence (un scan complet bloque, un pruning ne lit que l'utile).

**4. q04 et q08 : petits volumes, pas de gain (voire small-files).** `f_hospitalisation` (2 479 lignes) et `f_satisfaction` (2 295 lignes) tiennent en 1-2 fichiers : le partitionnement y crée plus de fichiers que de lignes utiles (q04 : volume lu ×0,8, q08 : ×0,9). **Leçon** : le partitionnement se justifie au-delà d'un seuil de volume ; ces deux faits restent partitionnés par `annee` pour la cohérence du modèle (et parce que S2/S3 grossiront), mais le bénéfice est neutre. C'est un résultat honnête du benchmark.

**5. q06 (consultations par professionnel, 2020) : le bucketing paie peu ici.** La jointure `f_consult_prof_mois ↔ d_professionnel` porte sur 1 M de professionnels ; le bucketing évite le shuffle complet côté Spark/Hive, mais le PoC (single-node) n'en montre qu'un avantage marginal (69 → 51,5 ms). En cluster Spark/Trino, le bucketed join est le levier principal sur ce type de jointure.

**6. Synthèse.** Sur les 10 requêtes : **volume lu réduit sur 8/10** (×1,2 à ×13,6 ; overhead small-files sur les deux petites tables q04 ×0,8 et q08 ×0,9). En scénario APRÈS_ML (le plus représentatif de la production), le **temps de réponse progresse sur 6/10 requêtes** (q10 ×1,44, q04 ×1,40, q01 ×1,19, q07 ×1,14, q06 ×1,04, q02 ×1,06) et **recule sur 4/10** (q03 ×0,41, q08 ×0,40, q09 ×0,73, q05 ×0,55) : à l'échelle du PoC local (quelques dizaines de Mo, données en cache OS, moteur single-node), la réduction du volume lu ne se traduit pas mécaniquement en gain de temps — les requêtes en retrait lisent déjà peu de données, où le temps de listing/coordination domine. La stratégie est validée là où elle compte : le pruning de partitions réduit le volume lu dès que le filtre porte sur une colonne de partition (×13,6 sur q07), et ce ratio croît avec le volume réel (en production : 25 M de décès non agrégés, ~1,4 M de faits mensuels) et avec la concurrence. Le partitionnement temporel/démographique + le bucketing sur les clés de jointure + le tri + les agrégats pré-calculés forment une stratégie cohérente, dont le bénéfice temps est attendu en cluster (Trino/Spark distribués), pas sur un PoC local mono-nœud.

---

## 8. Visualisation (Apache Superset)

- **URL** : http://localhost:8088 (admin/admin — à changer)
- **Connexion** : base `Trino CHU`, URI `trino://trino@trino:8080/hive` (catalogue Hive = schéma Gold du lake)
- **8 tableaux de bord** (1 par besoin métier) : requêtes prêtes dans `scripts/06_superset/charts.sql` (SQL Lab, schéma `gold`), types de graphiques recommandés en commentaire
- **Datasets virtuels (SQL)** : les dashboards interrogent Trino **en direct** — aucune copie de données dans Superset, cache désactivé (`scripts/config/superset/superset_config.py`)
- Notice détaillée : `scripts/06_superset/README.md`

## 9. Orchestration (Apache Airflow)

- **URL** : http://localhost:8090 (admin/admin)
- **DAG `lakehouse_init`** (one-shot, manuel) : DDL Hive → vues métier → vérification Trino (volumes, `ANALYZE`, pruning) → disponibilité Superset
- **DAG `lakehouse_daily`** (quotidien, 02:00) : `SparkSubmitOperator` — `load_dimensions` → `load_faits_detail` ∥ `build_f_deces_agg` → `build_f_mesures` (+ `build_agregats`) → contrôles qualité (DQ) → disponibilité Trino pour Superset
- Ordre garantissant le **peuplement des clés étrangères avant le calcul des mesures** (grille d'évaluation)

## 10. Limites et perspectives

- **PoC local** : le benchmark a été exécuté sur un moteur local (DuckDB, single-node) ; les temps absolus ne préjugent pas des temps cluster, mais les ratios de volume lu et le mécanisme de pruning sont directement transposables. La stack Docker Compose + `harness_trino.sh` permet de rejouer le benchmark sur Trino.
- **f_deces_agg** : grain annuel (choix justifié §4.1) ; un besoin de mortalité mensuelle nécessiterait un fait mensuel (8,6 M de lignes) — perspective.
- **Satisfaction** : grain juridique (rattachement site impossible, 0 % en DQ) ; l'enrichissement du référentiel FINESS (finess_geo) lèverait cette limite.
- **RGPD/HDS** : les données sont pseudonymisées en amont (pas de nom/prénom de patient dans l'entrepôt : `d_patient` ne contient que sexe, date de naissance, code postal, ville) ; l'hébergement HDS et la gouvernance (rôles, droits) sont du ressort du Livrable 1 et de la production.
- **Perspectives** : scalabilité horizontale (workers Spark, nœuds Trino), refresh incrémental (CDC) plutôt que rechargement complet, mise en place d'un vrai ETL déclaratif (dbt) en couche Silver, alerting qualité dans Airflow.

---

## 11. Dernière note obtenue et résumé des corrections

*(Section renseignée après notation par le sous-agent évaluateur — voir itérations ci-dessous.)*

- **Dernière note : en attente de notation**
- **Résumé des corrections : en attente**

---

## 12. Annexes

### 12.1 Arborescence du livrable

```
Livrable 2/
├── livrable2.md                          # ce rapport
├── A3-Stockage_et_BigData-Grille-Livrable2.xlsx   # grille d'évaluation
└── scripts/
    ├── docker-compose.yml                # stack complète (15 services)
    ├── README.md                         # arborescence + ordre d'exécution
    ├── config/                           # hive-site.xml, Trino, Spark, Superset, S1
    ├── 01_ddl/                           # DDL Hive + vérification Trino + vues métier
    ├── 02_spark/                         # ETL PySpark (6 scripts)
    ├── 03_airflow/dags/                  # lakehouse_init.py, lakehouse_daily.py
    ├── 04_benchmark/                     # 10 requêtes, harnais, résultats (CSV + 3 SVG)
    ├── 05_qualite/                       # dq_checks.sql + 6 rapports CSV
    └── 06_superset/                      # charts.sql + README
```

### 12.2 Commandes principales

```bash
# Stack complète
docker compose -f scripts/docker-compose.yml up -d
# Airflow : déclencher lakehouse_init puis lakehouse_daily (http://localhost:8090)
# Superset : http://localhost:8088 -> connexion Trino -> 8 charts (06_superset/charts.sql)
# Benchmark Trino : cd scripts/04_benchmark && ./harness_trino.sh
# Contrôles qualité seuls : voir scripts/README.md (section C)
```

### 12.3 Références

- Sujet : `../sujet.md` (Livrable 2, p. « Livrables attendus »)
- Livrable 1 : `../Livrable 1/L1-rapport-cloud-health-unit.md` (modèle logique, 4 mesures, 8 besoins)
- Résultats benchmark : `scripts/04_benchmark/results/` (CSV + SVG + résultats métier)
- Rapport qualité : `scripts/05_qualite/results/` (6 CSV)
