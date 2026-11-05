"""Textes de l'histoire (blocs Markdown du tableau de bord). Chaque chiffre est calcule ici depuis le lac (Trino) ou depuis
l'archive de la campagne complete du 30/10/2026 : aucun chiffre saisi a la main."""
import csv
from pathlib import Path

from sslib import trino

ARCH = Path(__file__).resolve().parent.parent / "ref" / "archive_donnees_completes"


def fr(n, d=0):
    return f"{n:,.{d}f}".replace(",", "\u202f").replace(".", ",")


def q1(sql):
    return trino(sql)[1][0][0]


def chiffres(mode="archive"):
    c = {}
    c["cons"] = q1("SELECT COUNT(*) FROM hive.silver.f_consultation_detail")
    c["pat"] = q1("SELECT COUNT(*) FROM hive.gold.d_patient")
    c["hosp"] = q1("SELECT COUNT(*) FROM hive.gold.f_hospitalisation")
    c["hosp_f"] = q1("SELECT COUNT(*) FROM hive.gold.f_hospitalisation WHERE sexe = 'F'")
    c["duree"] = q1("SELECT AVG(duree_jours) FROM hive.gold.f_hospitalisation")
    tot = q1("SELECT COUNT(*) FROM hive.silver.f_consultation_detail WHERE annee BETWEEN 2019 AND 2021")
    c["cons_1921"] = tot
    sel = lambda lst: q1("SELECT COUNT(*) FROM hive.silver.f_consultation_detail f JOIN hive.gold.d_diagnostic d "
                         "ON d.id_diagnostic = f.id_diagnostic WHERE f.annee BETWEEN 2019 AND 2021 AND SUBSTR(d.code_diag,1,1) IN (" + lst + ")")
    c["part_m"] = 100.0 * sel("'M'") / tot
    c["part_st"] = 100.0 * sel("'S','T'") / tot
    rows = trino("SELECT annee_mois, COUNT(*) FROM hive.silver.f_consultation_detail GROUP BY 1 ORDER BY 1")[1][1:-1]
    c["mois_min"] = min(r[1] for r in rows)
    c["mois_max"] = max(r[1] for r in rows)
    taux = trino("""WITH h AS (SELECT annee, COUNT(*) n FROM hive.gold.f_hospitalisation GROUP BY 1),
 c AS (SELECT annee, COUNT_IF(id_temps BETWEEN 20150621 AND 20210619) n FROM hive.silver.f_consultation_detail GROUP BY 1)
 SELECT c.annee, 100.0 * h.n / c.n FROM c JOIN h ON h.annee = c.annee ORDER BY 1""")[1]
    c["taux"] = {int(a): float(t) for a, t in taux}
    c["avant_h"] = q1("SELECT COUNT(*) FROM hive.gold.f_hospitalisation f JOIN hive.gold.d_temps t ON t.id_temps = f.id_temps "
                      "JOIN hive.gold.d_patient p ON p.id_patient = f.id_patient WHERE t.date_jour < p.date_naissance")
    c["avant_c"] = q1("SELECT COUNT(*) FROM hive.silver.f_consultation_detail f JOIN hive.gold.d_temps t ON t.id_temps = f.id_temps "
                      "JOIN hive.gold.d_patient p ON p.id_patient = f.id_patient WHERE t.date_jour < p.date_naissance")
    one = q1("SELECT COUNT(*) FROM (SELECT id_professionnel FROM hive.gold.f_consult_prof_mois WHERE annee = 2020 GROUP BY 1 HAVING SUM(nbre_consultation) = 1)")
    allp = q1("SELECT COUNT(DISTINCT id_professionnel) FROM hive.gold.f_consult_prof_mois WHERE annee = 2020")
    c["pro_un_pct"] = 100.0 * one / allp
    sat = trino("""SELECT MIN(s), MAX(s) FROM (SELECT region, SUM(score_satisfaction * nb_repondants) / SUM(CASE WHEN score_satisfaction IS NOT NULL THEN nb_repondants END) s
 FROM hive.gold.f_satisfaction WHERE annee = 2020 AND region NOT IN ('INCONNUE','Guadeloupe','Guyane','Martinique','Mayotte','La Réunion') GROUP BY 1)""")[1][0]
    c["sat_min"], c["sat_max"] = sat
    if mode == "archive":  # archive de la campagne complete
        q07 = list(csv.reader(open(ARCH / "q07_deces_region_2019.csv", encoding="utf-8")))
        d = {r[0]: int(r[1]) for r in q07}
        v = [int(r[3]) for r in csv.reader(open(ARCH / "q01_consult_etab_2019.csv", encoding="utf-8"))]
        fk = {r[0]: r for r in csv.reader(open(ARCH / "dq_integrite_fk.csv", encoding="utf-8"), delimiter=";")}
        c["rattach"] = float([r for k, r in fk.items() if "f_consultation_detail.id_etablissement" in k][0][-1])
    else:  # donnees du lac de cette machine
        d = {r[0]: int(r[1]) for r in trino("SELECT region, SUM(nbre_deces) FROM hive.gold.f_deces_agg GROUP BY 1")[1]}
        v = [int(r[0]) for r in trino("SELECT SUM(f.nbre_consultation) n FROM hive.gold.f_mesures f WHERE f.annee = 2019 AND f.id_etablissement > 0 "
                                      "GROUP BY f.id_etablissement ORDER BY n DESC LIMIT 20")[1]]
        c["rattach"] = float(q1("SELECT 100.0 * COUNT_IF(id_etablissement > 0) / COUNT(*) FROM hive.silver.f_consultation_detail"))
    c["deces"] = sum(d.values())
    c["deces_idf_ara_pct"] = 100.0 * (d.get("Île-de-France", 0) + d.get("Auvergne-Rhône-Alpes", 0)) / c["deces"]
    c["etab_min"], c["etab_max"] = min(v), max(v)
    b = list(csv.DictReader(open(ARCH / "bench_synthese.csv", encoding="utf-8"), delimiter=";"))
    c["bench_n"] = len(b)
    c["bench_moins_volume"] = sum(1 for r in b if float(r["ratio_volume_lu"]) > 1)
    c["bench_plus_rapide_ml"] = sum(1 for r in b if float(r["apres_ml_ms_median"]) < float(r["avant_ms_median"]))
    c["bench_max_ratio"] = max(float(r["ratio_volume_lu"]) for r in b)
    return c


def textes(c, mode):
    carte = "campagne complète du 30/10/2026" if mode == "archive" else "données du lac"
    t = {}
    t["contexte"] = f"""## Le CHU peut-il piloter son activité avec ses données ?
Le CHU veut suivre **l'activité de consultation, les hospitalisations, les décès et la satisfaction** des patients.
Ce tableau de bord répond aux huit besoins des décideurs, dans l'ordre d'une histoire : **contexte, activité, hospitalisations, mortalité et qualité, recommandations**.

**Périmètre.** {fr(c['cons'])} consultations, {fr(c['pat'])} patients, {fr(c['hosp'])} hospitalisations, lus directement dans le lac (couches raw, silver et gold) par Trino.
Filtres à gauche : année, sexe, tranche d'âge, région, établissement, chapitre CIM-10."""
    t["contexte_lecture"] = f"""### Comment lire ces chiffres
* Les **nombres de lignes** du tableau ci-contre sont comptés en direct dans le lac : ils prouvent que les trois couches sont visibles.
* Les décès et les établissements les plus consultés viennent de : {carte}.
* Aucun nom de patient ni de professionnel n'est affiché."""
    t["activite"] = f"""### Une activité très irrégulière, sans tendance de fond, et sans concentration
* **{fr(c['cons'])} consultations** de juin 2015 à mars 2023 ; par mois, de **{fr(c['mois_min'])} à {fr(c['mois_max'])}** (hors premier et dernier mois, partiels).
* Deux chapitres CIM-10 pèsent le plus en 2019 à 2021 : l'ostéo-articulaire (**{fr(c['part_m'],1)} %**) et les traumatismes (**{fr(c['part_st'],1)} %**).
* Aucun diagnostic ne domine et aucun établissement ne se détache : les 20 premiers établissements de 2019 comptent de **{c['etab_min']} à {c['etab_max']} consultations**.
* **{fr(c['pro_un_pct'])} %** des professionnels n'ont qu'une consultation en 2020 : la distribution est très dispersée."""
    t["activite_garde"] = """**À savoir.** Août à décembre 2022 reproduit exactement les volumes de 2021 : artefact des données sources, à ne pas interpréter.
Les données sont des données de formation : des distributions plates ne portent pas de signal clinique réel."""
    t["hospit"] = f"""### Les hospitalisations sont rares, plutôt féminines, et durent environ {fr(c['duree'],0)} jours
* **{fr(c['hosp'])} hospitalisations**, dont **{fr(100.0*c['hosp_f']/c['hosp'],0)} % de femmes** ; durée moyenne de séjour : **{fr(c['duree'],1)} jours**.
* Taux : de **{fr(min(c['taux'][a] for a in range(2016,2021)),2)} à {fr(max(c['taux'][a] for a in range(2016,2021)),2)} hospitalisations pour 100 consultations** de 2016 à 2020.
  Les années 2015 ({fr(c['taux'][2015],2)}) et 2021 ({fr(c['taux'][2021],2)}) sont partielles : les hospitalisations ne couvrent que du 21/06/2015 au 19/06/2021, les consultations sont comptées sur la même fenêtre.
* Rapporté aux consultations, le risque d'hospitalisation est plat selon l'âge et le sexe."""
    t["hospit_garde"] = f"""**Mise en garde.** {c['avant_h']} hospitalisations ({fr(100.0*c['avant_h']/c['hosp'],1)} %) et {fr(c['avant_c'])} consultations ({fr(100.0*c['avant_c']/c['cons'],1)} %) sont **datées avant la naissance du patient** dans les sources.
Elles sont comptées dans la tranche 0-14 ans par la règle d'âge révolu à la date de l'événement."""
    t["mortalite"] = f"""### Les décès suivent la taille des régions ; la satisfaction est homogène ; la fiabilité limite l'analyse par établissement
* **{fr(c['deces'])} décès{' en 2019' if mode == 'archive' else ''}** ({carte}) ; l'Île-de-France et Auvergne-Rhône-Alpes en concentrent **{fr(c['deces_idf_ara_pct'],0)} %**. Valeurs brutes, non rapportées à la population régionale.
* Satisfaction 2020 en métropole : de **{fr(c['sat_min'],1)} à {fr(c['sat_max'],1)} sur 100**, un écart faible.
* Seulement **{fr(c['rattach'],2)} %** des consultations sont rattachées à un établissement."""
    t["mortalite_garde"] = """**À savoir.** Les décès par région sont des valeurs brutes (le fichier contient des doublons non retirés).
La satisfaction est mesurée par établissement juridique, pas par site ; un quart des lignes source n'a pas de score."""
    t["reco"] = f"""## Ce que nous recommandons au CHU
1. **Fiabiliser le rattachement consultation vers établissement** : {fr(c['rattach'],2)} % seulement des consultations sont rattachées ; ajouter l'identifiant d'établissement dans la consultation.
2. **Ne pas piloter par professionnel** avec ces données : {fr(c['pro_un_pct'])} % des professionnels n'ont qu'une consultation en 2020.
3. **Rapporter les décès à la population régionale** avant toute comparaison : la carte suit la taille des régions.
4. **Compléter les scores de satisfaction** : un quart des lignes source sont sans score et le score est au niveau de l'établissement juridique.
5. **Garder le partitionnement par année** : volume lu divisé jusqu'à {fr(c['bench_max_ratio'],1)}, mais pas de gain de temps à cette échelle (voir l'annexe technique).
6. **Corriger les dates antérieures à la naissance** à la source : {c['avant_h']} hospitalisations et {fr(c['avant_c'])} consultations."""
    t["perspectives"] = """## Perspectives
* **Nouvelles sources** : population régionale (taux de mortalité), identifiant d'établissement dans la consultation, référentiel FINESS complet.
* **Modèles prédictifs** : prévision des consultations mensuelles et du risque d'hospitalisation, une fois la fiabilité améliorée.
* **Conformité continue** : contrôles qualité rejoués à chaque exécution du DAG quotidien, journalisation des accès (RGPD, hébergement de données de santé)."""
    t["annexe"] = f"""### Annexe technique : modèle en étoile et optimisation
**Étoile.** Faits : `silver.f_consultation_detail`, `gold.f_hospitalisation`, `gold.f_deces_agg`, `gold.f_satisfaction`, `gold.f_mesures`. Dimensions : `d_temps`, `d_patient`, `d_etablissement`, `d_localisation`, `d_diagnostic`, `d_professionnel`.
Dans Superset : datasets physiques pour chaque table, jeux virtuels `etoile_consultations` et `etoile_hospitalisations` qui joignent l'étoile, métriques nommées (consultations, hospitalisations pour 100 consultations, score pondéré).
**Benchmark.** Le partitionnement lit moins de données sur **{c['bench_moins_volume']} requêtes sur {c['bench_n']}** mais n'est plus rapide que sur **{c['bench_plus_rapide_ml']}** requêtes, avec un pruning façon metastore."""
    return t
