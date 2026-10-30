# -*- coding: utf-8 -*-
"""DDL HiveQL de la couche Gold (tables EXTERNES, TEXTFILE), généré depuis model.py (source unique).
Cible : Hive 1.1 de la VM Cloudera QuickStart 5.12 (CDH 5.12). Variables Hive : ${hivevar:racine}
(ex. hdfs://quickstart.cloudera:8020/user/cloudera/chu) et ${hivevar:chargement} (AAAAMM du lot)."""
import importlib.util, pathlib, re
H = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("m", H / "model.py"); m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
# Faits d'événements alimentés par ajout mensuel : une partition par chargement (répertoire chargement=AAAAMM).
PART = {"fait_consultation", "fait_deces", "fait_indicateur_qualite"}
def typ(t):
    t = t.upper()
    if t.startswith("CHAR("): return "VARCHAR(" + t[5:]          # pas de remplissage par des blancs
    if t == "INTEGER": return "INT"
    if t == "TIME": return "STRING"                             # Hive n'a pas de type heure : 'HH:mm:ss'
    if t.startswith("TIMESTAMP"): return "TIMESTAMP"            # 'yyyy-MM-dd HH:mm:ss' dans le fichier
    return t                                                    # SMALLINT, BIGINT, VARCHAR(n), DECIMAL(p,s), DATE, BOOLEAN
FMT = ("ROW FORMAT DELIMITED FIELDS TERMINATED BY ';' NULL DEFINED AS ''\n"
       "STORED AS TEXTFILE")
out = ["-- DDL Gold (HiveQL, tables EXTERNES en TEXTFILE) -- généré par model/ddl.py depuis model/model.py",
       "-- Exécution : beeline -u jdbc:hive2://quickstart.cloudera:10000 --hivevar racine=... --hivevar chargement=AAAAMM -f ddl_gold.sql",
       "-- Hive 1.1 n'impose ni clé primaire, ni clé étrangère, ni NOT NULL : unicité, intégrité référentielle",
       "-- et non-nullité sont vérifiées par les requêtes de contrôle du job J_PUB (RQ-08, RQ-20, RQ-21) avant publication.",
       "-- Dates au format yyyy-MM-dd, horodatages yyyy-MM-dd HH:mm:ss, booléens true/false, champ vide = NULL.",
       "", "CREATE DATABASE IF NOT EXISTS chu_gold LOCATION '${hivevar:racine}/gold';", "USE chu_gold;", ""]
order = [n for n in m.T if n.startswith("dim_")] + [n for n in m.T if n.startswith("fait_")]
for n in order:
    t = m.T[n]; cols = list(t["cols"])
    pk = [c[0] for c in cols if "PK" in c[3]]
    nn = [c[0] for c in cols if c[2] == "NN"]
    fk = [f"{c[0]}->{c[4]}" for c in cols if "FK" in c[3]]
    out.append(f"-- {t['label']} | grain : {t['grain']} | PK contrôlée : ({', '.join(pk)})")
    if fk: out.append("-- FK contrôlées : " + ", ".join(fk))
    out.append(f"CREATE EXTERNAL TABLE IF NOT EXISTS {n} (")
    out.append(",\n".join(f"  {c[0]:<30} {typ(c[1])}" for c in cols))
    if n in PART:
        out.append(")\nPARTITIONED BY (chargement STRING)")
        out.append(FMT + f"\nLOCATION '${{hivevar:racine}}/gold/{n}';")
    else:
        out.append(")")
        out.append(FMT + f"\nLOCATION '${{hivevar:racine}}/gold/{n}/chargement=${{hivevar:chargement}}';")
    out.append("")
out += ["-- Publication d'un lot (job J_PUB), après succès des contrôles sur les tables de travail *_stg :",
        "--   dimensions et fait_hospitalisation : ALTER TABLE <t> SET LOCATION '${hivevar:racine}/gold/<t>/chargement=${hivevar:chargement}';",
        "--   faits partitionnés : ALTER TABLE <t> ADD IF NOT EXISTS PARTITION (chargement='${hivevar:chargement}')",
        "--                        LOCATION '${hivevar:racine}/gold/<t>/chargement=${hivevar:chargement}';",
        "-- Retour arrière : SET LOCATION sur le répertoire du lot précédent, ou DROP PARTITION (table externe : fichiers conservés)."]
(H.parent / "generated" / "ddl_gold.sql").write_text("\n".join(out), encoding="utf-8")
print(len(order), "tables")
