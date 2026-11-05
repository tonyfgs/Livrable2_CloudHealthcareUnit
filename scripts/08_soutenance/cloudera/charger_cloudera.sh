#!/usr/bin/env bash
# Charge l'export (export_snappy.py + export_raw.sh) dans la VM Cloudera Quickstart 5.12 : HDFS puis tables Hive.
# Les trois couches du lac deviennent visibles : /user/cloudera/lake/{raw,silver,gold} et bases Hive raw, silver, gold.
# Usage : ./charger_cloudera.sh [adresse_VM]      (defaut : quickstart.cloudera, utilisateur cloudera / mot de passe cloudera)
# Prerequis : le dossier export_cloudera/ (issu de export_snappy.py, et de export_raw.sh pour la couche raw) est dans le dossier courant.
set -euo pipefail
VM="${1:-quickstart.cloudera}"; U=cloudera
SRC="${EXPORT_DIR:-./export_cloudera}"
[ -d "$SRC/gold" ] || { echo "Dossier $SRC/gold introuvable : lancer d'abord export_snappy.py"; exit 1; }
[ -d "$SRC/raw" ] || echo "ATTENTION : $SRC/raw absent (lancer export_raw.sh) : la couche raw ne sera pas visible dans Cloudera."
SSH="ssh -n -o StrictHostKeyChecking=no $U@$VM"   # -n : ssh ne doit pas lire l'entree standard (boucle while ci-dessous)
echo "1/4 copie vers la VM"; scp -o StrictHostKeyChecking=no -r "$SRC" "$U@$VM:/home/cloudera/export_cloudera"
echo "2/4 mise en place dans HDFS (/user/cloudera/lake/{raw,silver,gold}/...)"
$SSH 'set -e; hdfs dfs -rm -r -f /user/cloudera/lake >/dev/null 2>&1 || true
for base in gold silver; do for t in /home/cloudera/export_cloudera/$base/*/; do n=$(basename $t);
  hdfs dfs -mkdir -p /user/cloudera/lake/$base/$n; hdfs dfs -put -f $t/*.parquet /user/cloudera/lake/$base/$n/; done; done
if [ -d /home/cloudera/export_cloudera/raw ]; then for t in /home/cloudera/export_cloudera/raw/*/*/; do rel=${t#/home/cloudera/export_cloudera/raw/};
  hdfs dfs -mkdir -p /user/cloudera/lake/raw/$rel; hdfs dfs -put -f $t/*.csv /user/cloudera/lake/raw/$rel; done; fi
hdfs dfs -ls -R /user/cloudera/lake | grep -c -E "parquet|csv"'
echo "3/4 creation des tables Hive 1.1 (gold, silver et raw)"
scp -o StrictHostKeyChecking=no "$SRC/ddl_hive_cloudera.sql" "$U@$VM:/home/cloudera/"
$SSH 'beeline -u jdbc:hive2://localhost:10000 -n cloudera -f /home/cloudera/ddl_hive_cloudera.sql >/dev/null && echo DDL gold et silver OK'
if [ -f "$SRC/ddl_raw_cloudera.sql" ] && [ -d "$SRC/raw" ]; then
  scp -o StrictHostKeyChecking=no "$SRC/ddl_raw_cloudera.sql" "$U@$VM:/home/cloudera/"
  $SSH 'beeline -u jdbc:hive2://localhost:10000 -n cloudera -f /home/cloudera/ddl_raw_cloudera.sql >/dev/null && echo DDL raw OK'
fi
echo "4/4 controle des volumes (a comparer avec volumes_export.csv et volumes_raw.csv)"
for f in "$SRC/volumes_export.csv" "$SRC/volumes_raw.csv"; do
  [ -f "$f" ] || continue
  while IFS=';' read -r t n; do [ "$t" = "table" ] && continue
    r=$($SSH "beeline -u jdbc:hive2://localhost:10000 -n cloudera --silent=true --outputformat=csv2 -e 'SELECT COUNT(*) FROM $t' 2>/dev/null | tail -1")
    [ "$r" = "$n" ] && echo "OK   $t $r" || echo "ECART $t Hive=$r attendu=$n"
  done < "$f"
done
