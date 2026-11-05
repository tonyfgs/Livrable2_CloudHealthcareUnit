#!/usr/bin/env bash
# Exporte la couche RAW (bronze) du lac MinIO vers ./export_cloudera/raw/<source>/<fichier>/<fichier>.csv
# pour l'exposer dans HDFS et Hive (base `raw`) sur la VM Cloudera : ainsi les trois couches du lac
# (raw, silver, gold) sont visibles dans Hue et dans le Navigateur Power BI.
# Usage (depuis le dossier scripts/, stack demarree) :  ./08_soutenance/cloudera/export_raw.sh
#   RAW_COMPLET=1 ./08_soutenance/cloudera/export_raw.sh   # fichiers volumineux en entier (decès : plusieurs Go)
# Par defaut, un fichier de plus de 500 Mo est tronque a ses 200 000 premieres lignes (extrait) pour tenir dans la VM.
set -euo pipefail
OUT="${EXPORT_DIR:-./export_cloudera}"; NET="${CHU_NET:-chu-l2_chu-net}"; SUDO="${SUDO:-}"
IMG="amazon/aws-cli@sha256:3517c17b5f01637df3b6184a4280fbcc36e4ea38105954cade628ff17bb9a7b9"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
$SUDO docker run --rm --user "$(id -u):$(id -g)" -e HOME=/tmp --network "$NET" -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e AWS_DEFAULT_REGION=us-east-1 \
  -v "$TMP:/out" --entrypoint aws "$IMG" --endpoint-url http://minio:9000 s3 cp s3://lake/raw /out/raw --recursive \
  --exclude "*" --include "hospitalisation/*" --include "satisfaction/*" --include "referentiels/*" --include "etablissement/*" --include "deces/*" >/dev/null
rm -rf "$OUT/raw"; echo "table;lignes" > "$TMP/volumes_raw.csv"
# source|fichier|table Hive (doit correspondre a ddl_raw_cloudera.sql)
while IFS='|' read -r src f t; do
  d="$OUT/raw/$src/${f%.csv}"; mkdir -p "$d"
  taille=$(stat -c %s "$TMP/raw/$src/$f")
  if [ "${RAW_COMPLET:-0}" != "1" ] && [ "$taille" -gt 524288000 ]; then
    head -n 200001 "$TMP/raw/$src/$f" > "$d/$f"; echo "EXTRAIT (200 000 lignes) : $src/$f"
  else cp "$TMP/raw/$src/$f" "$d/$f"; fi
  echo "$t;$(( $(wc -l < "$d/$f") - 1 ))" >> "$TMP/volumes_raw.csv"
  echo "OK raw/$src/$f"
done <<'LISTE'
hospitalisation|Hospitalisations.csv|raw.hospitalisations
satisfaction|ESATIS_2019.csv|raw.satisfaction_2019
satisfaction|ESATIS_2020.csv|raw.satisfaction_2020
referentiels|referentiel_departements.csv|raw.referentiel_departements
referentiels|activite_professionnel_sante.csv|raw.activite_professionnel_sante
etablissement|etablissement_sante.csv|raw.etablissement_sante
deces|deces.csv|raw.deces
LISTE
cp "$TMP/volumes_raw.csv" "$OUT/volumes_raw.csv"; cp "$(dirname "$0")/ddl_raw_cloudera.sql" "$OUT/ddl_raw_cloudera.sql"
echo "Couche raw exportee dans $OUT/raw (volumes : $OUT/volumes_raw.csv)"
