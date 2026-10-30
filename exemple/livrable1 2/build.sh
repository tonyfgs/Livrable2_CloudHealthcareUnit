#!/usr/bin/env bash
# Construction complète : modèle -> diagrammes -> figures PDF -> PDF LaTeX
set -euo pipefail
cd "$(dirname "$0")"
PLANTUML_JAR=${PLANTUML_JAR:-$HOME/tools/plantuml.jar}
python3 model/gen.py
python3 model/ddl.py
( cd diagrams && java -jar "$PLANTUML_JAR" -tsvg -charset UTF-8 [0-9]*.puml )
mkdir -p figures
for f in diagrams/[0-9]*.svg; do
  b=$(basename "$f" .svg)
  rsvg-convert -f pdf -o "figures/$b.pdf" "$f"
  rsvg-convert -z 2 -f png -o "figures/$b.png" "$f"
done
latexmk -lualatex -interaction=nonstopmode -halt-on-error livrable1.tex
cp livrable1.pdf ../livrable1.pdf
