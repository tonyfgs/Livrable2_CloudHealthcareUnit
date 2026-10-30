# Livrable 1 : référentiel de données et architecture (sources LaTeX)

## Modifier le texte
- Le texte du rapport est dans `chapitres/` (un fichier par chapitre) ; `livrable1.tex` les assemble, `preambule.tex` contient la mise en page.
- Compilation simple (texte seul, figures déjà rendues dans `figures/`) : LuaLaTeX puis biber puis LuaLaTeX deux fois.
  `lualatex livrable1 && biber livrable1 && lualatex livrable1 && lualatex livrable1`
- Fonctionne aussi sur Overleaf : importer le zip, compilateur LuaLaTeX.

## Modifier le modèle ou les diagrammes
- `model/model.py` est la source unique du modèle (16 tables) : il génère l'annexe A (dictionnaire), le tableau des cardinalités, le DDL HiveQL (`generated/ddl_gold.sql`) et les diagrammes constellation et étoile. Ne pas modifier `generated/` à la main.
- `diagrams/*.puml` : sources PlantUML des 9 figures ; les rendus sont dans `figures/`.
- Reconstruction complète : `./build.sh` (Python 3, Java + PlantUML, rsvg-convert, TeX Live avec LuaLaTeX et biber).

## Règles de rédaction suivies
- Aucun tiret cadratin ni demi-cadratin (utiliser virgule, deux-points, parenthèses, « à » pour les intervalles).
- Le PDF doit rester autonome : aucune référence à ce zip, à des fichiers ou à des chemins.
- Aucune référence à la grille de notation ni aux documents du tuteur ; s'appuyer sur le sujet et les données.
- Aucun résultat d'exécution Talend ni capture inventés.
- Environ 20 pages de corps hors annexe.
