"""Captures d'ecran Superset (Playwright + Chrome) : bureau 1440 px et telephone 390 px, un fichier par onglet,
puis par etape de l'histoire (liens permanents), plus la preuve des couches du lac (SQL Lab, liste des datasets).
Usage : python3 capture.py [dossier_sortie]    (necessite : pip install playwright ; Chrome ou Chromium installe)"""
import json
import os
import shutil
import sys
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent
URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else HERE.parent / "visuels" / "superset"
CHROME = next((c for c in ("/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser") if os.path.exists(c)), None)
R = json.loads((HERE / "registry.json").read_text())
TABS = [("contexte", "1. Contexte"), ("activite", "2. Activité"), ("hospit", "3. Hospitalisations"),
        ("mortalite", "4. Mortalité et qualité"), ("reco", "5. Recommandations"), ("annexe", "6. Annexe technique")]


def attendre(pg, mini=10, maxi=180):
    t = time.time()
    while time.time() - t < maxi:
        pg.wait_for_timeout(1500)
        n = pg.locator("text=/Waiting on|En attente/").count() + pg.locator(".loading, [data-test='loading-indicator']").count()
        if n == 0 and time.time() - t > mini:
            break
    pg.wait_for_timeout(1500)


def erreurs(pg):
    """Nombre de graphiques en erreur affiches (doit etre 0)."""
    return pg.locator("text=/Erreur inattendue|Unexpected error|Bad request|Query not found/").count()


def login(ctx):
    pg = ctx.new_page()
    pg.goto(f"{URL}/login/")
    pg.fill("#username", "admin"); pg.fill("#password", "admin"); pg.click("input[type=submit]")
    pg.wait_for_load_state("networkidle")
    return pg


def main():
    only = os.environ.get("CAPTURE_ONLY", "")  # "mobile" ou "bureau" pour ne rejouer qu'une partie
    OUT.mkdir(parents=True, exist_ok=True)
    resume = []
    with sync_playwright() as p:
        b = p.chromium.launch(executable_path=CHROME, args=["--no-sandbox"])
        if only in ("", "bureau"):
            ctx = b.new_context(viewport={"width": 1440, "height": 900}, locale="fr-FR")
            pg = login(ctx)
            for i, (key, titre) in enumerate(TABS, 1):
                pg.goto(f"{URL}/superset/dashboard/chu-histoire/?expand_filters=0")
                pg.wait_for_timeout(2500)
                pg.get_by_role("tab", name=titre).click()
                attendre(pg)
                f = OUT / f"bureau_{i}_{key}.png"
                pg.screenshot(path=str(f), full_page=True)
                resume.append((f.name, erreurs(pg)))
            pg.goto(f"{URL}/superset/dashboard/chu-histoire/?expand_filters=1"); pg.wait_for_timeout(2500)
            pg.get_by_role("tab", name="2. Activité").click(); attendre(pg, 8)
            pg.screenshot(path=str(OUT / "bureau_filtres_natifs.png"), full_page=False)
            # preuve des couches du lac : SQL Lab et liste des datasets
            sql = ("SELECT table_schema AS couche, COUNT(*) AS nb_tables, array_join(array_agg(table_name ORDER BY table_name), ', ') AS tables "
                   "FROM hive.information_schema.tables WHERE table_schema IN ('raw','silver','gold','archive','vues_t') GROUP BY 1 ORDER BY 1")
            from urllib.parse import quote
            pg.goto(f"{URL}/sqllab/?dbid={R['database']}&schema=gold&sql={quote(sql)}"); pg.wait_for_timeout(5000)
            pg.keyboard.press("Control+Enter"); pg.wait_for_timeout(12000)
            pg.screenshot(path=str(OUT / "sqllab_couches_du_lac.png"), full_page=False)
            try:  # liste deroulante des schemas du catalogue hive
                pg.locator("div.ant-select[aria-label^=\"Select schema\"]").last.click(timeout=8000); pg.wait_for_timeout(2500)
                pg.screenshot(path=str(OUT / "sqllab_schemas_deroulant.png"), full_page=False)
                pg.keyboard.press("Escape")
            except Exception as e:
                print("liste des schemas non capturee :", e)
            pg.goto(f"{URL}/tablemodelview/list/?pageIndex=0&sortColumn=schema&sortOrder=desc&viewMode=table")
            pg.wait_for_timeout(6000)
            pg.screenshot(path=str(OUT / "liste_des_datasets.png"), full_page=True)
            # etapes de l'histoire (liens permanents)
            for e in R["permaliens"]:
                if e["mobile"]:
                    continue
                pg.goto(e["url"]); pg.wait_for_timeout(2500); attendre(pg, 8)
                f = OUT / f"etape_{e['etape']}_{e['tab']}{'_filtre_' + str(e['filtre'][1]).replace(' ', '_') if e['filtre'] else ''}.png"
                pg.screenshot(path=str(f), full_page=True)
                resume.append((f.name, erreurs(pg)))
            ctx.close()
        if only in ("", "mobile"):
            ctx = b.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2, has_touch=True, locale="fr-FR",
                                user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1")
            pg = login(ctx)
            for i, (key, titre) in enumerate(TABS, 1):
                pg.goto(f"{URL}/superset/dashboard/chu-histoire-mobile/?standalone=2&expand_filters=0")
                pg.wait_for_timeout(2500)
                pg.get_by_role("tab", name=titre).dispatch_event("click")
                attendre(pg)
                f = OUT / f"telephone_{i}_{key}.png"
                pg.screenshot(path=str(f), full_page=True)
                larg = pg.evaluate("document.documentElement.scrollWidth")
                resume.append((f.name, erreurs(pg), larg))
            ctx.close()
        b.close()
    (OUT / "resume_capture.json").write_text(json.dumps(resume, indent=1), encoding="utf-8")
    for r in resume:
        print(r)


if __name__ == "__main__":
    main()
