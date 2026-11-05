"""Exporte les deux dashboards (avec charts, datasets, connexion) dans un paquet importable : export/chu_superset_<mode>.zip.
Usage : python3 export_bundle.py"""
import json
from pathlib import Path

from sslib import Superset

HERE = Path(__file__).resolve().parent
R = json.loads((HERE / "registry.json").read_text())
ss = Superset()
out = HERE / "export"
out.mkdir(exist_ok=True)
ids = [R["dashboard"], R["dashboard_mobile"]]
r = ss.req("GET", "dashboard/export/", params={"q": json.dumps(ids)})
dest = out / f"chu_superset_{R['mode']}.zip"
dest.write_bytes(r.content)
print(dest, len(r.content), "octets")
