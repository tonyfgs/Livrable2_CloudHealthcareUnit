"""Importe un paquet exporte par export_bundle.py dans un Superset vierge (alternative a provision.py).
Usage : python3 import_bundle.py export/chu_superset_archive.zip
Les liens permanents ne font pas partie du paquet : rejouer ensuite  python3 provision.py --only permalinks"""
import json
import sys
from pathlib import Path

from sslib import Superset

zip_path = Path(sys.argv[1])
ss = Superset()
with open(zip_path, "rb") as f:
    r = ss.s.post(f"{ss.url}/api/v1/dashboard/import/", files={"formData": (zip_path.name, f, "application/zip")},
                  data={"overwrite": "true", "passwords": json.dumps({})}, timeout=600)
print(r.status_code, r.text[:500])
sys.exit(0 if r.status_code == 200 else 1)
