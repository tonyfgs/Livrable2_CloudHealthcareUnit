"""Clients minimaux : Trino (API REST /v1/statement) et Superset (API REST /api/v1). Bibliotheque standard + requests."""
import os
import time
import requests

TRINO_URL = os.environ.get("TRINO_URL", "http://localhost:8080")
SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.environ.get("SUPERSET_USER", "admin")
SUPERSET_PASSWORD = os.environ.get("SUPERSET_PASSWORD", "admin")


def trino(sql, catalog="hive", schema="default"):
    """Execute une requete Trino et retourne (colonnes, lignes)."""
    h = {"X-Trino-User": "provision", "X-Trino-Catalog": catalog, "X-Trino-Schema": schema}
    r = requests.post(f"{TRINO_URL}/v1/statement", data=sql.encode("utf-8"), headers=h, timeout=120)
    r.raise_for_status()
    j = r.json(); cols, rows = None, []
    while True:
        if "error" in j:
            raise RuntimeError(f"Trino : {j['error']['message']}\nSQL : {sql[:400]}")
        if j.get("columns") and cols is None:
            cols = [c["name"] for c in j["columns"]]
        rows += j.get("data", [])
        if "nextUri" not in j:
            return cols or [], rows
        time.sleep(0.05)
        j = requests.get(j["nextUri"], headers=h, timeout=300).json()


class Superset:
    def __init__(self, url=SUPERSET_URL, user=SUPERSET_USER, password=SUPERSET_PASSWORD):
        self.url = url.rstrip("/")
        self.s = requests.Session()
        r = self.s.post(f"{self.url}/api/v1/security/login", json={"username": user, "password": password,
                                                                    "provider": "db", "refresh": True}, timeout=60)
        r.raise_for_status()
        self.s.headers["Authorization"] = "Bearer " + r.json()["access_token"]
        c = self.s.get(f"{self.url}/api/v1/security/csrf_token/", timeout=60)
        c.raise_for_status()
        self.s.headers.update({"X-CSRFToken": c.json()["result"], "Referer": self.url})

    def req(self, method, path, **kw):
        kw.setdefault("timeout", 300)
        r = self.s.request(method, f"{self.url}/api/v1/{path.lstrip('/')}", **kw)
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code} : {r.text[:800]}")
        return r.json() if r.content and "json" in r.headers.get("content-type", "") else r

    def find(self, resource, col, value, opr="eq"):
        import json
        q = json.dumps({"filters": [{"col": col, "opr": opr, "value": value}], "page_size": 100})
        res = self.req("GET", f"{resource}/", params={"q": q})
        return res.get("result", [])
