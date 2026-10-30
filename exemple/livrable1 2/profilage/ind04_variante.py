"""Variante d'IND-04 : séjours précédés d'une consultation du même patient pour le même diagnostic.
Entrées : export psql  copy (select "Id_patient","Code_diag",min("Date") from "Consultation" group by 1,2) to stdout
          > pdd.txt ; Hospitalisations.csv.  Résultat attendu sur les données reçues : 2 (code exact), 18 (catégorie)."""
import csv, datetime, sys
pdd, hosp = sys.argv[1], sys.argv[2]
m, mc = {}, {}
for l in open(pdd):
    p, d, dt = l.rstrip('\n').split('\t'); dt = datetime.date.fromisoformat(dt)
    m[(p, d)] = min(m.get((p, d), dt), dt); k = (p, d[:3]); mc[k] = min(mc.get(k, dt), dt)
ex = ca = 0
for r in csv.DictReader(open(hosp, encoding='utf-8'), delimiter=';'):
    p, d = r['Id_patient'], r['Code_diagnostic']
    de = datetime.datetime.strptime(r['Date_Entree'], '%d/%m/%Y').date()
    ex += (p, d) in m and m[(p, d)] <= de
    ca += (p, d[:3]) in mc and mc[(p, d[:3])] <= de
print("code exact :", ex, "| catégorie :", ca)
