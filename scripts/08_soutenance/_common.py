"""Fonctions communes de la soutenance (Livrable 3) : lecture des CSV, formats FR, theme."""
import csv
import json
from pathlib import Path

import pandas as pd

BASE = Path(__file__).resolve().parent
DATA = BASE / "data"
REF = BASE / "ref"
ARCH = REF / "archive_donnees_completes"
OUT = BASE / "visuels"
OUT.mkdir(exist_ok=True)

# Totaux de reference releves a la source (psql sur le dump S1, CSV Hospitalisations) :
# les visuels refusent de se generer si le lakehouse ne les restitue pas.
S1_CONSULTATIONS = 1_027_157
S1_PATIENTS = 100_000
HOSP_TOTAL = 2_479
S1_PAR_ANNEE = {2015: 33896, 2016: 184308, 2017: 133403, 2018: 160373, 2019: 87497,
                2020: 162778, 2021: 145883, 2022: 101991, 2023: 17028}  # psql sur le dump S1

BLEU = "#0B4F8A"
BLEU_CLAIR = "#5FA8D3"
TEAL = "#1B998B"
ORANGE = "#E07A1F"
ROUGE = "#C0392B"
GRIS = "#6B7280"
GRIS_CLAIR = "#E5E7EB"
FOND = "#FFFFFF"
SEQ = ["#E8F1FA", "#BCD8EE", "#8DBBE0", "#5FA8D3", "#2F7FBF", "#0B4F8A"]

CHAPITRES = {
    "A": "Infectieuses et parasitaires", "B": "Infectieuses et parasitaires",
    "C": "Tumeurs malignes", "D": "Tumeurs bénignes, sang",
    "E": "Endocriniennes, métaboliques", "F": "Troubles mentaux",
    "G": "Système nerveux", "H": "Oeil et oreille",
    "I": "Appareil circulatoire", "J": "Appareil respiratoire",
    "K": "Appareil digestif", "L": "Peau", "M": "Ostéo-articulaire",
    "N": "Génito-urinaire", "O": "Grossesse, accouchement",
    "P": "Période périnatale", "Q": "Malformations",
    "R": "Symptômes et signes", "S": "Traumatismes, empoisonnements",
    "T": "Traumatismes, empoisonnements", "U": "Codes spéciaux",
    "V": "Causes externes", "W": "Causes externes", "X": "Causes externes",
    "Y": "Causes externes", "Z": "Facteurs de santé",
}


def fr(n, d=0):
    """Nombre au format francais : espace insecable pour les milliers, virgule decimale."""
    s = f"{n:,.{d}f}".replace(",", "\u202f").replace(".", ",")
    return s


def lire(nom):
    return pd.read_csv(DATA / f"{nom}.csv")


def lire_archive_q01():
    return pd.read_csv(ARCH / "q01_consult_etab_2019.csv", header=None,
                       names=["etablissement", "code_commune", "region", "consultations"])


def lire_archive_q07():
    return pd.read_csv(ARCH / "q07_deces_region_2019.csv", header=None, names=["region", "deces"])


def geojson_metropole():
    return json.load(open(REF / "regions_france_metropole.geojson", encoding="utf-8"))


def verifier_reconciliation():
    """Rapprochement lakehouse <-> sources : echec bloquant si ecart."""
    d01 = lire("d01_consultations_mensuelles")
    assert int(d01.consultations.sum()) == S1_CONSULTATIONS, "consultations mensuelles != S1"
    d02 = lire("d02_hospitalisation_annuelle")
    assert int(d02.hospitalisations.sum()) == HOSP_TOTAL, "hospitalisations != CSV source"
    for a, n in zip(d02.annee, d02.consultations_annee):
        assert int(n) == S1_PAR_ANNEE[int(a)], f"consultations {a} != S1"
    assert sum(S1_PAR_ANNEE.values()) == S1_CONSULTATIONS
    d03 = lire("d03_hospitalisation_sexe_age")
    assert int(d03.hospitalisations.sum()) == HOSP_TOTAL
    d04 = lire("d04_consultations_chapitre_cim10")
    ref_2019_21 = sum(S1_PAR_ANNEE[a] for a in (2019, 2020, 2021))
    assert int(d04.consultations.sum()) == ref_2019_21, "chapitres CIM-10 != consultations 2019-2021"
    d05 = lire("d05_hospitalisations_chapitre_cim10")
    assert int(d05.hospitalisations.sum()) == HOSP_TOTAL
    d07 = lire("d07_consultations_profession_2020")
    assert int(d07.consultations.sum()) == S1_PAR_ANNEE[2020]
    d07c = lire("d07c_distribution_consultations_par_professionnel_2020")
    assert int((d07c.consultations_par_professionnel * d07c.professionnels).sum()) == int(d07.consultations.sum())
    return True
