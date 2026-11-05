"""Soutenance (Livrable 3) : genere les visuels PNG (matplotlib) a partir des CSV du lakehouse.
Usage : python3 build_visuels.py   (apres export_donnees.sh)"""
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Polygon, FancyBboxPatch, FancyArrowPatch
from matplotlib.collections import PatchCollection
from matplotlib.colors import Normalize
from matplotlib import cm
from _common import *

verifier_reconciliation()
plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 13, "axes.spines.top": False,
    "axes.spines.right": False, "axes.edgecolor": GRIS, "axes.labelcolor": "#111827",
    "xtick.color": "#374151", "ytick.color": "#374151", "figure.facecolor": FOND,
    "axes.titleweight": "bold", "axes.titlesize": 16, "figure.dpi": 100,
})
SRC = "Source : lakehouse CHU (Trino, couche Gold). Données de formation CESI."


def finir(fig, nom, titre, sous_titre, source=SRC):
    import textwrap
    t = textwrap.fill(titre, 72)
    nl = t.count("\n") + 1
    fig.text(0.02, 0.985, t, ha="left", va="top", fontsize=18, fontweight="bold", color="#111827", linespacing=1.15)
    fig.text(0.02, 0.985 - 0.058 * nl - 0.005, textwrap.fill(sous_titre, 125), ha="left", va="top", fontsize=12.5, color=GRIS)
    fig.text(0.02, 0.012, textwrap.fill(source, 160), ha="left", va="bottom", fontsize=9.5, color=GRIS)
    fig.savefig(OUT / f"{nom}.png", dpi=160, bbox_inches=None)
    plt.close(fig)
    print("OK", nom)


def fig16x9():
    return plt.figure(figsize=(12, 6.75))


# ---------------------------------------------------------------- V01 consultations mensuelles
d01 = lire("d01_consultations_mensuelles")
x = pd.to_datetime(d01.annee_mois + "-01")
fig = fig16x9(); ax = fig.add_axes([0.09, 0.14, 0.9, 0.615])
ax.plot(x, d01.consultations, color=BLEU, lw=2.4)
ax.fill_between(x, d01.consultations, color=BLEU_CLAIR, alpha=0.25)
ax.set_ylabel("Consultations par mois"); ax.set_ylim(0, d01.consultations.max() * 1.18)
ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, p: fr(v)))
ax.grid(axis="y", color=GRIS_CLAIR)
pic = d01.loc[d01.consultations.idxmax()]
ax.annotate(f"Pic : {fr(pic.consultations)}\n({pic.annee_mois})", (pd.to_datetime(pic.annee_mois + "-01"), pic.consultations),
            xytext=(40, 0), textcoords="offset points", fontsize=12, color=ORANGE, fontweight="bold",
            arrowprops=dict(arrowstyle="->", color=ORANGE))
ax.annotate("Début partiel\n(à partir du 20 juin 2015)", (x.iloc[0], d01.consultations.iloc[0]), xytext=(20, 60),
            textcoords="offset points", fontsize=11, color=GRIS, arrowprops=dict(arrowstyle="->", color=GRIS))
ax.annotate("Fin au 31 mars 2023", (x.iloc[-1], d01.consultations.iloc[-1]), xytext=(-60, -52),
            textcoords="offset points", fontsize=11, color=GRIS, ha="center", arrowprops=dict(arrowstyle="->", color=GRIS))
ax.axvspan(pd.Timestamp("2022-08-01"), pd.Timestamp("2022-12-31"), color=ORANGE, alpha=0.12)
ax.text(pd.Timestamp("2022-08-10"), d01.consultations.max() * 1.06, "août à déc. 2022 :\nmêmes volumes qu'en 2021",
        fontsize=10, color=ORANGE, va="top", ha="center")
finir(fig, "V01_consultations_mensuelles", "Activité très irrégulière : de 4 700 à 18 800 consultations par mois, sans tendance de fond",
      f"Nombre de consultations par mois, de juin 2015 à mars 2023 ({fr(S1_CONSULTATIONS)} consultations au total)")

# ---------------------------------------------------------------- V02 top 20 etablissements 2019
q01 = lire_archive_q01().sort_values("consultations")
regs = list(dict.fromkeys(q01.region[::-1]))
pal = plt.get_cmap("tab20")
col = {r: pal(i % 20) for i, r in enumerate(regs)}
fig = fig16x9(); ax = fig.add_axes([0.37, 0.12, 0.6, 0.635])
bars = ax.barh(q01.etablissement.str.slice(0, 40), q01.consultations, color=[col[r] for r in q01.region])
for b, v in zip(bars, q01.consultations):
    ax.text(v + 4, b.get_y() + b.get_height() / 2, fr(v), va="center", fontsize=10)
ax.set_xlabel("Consultations en 2019"); ax.tick_params(axis="y", labelsize=9.5); ax.set_xlim(0, q01.consultations.max() * 1.12)
handles = [plt.Rectangle((0, 0), 1, 1, color=col[r]) for r in regs]
ax.legend(handles, regs, fontsize=8.5, loc="lower right", frameon=False, title="Région", title_fontsize=9)
nidf = int((q01.region == "Île-de-France").sum())
finir(fig, "V02_top20_etablissements_2019", f"Aucun établissement ne domine en 2019 : les 20 premiers vont de {int(q01.consultations.min())} à {int(q01.consultations.max())} consultations",
      f"Top 20 des établissements par nombre de consultations rattachées (2019) ; {nidf} des 20 sont en Île-de-France, dont les deux premiers",
      "Source : lakehouse CHU, requête q01 (données complètes, campagne du 30/10/2026). 63,87 % des consultations seulement sont rattachables à un établissement.")

# ---------------------------------------------------------------- V03 consultations par chapitre CIM-10
d04 = lire("d04_consultations_chapitre_cim10")
d04["chapitre"] = d04.lettre_cim10.map(CHAPITRES)
p = d04.pivot_table(index="chapitre", columns="annee", values="consultations", aggfunc="sum").fillna(0)
p["tot"] = p.sum(axis=1); p = p.sort_values("tot")
fig = fig16x9(); ax = fig.add_axes([0.3, 0.12, 0.67, 0.635])
left = np.zeros(len(p)); cols = {2019: BLEU_CLAIR, 2020: BLEU, 2021: TEAL}
for a in (2019, 2020, 2021):
    ax.barh(p.index, p[a], left=left, color=cols[a], label=str(a)); left += p[a].values
for i, v in enumerate(p.tot):
    ax.text(v + 150, i, fr(v), va="center", fontsize=10)
ax.set_xlabel("Consultations 2019 à 2021"); ax.legend(frameon=False, loc="lower right", title="Année")
ax.xaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, pos: fr(v))); ax.set_xlim(0, p.tot.max() * 1.12)
finir(fig, "V03_consultations_chapitre_cim10", "Ostéo-articulaire (17 %) et traumatismes (15 %) font le tiers des consultations, le reste est dispersé",
      "Consultations par chapitre de diagnostic (CIM-10), 2019 à 2021")

# ---------------------------------------------------------------- V04 taux d'hospitalisation
d02 = lire("d02_hospitalisation_annuelle")
fig = plt.figure(figsize=(12, 6.75)); ax = fig.add_axes([0.09, 0.14, 0.4, 0.615]); ax2 = fig.add_axes([0.57, 0.14, 0.4, 0.615])
partielle = d02.annee.isin([2015, 2021])
bars = ax.bar(d02.annee.astype(str), d02.hosp_pour_100_consultations, color=[ORANGE if q else BLEU for q in partielle])
for b, q in zip(bars, partielle):
    if q: b.set_hatch("//"); b.set_edgecolor("white")
for b, v in zip(bars, d02.hosp_pour_100_consultations):
    ax.text(b.get_x() + b.get_width() / 2, v + 0.01, fr(v, 2), ha="center", fontsize=11)
ax.set_ylabel("Hospitalisations pour 100 consultations"); ax.set_ylim(0, 0.95); ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, p: fr(v, 1))); ax.grid(axis="y", color=GRIS_CLAIR)
ax.set_title("Taux (à période égale)", loc="left", fontsize=14)
b2 = ax2.bar(d02.annee.astype(str), d02.hospitalisations, color=[ORANGE if q else TEAL for q in partielle])
for b, q in zip(b2, partielle):
    if q: b.set_hatch("//"); b.set_edgecolor("white")
for b, v in zip(b2, d02.hospitalisations):
    ax2.text(b.get_x() + b.get_width() / 2, v + 6, fr(v), ha="center", fontsize=11)
ax2.set_ylabel("Hospitalisations"); ax2.set_ylim(0, 540); ax2.grid(axis="y", color=GRIS_CLAIR)
ax2.set_title("Volume", loc="left", fontsize=14)
fig.text(0.07, 0.065, "Hachuré orange : année partielle (hospitalisations observées du 21/06/2015 au 19/06/2021 ; consultations comptées sur la même fenêtre).",
         fontsize=10.5, color=ORANGE)
finir(fig, "V04_taux_hospitalisation", "Environ 0,2 à 0,4 hospitalisation pour 100 consultations, sans tendance nette (2015 : début d'observation)",
      "Taux global d'hospitalisation par année, 2015 à 2021 (taux calculé sur la même fenêtre de dates que les hospitalisations)",
      "Source : lakehouse CHU (f_hospitalisation, silver.f_consultation_detail). Données de formation CESI.")

# ---------------------------------------------------------------- V05 hospitalisations par chapitre
d05 = lire("d05_hospitalisations_chapitre_cim10")
d05["chapitre"] = d05.lettre_cim10.map(CHAPITRES)
g = d05.groupby("chapitre").apply(lambda t: pd.Series({"h": t.hospitalisations.sum(),
        "duree": np.average(t.duree_moyenne_jours, weights=t.hospitalisations)}), include_groups=False).sort_values("h")
fig = fig16x9(); ax = fig.add_axes([0.3, 0.12, 0.67, 0.635])
bars = ax.barh(g.index, g.h, color=TEAL)
for b, (idx, r) in zip(bars, g.iterrows()):
    ax.text(r.h + 4, b.get_y() + b.get_height() / 2, f"{fr(r.h)}  (durée moy. {fr(r.duree, 1)} j)", va="center", fontsize=10)
ax.set_xlabel("Hospitalisations 2015 à 2021"); ax.set_xlim(0, g.h.max() * 1.45)
finir(fig, "V05_hospitalisations_chapitre_cim10", "Ostéo-articulaire et traumatismes : 29 % des hospitalisations, avec 15 jours de séjour moyen partout",
      f"Hospitalisations par chapitre CIM-10 et durée moyenne de séjour ({fr(HOSP_TOTAL)} hospitalisations)")

# ---------------------------------------------------------------- V06 hospitalisation sexe et age
d03 = lire("d03_hospitalisation_sexe_age")
fig = plt.figure(figsize=(12, 6.75))
ax0 = fig.add_axes([0.02, 0.12, 0.28, 0.635]); ax1 = fig.add_axes([0.38, 0.14, 0.28, 0.615]); ax2 = fig.add_axes([0.72, 0.14, 0.26, 0.615])
tot = d03.groupby("sexe").hospitalisations.sum()
ax0.pie([tot["F"], tot["M"]], labels=[f"Femmes\n{fr(tot['F'])}\n({fr(100*tot['F']/tot.sum(),1)} %)", f"Hommes\n{fr(tot['M'])}\n({fr(100*tot['M']/tot.sum(),1)} %)"], labeldistance=0.72,
        colors=[ORANGE, BLEU], startangle=90, counterclock=False, wedgeprops=dict(width=0.55, edgecolor="white"), textprops=dict(fontsize=10, color="white", fontweight="bold", ha="center"))
ax0.set_title("Répartition par sexe", fontsize=14)
ages = ["0-14", "15-29", "30-44", "45-59", "60-74", "75+"]; xi = np.arange(len(ages)); w = 0.38
for k, (s, c) in enumerate((("F", ORANGE), ("M", BLEU))):
    t = d03[d03.sexe == s].set_index("tranche_age").loc[ages]
    ax1.bar(xi + (k - 0.5) * w, t.hospitalisations, w, color=c, label="Femmes" if s == "F" else "Hommes")
    ax2.plot(range(6), t.hosp_pour_1000_consultations, marker="o", color=c, lw=2.4)
ax1.set_xticks(xi); ax1.set_xticklabels(ages, fontsize=10, rotation=30); ax1.set_ylabel("Hospitalisations"); ax1.legend(frameon=False); ax1.set_title("Par tranche d'âge", fontsize=14)
ax2.set_xticks(range(6)); ax2.set_xticklabels(ages, fontsize=10, rotation=30); ax2.set_ylabel("Pour 1 000 consultations"); ax2.set_ylim(0, 4.2); ax2.grid(axis="y", color=GRIS_CLAIR); ax2.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, p: fr(v, 1)))
ax2.plot([], [], color=ORANGE, marker="o", label="Femmes"); ax2.plot([], [], color=BLEU, marker="o", label="Hommes"); ax2.legend(frameon=False, loc="lower left", fontsize=10)
ax2.set_title("Taux par tranche d'âge", fontsize=14)
finir(fig, "V06_hospitalisation_sexe_age", "57 % des hospitalisations concernent des femmes, mais le taux par âge reste plat (environ 3 pour 1 000)",
      "Hospitalisations par sexe et tranche d'âge, 2015 à 2021 (âge à la date de l'événement)",
      "Source : lakehouse CHU (Trino, couche Gold). Réserve : 62 hospitalisations (2,5 %) sont datées avant la naissance du patient dans les données sources ; elles sont classées 0-14 ans (237 femmes et 158 hommes sans elles).")

# ---------------------------------------------------------------- V07 distribution par professionnel
d07c = lire("d07c_distribution_consultations_par_professionnel_2020"); d07 = lire("d07_consultations_profession_2020")
fig = fig16x9(); ax = fig.add_axes([0.08, 0.14, 0.89, 0.615])
bars = ax.bar(d07c.consultations_par_professionnel.astype(str), d07c.professionnels, color=BLEU)
for b, v in zip(bars, d07c.professionnels):
    ax.text(b.get_x() + b.get_width() / 2, v + 900, fr(v), ha="center", fontsize=11)
ax.set_xlabel("Consultations dans l'année par professionnel"); ax.set_ylabel("Nombre de professionnels"); ax.set_ylim(0, 90000)
ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, pos: fr(v)))
nb = int(d07.professionnels.iloc[0]); tot = int(d07.consultations.iloc[0])
ax.text(0.97, 0.9, f"{fr(tot)} consultations\nréparties sur {fr(nb)} médecins\nsoit {fr(tot/nb,1)} par médecin", transform=ax.transAxes,
        ha="right", va="top", fontsize=13, color=ORANGE, fontweight="bold")
finir(fig, "V07_activite_par_professionnel_2020", "L'activité n'est concentrée sur aucun médecin : 68 % n'ont vu qu'une seule consultation en 2020",
      "Distribution du nombre de consultations par professionnel, 2020 (le top 10 individuel se joue à 7 à 9 consultations)")

# ---------------------------------------------------------------- cartes
def dessiner_carte(ax, valeurs, cmap, vmin, vmax, etiqueter=True, fmt=lambda v: fr(v)):
    gj = geojson_metropole(); patches, cols, centres = [], [], []
    norm = Normalize(vmin, vmax)
    for f in gj["features"]:
        nom = f["properties"]["nom"]
        geoms = f["geometry"]["coordinates"] if f["geometry"]["type"] == "MultiPolygon" else [f["geometry"]["coordinates"]]
        biggest = max(geoms, key=lambda g: len(g[0]))
        for poly in geoms:
            ring = np.array(poly[0]); ring[:, 1] = ring[:, 1]
            patches.append(Polygon(ring, closed=True)); cols.append(cmap(norm(valeurs.get(nom, np.nan))) if nom in valeurs else "#DDDDDD")
        r = np.array(biggest[0]); centres.append((nom, r[:, 0].mean(), r[:, 1].mean()))
    ax.add_collection(PatchCollection(patches, facecolors=cols, edgecolors="white", linewidths=1.0))
    ax.set_xlim(-5.5, 10); ax.set_ylim(41, 51.5); ax.set_aspect(1 / np.cos(np.radians(46))); ax.axis("off")
    if etiqueter:
        for nom, cx, cy in centres:
            if nom in valeurs:
                ax.text(cx, cy, fmt(valeurs[nom]), ha="center", va="center", fontsize=9.5, fontweight="bold",
                        color="white" if norm(valeurs[nom]) > 0.55 else "#111827")
    return norm


def barre_couleur(fig, cmap, vmin, vmax, rect, titre, fmt=lambda v: fr(v)):
    """Echelle de couleurs sous la carte : sans elle la teinte ne se lit pas (correctif revue : V08 et V09)."""
    cax = fig.add_axes(rect)
    cax.imshow(np.linspace(0, 1, 256)[None, :], aspect="auto", cmap=cmap, extent=[vmin, vmax, 0, 1])
    cax.set_yticks([]); cax.set_xticks([vmin, (vmin + vmax) / 2, vmax]); cax.set_xticklabels([fmt(vmin), fmt((vmin + vmax) / 2), fmt(vmax)], fontsize=10)
    for sp in cax.spines.values(): sp.set_visible(False)
    cax.set_title(titre, fontsize=10.5, color=GRIS, loc="left", pad=3)


q07 = lire_archive_q07()
dep = q07[q07.region.isin(["Île-de-France", "Auvergne-Rhône-Alpes", "Nouvelle-Aquitaine", "Occitanie", "Hauts-de-France", "Grand Est",
        "Provence-Alpes-Côte d'Azur", "Bretagne", "Pays de la Loire", "Normandie", "Bourgogne-Franche-Comté", "Centre-Val de Loire", "Corse"])]
vals = dict(zip(dep.region, dep.deces))
fig = fig16x9(); ax = fig.add_axes([0.02, 0.15, 0.52, 0.61]); axb = fig.add_axes([0.66, 0.14, 0.31, 0.58])
cmap = matplotlib.colors.LinearSegmentedColormap.from_list("bleu", SEQ)
dessiner_carte(ax, vals, cmap, 0, max(vals.values()))
barre_couleur(fig, cmap, 0, max(vals.values()), [0.10, 0.09, 0.34, 0.02], "Décès par région (plus la teinte est foncée, plus il y en a)")
autres = q07[~q07.region.isin(vals.keys())].sort_values("deces")
axb.barh(autres.region, autres.deces, color=GRIS)
for i, v in enumerate(autres.deces): axb.text(v + 80, i, fr(v), va="center", fontsize=10)
axb.set_xlim(0, autres.deces.max() * 1.25); axb.set_title("Outre-mer et lieu inconnu", fontsize=13)
tot7 = int(q07.deces.sum())
finir(fig, "V08_carte_deces_2019", f"{fr(tot7)} décès en 2019 : l'Île-de-France et Auvergne-Rhône-Alpes en comptent 24 %",
      "Nombre de décès par région de lieu de décès, 2019 (valeurs brutes, non rapportées à la population)",
      "Source : lakehouse CHU, requête q07 (campagne plateforme sur données complètes du 30/10/2026), fichier INSEE des décès.")

d08 = lire("d08_satisfaction_region"); s20 = d08[d08.annee == 2020]
met = s20[~s20.region.isin(["INCONNUE", "Martinique", "Guadeloupe", "Guyane", "La Réunion", "Mayotte"])]
vals = dict(zip(met.region, met.score_pondere))
fig = fig16x9(); ax = fig.add_axes([0.02, 0.15, 0.45, 0.61]); axb = fig.add_axes([0.6, 0.18, 0.38, 0.575])
cm2 = matplotlib.colors.LinearSegmentedColormap.from_list("sat", ["#F6D7B0", "#E8A15A", TEAL, "#0F6B60"])
dessiner_carte(ax, vals, cm2, 72, 75, fmt=lambda v: fr(v, 1))
barre_couleur(fig, cm2, 72, 75, [0.08, 0.09, 0.32, 0.02], "Score de satisfaction (échelle limitée à 72 à 75)", fmt=lambda v: fr(v, 1))
a = d08.pivot_table(index="region", columns="annee", values="score_pondere")
a = a.loc[~a.index.isin(["INCONNUE"])].sort_values(2020)
yi = np.arange(len(a)); axb.barh(yi - 0.2, a[2019], 0.4, color=BLEU_CLAIR, label="2019"); axb.barh(yi + 0.2, a[2020], 0.4, color=TEAL, label="2020")
axb.set_yticks(yi); axb.set_yticklabels(a.index, fontsize=9); axb.set_xlim(60, 77); axb.set_xlabel("Score de satisfaction (0 à 100)")
axb.legend(frameon=False, loc="lower right")
finir(fig, "V09_carte_satisfaction_2020", "Satisfaction : de 72,6 à 75,0 en métropole en 2020, des écarts faibles entre régions",
      "Score ESATIS 48h MCO pondéré par les répondants, par région (carte 2020, comparaison 2019 et 2020 à droite)",
      "Source : lakehouse CHU (gold.f_satisfaction). Un score est renseigné sur 72,8 % (2020) à 74,5 % (2019) des lignes sources. L'axe de droite démarre à 60 (pas à 0).")

# ---------------------------------------------------------------- V10 schema en etoile
def boite(ax, x, y, w, h, titre, lignes, fond, texte="white", fs=9.5, step=0.30):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.12", fc=fond, ec="white", lw=2))
    ax.text(x + w / 2, y + h - 0.22, titre, ha="center", va="center", fontsize=12.5, fontweight="bold", color=texte)
    for i, l in enumerate(lignes):
        ax.text(x + 0.14, y + h - 0.58 - i * step, l, ha="left", va="center", fontsize=fs, color=texte)


fig = plt.figure(figsize=(12, 6.75)); ax = fig.add_axes([0, 0.04, 1, 0.72]); ax.set_xlim(0, 12); ax.set_ylim(0, 7.2); ax.axis("off")
boite(ax, 0.3, 5.3, 2.5, 1.75, "d_temps", ["id_temps (PK, AAAAMMJJ)", "date_jour, annee_mois", "mois, trimestre, annee"], BLEU)
boite(ax, 3.1, 5.3, 2.5, 1.75, "d_patient", ["id_patient (PK)", "sexe, tranche_age_ref", "date_naissance, ville"], BLEU)
boite(ax, 5.9, 5.3, 2.8, 1.75, "d_etablissement", ["id_etablissement (PK)", "raison_sociale_site", "id_localisation (FK), region"], BLEU)
boite(ax, 9.0, 5.3, 2.7, 1.75, "d_localisation", ["id_localisation (PK)", "nom_departement", "region, code_region"], BLEU)
boite(ax, 0.3, 2.45, 4.2, 2.0, "f_mesures (fait consolidé)", ["id_temps, id_patient (FK)", "id_etablissement, id_localisation (FK)", "annee_mois, sexe, tranche_age, region",
      "nbre_consultation, nbre_hospitalisation", "total_satisfaction, nbre_deces"], ORANGE)
boite(ax, 4.8, 2.6, 3.4, 1.55, "f_hospitalisation", ["id_hospitalisation, id_temps", "id_patient, id_diagnostic (FK)", "id_etablissement, duree_jours"], TEAL)
boite(ax, 8.5, 2.6, 3.3, 1.55, "f_consultation_detail", ["id_consultation, id_temps", "id_patient, id_diagnostic (FK)", "id_professionnel (FK), id_etablissement"], TEAL)
boite(ax, 0.3, 0.2, 4.2, 1.45, "f_deces_agg et f_satisfaction", ["annee, sexe, tranche_age, region", "nbre_deces, score_satisfaction"], TEAL)
boite(ax, 4.8, 0.2, 3.0, 1.45, "d_diagnostic", ["id_diagnostic (PK)", "code_diag, libelle_diagnostic"], BLEU)
boite(ax, 8.5, 0.2, 3.3, 1.45, "d_professionnel", ["identifiant (PK)", "profession, code_specialite"], BLEU)
def fl(x1, y1, x2, y2):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, color="#111827", lw=1.5))
for xb, xd in [(0.8, 1.2), (1.8, 3.6), (2.8, 6.8), (3.8, 10.0)]:
    fl(xb, 4.5, xd, 5.25)
fl(8.7, 6.2, 9.0, 6.2)
fl(6.5, 2.55, 6.3, 1.7); fl(10.1, 2.55, 10.1, 1.7); fl(9.0, 2.55, 7.3, 1.7)
ax.text(0.3, 2.0, "Les faits de détail utilisent les mêmes dimensions (temps, patient,\nétablissement, localisation) que f_mesures ; f_deces_agg se relie à d_temps\net d_localisation, f_satisfaction à la région (code ISO ajouté dans Superset).", fontsize=9, color=GRIS, ha="left", va="center")
finir(fig, "V10_schema_en_etoile", "Schéma en étoile du lakehouse CHU : f_mesures au centre, des faits de détail et 6 dimensions",
      "Vue simplifiée du modèle reproduit dans Superset (jeux de données physiques et jointures de l'étoile, détail dans le guide) ; flèche = clé étrangère vers la clé primaire",
      "Source : DDL 01_schema_gold_hive.sql du Livrable 2. PK = clé primaire, FK = clé étrangère.")

# ---------------------------------------------------------------- V11 fiabilite des donnees
fk = pd.read_csv(ARCH / "dq_integrite_fk.csv", sep=";"); comp = pd.read_csv(ARCH / "dq_completude.csv", sep=";"); ref = pd.read_csv(ARCH / "dq_referentiel.csv", sep=";")
def pct(df, col, key): return float(df[df[col].str.contains(key, regex=False)].iloc[:, -1].iloc[0])
lignes = [
    ("Hospitalisations rattachées à un établissement", pct(fk, "controle", "f_hospitalisation.id_etablissement")),
    ("Consultations avec diagnostic valide", pct(fk, "controle", "f_consultation_detail.id_diagnostic")),
    ("Décès avec département de lieu identifié", pct(ref, "controle", "deces.code_lieu_deces")),
    ("Consultations avec professionnel connu de l'activité", pct(ref, "controle", "S1.Consultation.Id_prof_sante -> activite.identifiant")),
    ("Consultations rattachées à un établissement", pct(fk, "controle", "f_consultation_detail.id_etablissement")),
    ("Lignes de satisfaction avec un score (2019)", 74.48),
    ("Lignes de satisfaction avec un score (2020)", 72.79),
]
lignes = sorted(lignes, key=lambda t: t[1])
fig = fig16x9(); ax = fig.add_axes([0.40, 0.19, 0.57, 0.56])
bars = ax.barh([l[0] for l in lignes], [l[1] for l in lignes], color=[ROUGE if v < 75 else (ORANGE if v < 95 else TEAL) for _, v in lignes])
for b, (_, v) in zip(bars, lignes): ax.text(v + 1, b.get_y() + b.get_height() / 2, fr(v, 2) + " %", va="center", fontsize=11)
ax.set_xlim(0, 112); ax.set_xlabel("Part des enregistrements exploitables (%)")
finir(fig, "V11_fiabilite_des_donnees", "Ce que l'on peut affirmer : les chiffres par établissement ne couvrent que 64 % des consultations",
      "Taux de rattachement et de complétude relevés par les contrôles qualité (données complètes)",
      "Source : contrôles qualité du 30/10/2026 (dq_integrite_fk, dq_referentiel, dq_completude). Vert au-dessus de 95 %, orange de 75 à 95 %, rouge en dessous.")

# ---------------------------------------------------------------- V12 benchmark
b = pd.read_csv(ARCH / "bench_synthese.csv", sep=";")
fig = plt.figure(figsize=(12, 6.75)); a1 = fig.add_axes([0.06, 0.16, 0.42, 0.57]); a2 = fig.add_axes([0.58, 0.16, 0.40, 0.57])
xi = np.arange(len(b)); w = 0.38
a1.bar(xi - w / 2, b.avant_octets / 1e6, w, color=GRIS, label="Avant (tables plates)"); a1.bar(xi + w / 2, b.apres_octets / 1e6, w, color=TEAL, label="Après (partitionné)")
a1.set_xticks(xi); a1.set_xticklabels([f"{q}\nx{fr(r,1)}" for q, r in zip(b.requete, b.ratio_volume_lu)], fontsize=9); a1.set_ylabel("Volume lu (Mo)"); a1.legend(frameon=False, fontsize=10)
for lab, r in zip(a1.get_xticklabels(), b.ratio_volume_lu):
    lab.set_color(TEAL if r > 1 else ROUGE)  # facteur sous le nom de la requete : x supérieur à 1 = moins de volume lu
a1.set_title("Volume de données lu", fontsize=14)
w3 = 0.27
a2.bar(xi - w3, b.avant_ms_median, w3, color=GRIS, label="Avant")
a2.bar(xi, b.apres_ms_median, w3, color=TEAL, label="Après")
a2.bar(xi + w3, b.apres_ml_ms_median, w3, color=ORANGE, label="Après + pruning metastore")
a2.set_xticks(xi); a2.set_xticklabels(b.requete, fontsize=9.5); a2.set_ylabel("Temps médian (ms)"); a2.legend(frameon=False, fontsize=9.5)
a2.set_title("Temps de réponse", fontsize=14)
finir(fig, "V12_benchmark_partitionnement", "Le partitionnement lit moins de données (8 requêtes sur 10) mais ne réduit le temps que sur 3 requêtes",
      "Benchmark avant et après partitionnement, 10 requêtes, médiane de 5 répétitions (DuckDB). Le gain en temps apparaît avec le pruning façon metastore sur q01, q02 et q07.",
      "Source : benchmark du Livrable 2 (bench_synthese.csv). Facteur x sous chaque requête = volume avant / volume après ; en rouge, le volume lu augmente (q04, q08).")

# ---------------------------------------------------------------- V13 architecture
fig = plt.figure(figsize=(12, 6.75)); ax = fig.add_axes([0.0, 0.03, 1.0, 0.725]); ax.set_xlim(0, 12); ax.set_ylim(0.6, 5.2); ax.axis("off")
etapes = [(0.2, "Sources", ["PostgreSQL S1", "CSV FINESS", "Satisfaction", "Décès INSEE"], GRIS),
          (2.5, "Data lake", ["MinIO (S3)", "raw / silver / gold", "Parquet partitionné"], BLEU),
          (4.8, "Traitement", ["Apache Spark", "Airflow (2 DAG)", "ETL vers Gold"], TEAL),
          (7.1, "Catalogue et SQL", ["Hive Metastore", "HiveServer2 (10000)", "Trino (8080)"], ORANGE),
          (9.4, "Restitution", ["Apache Superset", "Jeux de données étoile", "Tableaux de bord", "Filtres et liens"], "#7B3FA0")]
for x0, t, ls, c in etapes:
    boite(ax, x0 + 0.05, 2.4, 1.95, 2.2, t, ls, c, fs=10, step=0.42)
for i in range(4):
    ax.add_patch(FancyArrowPatch((etapes[i][0] + 2.05, 3.5), (etapes[i + 1][0] + 0.02, 3.5), arrowstyle="-|>", mutation_scale=18, color="#111827", lw=2))
ax.text(6, 1.6, "Ingestion  >  Stockage  >  Transformation  >  Catalogue  >  Visualisation et storytelling", ha="center", fontsize=13, color="#111827")
ax.text(6, 0.9, "Superset lit les trois couches (raw, silver, gold) directement par Trino : aucune copie des données", ha="center", fontsize=11, color=GRIS)
finir(fig, "V13_architecture_lakehouse", "Architecture : du fichier brut au tableau de bord, en cinq étages",
      "Lakehouse en trois couches (raw, silver, gold) exposé en SQL par Trino et restitué dans Superset",
      "Source : docker-compose.yml du Livrable 2 (17 services).")

# ---------------------------------------------------------------- V14 diagnostics les plus frequents
d06 = lire("d06_top_diagnostics").sort_values("valeur")
tot_cons = int(lire("d04_consultations_chapitre_cim10").consultations.sum())
def court(t, n=58): return t if len(t) <= n else t[:n - 1].rstrip() + "…"
fig = fig16x9(); ax = fig.add_axes([0.46, 0.14, 0.51, 0.60])
bars = ax.barh([f"{c}  {court(l)}" for c, l in zip(d06.code_diag, d06.libelle_diagnostic)], d06.valeur, color=BLEU)
for b, v in zip(bars, d06.valeur):
    ax.text(v + 0.8, b.get_y() + b.get_height() / 2, f"{int(v)}  ({fr(100*v/tot_cons,3)} %)", va="center", fontsize=10)
ax.set_xlim(0, 100); ax.set_xlabel("Consultations 2019 à 2021"); ax.tick_params(axis="y", labelsize=9)
finir(fig, "V14_top10_diagnostics", "Aucun diagnostic ne se détache : le premier ne pèse que 0,02 % des consultations",
      f"Les 10 diagnostics les plus fréquents en consultation, 2019 à 2021 ({fr(tot_cons)} consultations)",
      "Source : lakehouse CHU (silver.f_consultation_detail, gold.d_diagnostic). Les libellés viennent du référentiel CIM-10 du lac, tronqués à l'affichage.")
