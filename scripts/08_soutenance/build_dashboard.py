"""Tableau de bord HTML responsive (Plotly) : meme histoire que les visuels PNG, lisible sur mobile."""
import plotly.graph_objects as go
from plotly.offline import get_plotlyjs
from _common import *

verifier_reconciliation()
CFG = {"displayModeBar": False, "responsive": True}
LAY = dict(font=dict(family="Segoe UI, Arial, sans-serif", size=13, color="#111827"), paper_bgcolor="white",
           plot_bgcolor="white", margin=dict(l=10, r=44, t=10, b=10), hoverlabel=dict(font_size=14))
AXE = dict(gridcolor=GRIS_CLAIR, zeroline=False)
figs = {}
import textwrap
def w(t, n=20): return '<br>'.join(textwrap.wrap(t, n))

def put(key, fig, h=340):
    fig.update_layout(**LAY, height=h)
    figs[key] = fig.to_html(full_html=False, include_plotlyjs=False, config=CFG, div_id="g_" + key)

# 1 consultations mensuelles
d01 = lire("d01_consultations_mensuelles")
f = go.Figure(go.Scatter(x=d01.annee_mois, y=d01.consultations, mode="lines", line=dict(color=BLEU, width=2.5), fill="tozeroy",
                         fillcolor="rgba(95,168,211,0.2)", hovertemplate="%{x}<br>%{y:,.0f} consultations<extra></extra>"))
f.update_xaxes(type="category", tickmode="array", tickvals=[m for m in d01.annee_mois if m.endswith("-01")][::1], ticktext=[m[:4] if m.endswith("-01") else "" for m in d01.annee_mois if m.endswith("-01")], tickangle=0, **AXE).update_yaxes(title="Consultations par mois", **AXE)
put("conso", f)
# 2 taux hospitalisation
d02 = lire("d02_hospitalisation_annuelle")
part = d02.annee.isin([2015, 2021])
f = go.Figure(go.Bar(x=d02.annee.astype(str), y=d02.hosp_pour_100_consultations, marker_color=[ORANGE if p else BLEU for p in part],
                     text=[fr(v, 2) for v in d02.hosp_pour_100_consultations], textposition="outside", cliponaxis=False,
                     hovertemplate="%{x}<br>%{y:.2f} hospitalisations pour 100 consultations<extra></extra>"))
f.update_yaxes(title="Pour 100 consultations", range=[0, 0.95], **AXE)
put("taux", f)
# 3 chapitres consultations
d04 = lire("d04_consultations_chapitre_cim10"); d04["chapitre"] = d04.lettre_cim10.map(CHAPITRES)
g = d04.groupby("chapitre").consultations.sum().sort_values().tail(10)
f = go.Figure(go.Bar(y=[w(x) for x in g.index], x=g.values, orientation="h", marker_color=BLEU, text=[fr(v) for v in g.values], textposition="outside", cliponaxis=False,
                     hovertemplate="%{y}<br>%{x:,.0f} consultations<extra></extra>"))
f.update_xaxes(range=[0, g.max() * 1.4], **AXE); f.update_yaxes(automargin=True)
put("chap", f, 420)
# 4 chapitres hospitalisations
d05 = lire("d05_hospitalisations_chapitre_cim10"); d05["chapitre"] = d05.lettre_cim10.map(CHAPITRES)
g = d05.groupby("chapitre").hospitalisations.sum().sort_values().tail(10)
f = go.Figure(go.Bar(y=[w(x) for x in g.index], x=g.values, orientation="h", marker_color=TEAL, text=[fr(v) for v in g.values], textposition="outside", cliponaxis=False,
                     hovertemplate="%{y}<br>%{x:,.0f} hospitalisations<extra></extra>"))
f.update_xaxes(range=[0, g.max() * 1.4], **AXE); f.update_yaxes(automargin=True)
put("hchap", f, 420)
# 5 sexe et age
d03 = lire("d03_hospitalisation_sexe_age"); ages = ["0-14", "15-29", "30-44", "45-59", "60-74", "75+"]
f = go.Figure()
for s, c, n in (("F", ORANGE, "Femmes"), ("M", BLEU, "Hommes")):
    t = d03[d03.sexe == s].set_index("tranche_age").loc[ages]
    f.add_bar(x=ages, y=t.hospitalisations, name=n, marker_color=c, hovertemplate="%{x}<br>%{y} hospitalisations<extra>" + n + "</extra>")
f.update_layout(barmode="group", legend=dict(orientation="h", y=1.12)); f.update_yaxes(title="Hospitalisations", **AXE)
put("sexage", f)
# 6 professionnels
d07c = lire("d07c_distribution_consultations_par_professionnel_2020")
f = go.Figure(go.Bar(x=d07c.consultations_par_professionnel.astype(str), y=d07c.professionnels, marker_color=BLEU,
                     text=[fr(v) for v in d07c.professionnels], textposition="outside", cliponaxis=False,
                     hovertemplate="%{x} consultation(s)<br>%{y:,.0f} professionnels<extra></extra>"))
f.update_xaxes(title="Consultations dans l'année par professionnel"); f.update_yaxes(range=[0, 95000], **AXE)
put("prof", f)
# cartes
gj = geojson_metropole()
q07 = lire_archive_q07(); metro = q07[q07.region.isin([x["properties"]["nom"] for x in gj["features"]])]
f = go.Figure(go.Choropleth(geojson=gj, locations=metro.region, z=metro.deces, featureidkey="properties.nom",
        colorscale=[[0, SEQ[0]], [1, SEQ[-1]]], marker_line_color="white", colorbar=dict(title="Décès", orientation="h", thickness=10, len=0.8, y=-0.02, x=0.5),
        hovertemplate="%{location}<br>%{z:,.0f} décès<extra></extra>"))
f.update_geos(visible=False, projection_type="mercator", lonaxis_range=[-5.5, 9.8], lataxis_range=[41.2, 51.3], domain=dict(x=[0, 1], y=[0.1, 1]))
put("cartedeces", f, 400)
d08 = lire("d08_satisfaction_region"); s20 = d08[(d08.annee == 2020) & d08.region.isin([x["properties"]["nom"] for x in gj["features"]])]
f = go.Figure(go.Choropleth(geojson=gj, locations=s20.region, z=s20.score_pondere, featureidkey="properties.nom",
        colorscale=[[0, "#F6D7B0"], [0.5, TEAL], [1, "#0F6B60"]], zmin=72, zmax=75, marker_line_color="white",
        colorbar=dict(title="Score", orientation="h", thickness=10, len=0.8, y=-0.02, x=0.5), hovertemplate="%{location}<br>score %{z:.1f}<extra></extra>"))
f.update_geos(visible=False, projection_type="mercator", lonaxis_range=[-5.5, 9.8], lataxis_range=[41.2, 51.3], domain=dict(x=[0, 1], y=[0.1, 1]))
put("cartesat", f, 400)
# fiabilite
fk = pd.read_csv(ARCH / "dq_integrite_fk.csv", sep=";"); ref = pd.read_csv(ARCH / "dq_referentiel.csv", sep=";")
def pct(df, key): return float(df[df.controle.str.contains(key, regex=False)].iloc[:, -1].iloc[0])
L = sorted([("Consultations rattachées à un établissement", pct(fk, "f_consultation_detail.id_etablissement")),
            ("Consultations avec professionnel connu", pct(ref, "S1.Consultation.Id_prof_sante -> activite.identifiant")),
            ("Satisfaction : lignes avec un score (2020)", 72.79), ("Satisfaction : lignes avec un score (2019)", 74.48),
            ("Décès avec département identifié", pct(ref, "deces.code_lieu_deces")),
            ("Hospitalisations rattachées à un établissement", pct(fk, "f_hospitalisation.id_etablissement"))], key=lambda t: t[1])
f = go.Figure(go.Bar(y=[w(x[0], 24) for x in L], x=[x[1] for x in L], orientation="h",
        marker_color=[ROUGE if v < 75 else (ORANGE if v < 95 else TEAL) for _, v in L], text=[fr(v, 1) + " %" for _, v in L], textposition="outside", cliponaxis=False))
f.update_xaxes(range=[0, 125], tickvals=[0, 25, 50, 75, 100], **AXE); f.update_yaxes(automargin=True)
put("fiab", f, 420)

nb_cons = fr(S1_CONSULTATIONS)
KPI = [(nb_cons, "consultations"), (fr(HOSP_TOTAL), "hospitalisations"), (fr(S1_PATIENTS), "patients"), (fr(int(lire_archive_q07().deces.sum())), "décès en 2019")]
kpi_html = "".join(f'<div class="kpi"><b>{a.replace(" ", "&nbsp;")}</b><span>{b}</span></div>' for a, b in KPI)

def carte(id_, titre, msg, key, note=""):
    return f'<section id="{id_}"><h2>{titre}</h2><p class="msg">{msg}</p><div class="card">{figs[key]}</div>' + (f'<p class="note">{note}</p>' if note else "") + "</section>"

html = f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Cloud Healthcare Unit : activité, hospitalisations, territoires</title>
<style>
*{{box-sizing:border-box}} body{{margin:0;font-family:"Segoe UI",Arial,sans-serif;background:#F3F4F6;color:#111827;line-height:1.45}}
header{{background:{BLEU};color:white;padding:20px 16px 14px}} header h1{{margin:0 0 4px;font-size:1.5rem}} header p{{margin:0;opacity:.9}}
nav{{position:sticky;top:0;z-index:5;background:white;border-bottom:1px solid {GRIS_CLAIR};display:flex;overflow-x:auto;gap:4px;padding:6px 10px;white-space:nowrap}}
nav a{{padding:8px 14px;border-radius:18px;color:{BLEU};text-decoration:none;font-weight:600;font-size:.95rem}} nav a:hover,nav a:focus{{background:#E8F1FA}}
main{{max-width:1180px;margin:0 auto;padding:12px}} section{{margin:18px 0 30px}} h2{{margin:0 0 4px;font-size:1.25rem}}
.msg{{margin:0 0 10px;font-size:1.05rem;font-weight:600}} .note{{margin:6px 2px 0;color:{GRIS};font-size:.88rem}}
.card{{background:white;border-radius:12px;padding:10px;box-shadow:0 1px 3px rgba(0,0,0,.08)}}
.grid{{display:grid;grid-template-columns:1fr 1fr;gap:14px}} .kpis{{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-top:14px}}
.kpi{{background:rgba(255,255,255,.14);border-radius:10px;padding:10px;text-align:center}} .kpi b{{display:block;font-size:1.5rem}} .kpi span{{font-size:.85rem}}
footer{{padding:16px;color:{GRIS};font-size:.85rem;text-align:center}}
@media (max-width:760px){{.grid{{grid-template-columns:1fr}} .kpis{{grid-template-columns:1fr 1fr}} header h1{{font-size:1.25rem}} .msg{{font-size:1rem}} .kpi b{{font-size:1.25rem}}}}
</style></head><body>
<header><h1>Cloud Healthcare Unit : ce que disent les données du lakehouse</h1>
<p>Consultations, hospitalisations, décès et satisfaction, de 2015 à 2023</p><div class="kpis">{kpi_html}</div></header>
<nav><a href="#activite">1. Activité</a><a href="#hospi">2. Hospitalisations</a><a href="#territoires">3. Territoires</a><a href="#fiabilite">4. Fiabilité</a></nav>
<main>
{carte("activite", "1. L'activité de consultation", "Une activité très irrégulière, de 4 700 à 18 800 consultations par mois, sans tendance de fond.", "conso",
 "Mois de juin 2015 et de mars 2023 incomplets. D'août à décembre 2022, les volumes reproduisent ceux de 2021 (artefact des données sources).")}
<section><h2>Les motifs de consultation</h2><p class="msg">L'ostéo-articulaire (17 %) et les traumatismes (15 %) font le tiers des consultations, le reste est dispersé.</p><div class="grid"><div class="card">{figs["chap"]}</div><div class="card">{figs["prof"]}</div></div>
<p class="note">Second graphique (distribution des consultations par médecin) : 68 % des médecins n'ont vu qu'une seule consultation en 2020 (116 705 médecins pour 162 778 consultations). Aucun médecin ne concentre l'activité.</p></section>
{carte("hospi", "2. Les hospitalisations", "Environ 0,2 à 0,4 hospitalisation pour 100 consultations, sans tendance nette.", "taux",
 "Barres orange : années partielles (2015, 0,76 : début d'observation ; 2021 : jusqu'au 19 juin), taux calculé sur la même fenêtre de dates que les hospitalisations. Hors 2015, le taux va de 0,21 à 0,43.")}
<section><h2>Qui est hospitalisé, pourquoi ?</h2><p class="msg">57 % de femmes, des séjours de 15 jours en moyenne, et l'ostéo-articulaire en tête.</p><div class="grid"><div class="card">{figs["sexage"]}</div><div class="card">{figs["hchap"]}</div></div>
<p class="note">Réserve : 62 hospitalisations (2,5 %) sont datées avant la naissance du patient dans les données sources ; elles sont classées 0-14 ans.</p></section>
<section id="territoires"><h2>3. Les territoires</h2><p class="msg">L'Île-de-France et Auvergne-Rhône-Alpes concentrent 24 % des décès 2019 ; la satisfaction varie peu (72,6 à 75,0).</p>
<div class="grid"><div class="card"><h3 style="margin:2px 6px">Décès 2019 par région (valeurs brutes)</h3>{figs["cartedeces"]}</div><div class="card"><h3 style="margin:2px 6px">Satisfaction 2020 (score pondéré)</h3>{figs["cartesat"]}</div></div>
<p class="note">Les décès ne sont pas rapportés à la population : la carte reflète d'abord la taille des régions.</p></section>
{carte("fiabilite", "4. Ce que l'on peut affirmer", "Les chiffres par établissement ne couvrent que 64 % des consultations.", "fiab",
 "Part des enregistrements exploitables relevée par les contrôles qualité sur les données complètes.")}
</main><footer>Source : lakehouse CHU (Trino, couches silver et gold) ; décès 2019 et fiabilité : campagne du 30/10/2026 sur données complètes. Données de formation CESI.</footer>
<script>{get_plotlyjs()}</script>
<script>window.addEventListener('load',function(){{document.querySelectorAll('.js-plotly-plot').forEach(function(g){{Plotly.Plots.resize(g)}})}});</script>
</body></html>"""
# les graphiques sont initialises par des scripts inline : plotly.js doit etre charge avant
head, tail = html.split("<script>" + get_plotlyjs() + "</script>")
js = "<script>" + get_plotlyjs() + "</script>"
html = head.replace("</style></head><body>", "</style>" + js + "</head><body>") + tail
(BASE / "visuels" / "dashboard_chu.html").write_text(html, encoding="utf-8")
print("OK dashboard", len(html) // 1024, "Ko")
