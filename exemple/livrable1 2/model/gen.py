# -*- coding: utf-8 -*-
import os, re
import sys; sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
from model import T, FACTS, DIMS
import model as m
import pathlib
HERE = pathlib.Path(__file__).resolve().parent
D = str(HERE.parent / "diagrams"); G = str(HERE.parent / "generated")
os.makedirs(G, exist_ok=True)

TECH = ("id_lot","dt_chargement")
SCD2N = ("date_debut_validite","date_fin_validite","est_version_courante")
def puml_entity(t, mode="full", maxattr=None):
    v = T[t]; out = [f'entity "{t}" as {t} <<{v["kind"]}>> {{']
    rest = 0; shown = 0
    for (n,ty,nul,key,ref,desc) in v["cols"]:
        if n in TECH: continue
        if mode == "keys" and not key and v["kind"]=="dim": continue
        if mode == "keys" and v["kind"]=="fait" and not key and not n.startswith(("nb_","duree","valeur","effectif","age_","ic_")): continue
        if maxattr is not None and not key and shown >= maxattr:
            rest += 1; continue
        ty2 = ty.replace("TIMESTAMP(6) WITH TIME ZONE","TIMESTAMPTZ")
        nm = n
        if "FK" in key: nm = "#" + nm
        if "PK" in key: nm = f"<u>{nm}</u>"
        if key=="UK": nm = f"{nm} <size:11><color:#2F6DB5>(UK)</color></size>"
        if n in SCD2N:
            if n == "date_debut_validite":
                out.append("  {field} <i>triplet SCD 2 (début, fin, courante)</i>")
            continue
        if not key: shown += 1
        out.append(f"  {{field}} {nm} : {ty2}" if mode!="keys" or v["kind"]=="fait" else f"  {{field}} {nm}")
    if rest: out.append(f"  {{field}} <i>… {rest} autres attributs (dictionnaire)</i>")
    out.append("}")
    return "\n".join(out)

# Étoile détaillée : processus Consultation (les autres étoiles sont lues dans la constellation et le dictionnaire)
def brief(t, maxattr=3):
    v=T[t]; out=[f'entity "{t}" as {t} <<dim>> {{']; shown=0; rest=0
    for (n,ty,nul,key,ref,desc) in v["cols"]:
        if n in TECH or n in SCD2N: continue
        if "PK" in key: out.append(f"  {{field}} <u>{n}</u>"); continue
        if shown < maxattr: out.append(f"  {{field}} {n}{' (UK)' if key=='UK' else ''}"); shown+=1
        else: rest+=1
    if any(c[0]=="date_debut_validite" for c in v["cols"]): out.append("  {field} <i>+ triplet SCD 2</i>")
    if rest: out.append(f"  {{field}} <i>+ {rest} attribut{'s' if rest>1 else ''}</i>")
    out.append("}"); return "\n".join(out)
L = ["@startuml","!include style_data.iuml","skinparam linetype ortho","skinparam nodesep 14","skinparam ranksep 36",
     "title Étoile détaillée du processus Consultation",""]
L.append(puml_entity("fait_consultation"))
place = [("dim_date","up"),("dim_patient","up"),("dim_professionnel","up"),("dim_etablissement","left"),("dim_acte","left"),
         ("dim_mutuelle","right"),("dim_diagnostic","right"),("dim_sexe","down"),("dim_tranche_age","down")]
for d,_ in place: L.append(brief(d))
for d,dirn in place: L.append(f'fait_consultation "1,1" -{dirn}-> "0,n" {d}')
L += ['legend bottom','  Grain : une consultation. <u>souligné</u> : clé primaire ; # : clé étrangère ; (UK) : unicité',
      '  (1,1) côté fait, (0,n) côté dimension ; types et attributs complets : dictionnaire de données','endlegend',"@enduml"]
open(f"{D}/06_etoile_consultation.puml","w").write("\n".join(L))

# Constellation : faits (clés + mesures + dimensions dégénérées), dimensions (noms)
# Disposition : consultation et hospitalisation à gauche, dimensions conformes au centre,
# décès et qualité à droite ; dimensions propres à côté de leur fait.
EXTRA = {"fait_deces": ("annee_deces",), "fait_indicateur_qualite": ("classe","est_diffusable")}
def fact_box(t):
    box = puml_entity(t, mode="keys").split("\n")
    for n,ty,nul,key,ref,desc in T[t]["cols"]:
        if n in EXTRA.get(t,()):
            box.insert(-1, f"  {{field}} {n} : {ty}")
    return "\n".join(box)
CONF = ["dim_date","dim_patient","dim_etablissement","dim_diagnostic","dim_sexe","dim_tranche_age","dim_geographie"]
LEFT = ["fait_consultation","fait_hospitalisation"]; RIGHT = ["fait_deces","fait_indicateur_qualite"]
L=["@startuml","!include style_data.iuml","left to right direction","skinparam linetype ortho","skinparam ranksep 70","skinparam nodesep 14",
   "title Constellation Gold : 4 faits, 12 dimensions (dont 7 conformes)",""]
L.append("together {"); [L.append(fact_box(t)) for t in LEFT]; L.append("}")
L.append('package "dimensions conformes" as CONFPKG <<Rectangle>> {')
for t in CONF: L.append(f'entity "{t}" as {t} <<dim>>')
L.append("}")
for t in DIMS:
    if t not in CONF: L.append(f'entity "{t}" as {t} <<dim>>')
L.append("together {"); [L.append(fact_box(t)) for t in RIGHT]; L.append("}")
rels=[]
for t,v in T.items():
    for (n,ty,nul,key,ref,desc) in v["cols"]:
        if "FK" in key: rels.append((t,ref,n))
for a,b,n in rels:
    lab = f" : {n.replace('_sk','')}" if (a=="fait_hospitalisation" and b=="dim_date") else ""
    if a in RIGHT:
        L.append(f"{b} <-- {a}{lab}")      # fait à droite de la dimension
    else:
        L.append(f"{a} --> {b}{lab}")
L += ['legend bottom','  <color:#B8901A><b>■</b></color> faits (clés, mesures, dimension dégénérée soulignée)   <color:#2F6DB5><b>■</b></color> dimensions ; cadre : dimensions conformes',
      '  Chaque lien : (1,1) côté fait, (0,n) côté dimension ; flèche : référençante → référencée',
      '  fait_hospitalisation : dim_date à double rôle (date_entree, date_sortie)','endlegend',"@enduml"]
open(f"{D}/05_constellation.puml","w").write("\n".join(L))

# ---------------- LaTeX ----------------
def tex(s):
    s = s.replace("\\","\\textbackslash{}")
    for a,b in [("&","\\&"),("%","\\%"),("$","\\$"),("#","\\#"),("_","\\_"),("{","\\{"),("}","\\}"),("~","\\textasciitilde{}"),("^","\\^{}")]:
        s = s.replace(a,b)
    s = s.replace("≥","$\\geq$").replace("×","$\\times$").replace("→","$\\rightarrow$").replace("≈","$\\approx$").replace("·","$\\cdot$")
    s = re.sub(r"10\\\^\\\{\\\}(\d+)", r"10\\textsuperscript{\1}", s)
    return s
def short(s):
    # Version compacte pour l'annexe : première proposition de la description (règle complète dans model.py et au ch. 5).
    s = s.strip()
    cut = len(s)
    for sep in (" ; ", ". ", " ("):
        i = s.find(sep)
        if 12 < i < cut: cut = i
    return s[:cut].rstrip(" .;:")
def ttt(s): return "\\code{"+tex(s)+"}"

def keyfmt(key, ref):
    k = key.replace("PK,FK","PK, FK")
    if ref: k += " $\\rightarrow$ " + ttt(ref)
    return k

def common(v):
    names=[c[0] for c in v["cols"]]
    parts=[]
    if "date_debut_validite" in names: parts.append("triplet SCD~2")
    if "id_lot" in names: parts.append("\\code{id\\_lot}, \\code{dt\\_chargement}")
    return (" \\textbf{Colonnes communes} : " + " ; ".join(parts) + " (tableau~\\ref{tab:colcommunes}).") if parts else ""
out=[]
order = FACTS + DIMS
out.append("{\\small\\setlength{\\tabcolsep}{3pt}\\renewcommand{\\arraystretch}{1.0}\\begin{xltabular}{\\linewidth}{@{}L{4.1cm}L{2.3cm}L{0.65cm}L{0.95cm}Y@{}}\\dicthead")
for t in order:
    v=T[t]
    meta = f"\\textbf{{Grain}} : {tex(v['grain'])}. \\textbf{{SCD}} : {tex(short(v['scd']))}. \\textbf{{Job}} : {ttt(v['job'])}, mensuel. \\textbf{{Volume}} : {tex(v['rows'])}." + common(v)
    if t != order[0]: out.append("\\midrule")
    out.append(f"\\multicolumn{{5}}{{@{{}}>{{\\columncolor{{bleuclair}}}}p{{\\dimexpr\\linewidth\\relax}}@{{}}}}{{\\textbf{{\\code{{{tex(t)}}} : {tex(v['label'])}}}\\label{{dict:{t}}}}}\\\\*")
    out.append(f"\\multicolumn{{5}}{{@{{}}p{{\\linewidth}}@{{}}}}{{{meta}}}\\\\*\\midrule")
    for (n,ty,nul,key,ref,desc) in v["cols"]:
        if n in TECH or n in SCD2N: continue
        k = key.replace("PK,FK","PK, FK")
        d = ("$\\rightarrow$ " + ttt(ref) + ". " if ref else "") + tex(short(desc))
        out.append(f"{ttt(n)} & {ttt(ty.replace('TIMESTAMP(6) WITH TIME ZONE','TIMESTAMP(6) TZ'))} & {nul} & {k} & {d} \\\\")
out.append("\\end{xltabular}}")
open(f"{G}/dictionnaire.tex","w").write("\n".join(out))


# Colonnes communes (factorisées hors des fiches du dictionnaire)
out=["{\\small\\begin{tabularx}{\\linewidth}{@{}L{3.5cm}L{2.9cm}L{0.6cm}Y@{}}",
     "\\toprule\\entete\\textbf{Colonne} & \\textbf{Type logique} & \\textbf{Null} & \\textbf{Présence et règle}\\\\\\midrule"]
for (n,ty,nul,key,ref,desc) in m.TECH_DIM:
    out.append(f"{ttt(n)} & {ttt(ty.replace('TIMESTAMP(6) WITH TIME ZONE','TIMESTAMP(6) TZ'))} & {nul} & Faits et dimensions non statiques. {tex(short(desc))}. \\\\")
for (n,ty,nul,key,ref,desc) in m.SCD2:
    out.append(f"{ttt(n)} & {ttt(ty)} & {nul} & SCD~2 (\\code{{dim\\_etablissement}}, \\code{{dim\\_professionnel}}). {tex((lambda x: x[:1].upper()+x[1:])(desc.replace("SCD type 2 : ","").split(" (historique")[0].rstrip(".")))}. \\\\")
out.append("\\bottomrule\\end{tabularx}}")
open(f"{G}/colcommunes.tex","w").write("\n".join(out))

# MLD textuel (notation Merise)
out=["\\begin{mldlist}"]
for t in order:
    v=T[t]; parts=[]
    for (n,ty,nul,key,ref,desc) in v["cols"]:
        if n in ("id_lot","dt_chargement"): continue
        s=tex(n)
        if "FK" in key: s="\\#"+s
        if "PK" in key: s="\\underline{"+s+"}"
        parts.append(s)
    out.append(f"\\item \\textbf{{{tex(t)}}} ( {', '.join(parts)} )")
out.append("\\end{mldlist}")
open(f"{G}/mld_textuel.tex","w").write("\n".join(out))

# Relations et cardinalités
out=[]
for t in order:
    for (n,ty,nul,key,ref,desc) in T[t]["cols"]:
        if "FK" in key:
            if t.startswith("bridge"):
                card_child, card_parent = "(1,1)", "(1,n)"
            else:
                card_child, card_parent = "(1,1)", "(0,n)"
            out.append(f"{ttt(t)} & {ttt(n)} & {ttt(ref)} & {card_child} & {card_parent} \\\\")
open(f"{G}/relations.tex","w").write("\n".join(out))
# Cardinalités par étoile (annexe A)
out=["{\\small\\begin{tabularx}{\\linewidth}{@{}L{4.6cm}Y@{}}",
     "\\toprule\\entete\\textbf{Fait (étoile)} & \\textbf{Dimensions référencées (préfixe \\code{dim\\_}), chacune en N:1}\\\\\\midrule"]
for t in FACTS:
    fks=[(c[0],c[4]) for c in T[t]["cols"] if "FK" in c[3]]
    refs=[]
    for n,r in fks:
        if r not in refs: refs.append(r)
    lst=", ".join(ttt(r.replace("dim_",""))+(" $\\times$2" if sum(1 for _,x in fks if x==r)>1 else "") for r in refs)
    out.append(f"{ttt(t)} ({len(fks)} FK) & {lst} \\\\")
out.append("\\bottomrule\\end{tabularx}}")
open(f"{G}/cardinalites.tex","w").write("\n".join(out))
# stats
ncols=sum(len(v['cols']) for v in T.values())
open(f"{G}/stats.tex","w").write(f"\\newcommand{{\\nbTables}}{{{len(T)}}}\\newcommand{{\\nbColonnes}}{{{ncols}}}\\newcommand{{\\nbFK}}{{{len(rels)}}}")
print("tables",len(T),"cols",ncols,"fks",len(rels))
