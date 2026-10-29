#!/usr/bin/env node
// ============================================================
// Synthèse du benchmark PoC - Livrable 2 (v2)
// Scénarios :
//   - avant     : table plate (Parquet non partitionné, scan complet)
//   - apres     : partitionné, glob DuckDB (listing des footers inclus - PoC brut)
//   - apres_ml  : partitionné, pruning "metastore simulé" (liste explicite des
//                 fichiers retenus, équivalent Hive Metastore / Trino)
// Métriques : temps médian (runs 2-5), temps cold (run 1), volume de données lues.
// Sorties : bench_synthese.csv + 3 graphiques SVG.
// ============================================================
const fs = require('fs');
const path = require('path');

const POC = __dirname; // ce répertoire joue le rôle de poc/ dans le workspace
const AVANT = path.join(POC, 'avant');
const APRES = path.join(POC, 'apres');
const RESULTS = path.join(POC, 'results');

// ---------- 1. lecture des temps ----------
const csv = fs.readFileSync(path.join(RESULTS, 'bench_results.csv'), 'utf8').trim().split('\n');
const temps = {};
for (let i = 1; i < csv.length; i++) {
  const [requete, scenario, run, t] = csv[i].split(';');
  (temps[requete] = temps[requete] || {})[scenario] = temps[requete][scenario] || {};
  temps[requete][scenario][parseInt(run, 10)] = parseFloat(t);
}
const median = (arr) => {
  const s = [...arr].sort((a, b) => a - b);
  const m = Math.floor(s.length / 2);
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
};
const REQUETES = ['q01', 'q02', 'q03', 'q04', 'q05', 'q06', 'q07', 'q08', 'q09', 'q10'];
const med = (q, sc) => median([temps[q][sc][2], temps[q][sc][3], temps[q][sc][4], temps[q][sc][5]]);

// ---------- 2. volume de données lues ----------
const FILTRES = {
  q01: { f_mesures: { annee: ['2019'] }, d_etablissement: {} },
  q02: { f_consult_diag_mois: { annee: ['2019', '2020', '2021'] }, d_diagnostic: {} },
  q03: { f_mesures: { annee: ['2015', '2016', '2017', '2018', '2019', '2020', '2021'] } },
  q04: { f_hospitalisation: { annee: ['2015', '2016', '2017', '2018', '2019', '2020', '2021'] }, d_diagnostic: {} },
  q05: { f_mesures: { annee: ['2015', '2016', '2017', '2018', '2019', '2020', '2021'] } },
  q06: { f_consult_prof_mois: { annee: ['2020'] }, d_professionnel: {} },
  q07: { f_deces_agg: { annee: ['2019'] }, d_localisation: {} },
  q08: { f_satisfaction: { annee: ['2020'] } },
  q09: { f_mesures: { annee: ['2019', '2020', '2021', '2022', '2023'] } },
  q10: { f_mesures: { annee: ['2019'], sexe: ['M'] }, d_temps: { annee: ['2019'] }, d_patient: {}, d_etablissement: {}, d_localisation: {} },
};

function tailleFichierAvant(table) {
  return fs.statSync(path.join(AVANT, table + '.parquet')).size;
}
function parcoursApres(table, filtre, onFichier) {
  const root = path.join(APRES, table);
  const walk = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const p = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(p);
      else if (entry.name.endsWith('.parquet')) {
        const rel = path.relative(root, dir).split(path.sep).filter((x) => x.length);
        const kv = {};
        for (const comp of rel) {
          const idx = comp.indexOf('=');
          if (idx > 0) kv[decodeURIComponent(comp.slice(0, idx))] = decodeURIComponent(comp.slice(idx + 1));
        }
        if (Object.entries(filtre).every(([k, vals]) => vals.includes(kv[k]))) onFichier(p);
      }
    }
  };
  walk(root);
}
function tailleApres(table, filtre) {
  let total = 0;
  parcoursApres(table, filtre, (p) => { total += fs.statSync(p).size; });
  return total;
}

const volumes = {};
for (const q of REQUETES) {
  let avant = 0, apres = 0;
  for (const [table, filtre] of Object.entries(FILTRES[q])) {
    avant += tailleFichierAvant(table);
    apres += tailleApres(table, filtre);
  }
  volumes[q] = { avant, apres };
}

// ---------- 3. CSV de synthèse ----------
let out = 'requete;avant_ms_median;apres_ms_median;apres_ml_ms_median;speedup_ml;avant_ms_cold;apres_ms_cold;apres_ml_ms_cold;avant_octets;apres_octets;ratio_volume_lu\n';
for (const q of REQUETES) {
  const av = med(q, 'avant'), ap = med(q, 'apres'), ml = med(q, 'apres_ml');
  const v = volumes[q];
  out += `${q};${av.toFixed(1)};${ap.toFixed(1)};${ml.toFixed(1)};${(av / ml).toFixed(2)};${temps[q].avant[1]};${temps[q].apres[1]};${temps[q].apres_ml[1]};${v.avant};${v.apres};${(v.avant / v.apres).toFixed(1)}\n`;
}
fs.writeFileSync(path.join(RESULTS, 'bench_synthese.csv'), out);
console.log(out);

// ---------- 4. graphiques SVG ----------
const LARGEUR = 1000, HAUTEUR = 580, M = { g: 100, d: 30, h: 70, b: 100 };
const plotW = LARGEUR - M.g - M.d, plotH = HAUTEUR - M.h - M.b;
const SERIES = [
  { key: 'avant', label: 'Avant (table plate, scan complet)', couleur: '#8496b0' },
  { key: 'apres', label: 'Après (partitionné, listing inclus)', couleur: '#d9534f' },
  { key: 'apres_ml', label: 'Après (pruning metastore simulé)', couleur: '#4cae4c' },
];

function graphiqueBarres({ fichier, titre, sousTitre, unite, donnees, formateValeur, series, note }) {
  // donnees: [{q, avant, apres, apres_ml}] ; series: [{key,label,couleur}]
  const max = Math.max(...donnees.flatMap((d) => series.map((s) => d[s.key]))) * 1.15;
  const nbG = donnees.length, epG = plotW / nbG;
  const epB = Math.min(46, (epG - 16) / series.length);
  const y = (v) => M.h + plotH - (v / max) * plotH;
  let s = `<svg xmlns="http://www.w3.org/2000/svg" width="${LARGEUR}" height="${HAUTEUR}" font-family="Helvetica, Arial, sans-serif" font-size="12">`;
  s += `<rect width="${LARGEUR}" height="${HAUTEUR}" fill="#ffffff"/>`;
  s += `<text x="${LARGEUR / 2}" y="26" text-anchor="middle" font-size="17" font-weight="bold" fill="#222">${titre}</text>`;
  s += `<text x="${LARGEUR / 2}" y="46" text-anchor="middle" font-size="12" fill="#555">${sousTitre}</text>`;
  for (let i = 0; i <= 5; i++) {
    const v = (max / 5) * i, yy = y(v);
    s += `<line x1="${M.g}" y1="${yy}" x2="${LARGEUR - M.d}" y2="${yy}" stroke="#e0e0e0"/>`;
    s += `<text x="${M.g - 8}" y="${yy + 4}" text-anchor="end" fill="#555">${formateValeur(v)}</text>`;
  }
  s += `<line x1="${M.g}" y1="${M.h + plotH}" x2="${LARGEUR - M.d}" y2="${M.h + plotH}" stroke="#333" stroke-width="1.5"/>`;
  s += `<line x1="${M.g}" y1="${M.h}" x2="${M.g}" y2="${M.h + plotH}" stroke="#333" stroke-width="1.5"/>`;
  s += `<text x="18" y="${M.h + plotH / 2}" text-anchor="middle" transform="rotate(-90 18 ${M.h + plotH / 2})" fill="#333">${unite}</text>`;
  donnees.forEach((d, i) => {
    const cx = M.g + epG * i + epG / 2;
    series.forEach((sr, j) => {
      const x = cx - ((series.length * epB) + (series.length - 1) * 4) / 2 + j * (epB + 4);
      s += `<rect x="${x}" y="${y(d[sr.key])}" width="${epB}" height="${M.h + plotH - y(d[sr.key])}" fill="${sr.couleur}" rx="2"/>`;
      s += `<text x="${x + epB / 2}" y="${y(d[sr.key]) - 4}" text-anchor="middle" font-size="9" fill="#333">${formateValeur(d[sr.key])}</text>`;
    });
    s += `<text x="${cx}" y="${M.h + plotH + 18}" text-anchor="middle" font-weight="bold" fill="#222">${d.q}</text>`;
  });
  const ly = HAUTEUR - 42;
  series.forEach((sr, j) => {
    const lx = M.g + j * 330;
    s += `<rect x="${lx}" y="${ly - 12}" width="14" height="14" fill="${sr.couleur}" rx="2"/>`;
    s += `<text x="${lx + 20}" y="${ly}" fill="#333">${sr.label}</text>`;
  });
  if (note) s += `<text x="${LARGEUR / 2}" y="${HAUTEUR - 12}" text-anchor="middle" font-size="10" fill="#777">${note}</text>`;
  s += `</svg>`;
  fs.writeFileSync(path.join(RESULTS, fichier), s);
  console.log('SVG écrit : ' + fichier);
}

const donneesTemps = REQUETES.map((q) => ({ q, avant: med(q, 'avant'), apres: med(q, 'apres'), apres_ml: med(q, 'apres_ml') }));
graphiqueBarres({
  fichier: 'graph_temps_median.svg',
  titre: 'Temps de réponse médian (ms) - 4 exécutions chaudes',
  sousTitre: 'PoC DuckDB 2.0.0 en conteneur Docker - avant (table plate) vs après (partitionné)',
  unite: 'temps médian (ms)', donnees: donneesTemps, formateValeur: (v) => v.toFixed(1), series: SERIES,
  note: 'Le scénario "après (listing inclus)" pénalise le PoC local : sans metastore, DuckDB ouvre les footers de toutes les partitions. En cible Trino/Hive Metastore, ce listing est pris en charge par le catalogue (colonne verte).',
});

const donneesVol = REQUETES.map((q) => ({ q, avant: volumes[q].avant / 1048576, apres: volumes[q].apres / 1048576, apres_ml: volumes[q].apres / 1048576 }));
graphiqueBarres({
  fichier: 'graph_volume_lu.svg',
  titre: 'Volume de données lues (Mo) par requête',
  sousTitre: 'Avant : fichier plat (scan complet) - Après : somme des fichiers des partitions retenues par le filtre (pruning)',
  unite: 'volume lu (Mo)', donnees: donneesVol, formateValeur: (v) => (v / 1048576 * 1048576).toFixed ? (v).toFixed(1) : v, series: [SERIES[0], { key: 'apres', label: 'Après (partitions retenues)', couleur: '#4cae4c' }],
  note: 'q04 et q08 (petites tables) : le partitionnement produit plus d\'octets lus (overhead Parquet par petit fichier) - d\'où la règle : ne pas sur-partitionner les petites tables.',
});

// facteur de réduction du volume (barres horizontales)
{
  const ratios = REQUETES.map((q) => ({ q, ratio: volumes[q].avant / volumes[q].apres }));
  const max = Math.max(...ratios.map((r) => r.ratio)) * 1.15;
  let s = `<svg xmlns="http://www.w3.org/2000/svg" width="${LARGEUR}" height="${HAUTEUR}" font-family="Helvetica, Arial, sans-serif" font-size="12">`;
  s += `<rect width="${LARGEUR}" height="${HAUTEUR}" fill="#ffffff"/>`;
  s += `<text x="${LARGEUR / 2}" y="26" text-anchor="middle" font-size="17" font-weight="bold" fill="#222">Facteur de réduction du volume lu (avant ÷ après)</text>`;
  s += `<text x="${LARGEUR / 2}" y="46" text-anchor="middle" font-size="12" fill="#555">Combien de fois moins d'octets le moteur doit lire grâce au pruning des partitions</text>`;
  const rowH = plotH / ratios.length;
  ratios.forEach((r, i) => {
    const w = (r.ratio / max) * (plotW - 120);
    const yy = M.h + rowH * i + rowH / 2;
    const couleur = r.ratio >= 1 ? '#4cae4c' : '#d9534f';
    s += `<text x="${M.g - 10}" y="${yy + 4}" text-anchor="end" font-weight="bold" fill="#222">${r.q}</text>`;
    s += `<rect x="${M.g}" y="${yy - 11}" width="${Math.max(w, 2)}" height="22" fill="${couleur}" rx="2"/>`;
    s += `<text x="${M.g + Math.max(w, 2) + 8}" y="${yy + 4}" fill="#333">×${r.ratio.toFixed(1)}</text>`;
  });
  s += `<line x1="${M.g}" y1="${M.h}" x2="${M.g}" y2="${M.h + plotH}" stroke="#333" stroke-width="1.5"/>`;
  s += `<line x1="${M.g}" y1="${M.h + plotH}" x2="${LARGEUR - M.d}" y2="${M.h + plotH}" stroke="#333" stroke-width="1.5"/>`;
  s += `<text x="${M.g + (plotW - 120) / 2}" y="${M.h + plotH + 20}" text-anchor="middle" fill="#333">facteur de réduction (×) - vert : gain, rouge : perte (small-files)</text>`;
  s += `<text x="${LARGEUR / 2}" y="${HAUTEUR - 12}" text-anchor="middle" font-size="10" fill="#777">Le pruning des partitions est le bénéfice principal du partitionnement ; il se traduit par un gain de temps sur la cible Trino/Hive Metastore.</text>`;
  s += `</svg>`;
  fs.writeFileSync(path.join(RESULTS, 'graph_reduction_volume.svg'), s);
  console.log('SVG écrit : graph_reduction_volume.svg');
}
