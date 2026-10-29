#!/usr/bin/env node
// ============================================================
// Génère poc/queries/ml/_apres_<q>.sql : vues du scénario "après" où les tables
// impliquées dans la requête remplacent le glob par la LISTE EXPLICITE des fichiers
// retenus par le filtre de partition. Simule le pruning du metastore (Hive/Trino) :
// le listing de toutes les partitions est fait en amont, le moteur n'ouvre que les
// fichiers retenus.
// ============================================================
const fs = require('fs');
const path = require('path');

const POC = __dirname; // ce répertoire joue le rôle de poc/ dans le workspace
const APRES = path.join(POC, 'apres');
const ML = path.join(POC, 'queries', 'ml');
fs.mkdirSync(ML, { recursive: true });

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

function listeFichiers(table, filtre) {
  const root = path.join(APRES, table);
  const fichiers = [];
  const walk = (dir) => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const p = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        walk(p);
      } else if (entry.name.endsWith('.parquet')) {
        const rel = path.relative(root, dir).split(path.sep).filter((x) => x.length);
        const kv = {};
        for (const comp of rel) {
          const idx = comp.indexOf('=');
          if (idx > 0) kv[decodeURIComponent(comp.slice(0, idx))] = decodeURIComponent(comp.slice(idx + 1));
        }
        const ok = Object.entries(filtre).every(([k, vals]) => vals.includes(kv[k]));
        if (ok) fichiers.push(path.relative(path.dirname(POC), p));
      }
    }
  };
  walk(root);
  return fichiers.sort();
}

const apresSql = fs.readFileSync(path.join(POC, 'queries', '_apres.sql'), 'utf8');

for (const [q, tables] of Object.entries(FILTRES)) {
  let sql = apresSql;
  for (const [table, filtre] of Object.entries(tables)) {
    const fichiers = listeFichiers(table, filtre);
    if (fichiers.length === 0) throw new Error(`Aucun fichier retenu pour ${table} (${q})`);
    const liste = fichiers.map((f) => `'${f}'`).join(', ');
    const glob = `read_parquet('poc/apres/${table}/**/*.parquet', hive_partitioning=true)`;
    const vue = `CREATE OR REPLACE VIEW ${table} AS SELECT * FROM read_parquet([${liste}], hive_partitioning=true);`;
    if (!sql.includes(glob)) throw new Error(`Glob introuvable pour ${table}`);
    sql = sql.replace(`CREATE OR REPLACE VIEW ${table} AS SELECT * FROM ${glob};`, vue);
  }
  const fichier = path.join(ML, `_apres_${q}.sql`);
  fs.writeFileSync(fichier, sql);
  console.log(`${q} -> ${fichier} (${Object.keys(tables).length} table(s) en liste explicite)`);
}
