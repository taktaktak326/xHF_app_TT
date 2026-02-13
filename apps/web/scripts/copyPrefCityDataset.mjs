import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const src = path.resolve(__dirname, '../../../pref_city_p5.topo.json.gz');
const dest = path.resolve(__dirname, '../public/pref_city_p5.topo.json.gz');
const destJson = path.resolve(__dirname, '../public/pref_city_p5.topo.json');

async function exists(p) {
  try {
    await fs.access(p);
    return true;
  } catch {
    return false;
  }
}

async function main() {
  if (!(await exists(src))) {
    console.warn(`[copyPrefCityDataset] source not found: ${src}`);
    return;
  }
  await fs.mkdir(path.dirname(dest), { recursive: true });
  const [srcStat, destStat, hasJson] = await Promise.all([
    fs.stat(src),
    exists(dest).then((ok) => (ok ? fs.stat(dest) : null)),
    exists(destJson),
  ]);

  const shouldCopyGz = !destStat || destStat.size !== srcStat.size;
  if (shouldCopyGz) {
    await fs.copyFile(src, dest);
    console.log(`[copyPrefCityDataset] copied ${src} -> ${dest} (${srcStat.size} bytes)`);
  }

  // Cloudflare Pages has a 25MiB file limit per asset.
  // The plain JSON dataset is >100MiB, so we intentionally do NOT generate it.
  // We also remove it if it exists from previous builds.
  if (hasJson) {
    await fs.rm(destJson, { force: true });
    console.log(`[copyPrefCityDataset] removed oversized JSON fallback: ${destJson}`);
  }
}

main().catch((err) => {
  console.error('[copyPrefCityDataset] failed:', err);
  process.exitCode = 1;
});
