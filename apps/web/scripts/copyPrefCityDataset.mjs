import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const src = path.resolve(__dirname, '../../../pref_city_p5.topo.json.gz');
const dest = path.resolve(__dirname, '../public/pref_city_p5.topo.json.gz');

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
  const [srcStat, destStat] = await Promise.all([
    fs.stat(src),
    exists(dest).then((ok) => (ok ? fs.stat(dest) : null)),
  ]);
  if (destStat && destStat.size === srcStat.size) {
    return;
  }
  await fs.copyFile(src, dest);
  console.log(`[copyPrefCityDataset] copied ${src} -> ${dest} (${srcStat.size} bytes)`);
}

main().catch((err) => {
  console.error('[copyPrefCityDataset] failed:', err);
  process.exitCode = 1;
});
