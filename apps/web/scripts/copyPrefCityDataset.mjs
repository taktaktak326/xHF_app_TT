import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import zlib from 'node:zlib';
import { promisify } from 'node:util';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const src = path.resolve(__dirname, '../../../pref_city_p5.topo.json.gz');
const dest = path.resolve(__dirname, '../public/pref_city_p5.topo.json.gz');
const destJson = path.resolve(__dirname, '../public/pref_city_p5.topo.json');

const gunzip = promisify(zlib.gunzip);

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
  const [srcStat, destStat, destJsonStat] = await Promise.all([
    fs.stat(src),
    exists(dest).then((ok) => (ok ? fs.stat(dest) : null)),
    exists(destJson).then((ok) => (ok ? fs.stat(destJson) : null)),
  ]);

  const shouldCopyGz = !destStat || destStat.size !== srcStat.size;
  if (shouldCopyGz) {
    await fs.copyFile(src, dest);
    console.log(`[copyPrefCityDataset] copied ${src} -> ${dest} (${srcStat.size} bytes)`);
  }

  // Also provide a plain JSON fallback for browsers that don't support DecompressionStream('gzip').
  const shouldWriteJson =
    !destJsonStat ||
    shouldCopyGz ||
    (destJsonStat.mtimeMs ?? 0) < (srcStat.mtimeMs ?? 0);

  if (shouldWriteJson) {
    const gz = await fs.readFile(src);
    const jsonBuf = await gunzip(gz);
    await fs.writeFile(destJson, jsonBuf);
    console.log(`[copyPrefCityDataset] wrote ${destJson} (${jsonBuf.length} bytes)`);
  }
}

main().catch((err) => {
  console.error('[copyPrefCityDataset] failed:', err);
  process.exitCode = 1;
});
