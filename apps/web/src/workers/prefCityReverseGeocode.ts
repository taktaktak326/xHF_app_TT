type PrefCityLocation = {
  prefecture: string | null;
  municipality: string | null;
  subMunicipality: string | null;
  cityCode: string | null;
};

type LookupRequest = {
  type: 'lookup';
  id: string;
  lat: number;
  lon: number;
};

type InitRequest = {
  type: 'init';
  baseUrl: string;
};

type DatasetRequest = {
  type: 'dataset';
  gz: ArrayBuffer;
};

type DatasetAckResponse = {
  type: 'dataset_ack';
  bytes: number;
};

type WarmupRequest = {
  type: 'warmup';
};

type WarmupResponse = {
  type: 'warmup_done';
  ok: boolean;
  error?: string;
};

type LookupResponse = {
  type: 'result';
  id: string;
  location: PrefCityLocation | null;
  error?: string;
};

type ReadyResponse = {
  type: 'ready';
  loaded: boolean;
  geoms: number;
};

type Topology = {
  transform?: { scale: [number, number]; translate: [number, number] };
  arcs: number[][][];
  objects: Record<string, any>;
};

const TILE_DEG = 0.1;

let topology: Topology | null = null;
let geomList: any[] = [];
let geomBboxes: Float64Array | null = null; // [minx,miny,maxx,maxy] * n
let arcBboxes: Float64Array | null = null; // [minx,miny,maxx,maxy] * arcs
let tileIndex: Map<string, Uint32Array> | null = null;
let datasetUrl: string | null = null;
let datasetGz: ArrayBuffer | null = null;

const arcPointCache = new Map<number, Float64Array>();
const ARC_CACHE_MAX = 2000;

const tileKey = (lat: number, lon: number) =>
  `${Math.floor(lon / TILE_DEG)}:${Math.floor(lat / TILE_DEG)}`;

const normalizeArcIndex = (arcId: number) => (arcId < 0 ? ~arcId : arcId);
const isArcReversed = (arcId: number) => arcId < 0;

function bboxUnion(
  a: [number, number, number, number],
  b: [number, number, number, number],
): [number, number, number, number] {
  return [
    Math.min(a[0], b[0]),
    Math.min(a[1], b[1]),
    Math.max(a[2], b[2]),
    Math.max(a[3], b[3]),
  ];
}

function readArcBBox(index: number): [number, number, number, number] {
  if (!arcBboxes) throw new Error('arc bboxes not initialized');
  const o = index * 4;
  return [arcBboxes[o], arcBboxes[o + 1], arcBboxes[o + 2], arcBboxes[o + 3]];
}

function writeGeomBBox(geomIndex: number, bbox: [number, number, number, number]) {
  if (!geomBboxes) throw new Error('geom bboxes not initialized');
  const o = geomIndex * 4;
  geomBboxes[o] = bbox[0];
  geomBboxes[o + 1] = bbox[1];
  geomBboxes[o + 2] = bbox[2];
  geomBboxes[o + 3] = bbox[3];
}

function readGeomBBox(geomIndex: number): [number, number, number, number] {
  if (!geomBboxes) throw new Error('geom bboxes not initialized');
  const o = geomIndex * 4;
  return [geomBboxes[o], geomBboxes[o + 1], geomBboxes[o + 2], geomBboxes[o + 3]];
}

function pointInBbox(lon: number, lat: number, bbox: [number, number, number, number]) {
  return lon >= bbox[0] && lon <= bbox[2] && lat >= bbox[1] && lat <= bbox[3];
}

function decodeArcPoints(arcIndex: number): Float64Array {
  const cached = arcPointCache.get(arcIndex);
  if (cached) return cached;
  if (!topology) throw new Error('topology not loaded');
  const transform = topology.transform;
  if (!transform) throw new Error('topology.transform missing');
  const [sx, sy] = transform.scale;
  const [tx, ty] = transform.translate;

  const arc = topology.arcs[arcIndex];
  let x = 0;
  let y = 0;
  const out = new Float64Array(arc.length * 2);
  for (let i = 0; i < arc.length; i++) {
    const dx = arc[i][0];
    const dy = arc[i][1];
    x += dx;
    y += dy;
    out[i * 2] = x * sx + tx; // lon
    out[i * 2 + 1] = y * sy + ty; // lat
  }

  arcPointCache.set(arcIndex, out);
  if (arcPointCache.size > ARC_CACHE_MAX) {
    const firstKey = arcPointCache.keys().next().value as number | undefined;
    if (firstKey !== undefined) arcPointCache.delete(firstKey);
  }
  return out;
}

function pointsReversed(points: Float64Array): Float64Array {
  const n = points.length / 2;
  const out = new Float64Array(points.length);
  for (let i = 0; i < n; i++) {
    const src = (n - 1 - i) * 2;
    out[i * 2] = points[src];
    out[i * 2 + 1] = points[src + 1];
  }
  return out;
}

function ringContainsPoint(lon: number, lat: number, ring: Float64Array): boolean {
  const n = ring.length / 2;
  if (n < 3) return false;
  let inside = false;
  let j = n - 1;
  for (let i = 0; i < n; i++) {
    const xi = ring[i * 2];
    const yi = ring[i * 2 + 1];
    const xj = ring[j * 2];
    const yj = ring[j * 2 + 1];
    const intersects = (yi > lat) !== (yj > lat) && lon < ((xj - xi) * (lat - yi)) / ((yj - yi) || 1e-12) + xi;
    if (intersects) inside = !inside;
    j = i;
  }
  return inside;
}

function buildOuterRings(geom: any): Float64Array[] {
  const out: Float64Array[] = [];
  if (!geom) return out;
  if (geom.type === 'Polygon') {
    const rings = geom.arcs as number[][]; // rings -> arc ids
    if (!Array.isArray(rings) || rings.length === 0) return out;
    const outer = rings[0];
    out.push(decodeRingFromArcIds(outer));
    return out;
  }
  if (geom.type === 'MultiPolygon') {
    const polys = geom.arcs as number[][][];
    if (!Array.isArray(polys)) return out;
    polys.forEach((rings) => {
      if (!rings?.length) return;
      const outer = rings[0];
      out.push(decodeRingFromArcIds(outer));
    });
  }
  return out;
}

function decodeRingFromArcIds(arcIds: number[]): Float64Array {
  const segments: Float64Array[] = [];
  let totalPoints = 0;
  for (const arcId of arcIds) {
    const idx = normalizeArcIndex(arcId);
    const pts = decodeArcPoints(idx);
    const seg = isArcReversed(arcId) ? pointsReversed(pts) : pts;
    segments.push(seg);
    totalPoints += seg.length / 2;
  }
  if (segments.length === 0) return new Float64Array(0);
  const joined = new Float64Array(Math.max(0, (totalPoints - (segments.length - 1)) * 2));
  let offset = 0;
  segments.forEach((seg, i) => {
    const start = i === 0 ? 0 : 2; // skip first point (duplicate)
    for (let j = start; j < seg.length; j++) {
      joined[offset++] = seg[j];
    }
  });
  return joined;
}

function computeArcBboxes() {
  if (!topology) throw new Error('topology not loaded');
  const transform = topology.transform;
  if (!transform) throw new Error('topology.transform missing');
  const [sx, sy] = transform.scale;
  const [tx, ty] = transform.translate;
  const arcs = topology.arcs;
  arcBboxes = new Float64Array(arcs.length * 4);
  for (let a = 0; a < arcs.length; a++) {
    const arc = arcs[a];
    let x = 0;
    let y = 0;
    let minx = Infinity;
    let miny = Infinity;
    let maxx = -Infinity;
    let maxy = -Infinity;
    for (let i = 0; i < arc.length; i++) {
      x += arc[i][0];
      y += arc[i][1];
      const lon = x * sx + tx;
      const lat = y * sy + ty;
      if (lon < minx) minx = lon;
      if (lat < miny) miny = lat;
      if (lon > maxx) maxx = lon;
      if (lat > maxy) maxy = lat;
    }
    const o = a * 4;
    arcBboxes[o] = minx;
    arcBboxes[o + 1] = miny;
    arcBboxes[o + 2] = maxx;
    arcBboxes[o + 3] = maxy;
  }
}

function computeGeomBboxesAndTileIndex() {
  if (!topology || !arcBboxes) throw new Error('topology not initialized');
  const geomCount = geomList.length;
  geomBboxes = new Float64Array(geomCount * 4);
  const tiles = new Map<string, number[]>();

  for (let i = 0; i < geomCount; i++) {
    const geom = geomList[i];
    let bbox: [number, number, number, number] = [Infinity, Infinity, -Infinity, -Infinity];
    const collectArcIds = (arcIds: number[]) => {
      for (const arcId of arcIds) {
        const idx = normalizeArcIndex(arcId);
        bbox = bboxUnion(bbox, readArcBBox(idx));
      }
    };
    if (geom.type === 'Polygon') {
      const rings = geom.arcs as number[][];
      if (rings?.length) collectArcIds(rings[0]);
    } else if (geom.type === 'MultiPolygon') {
      const polys = geom.arcs as number[][][];
      polys?.forEach((rings) => {
        if (rings?.length) collectArcIds(rings[0]);
      });
    }

    // guard against invalid
    if (!Number.isFinite(bbox[0]) || !Number.isFinite(bbox[1]) || !Number.isFinite(bbox[2]) || !Number.isFinite(bbox[3])) {
      bbox = [0, 0, 0, 0];
    }
    writeGeomBBox(i, bbox);

    const x0 = Math.floor(bbox[0] / TILE_DEG);
    const x1 = Math.floor(bbox[2] / TILE_DEG);
    const y0 = Math.floor(bbox[1] / TILE_DEG);
    const y1 = Math.floor(bbox[3] / TILE_DEG);
    for (let x = x0; x <= x1; x++) {
      for (let y = y0; y <= y1; y++) {
        const key = `${x}:${y}`;
        const list = tiles.get(key) ?? [];
        list.push(i);
        tiles.set(key, list);
      }
    }
  }

  tileIndex = new Map<string, Uint32Array>();
  tiles.forEach((arr, key) => {
    tileIndex!.set(key, Uint32Array.from(arr));
  });
}

async function loadTopologyOnce() {
  if (topology) return;

  // If we were handed bytes directly from the main thread, accept either:
  // - gzip-compressed TopoJSON (.gz)
  // - already-decompressed JSON bytes (some servers may serve .gz with Content-Encoding: gzip)
  if (datasetGz) {
    const bytes = new Uint8Array(datasetGz);
    const isGzip = bytes.length >= 2 && bytes[0] === 0x1f && bytes[1] === 0x8b;
    if (!isGzip) {
      try {
        const text = new TextDecoder('utf-8').decode(bytes);
        topology = JSON.parse(text) as Topology;
      } catch (e) {
        const msg = e instanceof Error ? e.message : String(e);
        throw new Error(`loadTopologyOnce: failed to parse provided bytes: ${msg}`);
      }
      const objectName = topology.objects?.pref_city_minimal ? 'pref_city_minimal' : Object.keys(topology.objects ?? {})[0];
      if (!objectName) throw new Error('topology.objects missing');
      const object = topology.objects[objectName];
      if (object?.type !== 'GeometryCollection' || !Array.isArray(object.geometries)) {
        throw new Error(`unexpected object format: ${objectName}`);
      }
      geomList = object.geometries;
      computeArcBboxes();
      computeGeomBboxesAndTileIndex();
      postMessage({ type: 'ready', loaded: true, geoms: geomList.length } satisfies ReadyResponse);
      return;
    }

    // gzip-compressed bytes
    let text: string;
    try {
      // eslint-disable-next-line no-undef
      const stream = new Blob([datasetGz]).stream().pipeThrough(new DecompressionStream('gzip'));
      text = await new Response(stream).text();
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      throw new Error(`loadTopologyOnce: failed to decompress provided gzip: ${msg}`);
    }
    try {
      topology = JSON.parse(text) as Topology;
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      throw new Error(`loadTopologyOnce: failed to parse JSON: ${msg}`);
    }
    const objectName = topology.objects?.pref_city_minimal ? 'pref_city_minimal' : Object.keys(topology.objects ?? {})[0];
    if (!objectName) throw new Error('topology.objects missing');
    const object = topology.objects[objectName];
    if (object?.type !== 'GeometryCollection' || !Array.isArray(object.geometries)) {
      throw new Error(`unexpected object format: ${objectName}`);
    }
    geomList = object.geometries;
    computeArcBboxes();
    computeGeomBboxesAndTileIndex();
    postMessage({ type: 'ready', loaded: true, geoms: geomList.length } satisfies ReadyResponse);
    return;
  }

  let gzStream: ReadableStream;
  {
    const url =
      datasetUrl ??
      (() => {
        try {
          // In some dev setups the worker can be served from a blob: URL; prefer an absolute URL if possible.
          // eslint-disable-next-line no-undef
          const origin = (self as any)?.location?.origin;
          if (origin && origin !== 'null') {
            return `${origin}/pref_city_p5.topo.json.gz`;
          }
        } catch {
          // ignore
        }
        return '/pref_city_p5.topo.json.gz';
      })();

    let res: Response;
    try {
      res = await fetch(url);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      throw new Error(`loadTopologyOnce: failed to fetch dataset (${url}): ${msg}`);
    }
    if (!res.ok) throw new Error(`loadTopologyOnce: failed to fetch dataset (${url}): HTTP ${res.status}`);
    if (!res.body) throw new Error('loadTopologyOnce: dataset response has no body');
    gzStream = res.body;
  }

  // Browser-native gzip decompression (fetch branch should always be gzip bytes)
  let text: string;
  try {
    // eslint-disable-next-line no-undef
    const stream = gzStream.pipeThrough(new DecompressionStream('gzip'));
    text = await new Response(stream).text();
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`loadTopologyOnce: failed to decompress: ${msg}`);
  }

  let parsed: Topology;
  try {
    parsed = JSON.parse(text) as Topology;
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    throw new Error(`loadTopologyOnce: failed to parse JSON: ${msg}`);
  }
  topology = parsed;
  const objectName = parsed.objects?.pref_city_minimal ? 'pref_city_minimal' : Object.keys(parsed.objects ?? {})[0];
  if (!objectName) throw new Error('topology.objects missing');
  const object = parsed.objects[objectName];
  if (object?.type !== 'GeometryCollection' || !Array.isArray(object.geometries)) {
    throw new Error(`unexpected object format: ${objectName}`);
  }
  geomList = object.geometries;
  computeArcBboxes();
  computeGeomBboxesAndTileIndex();
  postMessage({ type: 'ready', loaded: true, geoms: geomList.length } satisfies ReadyResponse);
}

function pickLocationFromGeom(geom: any): PrefCityLocation | null {
  const props = geom?.properties;
  if (!props) return null;
  return {
    prefecture: props.prefecture ?? null,
    municipality: props.municipality ?? null,
    subMunicipality: props.subMunicipality ?? null,
    cityCode: props.cityCode ?? null,
  };
}

function lookup(lat: number, lon: number): PrefCityLocation | null {
  if (!tileIndex || !geomBboxes) return null;
  const candidates = tileIndex.get(tileKey(lat, lon));
  if (!candidates || candidates.length === 0) return null;

  for (let idx = 0; idx < candidates.length; idx++) {
    const geomIndex = candidates[idx]!;
    const bbox = readGeomBBox(geomIndex);
    if (!pointInBbox(lon, lat, bbox)) continue;
    const geom = geomList[geomIndex];
    const rings = buildOuterRings(geom);
    for (const ring of rings) {
      if (ringContainsPoint(lon, lat, ring)) {
        return pickLocationFromGeom(geom);
      }
    }
  }
  return null;
}

self.onmessage = async (ev: MessageEvent<LookupRequest | InitRequest | DatasetRequest | WarmupRequest>) => {
  const msg = ev.data;
  if (!msg) return;
  if (msg.type === 'init') {
    datasetUrl = `${String(msg.baseUrl).replace(/\/$/, '')}/pref_city_p5.topo.json.gz`;
    return;
  }
  if (msg.type === 'dataset') {
    datasetGz = msg.gz;
    postMessage({ type: 'dataset_ack', bytes: datasetGz.byteLength } satisfies DatasetAckResponse);
    return;
  }
  if (msg.type === 'warmup') {
    try {
      await loadTopologyOnce();
      postMessage({ type: 'warmup_done', ok: true } satisfies WarmupResponse);
    } catch (e) {
      const error = e instanceof Error ? e.message : 'unknown error';
      postMessage({ type: 'warmup_done', ok: false, error } satisfies WarmupResponse);
    }
    return;
  }
  if (msg.type !== 'lookup') return;
  try {
    await loadTopologyOnce();
    const location = lookup(msg.lat, msg.lon);
    postMessage({ type: 'result', id: msg.id, location } satisfies LookupResponse);
  } catch (e) {
    const error = e instanceof Error ? e.message : 'unknown error';
    postMessage({ type: 'result', id: msg.id, location: null, error } satisfies LookupResponse);
  }
};
