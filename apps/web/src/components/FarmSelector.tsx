import { useCallback, useEffect, useMemo, useRef, useState, type MouseEvent } from 'react';
import { useAuth } from '../context/AuthContext';
import { useFarms } from '../context/FarmContext';
import { useData } from '../context/DataContext';
import { withApiBase } from '../utils/apiBase';
import { useWarmup } from '../context/WarmupContext';
import { useLanguage } from '../context/LanguageContext';
import '../pages/FarmsPage.css'; // スタイルを再利用
import LoadingSpinner from '../components/LoadingSpinner';
import { postJsonCached } from '../utils/cachedJsonFetch';

// FarmsPage.tsxから型定義とAPIクライアントを移動またはインポート
type LoginAndTokenResp = any;
type Farm = any;
type FarmsOut = any;
type HfrFarmCandidatesOut = any;
type HfrCsvFieldsOut = any;
type CropProtectionProductsBulkOut = any;

const fnv1aHex = (input: string): string => {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
};

const normalizeLatLon = (lat: number, lon: number) => {
  const inRange = (la: number, lo: number) => Math.abs(la) <= 90 && Math.abs(lo) <= 180;
  if (inRange(lat, lon)) return { lat, lon };
  if (inRange(lon, lat)) return { lat: lon, lon: lat };
  return { lat, lon };
};

async function fetchFarmsApi(auth: LoginAndTokenResp): Promise<FarmsOut> {
  const payload = {
    login_token: auth.login.login_token,
    api_token: auth.api_token,
    includeTokens: false,
  };
  const { ok, status, json: j } = await postJsonCached<any>(
    withApiBase('/farms'),
    payload,
    undefined,
    { cacheKey: `farms:list:${auth?.login?.gigya_uuid ?? 'unknown'}`, cache: 'session' },
  );
  if (!ok || j?.ok === false) {
    const detail = j?.detail ?? (typeof j?.response_text === "string" ? j.response_text.slice(0, 300) : "");
    throw new Error(`GraphQL error (status ${j?.status ?? status})${detail ? " - " + detail : ""}`);
  }
  return j as FarmsOut;
}

async function fetchHfrFarmCandidatesApi(params: {
  auth: LoginAndTokenResp;
  farmUuids: string[];
  suffix?: string;
  signal?: AbortSignal;
}): Promise<HfrFarmCandidatesOut> {
  const suffix = params.suffix === undefined ? 'HFR' : String(params.suffix).trim();
  const farmUuids = [...new Set((params.farmUuids || []).map((u) => String(u || '')).filter(Boolean))];
  const keyPayload = JSON.stringify({ suffix, farmUuids: [...farmUuids].sort() });
  const cacheKey = `farms:hfr:${params?.auth?.login?.gigya_uuid ?? 'unknown'}:${fnv1aHex(keyPayload)}`;

  const payload = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    farm_uuids: farmUuids,
    suffix,
    includeTokens: false,
  };
  const { ok, status, json: j } = await postJsonCached<any>(
    withApiBase('/farms/hfr-candidates'),
    payload,
    { signal: params.signal },
    { cacheKey, cache: 'session' },
  );
  if (!ok || j?.ok === false) {
    const detailObj = j?.detail && typeof j.detail === 'object' ? j.detail : null;
    if (detailObj?.reason === 'too_many_farms') {
      const max = Number(detailObj.max_farms ?? detailObj.maxFarms ?? 500);
      const count = Number(detailObj.received_farms ?? detailObj.receivedFarms ?? farmUuids.length);
      throw new Error(`too_many_farms:${count}:${max}`);
    }
    const detail = j?.detail ?? (typeof j?.response_text === "string" ? j.response_text.slice(0, 300) : "");
    throw new Error(`GraphQL error (status ${j?.status ?? status})${detail ? " - " + detail : ""}`);
  }
  return j as HfrFarmCandidatesOut;
}

async function fetchHfrCsvFieldsApi(params: {
  auth: LoginAndTokenResp;
  farmUuids: string[];
  languageCode: 'ja' | 'en';
  suffix?: string;
  signal?: AbortSignal;
}): Promise<HfrCsvFieldsOut> {
  const suffix = params.suffix === undefined ? 'HFR' : String(params.suffix).trim();
  const payload = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    farm_uuids: params.farmUuids,
    languageCode: params.languageCode,
    suffix,
    includeTokens: false,
  };
  const { ok, status, json: j } = await postJsonCached<any>(
    withApiBase('/farms/hfr-csv-fields'),
    payload,
    { signal: params.signal },
    { cache: 'none' },
  );
  if (!ok || j?.ok === false) {
    const detail = j?.detail ?? (typeof j?.response_text === "string" ? j.response_text.slice(0, 300) : "");
    throw new Error(`HFR csv fields error (status ${j?.status ?? status})${detail ? " - " + detail : ""}`);
  }
  return j as HfrCsvFieldsOut;
}

async function fetchCropProtectionProductsBulkApi(params: {
  auth: LoginAndTokenResp;
  farmUuids: string[];
  cropUuids: string[];
  countryUuid: string;
  signal?: AbortSignal;
}): Promise<CropProtectionProductsBulkOut> {
  const dedupedFarmUuids = [...new Set((params.farmUuids || []).map((u) => String(u || '')).filter(Boolean))];
  const dedupedCropUuids = [...new Set((params.cropUuids || []).map((u) => String(u || '')).filter(Boolean))];
  const keyPayload = JSON.stringify({
    farmUuids: [...dedupedFarmUuids].sort(),
    cropUuids: [...dedupedCropUuids].sort(),
    countryUuid: params.countryUuid,
  });
  const cacheKey = `crop-protection-products:bulk:${fnv1aHex(keyPayload)}`;
  const payload = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    farm_uuids: dedupedFarmUuids,
    country_uuid: params.countryUuid,
    crop_uuids: dedupedCropUuids,
    task_type_code: 'FIELDTREATMENT',
    includeTokens: false,
  };
  const { ok, status, json: j } = await postJsonCached<any>(
    withApiBase('/crop-protection-products/bulk'),
    payload,
    { signal: params.signal },
    { cacheKey, cache: 'session' },
  );
  if (!ok || j?.ok === false) {
    const detail = j?.detail ?? (typeof j?.response_text === 'string' ? j.response_text.slice(0, 300) : '');
    throw new Error(`Crop protection products error (status ${j?.status ?? status})${detail ? ` - ${detail}` : ''}`);
  }
  return j as CropProtectionProductsBulkOut;
}

const DEFAULT_HFR_SUFFIX = 'HFR';
const COUNTRY_UUID_JP = '0f59ff55-c86b-4b7b-4eaa-eb003d47dcd3';
const HFR_CATEGORY_NAMES = (() => {
  const raw = (import.meta as any)?.env?.VITE_HFR_CSV_CATEGORY_NAMES;
  const candidates = String(raw ?? 'FUNGICIDE')
    .split(',')
    .map((v) => v.trim().toUpperCase())
    .filter(Boolean);
  return new Set(candidates.length > 0 ? candidates : ['FUNGICIDE']);
})();
const HFR_CSV_CHUNK_SIZE = (() => {
  const raw = (import.meta as any)?.env?.VITE_HFR_CSV_CHUNK_SIZE;
  const n = raw !== undefined ? Number(raw) : 40;
  if (!Number.isFinite(n) || n < 1) return 40;
  return Math.min(200, Math.floor(n));
})();

const chunkArray = <T,>(arr: T[], chunkSize: number): T[][] => {
  const size = Math.max(1, Math.floor(chunkSize));
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
};

const escapeRegExp = (value: string): string => value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');

const csvEscape = (value: unknown): string => {
  const s = String(value ?? '');
  if (!/[",\n]/.test(s)) return s;
  return `"${s.replace(/"/g, '""')}"`;
};

const buildCsv = (header: string[], rows: string[][]): string => {
  const lines: string[] = [];
  lines.push(header.map(csvEscape).join(','));
  rows.forEach((r) => lines.push(r.map(csvEscape).join(',')));
  return `\uFEFF${lines.join('\n')}`;
};

const downloadCsv = (filename: string, csv: string) => {
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
};

const joinedName = (owner: any): string => {
  if (!owner) return '';
  const full = [owner.lastName, owner.firstName].filter(Boolean).join(' ').trim();
  return full || owner.email || '';
};

const pickPrimarySeason = (seasons: any[]): any | null => {
  if (!Array.isArray(seasons) || seasons.length === 0) return null;
  const sorted = [...seasons].sort((a, b) => {
    const ta = Date.parse(String(a?.startDate || ''));
    const tb = Date.parse(String(b?.startDate || ''));
    const va = Number.isFinite(ta) ? ta : 0;
    const vb = Number.isFinite(tb) ? tb : 0;
    return vb - va;
  });
  return sorted[0] ?? null;
};

const toUniqueJoined = (items: Array<string | null | undefined>, sep = ' | '): string => {
  const seen = new Set<string>();
  const out: string[] = [];
  items.forEach((raw) => {
    const s = String(raw ?? '').trim();
    if (!s || seen.has(s)) return;
    seen.add(s);
    out.push(s);
  });
  return out.join(sep);
};

const toRecipeArray = (value: unknown): any[] => {
  if (Array.isArray(value)) return value.filter((r) => r && typeof r === 'object');
  return value && typeof value === 'object' ? [value] : [];
};

const formatTotalWithUnit = (totalApplication: unknown, unit: unknown): string => {
  if (totalApplication == null) return '';
  const total = String(totalApplication).trim();
  if (!total) return '';
  const unitText = String(unit ?? '').trim();
  return unitText ? `${total} ${unitText}` : total;
};

const getSowingAmount = (season: any): string => {
  const establishments = Array.isArray(season?.cropEstablishments) ? season.cropEstablishments : [];
  const amounts = establishments.flatMap((establishment: any) => {
    const recipe = establishment?.dosedMap?.recipeV2 ?? establishment?.doseMap?.recipeV2;
    return toRecipeArray(recipe).map((item) => formatTotalWithUnit(item?.totalApplication, item?.unit));
  });
  return toUniqueJoined(amounts, ' / ');
};

const normalizeProductUuid = (value: unknown): string => String(value ?? '').trim().toLowerCase();

const getRecipeObjects = (spraying: any): any[] => {
  return toRecipeArray(spraying?.dosedMap?.recipeV2 ?? spraying?.doseMap?.recipeV2);
};

const isProductRecipe = (recipe: any): boolean =>
  String(recipe?.type ?? '').trim().toUpperCase() === 'PRODUCT';

const getProductRecipes = (spraying: any): any[] => getRecipeObjects(spraying).filter(isProductRecipe);

const buildTargetProductUuidsByCrop = (rawItems: Record<string, any>): Map<string, Set<string>> => {
  const out = new Map<string, Set<string>>();
  Object.entries(rawItems || {}).forEach(([cropUuid, productList]) => {
    if (!Array.isArray(productList)) return;
    const matched = new Set<string>();
    productList.forEach((product) => {
      const categories = Array.isArray(product?.categories) ? product.categories : [];
      const hit = categories.some((category: any) => HFR_CATEGORY_NAMES.has(String(category?.name ?? '').trim().toUpperCase()));
      if (!hit) return;
      const normalized = normalizeProductUuid(product?.uuid);
      if (normalized) matched.add(normalized);
    });
    out.set(cropUuid, matched);
  });
  return out;
};

const isTargetCategorySpraying = (
  spraying: any,
  cropUuid: string,
  targetProductUuidsByCrop: Map<string, Set<string>>,
): boolean => {
  if (!cropUuid) return getProductRecipes(spraying).length > 0;
  if (!targetProductUuidsByCrop.has(cropUuid)) return getProductRecipes(spraying).length > 0;
  const targetUuids = targetProductUuidsByCrop.get(cropUuid);
  if (!targetUuids || targetUuids.size === 0) return false;
  const recipeUuids = getProductRecipes(spraying)
    .map((recipe) => normalizeProductUuid(recipe?.uuid))
    .filter(Boolean);
  return recipeUuids.some((uuid) => targetUuids.has(uuid));
};

const sprayingSortTs = (spraying: any): number => {
  const raw = String(spraying?.plannedDate ?? spraying?.executionDate ?? '').trim();
  const ts = Date.parse(raw);
  return Number.isFinite(ts) ? ts : Number.MAX_SAFE_INTEGER;
};

const isAbortLikeError = (error: unknown): boolean => {
  const e = error as any;
  if (e?.name === 'AbortError') return true;
  return /abort/i.test(String(e?.message ?? ''));
};

function farmLabel(f: Farm) {
  const owner = [f.owner?.firstName, f.owner?.lastName].filter(Boolean).join(" ") || f.owner?.email || "";
  return `${f.name ?? "(no name)"} — ${owner}`.trim();
}

export function FarmSelector() {
  const { auth } = useAuth();
  const { selectedFarms, setSelectedFarms, submitSelectedFarms, replaceSelectedAndSubmittedFarms, cancelCombinedFetch } = useFarms();
  const { combinedLoading, combinedInProgress } = useData();
  const { status: warmupStatus, startWarmup } = useWarmup();
  const { language, t } = useLanguage();

  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [out, setOut] = useState<FarmsOut | null>(null);

  const [prefCityByFarmUuid, setPrefCityByFarmUuid] = useState<
    Record<string, { prefecture: string | null; municipality: string | null }>
  >({});
  const prefCityWorkerRef = useRef<Worker | null>(null);
  const prefCityPendingRef = useRef<Set<string>>(new Set());
  const [prefCityDatasetReady, setPrefCityDatasetReady] = useState(false);

  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortKey, setSortKey] = useState<'name_asc' | 'name_desc'>('name_asc');
  const [hfrLoading, setHfrLoading] = useState(false);
  const [hfrCsvLoading, setHfrCsvLoading] = useState(false);
  const [hfrStatus, setHfrStatus] = useState<string | null>(null);
  const [hfrElapsedSec, setHfrElapsedSec] = useState(0);
  const [hfrProgress, setHfrProgress] = useState<{ current: number; total: number }>({ current: 0, total: 0 });
  const [hfrSuffixInput, setHfrSuffixInput] = useState(DEFAULT_HFR_SUFFIX);
  const [useSuffixScanOnFetch, setUseSuffixScanOnFetch] = useState(false);
  const hfrAbortControllerRef = useRef<AbortController | null>(null);

  const farms: Farm[] = useMemo(
    () => out?.response?.data?.farms?.filter(Boolean) ?? [],
    [out]
  );

  const collator = useMemo(() => new Intl.Collator(language === 'en' ? 'en' : 'ja'), [language]);

  const filteredFarms = useMemo(() => {
    const sorted = [...farms].sort((a, b) => {
      const nameA = (a.name || '').toString();
      const nameB = (b.name || '').toString();
      const dir = sortKey === 'name_desc' ? -1 : 1;
      return collator.compare(nameA, nameB) * dir;
    });

    if (!searchTerm) return sorted;
    // Multi-keyword search:
    // - Split by comma/newline/、/; into OR-groups
    // - Split each group by whitespace into AND terms
    // Examples:
    //   "alpha beta" -> alpha AND beta
    //   "farmA, farmB" -> farmA OR farmB
    const rawGroups = searchTerm
      .split(/[\n,、;]+/g)
      .map((s) => s.trim())
      .filter(Boolean);
    const groups = rawGroups
      .map((g) => g.split(/\s+/g).map((t) => t.trim()).filter(Boolean))
      .filter((tokens) => tokens.length > 0);
    if (groups.length === 0) return sorted;

    return sorted.filter((farm) => {
      const label = farmLabel(farm).toLowerCase();
      return groups.some((tokens) => tokens.every((token) => label.includes(token.toLowerCase())));
    });
  }, [farms, searchTerm, sortKey, collator]);

  const allFarmIds = useMemo(
    () => farms.map((farm) => String(farm.uuid ?? '')).filter(Boolean),
    [farms],
  );
  const hfrSuffix = useMemo(() => {
    return String(hfrSuffixInput || '').trim();
  }, [hfrSuffixInput]);
  const hfrSuffixRegex = useMemo(
    () => (hfrSuffix ? new RegExp(`${escapeRegExp(hfrSuffix)}$`, 'i') : /^.*$/),
    [hfrSuffix],
  );

  const filteredFarmIds = useMemo(
    () => filteredFarms.map((farm) => String(farm.uuid ?? '')).filter(Boolean),
    [filteredFarms],
  );

  const selectedFarmIdSet = useMemo(() => new Set(selectedFarms), [selectedFarms]);

  const allSelectedCount = useMemo(
    () => allFarmIds.reduce((acc, id) => acc + (selectedFarmIdSet.has(id) ? 1 : 0), 0),
    [allFarmIds, selectedFarmIdSet],
  );

  const visibleSelectedCount = useMemo(
    () => filteredFarmIds.reduce((acc, id) => acc + (selectedFarmIdSet.has(id) ? 1 : 0), 0),
    [filteredFarmIds, selectedFarmIdSet],
  );

  const isAllSelected = allFarmIds.length > 0 && allSelectedCount === allFarmIds.length;
  const isAllVisibleSelected = filteredFarmIds.length > 0 && visibleSelectedCount === filteredFarmIds.length;

  const allToggleRef = useRef<HTMLInputElement | null>(null);
  const visibleToggleRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!allToggleRef.current) return;
    allToggleRef.current.indeterminate = allSelectedCount > 0 && !isAllSelected;
  }, [allSelectedCount, isAllSelected]);

  useEffect(() => {
    if (!visibleToggleRef.current) return;
    visibleToggleRef.current.indeterminate = visibleSelectedCount > 0 && !isAllVisibleSelected;
  }, [visibleSelectedCount, isAllVisibleSelected]);

  const selectedFarmNames = useMemo(() => {
    const selectedSet = new Set(selectedFarms);
    return farms
      .filter(f => selectedSet.has(f.uuid))
      .map(f => f.name ?? "(no name)");
  }, [farms, selectedFarms]);

  const tooltipText = useMemo(() => {
    const formatList = (items: string[]) => {
      const limit = 20;
      const joiner = language === 'ja' ? '、' : ', ';
      if (items.length <= limit) return items.join(joiner);
      const head = items.slice(0, limit).join(joiner);
      const tailCount = items.length - limit;
      return `${head}${t('farm_selector.more_suffix', { count: tailCount })}`;
    };
    if (selectedFarmNames.length > 0) {
      return t('farm_selector.tooltip_selected', { names: formatList(selectedFarmNames) });
    }
    return t('farm_selector.tooltip_none');
  }, [selectedFarmNames, language, t]);

  const loadFarms = useCallback(async () => {
    if (!auth) return;
    setErr(null);
    setOut(null);
    setLoading(true);
    try {
      const resp = await fetchFarmsApi(auth);
      setOut(resp);
      try {
        const list: any[] = resp?.response?.data?.farms ?? [];
        const map: Record<string, string> = {};
        list.forEach((f: any) => {
          const uuid = String(f?.uuid ?? '');
          const name = String(f?.name ?? '');
          if (uuid && name) map[uuid] = name;
        });
        window.sessionStorage.setItem('xhf-farmNameByUuid', JSON.stringify(map));
      } catch {
        // ignore
      }
      startWarmup().catch(() => {
        /* ウォームアップステータスの更新はトースト側で扱う */
      });
    } catch (e: any) {
      setErr(e?.message || t('farm_selector.fetch_failed'));
    } finally {
      setLoading(false);
    }
  }, [auth, startWarmup, t]);

  useEffect(() => {
    loadFarms();
  }, [loadFarms]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (prefCityWorkerRef.current) return;

    const worker = new Worker(new URL('../workers/prefCityReverseGeocode.ts', import.meta.url), {
      type: 'module',
    });
    prefCityWorkerRef.current = worker;
    let cancelled = false;

    const datasetPath = '/pref_city_p5.topo.json.gz';
    const datasetUrl = `${window.location.origin.replace(/\/$/, '')}${datasetPath}`;

    async function preloadDataset(attempt: number) {
      try {
        const res = await fetch(datasetUrl, { cache: 'no-store' });
        if (cancelled) return;
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const buf = await res.arrayBuffer();
        if (cancelled) return;
        worker.postMessage({ type: 'dataset', gz: buf }, [buf]);
      } catch (err) {
        if (cancelled) return;
        if (import.meta.env.DEV) {
          console.warn('[pref-city] dataset preload failed', { attempt, err });
        }
        if (attempt < 3) {
          window.setTimeout(() => preloadDataset(attempt + 1), 800 * attempt);
        }
      }
    }

    worker.onmessage = (event: MessageEvent<any>) => {
      const data = event.data;
      if (!data) return;
      if (data.type === 'dataset_ack') {
        worker.postMessage({ type: 'warmup' });
        return;
      }
      if (data.type === 'warmup_done') {
        if (!data.ok) {
          setPrefCityDatasetReady(false);
          if (import.meta.env.DEV) {
            console.warn('[pref-city] warmup failed', String(data.error ?? 'unknown error'));
          }
          return;
        }
        setPrefCityDatasetReady(true);
        return;
      }
      if (data.type === 'ready') {
        setPrefCityDatasetReady(Boolean(data.loaded));
        return;
      }
      if (data.type !== 'result') return;
      const id = String(data.id ?? '');
      if (!id) return;
      prefCityPendingRef.current.delete(id);
      const loc = data.location ?? null;
      if (!loc) return;
      setPrefCityByFarmUuid((prev) => ({
        ...prev,
        [id]: {
          ...(prev[id] ?? { prefecture: null, municipality: null }),
          prefecture: loc.prefecture ?? null,
          municipality: loc.municipality ?? null,
        },
      }));
    };

    worker.onerror = (e: any) => {
      if (import.meta.env.DEV) {
        console.warn('[pref-city] worker error', e);
      }
      setPrefCityDatasetReady(false);
    };

    worker.postMessage({ type: 'init', baseUrl: window.location.origin });
    // Always request warmup as a fallback so the worker can fetch the dataset by itself
    // even if main-thread preload fails.
    worker.postMessage({ type: 'warmup' });
    preloadDataset(1);
    const warmupNudgeTimer = window.setTimeout(() => {
      if (!cancelled && !prefCityDatasetReady) {
        worker.postMessage({ type: 'warmup' });
      }
    }, 1500);

    return () => {
      window.clearTimeout(warmupNudgeTimer);
      worker.terminate();
      prefCityWorkerRef.current = null;
      prefCityPendingRef.current.clear();
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    if (!dropdownOpen) return;
    const worker = prefCityWorkerRef.current;
    if (!worker) return;
    if (!prefCityDatasetReady) return;

    filteredFarms.forEach((farm) => {
      const uuid = String(farm?.uuid ?? '');
      if (!uuid) return;
      if (prefCityByFarmUuid[uuid]) return;
      if (prefCityPendingRef.current.has(uuid)) return;
      const latRaw = farm?.latitude;
      const lonRaw = farm?.longitude;
      const lat = typeof latRaw === 'number' ? latRaw : Number(latRaw);
      const lon = typeof lonRaw === 'number' ? lonRaw : Number(lonRaw);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) return;

      const normalized = normalizeLatLon(lat, lon);
      prefCityPendingRef.current.add(uuid);
      worker.postMessage({ type: 'lookup', id: uuid, lat: normalized.lat, lon: normalized.lon });
    });
  }, [dropdownOpen, filteredFarms, prefCityByFarmUuid, prefCityDatasetReady]);

  useEffect(() => {
    if (!auth) return;
    if (warmupStatus !== 'success') return;
    if (loading) return;
    if (out) return;
    // warmup完了後にまだデータがなければ再取得
    loadFarms();
  }, [auth, warmupStatus, loading, out, loadFarms]);

  function onCardClick(uuid: string) {
    const newSelected = selectedFarms.includes(uuid)
      ? selectedFarms.filter(id => id !== uuid)
      : [...selectedFarms, uuid];
    setSelectedFarms(newSelected);
  }

  const setSelectedFarmIdSet = (next: Set<string>) => {
    const ordered = allFarmIds.filter((id) => next.has(id));
    setSelectedFarms(ordered);
  };

  const selectAllFarms = () => {
    if (allFarmIds.length === 0) return;
    if (allFarmIds.length >= 20) {
      const ok = window.confirm(t('farm_selector.confirm_select_all', { count: allFarmIds.length }));
      if (!ok) return;
    }
    setSelectedFarms(allFarmIds);
  };

  const clearAllFarms = () => {
    setSelectedFarms([]);
  };

  const selectVisibleFarms = () => {
    if (filteredFarmIds.length === 0) return;
    const next = new Set(selectedFarms);
    filteredFarmIds.forEach((id) => next.add(id));
    setSelectedFarmIdSet(next);
  };

  const clearVisibleFarms = () => {
    if (filteredFarmIds.length === 0) return;
    const next = new Set(selectedFarms);
    filteredFarmIds.forEach((id) => next.delete(id));
    setSelectedFarmIdSet(next);
  };

  const handleFetchHfrFarms = useCallback(async (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    if (!auth) return;

    const hardMax = 500;
    const targetFarmUuids = [...new Set((selectedFarms.length > 0 ? selectedFarms : allFarmIds).map((u) => String(u || '')).filter(Boolean))];
    if (targetFarmUuids.length === 0) return;
    if (targetFarmUuids.length > hardMax) {
      setHfrStatus(t('error.too_many_farms', { count: targetFarmUuids.length, max: hardMax }));
      return;
    }

    const controller = new AbortController();
    hfrAbortControllerRef.current = controller;
    setHfrLoading(true);
    setHfrProgress({ current: 0, total: 1 });
    setHfrStatus(t('farm_selector.hfr_scanning', { count: targetFarmUuids.length }));
    try {
      const out = await fetchHfrFarmCandidatesApi({
        auth,
        farmUuids: targetFarmUuids,
        suffix: hfrSuffix,
        signal: controller.signal,
      });
      const matchedFarmUuids: string[] = (out?.response?.data?.matchedFarmUuids || [])
        .map((u: any) => String(u || ''))
        .filter(Boolean);
      if (matchedFarmUuids.length === 0) {
        setHfrStatus(t('farm_selector.hfr_no_match', { suffix: hfrSuffix || t('farm_selector.hfr_suffix_empty') }));
        return;
      }

      replaceSelectedAndSubmittedFarms(matchedFarmUuids);
      setHfrStatus(t('farm_selector.hfr_matched_count', { count: matchedFarmUuids.length }));
      setDropdownOpen(false);
    } catch (e: any) {
      const msg = String(e?.message || '');
      if (msg.startsWith('too_many_farms:')) {
        const parts = msg.split(':');
        const count = Number(parts[1] || targetFarmUuids.length);
        const max = Number(parts[2] || hardMax);
        setHfrStatus(t('error.too_many_farms', { count, max }));
      } else if (isAbortLikeError(e)) {
        setHfrStatus(t('farm_selector.request_canceled'));
      } else {
        setHfrStatus(t('farm_selector.hfr_scan_failed'));
      }
    } finally {
      if (hfrAbortControllerRef.current === controller) {
        hfrAbortControllerRef.current = null;
      }
      setHfrLoading(false);
      setHfrProgress({ current: 0, total: 0 });
    }
  }, [auth, selectedFarms, allFarmIds, t, replaceSelectedAndSubmittedFarms, hfrSuffix]);

  const handleFetchData = useCallback(async (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    if (useSuffixScanOnFetch) {
      await handleFetchHfrFarms(event);
      return;
    }
    setHfrStatus(null);
    submitSelectedFarms({ mode: 'replace' });
    setDropdownOpen(false);
  }, [useSuffixScanOnFetch, handleFetchHfrFarms, submitSelectedFarms]);

  const handleDownloadHfrCsv = useCallback(async (event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    if (!auth) return;

    const hardMax = 500;
    const targetFarmUuids = [...new Set((selectedFarms.length > 0 ? selectedFarms : allFarmIds).map((u) => String(u || '')).filter(Boolean))];
    if (targetFarmUuids.length === 0) return;
    if (targetFarmUuids.length > hardMax) {
      setHfrStatus(t('error.too_many_farms', { count: targetFarmUuids.length, max: hardMax }));
      return;
    }

    const controller = new AbortController();
    hfrAbortControllerRef.current = controller;
    setHfrCsvLoading(true);
    setHfrProgress({ current: 0, total: 1 });
    setHfrStatus(t('farm_selector.hfr_csv_scanning', { count: targetFarmUuids.length }));
    try {
      const candidates = await fetchHfrFarmCandidatesApi({
        auth,
        farmUuids: targetFarmUuids,
        suffix: hfrSuffix,
        signal: controller.signal,
      });
      const matchedFarmUuids: string[] = (candidates?.response?.data?.matchedFarmUuids || [])
        .map((u: any) => String(u || ''))
        .filter(Boolean);
      if (matchedFarmUuids.length === 0) {
        setHfrStatus(t('farm_selector.hfr_no_match', { suffix: hfrSuffix || t('farm_selector.hfr_suffix_empty') }));
        return;
      }

      const chunks = chunkArray(matchedFarmUuids, HFR_CSV_CHUNK_SIZE);
      const totalSteps = chunks.length + 1;
      setHfrProgress({ current: 0, total: totalSteps });
      const hfrFields: any[] = [];
      for (let i = 0; i < chunks.length; i += 1) {
        const chunk = chunks[i];
        setHfrStatus(t('farm_selector.hfr_csv_fetching_chunk', { current: i + 1, total: totalSteps }));
        const out = await fetchHfrCsvFieldsApi({
          auth,
          farmUuids: chunk,
          languageCode: language,
          suffix: hfrSuffix,
          signal: controller.signal,
        });
        const part: any[] = out?.response?.data?.hfrFields || [];
        hfrFields.push(...part.filter((f) => {
          const name = String(f?.name || '').trim();
          return Boolean(name && hfrSuffixRegex.test(name));
        }));
        setHfrProgress({ current: i + 1, total: totalSteps });
      }

      setHfrStatus(t('farm_selector.hfr_csv_loading_products'));
      const cropUuids = [...new Set(hfrFields.flatMap((field: any) => {
        const seasons = Array.isArray(field?.cropSeasonsV2) ? field.cropSeasonsV2 : [];
        return seasons.map((season: any) => String(season?.crop?.uuid ?? '')).filter(Boolean);
      }))];
      const productsOut = cropUuids.length > 0
        ? await fetchCropProtectionProductsBulkApi({
          auth,
          farmUuids: matchedFarmUuids,
          cropUuids,
          countryUuid: COUNTRY_UUID_JP,
          signal: controller.signal,
        })
        : { items: {} };
      const targetProductUuidsByCrop = buildTargetProductUuidsByCrop(productsOut?.items || {});
      setHfrProgress({ current: chunks.length + 1, total: totalSteps });

      const rowItems: Array<{ base: string[]; sprayings: Array<{ date: string; pesticide: string; note: string }> }> = [];
      hfrFields.forEach((field: any) => {
        const farm = field?.farmV2 ?? field?.farm ?? {};
        const owner = farm?.owner ?? null;
        const userName = joinedName(owner);
        const farmName = String(farm?.name ?? '');
        const fieldUuid = String(field?.uuid ?? '');
        const fieldName = String(field?.name ?? '');
        const seasons: any[] = Array.isArray(field?.cropSeasonsV2) ? field.cropSeasonsV2 : [];
        const season = pickPrimarySeason(seasons);
        const sowingDate = String(season?.startDate ?? '');
        const sowingComment = toUniqueJoined(((season?.cropEstablishments || []) as any[]).map((c) => c?.note));
        const sowingAmount = getSowingAmount(season);
        const variety = String(season?.variety?.name ?? '');

        const sprayingsWithCrop: Array<{ spraying: any; cropUuid: string }> = seasons.flatMap((s) => {
          const sprayingList = Array.isArray(s?.sprayingsV2) ? s.sprayingsV2 : [];
          const cropUuid = String(s?.crop?.uuid ?? '');
          return sprayingList.map((spraying: any) => ({ spraying, cropUuid }));
        });
        const categoryMatchedSprayings = sprayingsWithCrop
          .filter(({ spraying, cropUuid }) => isTargetCategorySpraying(spraying, cropUuid, targetProductUuidsByCrop))
          .sort((a, b) => sprayingSortTs(a.spraying) - sprayingSortTs(b.spraying));
        const matchedSprayings = categoryMatchedSprayings.length > 0
          ? categoryMatchedSprayings
          : sprayingsWithCrop
            .filter(({ spraying }) => getProductRecipes(spraying).length > 0)
            .sort((a, b) => sprayingSortTs(a.spraying) - sprayingSortTs(b.spraying));
        const sprayingTriples = matchedSprayings.map(({ spraying }) => ({
          date: String(spraying?.plannedDate ?? spraying?.executionDate ?? ''),
          pesticide: toUniqueJoined(getProductRecipes(spraying).map((r) => r?.name), ' / '),
          note: String(spraying?.note ?? ''),
        }));

        rowItems.push({
          base: [
            fieldUuid,
            fieldName,
            userName,
            farmName,
            sowingDate,
            sowingComment,
            sowingAmount,
            variety,
          ],
          sprayings: sprayingTriples,
        });
      });

      if (rowItems.length === 0) {
        setHfrStatus(t('farm_selector.hfr_csv_no_rows'));
        return;
      }

      const sprayColumnCount = Math.max(1, rowItems.reduce((max, row) => Math.max(max, row.sprayings.length), 0));
      const header: string[] = [
        '圃場UUID',
        '圃場名',
        'ユーザー名',
        '農場名',
        '播種日',
        '播種コメント',
        '播種量',
        '品種',
      ];
      for (let i = 0; i < sprayColumnCount; i += 1) {
        const n = i + 1;
        header.push(`防除予定日(Herbicides)${n}`);
        header.push(`農薬名${n}`);
        header.push(`防除コメント${n}`);
      }
      const rows: string[][] = rowItems.map((row) => {
        const cells = [...row.base];
        for (let i = 0; i < sprayColumnCount; i += 1) {
          const spraying = row.sprayings[i];
          cells.push(String(spraying?.date ?? ''));
          cells.push(String(spraying?.pesticide ?? ''));
          cells.push(String(spraying?.note ?? ''));
        }
        return cells;
      });
      const csv = buildCsv(header, rows);
      const stamp = new Date().toISOString().replace(/[-:T]/g, '').slice(0, 14);
      downloadCsv(`hfr_fields_${stamp}.csv`, csv);
      setHfrStatus(t('farm_selector.hfr_csv_ready', { count: rows.length }));
    } catch (e) {
      if (isAbortLikeError(e)) {
        setHfrStatus(t('farm_selector.request_canceled'));
      } else {
        setHfrStatus(t('farm_selector.hfr_csv_failed'));
      }
    } finally {
      if (hfrAbortControllerRef.current === controller) {
        hfrAbortControllerRef.current = null;
      }
      setHfrCsvLoading(false);
      setHfrProgress({ current: 0, total: 0 });
    }
  }, [auth, selectedFarms, allFarmIds, t, language, hfrSuffix, hfrSuffixRegex]);

  const hfrBusy = hfrLoading || hfrCsvLoading;
  const combinedBusy = combinedLoading || combinedInProgress;
  const handleCancelLoading = useCallback((event: MouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    if (hfrAbortControllerRef.current) {
      hfrAbortControllerRef.current.abort();
      hfrAbortControllerRef.current = null;
    }
    if (hfrBusy) {
      setHfrLoading(false);
      setHfrCsvLoading(false);
      setHfrProgress({ current: 0, total: 0 });
      setHfrStatus(t('farm_selector.request_canceled'));
    }
    if (combinedBusy) {
      cancelCombinedFetch();
    }
  }, [hfrBusy, combinedBusy, cancelCombinedFetch, t]);

  useEffect(() => {
    if (!hfrBusy) {
      setHfrElapsedSec(0);
      return;
    }
    const startedAt = Date.now();
    setHfrElapsedSec(0);
    const timer = window.setInterval(() => {
      setHfrElapsedSec(Math.floor((Date.now() - startedAt) / 1000));
    }, 1000);
    return () => window.clearInterval(timer);
  }, [hfrBusy]);

  return (
    <div className="farm-selection-container">
      <div
        className="farm-selection-header"
        onClick={() => setDropdownOpen(!dropdownOpen)}
        title={tooltipText}
        aria-label={tooltipText}
      >
        <span>{t('farm_selector.selected_count', { count: selectedFarms.length })}</span>
        {(combinedLoading || combinedInProgress) && (
          <span className="farm-selection-loading" aria-live="polite">
            <LoadingSpinner size={14} />
            <span>{t('farm_selector.loading_inline')}</span>
          </span>
        )}
        <span style={{ color: '#9e9e9e', fontSize: '0.9em' }}>{t('farm_selector.total_count', { count: farms.length })}</span>
        <span className="farm-selection-header-toggle">
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
            style={{ transform: dropdownOpen ? 'rotate(180deg)' : 'rotate(0deg)', transition: 'transform 0.2s' }}
          >
            <path d="M7 10L12 15L17 10" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </span>
      </div>

      {dropdownOpen && (
        <div className="farm-dropdown">
	          <div className="farm-dropdown-toolbar farm-dropdown-toolbar--sticky">
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                setSortKey((prev) => (prev === 'name_asc' ? 'name_desc' : 'name_asc'));
	              }}
	              title={t('farm_selector.sort_title', { order: t(sortKey === 'name_asc' ? 'order.asc' : 'order.desc') })}
	              style={{ width: 36, height: 36, display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}
	            >
	              {sortKey === 'name_asc' ? '△' : '▽'}
	            </button>
	            <input
	              type="text"
	              placeholder={t('farm_selector.search_placeholder')}
	              className="farm-search-input"
	              value={searchTerm}
	              onChange={(e) => setSearchTerm(e.target.value)}
	              onClick={(e) => e.stopPropagation()} // ヘッダーのクリックイベントが発火しないように
	            />
            <button
              type="button"
              onClick={handleFetchData}
		              disabled={hfrBusy || loading || (useSuffixScanOnFetch ? allFarmIds.length === 0 : selectedFarms.length === 0)}
		              className="fields-action-btn fields-action-btn--accent farm-dropdown-submit"
		            >{useSuffixScanOnFetch ? t('farm_selector.fetch_data_scan') : t('farm_selector.fetch_data')}</button>

		            <div className="farm-dropdown-bulk">
              <div className="farm-fetch-mode" role="radiogroup" aria-label={t('farm_selector.fetch_mode_label')}>
                <label className="farm-fetch-mode-option">
                  <input
                    type="radio"
                    name="farm-fetch-mode"
                    checked={!useSuffixScanOnFetch}
                    onChange={() => setUseSuffixScanOnFetch(false)}
                    onClick={(event) => event.stopPropagation()}
                  />
                  {t('farm_selector.fetch_mode_normal')}
                </label>
                <label className="farm-fetch-mode-option">
                  <input
                    type="radio"
                    name="farm-fetch-mode"
                    checked={useSuffixScanOnFetch}
                    onChange={() => setUseSuffixScanOnFetch(true)}
                    onClick={(event) => event.stopPropagation()}
                  />
                  {t('farm_selector.fetch_mode_suffix')}
                </label>
              </div>
              {useSuffixScanOnFetch && (
                <div className="farm-hfr-suffix">
                  <label htmlFor="farm-hfr-suffix-input">{t('farm_selector.hfr_suffix_label')}</label>
                  <input
                    id="farm-hfr-suffix-input"
                    type="text"
                    value={hfrSuffixInput}
                    placeholder={t('farm_selector.hfr_suffix_placeholder')}
                    onChange={(event) => setHfrSuffixInput(event.target.value)}
                    onClick={(event) => event.stopPropagation()}
                    title={t('farm_selector.hfr_suffix_title')}
                  />
                </div>
              )}
              {useSuffixScanOnFetch && (
                <button
                  type="button"
                  onClick={handleDownloadHfrCsv}
                  disabled={hfrBusy || loading || allFarmIds.length === 0}
                  className="fields-action-btn farm-hfr-action"
                  title={t('farm_selector.hfr_csv_tooltip', { suffix: hfrSuffix || t('farm_selector.hfr_suffix_empty') })}
                >
                  {hfrCsvLoading
                    ? t('farm_selector.hfr_csv_building_short')
                    : t('farm_selector.download_hfr_csv')}
                </button>
              )}
              {(hfrBusy || combinedBusy) && (
                <button
                  type="button"
                  onClick={handleCancelLoading}
                  className="fields-action-btn farm-hfr-action farm-cancel-action"
                >
                  {t('farm_selector.cancel_loading')}
                </button>
              )}

		              <label className="farm-bulk-toggle" title={t('farm_selector.toggle_all_title')}>
		                <input
		                  ref={allToggleRef}
                  type="checkbox"
                  checked={isAllSelected}
                  onChange={(event) => {
                    event.stopPropagation();
                    if (event.target.checked) selectAllFarms();
                    else clearAllFarms();
	                  }}
	                />
	                {t('farm_selector.all_label')} <strong>{allFarmIds.length}</strong>
	              </label>
	
	              <label className="farm-bulk-toggle" title={t('farm_selector.toggle_visible_title')}>
	                <input
	                  ref={visibleToggleRef}
                  type="checkbox"
                  checked={isAllVisibleSelected}
                  onChange={(event) => {
                    event.stopPropagation();
                    if (event.target.checked) selectVisibleFarms();
                    else clearVisibleFarms();
	                  }}
	                />
	                {t('farm_selector.visible_label')} <strong>{filteredFarmIds.length}</strong>
	              </label>

              <button
                type="button"
                className="fields-action-btn"
                onClick={(event) => {
                  event.stopPropagation();
                  clearAllFarms();
	                }}
	                disabled={selectedFarms.length === 0}
	              >
	                {t('farm_selector.clear_selection')}
	              </button>
	
	              <span className="farm-bulk-meta">
		                {t('farm_selector.selected_meta', { selected: selectedFarms.length, visible: filteredFarmIds.length })}
		              </span>
		            </div>
              {hfrStatus && (
                <p className="farm-dropdown-status" aria-live="polite">{hfrStatus}</p>
              )}
              {hfrBusy && (
                <p className="farm-dropdown-status farm-dropdown-status--sub" aria-live="polite">
                  {t('farm_selector.hfr_elapsed', { seconds: hfrElapsedSec })}
                  {hfrProgress.total > 0 ? ` / ${t('farm_selector.hfr_progress', { current: hfrProgress.current, total: hfrProgress.total })}` : ''}
                </p>
              )}
              {hfrBusy && (
                <div className="farm-dropdown-loadingbar" aria-hidden="true">
                  <div className="farm-dropdown-loadingbar__inner" />
                </div>
              )}
		          </div>
	
	          {loading && <p>{t('farm_selector.loading_farms')}</p>}
	          {err && <p style={{ color: "crimson" }}>{t('farm_selector.error_prefix', { message: err })}</p>}

	          {filteredFarms.length > 0 && (
	            <div className="farm-list">
	              {filteredFarms.map((f) => (
	                <div
	                  key={f.uuid}
	                  className={`farm-card ${selectedFarms.includes(f.uuid) ? 'selected' : ''}`}
	                  onClick={(e) => { e.stopPropagation(); onCardClick(f.uuid); }}
	                >
	                  <input
	                    type="checkbox"
	                    className="farm-card-checkbox"
	                    checked={selectedFarms.includes(f.uuid)}
	                    readOnly
	                  />
	                  <div className="farm-card-info">
	                    <h4>{f.name ?? "(no name)"}</h4>
	                    <p>{[f.owner?.firstName, f.owner?.lastName].filter(Boolean).join(" ") || f.owner?.email || ""}</p>
                      <p style={{ color: '#b9b9c6', marginTop: 2 }}>
                        {(() => {
                          const uuid = String(f?.uuid ?? '');
                          const loc = uuid ? prefCityByFarmUuid[uuid] : null;
                          return loc?.prefecture ?? '-';
                        })()}
                      </p>
	                  </div>
	                </div>
	              ))}
	            </div>
	          )}
	        </div>
      )}
    </div>
  );
}
