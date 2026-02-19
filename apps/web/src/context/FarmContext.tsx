import { createContext, useContext, useState, useCallback, useRef, useEffect } from 'react';
import type { ReactNode } from 'react';
import { useAuth } from './AuthContext';
import { useData } from './DataContext';
import { useWarmup } from './WarmupContext';
import { useLanguage } from './LanguageContext';
import type { LoginAndTokenResp } from '../types/farm';
import { withApiBase } from '../utils/apiBase';
import { tr } from '../i18n/runtime';

const COMBINED_FIELDS_CACHE_KEY_PREFIX = 'xhf:combined-fields:v1';
const FARM_NAME_BY_UUID_CACHE_KEY = 'xhf-farmNameByUuid';
const COMBINED_FIELDS_CACHE_TTL_MS = (() => {
  const raw = (import.meta as any)?.env?.VITE_COMBINED_FIELDS_CACHE_TTL_SEC;
  const sec = raw !== undefined ? Number(raw) : 60 * 30; // default: 30 min
  if (!Number.isFinite(sec) || sec <= 0) return 0;
  return sec * 1000;
})();
const COMBINED_FIELDS_CACHE_MAX_CHARS = 2_000_000; // ~2MB to reduce sessionStorage quota risk
const COMBINED_FIELDS_REQUIRE_COMPLETE_THRESHOLD = (() => {
  const raw = (import.meta as any)?.env?.VITE_COMBINED_FIELDS_REQUIRE_COMPLETE_THRESHOLD;
  const n = raw !== undefined ? Number(raw) : 20;
  if (!Number.isFinite(n) || n < 1) return 20;
  return Math.floor(n);
})();
const COMBINED_FIELDS_SYNC_MAX_FARMS = (() => {
  const raw = (import.meta as any)?.env?.VITE_COMBINED_FIELDS_SYNC_MAX_FARMS;
  const n = raw !== undefined ? Number(raw) : 200;
  if (!Number.isFinite(n) || n < 1) return 200;
  return Math.floor(n);
})();
const COMBINED_FIELDS_HARD_MAX_FARMS = (() => {
  const raw = (import.meta as any)?.env?.VITE_COMBINED_FIELDS_HARD_MAX_FARMS;
  const n = raw !== undefined ? Number(raw) : 500;
  if (!Number.isFinite(n) || n < 1) return 500;
  return Math.floor(n);
})();

const fnv1a = (input: string): string => {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i++) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16);
};

const getSessionJson = <T,>(key: string): { ts: number; value: T } | null => {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed.ts !== 'number' || !('value' in parsed)) return null;
    return parsed as { ts: number; value: T };
  } catch {
    return null;
  }
};

const setSessionJson = (key: string, payload: unknown) => {
  if (typeof window === 'undefined') return;
  try {
    const raw = JSON.stringify(payload);
    if (raw.length > COMBINED_FIELDS_CACHE_MAX_CHARS) return;
    window.sessionStorage.setItem(key, raw);
  } catch {
    // ignore quota / serialization failures
  }
};

const isTimeoutLikeCombinedFieldsError = (error: any): boolean => {
  const msg = String(error?.message ?? '');
  if (/timed out/i.test(msg) || /readtimeout/i.test(msg)) return true;
  if (/incomplete_chunked_encoding/i.test(msg)) return true;
  const body = error?.responseBody;
  const detail = body?.detail ?? body?.error ?? body?.message ?? null;
  const diagnostics = detail?.diagnostics ?? body?.detail?.diagnostics ?? null;
  if (detail?.reason === 'combined_fields_failed') return true;
  if (diagnostics) {
    const text = JSON.stringify(diagnostics);
    if (/TimeoutError|ReadTimeout|timed out/i.test(text)) return true;
  }
  return false;
};

const isNetworkLikeCombinedFieldsError = (error: any): boolean => {
  if (!error) return false;
  // fetch() failures are often TypeError("network error") in browsers.
  if (error instanceof TypeError) return true;
  const cause = (error as any)?.cause;
  if (cause instanceof TypeError) return true;
  const msg = String(error?.message ?? '');
  // e.g. net::ERR_INCOMPLETE_CHUNKED_ENCODING can surface as a stream read error,
  // and we still want to treat it as recoverable and fall back to chunked fetch.
  return /network error|failed to fetch|networkerror|load failed|stream_incomplete|incomplete_chunked_encoding/i.test(msg);
};

const isRecoverableCombinedFieldsError = (error: any): boolean => {
  return isTimeoutLikeCombinedFieldsError(error) || isNetworkLikeCombinedFieldsError(error);
};

const isAbortLikeError = (error: any): boolean => {
  if (!error) return false;
  if (error?.name === 'AbortError') return true;
  const msg = String(error?.message ?? '');
  return /abort/i.test(msg);
};

const chunkInHalf = <T,>(arr: T[]): [T[], T[]] => {
  const mid = Math.ceil(arr.length / 2);
  return [arr.slice(0, mid), arr.slice(mid)];
};

const chunkArray = <T,>(arr: T[], chunkSize: number): T[][] => {
  const size = Math.max(1, Math.floor(chunkSize));
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) {
    out.push(arr.slice(i, i + size));
  }
  return out;
};

const COMBINED_FIELDS_CHUNK_SIZE = (() => {
  const raw = (import.meta as any)?.env?.VITE_COMBINED_FIELDS_CHUNK_SIZE;
  const n = raw !== undefined ? Number(raw) : 1; // reliability-first default: 1 farm per request
  if (!Number.isFinite(n) || n < 1) return 1;
  return Math.floor(n);
})();

const COMBINED_FIELDS_THROTTLE_MS = (() => {
  const raw = (import.meta as any)?.env?.VITE_COMBINED_FIELDS_THROTTLE_MS;
  const n = raw !== undefined ? Number(raw) : 0; // prefer using COMBINED_FIELDS_MAX_RPS instead
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
})();

const COMBINED_FIELDS_PER_CHUNK_MAX_ATTEMPTS = (() => {
  const raw = (import.meta as any)?.env?.VITE_COMBINED_FIELDS_PER_CHUNK_MAX_ATTEMPTS;
  const n = raw !== undefined ? Number(raw) : 3;
  if (!Number.isFinite(n) || n < 1) return 1;
  return Math.floor(n);
})();

const COMBINED_FIELDS_CONCURRENCY = (() => {
  const raw = (import.meta as any)?.env?.VITE_COMBINED_FIELDS_CONCURRENCY;
  const n = raw !== undefined ? Number(raw) : 5;
  if (!Number.isFinite(n) || n < 1) return 1;
  return Math.min(10, Math.floor(n));
})();

const COMBINED_FIELDS_MAX_RPS = (() => {
  const raw = (import.meta as any)?.env?.VITE_COMBINED_FIELDS_MAX_RPS;
  const n = raw !== undefined ? Number(raw) : 5;
  if (!Number.isFinite(n) || n < 1) return 1;
  return Math.min(20, Math.floor(n));
})();

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// =============================================================================
// API Client
// =============================================================================
type CombinedChunk =
  | { type: 'base' | 'insights' | 'predictions' | 'tasks' | 'tasks_sprayings' | 'risk1' | 'risk2'; data: any }
  | { type: 'done'; warmup?: any };

const mergeFieldsData = (
  baseData: any,
  insightsData: any,
  predictionsData: any,
  tasksData: any,
) => {
  if (!baseData?.data?.fieldsV2) return [];
  const fieldsMap: Record<string, any> = {};
  baseData.data.fieldsV2.forEach((f: any) => {
    fieldsMap[f.uuid] = { ...f };
  });
  const mergeList = [insightsData, predictionsData, tasksData];
  mergeList.forEach((src) => {
    if (!src?.data?.fieldsV2) return;
    src.data.fieldsV2.forEach((update: any) => {
      const target = fieldsMap[update.uuid];
      if (!target) return;
      if (update.cropSeasonsV2) {
        const map = new Map<string, any>();
        (target.cropSeasonsV2 || []).forEach((cs: any) => map.set(cs.uuid, { ...cs }));
        update.cropSeasonsV2.forEach((cs: any) => {
          if (!cs?.uuid) return;
          const existing = map.get(cs.uuid) || {};
          map.set(cs.uuid, { ...existing, ...cs });
        });
        target.cropSeasonsV2 = Array.from(map.values());
      }
      Object.keys(update).forEach((k) => {
        if (k !== "uuid" && k !== "cropSeasonsV2") {
          target[k] = update[k];
        }
      });
    });
  });
  return Object.values(fieldsMap);
};

const mergeCropseasonPayload = (core: any, extra: any) => {
  if (!extra) return core;
  if (!core) return extra;
  try {
    const coreFields = core?.response?.data?.fieldsV2 || [];
    const extraFields = extra?.response?.data?.fieldsV2 || [];
    const extraMap: Record<string, any> = {};
    extraFields.forEach((f: any) => {
      if (f?.uuid) extraMap[f.uuid] = f;
    });
    coreFields.forEach((f: any) => {
      const extraField = extraMap[f.uuid];
      if (!extraField) return;
      const coreCsMap = new Map<string, any>();
      (f.cropSeasonsV2 || []).forEach((cs: any) => {
        if (cs?.uuid) coreCsMap.set(cs.uuid, { ...cs });
      });
      (extraField.cropSeasonsV2 || []).forEach((cs: any) => {
        if (!cs?.uuid) return;
        const prev = coreCsMap.get(cs.uuid) || {};
        coreCsMap.set(cs.uuid, { ...prev, ...cs });
      });
      f.cropSeasonsV2 = Array.from(coreCsMap.values());
    });
    return core;
  } catch {
    return core;
  }
};

async function fetchCombinedFieldsApi(params: {
  auth: LoginAndTokenResp;
  farmUuids: string[];
  languageCode: 'ja' | 'en';
  onChunk?: (chunk: CombinedChunk) => void;
  stream?: boolean;
  includeTasks?: boolean;
  withBoundarySvg?: boolean;
  requireComplete?: boolean;
  signal?: AbortSignal;
}) {
  const stream = Boolean(params.stream);
  const includeTasks = params.includeTasks !== undefined ? Boolean(params.includeTasks) : undefined;
  const requireComplete = params.requireComplete !== undefined ? Boolean(params.requireComplete) : undefined;
  const cacheEnabled = !stream && COMBINED_FIELDS_CACHE_TTL_MS > 0;
  const cacheKey = cacheEnabled
    ? (() => {
        const farms = [...params.farmUuids].sort();
        const keyPayload = JSON.stringify({
          farms,
          languageCode: params.languageCode,
          includeTasks,
          requireComplete,
          countryCode: 'JP',
        });
        return `${COMBINED_FIELDS_CACHE_KEY_PREFIX}:${fnv1a(keyPayload)}`;
      })()
    : null;
  if (cacheKey) {
    const cached = getSessionJson<any>(cacheKey);
    if (cached && Date.now() - cached.ts <= COMBINED_FIELDS_CACHE_TTL_MS) {
      return { ...cached.value, source: cached.value?.source ?? 'cache' };
    }
  }

  const requestBody: any = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    farm_uuids: params.farmUuids,
    languageCode: params.languageCode,
    countryCode: 'JP',
  };
  if (requireComplete !== undefined) {
    requestBody.requireComplete = requireComplete;
  }
  if (params.includeTasks !== undefined) {
    requestBody.includeTasks = params.includeTasks;
  }
  if (params.withBoundarySvg !== undefined) {
    requestBody.withBoundarySvg = params.withBoundarySvg;
  }
  if (stream) requestBody.stream = true;

  const res = await fetch(withApiBase('/combined-fields'), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestBody),
    signal: params.signal,
  });

  if (!res.ok) {
    const text = await res.text();
    let data: any = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = null; }
    const detailObj = data?.detail && typeof data.detail === 'object' ? data.detail : null;
    if (detailObj?.reason === 'too_many_farms') {
      const max = Number(detailObj.max_farms ?? detailObj.maxFarms ?? COMBINED_FIELDS_HARD_MAX_FARMS);
      const count = Number(detailObj.received_farms ?? detailObj.receivedFarms ?? params.farmUuids.length);
      const error = new Error(`combined-fields: ${tr('error.too_many_farms', { count, max })}`);
      (error as any).status = res.status;
      (error as any).responseBody = data ?? text;
      throw error;
    }
    const rawDetail = (data && (data.error || data.message || data.detail)) || `HTTP ${res.status}`;
    const detail =
      typeof rawDetail === 'string'
        ? rawDetail
        : (() => {
            try { return JSON.stringify(rawDetail); } catch { return String(rawDetail); }
          })();
    const error = new Error(`combined-fields: ${detail || `HTTP ${res.status}`}`);
    (error as any).status = res.status;
    (error as any).responseBody = data ?? text;
    throw error;
  }

  if (!stream) {
    const text = await res.text();
    let parsed: any = {};
    try {
      parsed = text ? JSON.parse(text) : {};
    } catch {
      parsed = {};
    }
    const cacheable = parsed && parsed.ok !== false && Number(parsed.status ?? 200) < 300;
    if (cacheKey && cacheable) {
      setSessionJson(cacheKey, { ts: Date.now(), value: parsed });
    }
    return parsed;
  }

  // streaming NDJSON
  const reader = res.body?.getReader();
  if (!reader) throw new Error(tr('error.stream_read_failed'));
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) {
        buffer += decoder.decode(value, { stream: true });
        let idx: number;
        while ((idx = buffer.indexOf("\n")) >= 0) {
          const line = buffer.slice(0, idx).trim();
          buffer = buffer.slice(idx + 1);
          if (!line) continue;
          try {
            const parsed: CombinedChunk = JSON.parse(line);
            params.onChunk?.(parsed);
          } catch {
            // ignore malformed line
          }
        }
      }
    }
  } catch (err) {
    // e.g. net::ERR_INCOMPLETE_CHUNKED_ENCODING (200 OK)
    const e = new Error('combined-fields: stream_incomplete');
    (e as any).cause = err;
    throw e;
  }
  if (buffer.trim()) {
    try {
      const parsed: CombinedChunk = JSON.parse(buffer.trim());
      params.onChunk?.(parsed);
    } catch {
      // ignore
    }
  }
  return null;
}

function mergeFieldsV2ByUuid(fieldLists: any[][]): any[] {
  const mergePreferNonEmpty = (base: any, override: any) => {
    if (!override) return base;
    if (!base) return override;
    const out: any = { ...base };
    Object.entries(override).forEach(([k, v]) => {
      if (v === null || v === undefined) return;
      if (typeof v === 'string' && v.trim() === '') return;
      out[k] = v;
    });
    return out;
  };

  const map: Record<string, any> = {};
  fieldLists.flat().forEach((f: any) => {
    const uuid = f?.uuid;
    if (!uuid) return;
    const prev = map[uuid] || {};
    map[uuid] = { ...prev, ...f };

    // Preserve non-empty location fields across partial/chunked responses.
    if (prev.location || f.location) {
      const mergedLocation = mergePreferNonEmpty(prev.location, f.location);
      // Special-case nested center to avoid losing coordinates.
      if (prev.location?.center || f.location?.center) {
        mergedLocation.center = mergePreferNonEmpty(prev.location?.center, f.location?.center);
      }
      map[uuid].location = mergedLocation;
    }

    // Also preserve top-level center/centroid if present (frontend compatibility).
    if (prev.center || f.center) {
      map[uuid].center = mergePreferNonEmpty(prev.center, f.center);
    }
    if (prev.centroid || f.centroid) {
      map[uuid].centroid = mergePreferNonEmpty(prev.centroid, f.centroid);
    }

    if (prev.cropSeasonsV2 || f.cropSeasonsV2) {
      const csMap = new Map<string, any>();
      (prev.cropSeasonsV2 || []).forEach((cs: any) => cs?.uuid && csMap.set(cs.uuid, { ...cs }));
      (f.cropSeasonsV2 || []).forEach((cs: any) => {
        if (!cs?.uuid) return;
        const existing = csMap.get(cs.uuid) || {};
        csMap.set(cs.uuid, { ...existing, ...cs });
      });
      map[uuid].cropSeasonsV2 = Array.from(csMap.values());
    }
  });
  return Object.values(map);
}

interface FarmContextType {
  selectedFarms: string[];
  setSelectedFarms: (farms: string[]) => void;
  submittedFarms: string[];
  replaceSelectedAndSubmittedFarms: (farms: string[]) => void;
  submitSelectedFarms: (opts?: { mode?: 'replace' | 'append' }) => void;
  clearSelectedFarms: () => void;
  clearCombinedCache: () => void;
  cancelCombinedFetch: () => void;
  // データ取得ロジックを追加
  fetchCombinedDataIfNeeded: (opts?: { force?: boolean; includeTasks?: boolean; requireComplete?: boolean }) => void;
}

const FarmContext = createContext<FarmContextType | undefined>(undefined);

export const FarmProvider = ({ children }: { children: ReactNode }) => {
  const [selectedFarms, setSelectedFarms] = useState<string[]>([]);
  const [submittedFarms, setSubmittedFarms] = useState<string[]>([]);
  const selectedFarmsRef = useRef<string[]>([]);
  const submittedFarmsRef = useRef<string[]>([]);
  const lastFarmUuidsRef = useRef<string[] | null>(null);
  const lastRefreshRef = useRef<number>(0);
  const { language } = useLanguage();
  const STORAGE_KEY = `xhf-combinedOut:${language}`;
  const { auth } = useAuth();
  const { status: warmupStatus } = useWarmup();
  const {
    combinedOut,
    combinedErr,
    combinedLoading,
    setCombinedOut,
    setCombinedLoading,
    setCombinedInProgress,
    setCombinedErr,
    setCombinedFetchAttempt,
    setCombinedFetchMaxAttempts,
    setCombinedRetryCountdown,
    setCombinedFetchProgress,
  } = useData();
  
  // combinedOut の最新値を useRef で追跡し、useCallback の依存関係から外す
  const combinedOutRef = useRef(combinedOut);
  useEffect(() => {
    combinedOutRef.current = combinedOut;
  }, [combinedOut]);
  useEffect(() => {
    selectedFarmsRef.current = selectedFarms;
  }, [selectedFarms]);
  useEffect(() => {
    submittedFarmsRef.current = submittedFarms;
  }, [submittedFarms]);
  const requestIdRef = useRef(0);
  const combinedFetchInFlightRef = useRef<Map<string, Promise<void>>>(new Map());
  const combinedAbortControllerRef = useRef<AbortController | null>(null);

  const cancelCombinedFetch = useCallback(() => {
    requestIdRef.current += 1;
    combinedFetchInFlightRef.current.clear();
    if (combinedAbortControllerRef.current) {
      combinedAbortControllerRef.current.abort();
      combinedAbortControllerRef.current = null;
    }
    setCombinedLoading(false);
    setCombinedInProgress(false);
    setCombinedFetchAttempt(0);
    setCombinedRetryCountdown(null);
    setCombinedFetchProgress(null);
  }, [setCombinedLoading, setCombinedInProgress, setCombinedFetchAttempt, setCombinedRetryCountdown, setCombinedFetchProgress]);

  const submitSelectedFarms = (opts?: { mode?: 'replace' | 'append' }) => {
    const mode = opts?.mode ?? 'replace';
    if (mode === 'replace') {
      const next = selectedFarmsRef.current;
      setSubmittedFarms(next);
      return;
    }
    // append: keep existing submitted farms and add newly selected farms (stable order)
    const next = (() => {
      const seen = new Set<string>();
      const out: string[] = [];
      submittedFarmsRef.current.forEach((id) => {
        if (!id || seen.has(id)) return;
        seen.add(id);
        out.push(id);
      });
      selectedFarmsRef.current.forEach((id) => {
        if (!id || seen.has(id)) return;
        seen.add(id);
        out.push(id);
      });
      return out;
    })();
    setSelectedFarms(next);
    setSubmittedFarms(next);
  };
  const replaceSelectedAndSubmittedFarms = (farms: string[]) => {
    const seen = new Set<string>();
    const next = farms
      .map((f) => String(f || '').trim())
      .filter((f) => {
        if (!f || seen.has(f)) return false;
        seen.add(f);
        return true;
      });
    setSelectedFarms(next);
    setSubmittedFarms(next);
  };
  const clearSelectedFarms = () => {
    cancelCombinedFetch();
    setSelectedFarms([]);
    setSubmittedFarms([]);
    // データもクリアする
    setCombinedOut(null);
    setCombinedErr(null);
    setCombinedLoading(false);
    setCombinedFetchAttempt(0);
    setCombinedFetchMaxAttempts(1);
    setCombinedRetryCountdown(null);
  };
  const clearCombinedCache = () => {
    if (typeof window === 'undefined') return;
    sessionStorage.removeItem(STORAGE_KEY);
    lastRefreshRef.current = 0;
  };

  // セッションストレージから復元（F5でも即表示）
  useEffect(() => {
    if (typeof window === 'undefined') return;
    try {
      const raw = sessionStorage.getItem(STORAGE_KEY);
      if (!raw) return;
      const restored = JSON.parse(raw);
      if (restored?.response?.data?.fieldsV2) {
        setCombinedOut(restored);
        const payload = restored.request?.payload as any;
        const farms: string[] =
          payload?.farm_uuids ??
          payload?.farmUuids ??
          [];
        if (farms.length) {
          setSelectedFarms(farms);
          setSubmittedFarms(farms);
          lastFarmUuidsRef.current = [...farms].sort();
        }
        lastRefreshRef.current = Date.now();
      }
    } catch (err) {
      console.warn('[FarmContext] failed to restore combinedOut from storage', err);
      sessionStorage.removeItem(STORAGE_KEY);
    }
  }, [setCombinedOut, setSubmittedFarms, setSelectedFarms]);

  // combinedOut を保存
  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (!combinedOut) {
      sessionStorage.removeItem(STORAGE_KEY);
      return;
    }
    // サイズを抑えるため、レスポンス本体のみを保存（_sub_responses 等は除外）
    const slim: any = {
      ok: combinedOut.ok,
      status: combinedOut.status,
      source: combinedOut.source,
      response: combinedOut.response,
      request: combinedOut.request,
      warmup: (combinedOut as any).warmup,
    };
    const subs = (combinedOut as any)?._sub_responses || {};
    const tasksSub = subs?.tasks;
    const sprayingsSub = subs?.tasks_sprayings;
    if (tasksSub || sprayingsSub) {
      slim._sub_responses = {
        ...(tasksSub ? { tasks: tasksSub } : {}),
        ...(sprayingsSub ? { tasks_sprayings: sprayingsSub } : {}),
      };
    }
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(slim));
    } catch (err) {
      console.warn('[FarmContext] failed to persist combinedOut to storage', err);
      sessionStorage.removeItem(STORAGE_KEY);
    }
  }, [combinedOut]);

  const fetchCombinedDataIfNeeded = useCallback(async (opts?: { force?: boolean; includeTasks?: boolean; requireComplete?: boolean }) => {
    if (!auth) return;
    const force = opts?.force ?? false;
    const includeTasks = opts?.includeTasks ?? false;
    const hardMaxFarms = Math.max(COMBINED_FIELDS_SYNC_MAX_FARMS, COMBINED_FIELDS_HARD_MAX_FARMS);
    if (submittedFarms.length > hardMaxFarms) {
      setCombinedErr(tr('error.too_many_farms', { count: submittedFarms.length, max: hardMaxFarms }));
      setCombinedLoading(false);
      setCombinedInProgress(false);
      setCombinedFetchAttempt(0);
      setCombinedFetchMaxAttempts(1);
      setCombinedRetryCountdown(null);
      setCombinedFetchProgress(null);
      return;
    }
    const forceReliabilityMode = submittedFarms.length > COMBINED_FIELDS_SYNC_MAX_FARMS;
    const requireComplete = forceReliabilityMode
      ? true
      : (opts?.requireComplete ?? (submittedFarms.length >= COMBINED_FIELDS_REQUIRE_COMPLETE_THRESHOLD));

    // Same session: avoid firing the same request twice (e.g. React StrictMode effect replay).
    const requestKey = `${language}:${includeTasks ? '1' : '0'}:${requireComplete ? '1' : '0'}:${[...submittedFarms].sort().join(',')}`;
    if (!force) {
      const pending = combinedFetchInFlightRef.current.get(requestKey);
      if (pending) return pending;
    }

    const run = async () => {
      const runAbortController = new AbortController();
      combinedAbortControllerRef.current = runAbortController;
      try {
	      // For strict completeness mode, avoid streaming to prevent partial-success paths.
	      const STREAM_FARM_LIMIT = 5;
	      const USE_STREAM = !requireComplete && submittedFarms.length <= STREAM_FARM_LIMIT;
      const LAUNCH_BACKGROUND_FULL_FETCH = true;
      requestIdRef.current += 1;
      const requestId = requestIdRef.current;
      const isActiveRequest = () => requestIdRef.current === requestId;

	      const initProgressParts = () => {
	        const baseParts: string[] = ['base', 'insights', 'predictions', 'risk1', 'risk2'];
	        if (includeTasks) baseParts.push('tasks', 'tasks_sprayings');
	        const parts: Record<string, { status: 'pending' | 'ok' | 'error'; error?: string }> = {};
	        baseParts.forEach((p) => {
	          parts[p] = { status: 'pending' };
	        });
	        setCombinedFetchProgress({ mode: 'stream', includeTasks, farmUuids: submittedFarms, parts });
	      };

      // 選択中の農場がない場合は、表示をクリア
      if (submittedFarms.length === 0) {
        setCombinedOut(null);
        setCombinedLoading(false);
        setCombinedInProgress(false);
        setCombinedErr(null);
        setCombinedFetchAttempt(0);
        setCombinedFetchMaxAttempts(1);
        setCombinedRetryCountdown(null);
        setCombinedFetchProgress(null);
        return;
      }

    // 現在のデータが選択中の農場と一致するかチェック
    const hasTasksData = (combined: any) => {
      if (!combined) return false;
      const subs = combined._sub_responses || {};
      if (subs.tasks?.response?.data?.fieldsV2) return true;
      if (subs.tasks_sprayings?.response?.data?.fieldsV2) return true;
      return false;
    };

    const isDataMatching = () => {
      const currentCombinedOut = combinedOutRef.current;
      if (!currentCombinedOut) return false;
      const last = lastFarmUuidsRef.current;
      const target = [...submittedFarms].sort();
      if (last && JSON.stringify(last) === JSON.stringify(target)) return true;
      const payload = (currentCombinedOut.request?.payload as any) || {};
      const payloadLanguage = payload.languageCode ?? payload.language_code ?? 'ja';
      if (payloadLanguage !== language) return false;
      const currentFarmUuids =
        payload.farm_uuids ??
        payload.farmUuids ??
        [];
      if (JSON.stringify([...currentFarmUuids].sort()) === JSON.stringify(target)) {
        return true;
      }
      return false;
    };

    if (!force && isDataMatching() && (!includeTasks || hasTasksData(combinedOutRef.current))) {
      if (combinedOutRef.current && combinedOutRef.current.source !== 'cache') {
        setCombinedOut({ ...combinedOutRef.current, source: 'cache' });
      }
      setCombinedFetchAttempt(0);
      setCombinedFetchMaxAttempts(1);
      setCombinedRetryCountdown(null);
      setCombinedFetchProgress(null);
      return;
    }

    // データが古い、または無ければ、APIを呼び出す
    const MAX_ATTEMPTS = requireComplete ? 3 : 2;
    const RETRY_DELAY_SECONDS = 5;

    setCombinedFetchMaxAttempts(MAX_ATTEMPTS);
    setCombinedErr(null);
    // UI はそのまま表示し続け、右下トーストのみ
    setCombinedLoading(false);
    setCombinedInProgress(true);
    setCombinedRetryCountdown(null);
    initProgressParts();

	    let attempt = 1;
	    let lastError: any = null;
	    let success = false;

		    const tryChunkedFallback = async (fallbackIncludeTasks: boolean) => {
	      // Safety valve: prevent infinite loops. In the worst case we end up with
	      // 1 request per farm UUID (after repeated splitting), so the number of
	      // processed chunks should never exceed submittedFarms.length.
	      const MAX_CHUNKS = Math.max(1, submittedFarms.length + 5);
	      let processedChunks = 0;
	      const successes: any[] = [];
	      const failures: Array<{ farmUuids: string[]; error: any; includeTasks: boolean }> = [];
		      let completed = 0;
		      let total = 0;
		      const rpsWindowMs = 1000;
		      const recentStarts: number[] = [];
		      const farmNameByUuid: Record<string, string> = (() => {
		        try {
		          const raw = typeof window !== 'undefined' ? window.sessionStorage.getItem(FARM_NAME_BY_UUID_CACHE_KEY) : null;
		          const parsed = raw ? JSON.parse(raw) : null;
		          return parsed && typeof parsed === 'object' ? (parsed as Record<string, string>) : {};
		        } catch {
		          return {};
		        }
		      })();
		      const activeChunks = new Map<number, string[]>();
		      let activeTokenSeq = 0;

		      const getActiveFarmUuidsOrdered = () => {
		        const out: string[] = [];
		        const seen = new Set<string>();
		        Array.from(activeChunks.values()).forEach((uuids) => {
		          uuids.forEach((u) => {
		            if (!u || seen.has(u)) return;
		            seen.add(u);
		            out.push(u);
		          });
		        });
		        return out;
		      };

		      const updateChunkedProgress = () => {
		        if (!isActiveRequest()) return;
		        const activeFarmUuids = getActiveFarmUuidsOrdered();
		        const activeFarmLabels = activeFarmUuids
		          .map((u) => farmNameByUuid[u] ?? `${u.slice(0, 8)}…`)
		          .slice(0, 5);
		        setCombinedFetchProgress({
	          mode: 'chunked',
	          includeTasks: fallbackIncludeTasks,
	          farmUuids: submittedFarms,
	          requestsDone: completed,
	          requestsTotal: total,
	          activeFarmUuids,
	          activeFarmLabels,
	          message: tr('combined.loading.fetching', { label: tr('label.fields_data') }),
	        });
	      };

      // Reliability-first: fetch sequentially with small chunks (default: 1 farm per request).
	      const initialChunks = chunkArray(submittedFarms, COMBINED_FIELDS_CHUNK_SIZE).map((farmUuids) => ({
	        farmUuids,
	        includeTasks: fallbackIncludeTasks,
	      }));

	      const queue: Array<{ farmUuids: string[]; includeTasks: boolean }> = [...initialChunks];
	      total = queue.length;
	      updateChunkedProgress();

		      const effectiveMaxRps = requireComplete ? Math.min(COMBINED_FIELDS_MAX_RPS, 2) : COMBINED_FIELDS_MAX_RPS;
		      const effectiveConcurrency = requireComplete ? Math.min(COMBINED_FIELDS_CONCURRENCY, 2) : COMBINED_FIELDS_CONCURRENCY;

		      const takeRpsSlot = async () => {
		        if (effectiveMaxRps <= 0) return;
	        // Simple sliding-window limiter shared across concurrent workers.
	        while (true) {
	          const now = Date.now();
	          while (recentStarts.length > 0 && now - recentStarts[0] >= rpsWindowMs) {
	            recentStarts.shift();
	          }
	          if (recentStarts.length < effectiveMaxRps) {
	            recentStarts.push(now);
	            if (COMBINED_FIELDS_THROTTLE_MS > 0) {
	              await sleep(COMBINED_FIELDS_THROTTLE_MS);
	            }
	            return;
	          }
	          const waitMs = Math.max(10, rpsWindowMs - (now - recentStarts[0]));
	          await sleep(waitMs);
	        }
	      };

		      const runOne = async (item: { farmUuids: string[]; includeTasks: boolean }) => {
		        if (!isActiveRequest()) return;
		        const token = (activeTokenSeq += 1);
		        activeChunks.set(token, item.farmUuids);
		        updateChunkedProgress();
		        try {
		        if (processedChunks >= MAX_CHUNKS) {
		          failures.push({
		            farmUuids: item.farmUuids,
		            error: new Error('chunked fallback: safety limit reached'),
		            includeTasks: item.includeTasks,
		          });
		          completed += 1;
		          updateChunkedProgress();
		          return;
		        }

		        processedChunks += 1;
		        updateChunkedProgress();

	        let lastErr: any = null;
	        let effectiveIncludeTasks = item.includeTasks;
	        let got: any = null;

	        for (let attemptNo = 1; attemptNo <= COMBINED_FIELDS_PER_CHUNK_MAX_ATTEMPTS; attemptNo += 1) {
	          if (!isActiveRequest()) break;
	          await takeRpsSlot();
	          try {
		            got = await fetchCombinedFieldsApi({
		              auth,
		              farmUuids: item.farmUuids,
		              languageCode: language,
		              stream: false,
		              includeTasks: effectiveIncludeTasks,
		              requireComplete,
		              // Boundary SVG is huge; omit it for chunked fallback to maximize reliability.
		              withBoundarySvg: false,
                  signal: runAbortController.signal,
		            });
	            if (got?.response?.data?.fieldsV2) break;
	            lastErr = new Error('combined-fields: empty response');
	          } catch (err: any) {
	            lastErr = err;
	            // If single-farm with tasks is failing, retry once without tasks to at least get base/predictions.
	            if (isRecoverableCombinedFieldsError(err) && item.farmUuids.length === 1 && effectiveIncludeTasks) {
	              failures.push({ farmUuids: item.farmUuids, error: err, includeTasks: true });
	              effectiveIncludeTasks = false;
	            } else if (!isRecoverableCombinedFieldsError(err)) {
	              break;
	            }
	          }
	          if (attemptNo < COMBINED_FIELDS_PER_CHUNK_MAX_ATTEMPTS) {
	            const backoff = Math.min(5000, 400 * attemptNo);
	            await sleep(backoff);
	          }
	        }

		        completed += 1;
		        updateChunkedProgress();

		        if (got?.response?.data?.fieldsV2) {
		          try {
		            (got.response.data.fieldsV2 as any[]).forEach((f: any) => {
		              const farm = f?.farmV2 ?? f?.farm ?? null;
		              const uuid = String(farm?.uuid ?? '');
		              const name = String(farm?.name ?? '');
		              if (uuid && name) farmNameByUuid[uuid] = name;
		            });
		          } catch {
		            // ignore
		          }
		          successes.push(got);
		          return;
		        }

	        const err = lastErr ?? new Error('combined-fields: unknown failure');
	        if (isRecoverableCombinedFieldsError(err) && item.farmUuids.length > 1) {
	          const [a, b] = chunkInHalf(item.farmUuids);
	          queue.push({ farmUuids: a, includeTasks: item.includeTasks });
	          queue.push({ farmUuids: b, includeTasks: item.includeTasks });
	          // One chunk was already counted in `total`; replacing it with two chunks increases the total by 1.
	          total += 1;
	          updateChunkedProgress();
	          return;
	        }

		        failures.push({ farmUuids: item.farmUuids, error: err, includeTasks: item.includeTasks });
		        } finally {
		          activeChunks.delete(token);
		          updateChunkedProgress();
		        }
		      };

	      const inFlight = new Set<Promise<void>>();
	      while ((queue.length > 0 || inFlight.size > 0) && isActiveRequest()) {
	        while (queue.length > 0 && inFlight.size < effectiveConcurrency && isActiveRequest()) {
	          const item = queue.shift()!;
	          let p: Promise<void>;
	          p = runOne(item).finally(() => {
	            inFlight.delete(p);
	          });
	          inFlight.add(p);
	        }
	        if (inFlight.size > 0) {
	          await Promise.race(inFlight);
	        }
	      }

	      if (!isActiveRequest()) return { ok: false, applied: false };
	      if (successes.length === 0) return { ok: false, applied: false };

      const mergedFields = mergeFieldsV2ByUuid(successes.map((s) => s.response.data.fieldsV2 as any[]));
      const warnings: any[] = [];
      if (failures.length > 0) {
        warnings.push({
          reason: 'chunked_fetch_partial',
          failed_chunks: failures.length,
          dropped_tasks_chunks: failures.filter(f => f.includeTasks).length,
        });
      }

      if (requireComplete && failures.length > 0) {
        return { ok: false, applied: false, failures };
      }

      const first = successes[0];
      setCombinedOut({
        ok: failures.length === 0,
        status: failures.length === 0 ? 200 : 206,
        source: 'api',
        response: { data: { fieldsV2: mergedFields } },
        warnings,
        request: {
          url: first?.request?.url ?? '',
          headers: first?.request?.headers ?? {},
          payload: {
            farm_uuids: submittedFarms,
            languageCode: language,
            includeTasks: fallbackIncludeTasks,
            requireComplete,
            chunkedFallback: true,
          },
        },
      } as any);
      return { ok: failures.length === 0, applied: true };
    };

    while (attempt <= MAX_ATTEMPTS) {
      setCombinedFetchAttempt(attempt);
      try {
        // For large farm selections, skip streaming and directly use chunked fallback.
	        if (!USE_STREAM && submittedFarms.length > 1) {
	          const fallbackRes: any = await tryChunkedFallback(includeTasks);
	          if (fallbackRes.applied) {
	            lastFarmUuidsRef.current = [...submittedFarms].sort();
	            lastRefreshRef.current = Date.now();
	            // Even if partial (206), prefer a usable UI over surfacing a hard error.
	            success = true;
	            break;
	          }
	          if (requireComplete && fallbackRes?.failures?.length) {
	            const e: any = new Error('combined-fields: chunked_fallback_incomplete');
	            e.failures = fallbackRes.failures;
	            throw e;
	          }
	          throw new Error('combined-fields: chunked_fallback_failed');
	        }
        if (USE_STREAM) {
          // 背景で非ストリームを並列起動しつつ、stream で即反映
          const fullFetchPromise = LAUNCH_BACKGROUND_FULL_FETCH
	            ? fetchCombinedFieldsApi({
	                auth,
	                farmUuids: submittedFarms,
	                languageCode: language,
	                stream: false,
	                includeTasks,
	                requireComplete,
                  signal: runAbortController.signal,
	              }).catch((e) => {
	                console.warn('[FarmContext] background full fetch failed', e);
	                return null;
	              })
            : null;

          // streaming で逐次反映（base を最優先で即反映）
          let baseRes: any = null;
          let insightsRes: any = null;
          let predictionsRes: any = null;
          let tasksRes: any = null;
          let tasksSprayingsRes: any = null;
          let risk1Res: any = null;
          let risk2Res: any = null;
          let baseReceived = false;
          let streamError: any = null;

          const normalize = (res: any) => {
            if (!res) return null;
            // エラーでも response を持っていれば利用する（部分的なデータを活かす）
            if (res.ok === false && !res.response) return null;
            return res;
          };

          const flush = () => {
            if (!normalize(baseRes)) return;
            const safeBase = normalize(baseRes);
            const safeInsights = normalize(insightsRes);
            const safePredictions = normalize(predictionsRes);
            const safeTasks = normalize(tasksRes);
            const safeSpray = normalize(tasksSprayingsRes);
            const safeRisk1 = normalize(risk1Res);
            const safeRisk2 = normalize(risk2Res);
            const mergedTasks = mergeCropseasonPayload(
              mergeCropseasonPayload(
                mergeCropseasonPayload(safeTasks, safeSpray),
                safeRisk1,
              ),
              safeRisk2,
            );
            const mergedFields = mergeFieldsData(
              safeBase.response,
              safeInsights?.response,
              safePredictions?.response,
              mergedTasks?.response,
            );
            const warnings: any[] = [];
            if (!safeInsights) warnings.push({ reason: 'insights_pending' });
            if (!safePredictions) warnings.push({ reason: 'predictions_pending' });
            if (includeTasks && !safeTasks) warnings.push({ reason: 'tasks_pending' });
            const basePayload = (safeBase?.request?.payload as any) ?? { farmUuids: submittedFarms };
            const requestPayload = { ...basePayload, farm_uuids: basePayload.farm_uuids ?? basePayload.farmUuids ?? submittedFarms };
            const requestHeaders = (safeBase?.request?.headers as any) ?? {};
            const partial = {
              ok: true,
              status: 200,
              source: 'api' as const,
              response: { data: { fieldsV2: mergedFields } },
              warnings,
              request: {
                url: safeBase?.request?.url ?? '',
                headers: requestHeaders,
                payload: requestPayload,
              },
              _sub_responses: {
                base: baseRes,
                insights: insightsRes,
                predictions: predictionsRes,
                tasks: mergedTasks,
                tasks_sprayings: tasksSprayingsRes,
                risk1: risk1Res,
                risk2: risk2Res,
              },
            };
            setCombinedOut(partial as any);
          };

	          const streamPromise = fetchCombinedFieldsApi({
	            auth,
	            farmUuids: submittedFarms,
	            languageCode: language,
	            stream: true,
	            includeTasks,
              signal: runAbortController.signal,
	            onChunk: (chunk) => {
              if (!isActiveRequest()) return;
              const inferLabel = (data: any): CombinedChunk["type"] | "insights" | "predictions" | "tasks" | "tasks_sprayings" | "risk1" | "risk2" | null => {
                const op = data?.request?.payload?.operationName;
                if (op === 'CombinedDataBase') return 'base';
                if (op === 'CombinedDataInsights') return 'insights';
                if (op === 'CombinedDataPredictions') return 'predictions';
                if (op === 'CombinedFieldData') {
                  const p = data.request?.payload || {};
                  if (p.withactionRecommendations) return 'risk1';
                  if (p.withrisk || p.withCropSeasonStatus || p.withNutritionStatus || p.withWaterStatus) return 'risk2';
                  if (p.withSprayingsV2 && !p.withHarvests && !p.withCropEstablishments && !p.withLandPreparations && !p.withSeedTreatments && !p.withSeedBoxTreatments) {
                    return 'tasks_sprayings';
                  }
                  return 'tasks';
                }
                return null;
              };

              if (chunk.type === 'done') {
                flush();
                return;
              }
              const label = chunk.type as any ?? inferLabel((chunk as any).data);
              if (label) {
                const data = (chunk as any).data;
                const status: 'ok' | 'error' =
                  data && typeof data.ok === 'boolean' && data.ok === false ? 'error' : 'ok';
                const errMsg =
                  status === 'error'
                    ? String(data?.error ?? data?.detail ?? data?.message ?? '')
                    : undefined;
                setCombinedFetchProgress((prev) => {
                  if (!prev || prev.mode === 'chunked') return prev;
                  const nextParts = { ...prev.parts };
                  nextParts[label] = { status, ...(errMsg ? { error: errMsg } : {}) };
                  return { ...prev, mode: 'stream', includeTasks, parts: nextParts };
                });
              }
              if (label === 'base') baseRes = (chunk as any).data;
              else if (label === 'insights') insightsRes = (chunk as any).data;
              else if (label === 'predictions') predictionsRes = (chunk as any).data;
              else if (label === 'tasks') tasksRes = (chunk as any).data;
              else if (label === 'tasks_sprayings') tasksSprayingsRes = (chunk as any).data;
              else if (label === 'risk1') risk1Res = (chunk as any).data;
              else if (label === 'risk2') risk2Res = (chunk as any).data;
              flush();
              if (!baseReceived && chunk.type === 'base') {
                baseReceived = true;
                setCombinedLoading(false); // base 到着で操作可能に
              }
            },
          });
          await streamPromise.catch((e) => {
            streamError = e;
          });
          flush(); // 最終状態を反映

          // 背景の一括取得が完了したら最新を反映（stream が途切れてもここで救済できる）
          if (fullFetchPromise) {
            try {
              const fullRes = await fullFetchPromise;
              if (isActiveRequest() && fullRes?.response?.data?.fieldsV2) {
                setCombinedOut(fullRes as any);
                // Full fetch succeeded; treat as success even if stream was incomplete.
                streamError = null;
                setCombinedFetchProgress((prev) => {
                  if (!prev || prev.mode === 'chunked') return prev;
                  const nextParts = { ...prev.parts };
                  Object.keys(nextParts).forEach((k) => {
                    if (nextParts[k].status === 'pending') nextParts[k] = { status: 'ok' };
                  });
                  return { ...prev, mode: 'full', includeTasks, parts: nextParts };
                });
              }
            } catch (e) {
              // already logged in catch above
            }
          }

          if (streamError) {
            // If we already showed partial data (base received), keep UI usable and fallback to chunked fetch.
            // If we have nothing, bubble error to outer catch to use retry/backoff.
            if (!baseReceived) {
              throw streamError;
            }
            // Stream partial ok but connection dropped; attempt chunked fallback to improve completeness.
            if (submittedFarms.length > 1 && isRecoverableCombinedFieldsError(streamError)) {
              try {
	                const fallbackRes = await tryChunkedFallback(includeTasks);
	                if (fallbackRes.applied) {
	                  lastFarmUuidsRef.current = [...submittedFarms].sort();
	                  lastRefreshRef.current = Date.now();
	                  // Even if partial (206), prefer a usable UI over surfacing a hard error.
	                  success = true;
	                  break;
	                }
	              } catch (e) {
	                console.warn('[FarmContext] chunked fallback after stream error failed', e);
	              }
            }
            // If chunked fallback didn't apply, still treat partial base as a successful (degraded) state.
            const current = combinedOutRef.current as any;
            if (current) {
              const warnings = Array.isArray(current.warnings) ? current.warnings : [];
              setCombinedOut({ ...current, warnings: [...warnings, { reason: 'stream_incomplete' }] });
            }
          }
        } else {
          // 非ストリームの一括取得
	          const res = await fetchCombinedFieldsApi({
	            auth,
	            farmUuids: submittedFarms,
	            languageCode: language,
	            stream: false,
	            includeTasks,
	            requireComplete,
              signal: runAbortController.signal,
	          });
          if (!isActiveRequest()) return;
          if (requireComplete && (res?.ok === false || Number(res?.status ?? 200) >= 300)) {
            const e: any = new Error('combined-fields: incomplete_response');
            e.responseBody = res;
            throw e;
          }
          if (!res || !res.response?.data?.fieldsV2) {
            throw new Error('combined-fields: empty response');
          }
          setCombinedOut(res as any);
        }

        if (!isActiveRequest()) {
          return;
        }
        lastFarmUuidsRef.current = [...submittedFarms].sort();
        lastRefreshRef.current = Date.now();
        success = true;
        break;
	      } catch (error: any) {
	        if (isAbortLikeError(error)) {
	          return;
	        }
	        lastError = error;
	        console.warn(`[FarmContext] combined-fields attempt ${attempt} failed`, error);
        if (error?.responseBody) {
          // Surface diagnostics payload from API (includes graphql error summary)
          console.warn("[FarmContext] combined-fields responseBody", error.responseBody);
        }
        const status = Number(error?.status ?? 0);

        // Fallback: if large payload causes timeout, split farms and fetch in smaller batches.
        if (isRecoverableCombinedFieldsError(error) && submittedFarms.length > 1) {
          try {
	            const fallbackRes = await tryChunkedFallback(includeTasks);
	            if (fallbackRes.applied) {
	              lastFarmUuidsRef.current = [...submittedFarms].sort();
	              lastRefreshRef.current = Date.now();
	              // Even if partial (206), prefer a usable UI over surfacing a hard error.
	              success = true;
	              break;
	            }
	          } catch (e) {
	            console.warn('[FarmContext] chunked fallback failed', e);
	          }
	        }

        // 4xx are usually non-recoverable request issues (e.g. too many farms).
        if (status >= 400 && status < 500 && !isRecoverableCombinedFieldsError(error)) {
          break;
        }

        if (attempt >= MAX_ATTEMPTS) {
          break;
        }
        for (let remaining = RETRY_DELAY_SECONDS; remaining > 0; remaining -= 1) {
          if (!isActiveRequest()) {
            return;
          }
          setCombinedRetryCountdown(remaining);
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
        if (!isActiveRequest()) {
          return;
        }
        setCombinedRetryCountdown(null);
        attempt += 1;
        continue;
      }
    }

    if (!isActiveRequest()) {
      return;
    }

    if (!success && lastError) {
      const baseMessage = lastError?.message || tr('error.combined_failed');
      const detailedMessage =
        attempt > 1
          ? tr('error.combined_failed_retry', { message: baseMessage, attempt, max: MAX_ATTEMPTS })
          : baseMessage;
      setCombinedErr(detailedMessage);
    }

    setCombinedLoading(false);
    setCombinedInProgress(false);
	    setCombinedFetchAttempt(0);
	    setCombinedRetryCountdown(null);
	    setCombinedFetchProgress(null);
      } finally {
        if (combinedAbortControllerRef.current === runAbortController) {
          combinedAbortControllerRef.current = null;
        }
      }
	    };

    const promise = run();
    if (!force) {
      combinedFetchInFlightRef.current.set(requestKey, promise);
    }
    try {
      await promise;
    } finally {
      if (!force) {
        combinedFetchInFlightRef.current.delete(requestKey);
      }
    }
  }, [
    auth,
    language,
    submittedFarms,
    setCombinedOut,
    setCombinedLoading,
    setCombinedErr,
    setCombinedFetchAttempt,
    setCombinedFetchMaxAttempts,
    setCombinedRetryCountdown,
    setCombinedInProgress,
    setCombinedFetchProgress,
  ]);


  useEffect(() => {
    if (warmupStatus !== 'success') {
      return;
    }
    if (!auth) {
      return;
    }
    if (submittedFarms.length === 0) {
      return;
    }
    if (combinedLoading) {
      return;
    }
    const hasPendingEnrichment = Boolean(combinedOutRef.current?.locationEnrichmentPending);
    if (combinedOutRef.current && !combinedErr && !hasPendingEnrichment) {
      return;
    }
    fetchCombinedDataIfNeeded();
  }, [warmupStatus, auth, submittedFarms, combinedLoading, combinedErr, fetchCombinedDataIfNeeded]);

  return (
    <FarmContext.Provider value={{
      selectedFarms,
      setSelectedFarms,
      submittedFarms,
      replaceSelectedAndSubmittedFarms,
      submitSelectedFarms,
      clearSelectedFarms,
      clearCombinedCache,
      cancelCombinedFetch,
      fetchCombinedDataIfNeeded,
    }}>
      {children}
    </FarmContext.Provider>
  );
};

export const useFarms = () => {
  const context = useContext(FarmContext);
  if (context === undefined) {
    throw new Error('useFarms must be used within a FarmProvider');
  }
  return context;
};
