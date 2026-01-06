import { createContext, useContext, useState, useCallback, useRef, useEffect } from 'react';
import type { ReactNode } from 'react';
import { useAuth } from './AuthContext';
import { useData } from './DataContext';
import { useWarmup } from './WarmupContext';
import type { LoginAndTokenResp } from '../types/farm';
import { withApiBase } from '../utils/apiBase';

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
  onChunk?: (chunk: CombinedChunk) => void;
  stream?: boolean;
  includeTasks?: boolean;
}) {
  const requestBody: any = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    farm_uuids: params.farmUuids,
    countryCode: 'JP',
  };
  if (params.includeTasks !== undefined) {
    requestBody.includeTasks = params.includeTasks;
  }
  if (params.stream) requestBody.stream = true;

  const res = await fetch(withApiBase('/combined-fields'), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestBody),
  });

  if (!res.ok) {
    const text = await res.text();
    let data: any = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = null; }
    const detail = (data && (data.error || data.message || data.detail)) || `HTTP ${res.status}`;
    const error = new Error(`combined-fields: ${detail}`);
    (error as any).status = res.status;
    (error as any).responseBody = data ?? text;
    throw error;
  }

  if (!params.stream) {
    const text = await res.text();
    try { return text ? JSON.parse(text) : {}; } catch { return {}; }
  }

  // streaming NDJSON
  const reader = res.body?.getReader();
  if (!reader) throw new Error("ストリームを読み込めませんでした");
  const decoder = new TextDecoder();
  let buffer = "";

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

interface FarmContextType {
  selectedFarms: string[];
  setSelectedFarms: (farms: string[]) => void;
  submittedFarms: string[];
  submitSelectedFarms: () => void;
  clearSelectedFarms: () => void;
  // データ取得ロジックを追加
  fetchCombinedDataIfNeeded: (opts?: { force?: boolean; includeTasks?: boolean }) => void;
}

const FarmContext = createContext<FarmContextType | undefined>(undefined);

export const FarmProvider = ({ children }: { children: ReactNode }) => {
  const [selectedFarms, setSelectedFarms] = useState<string[]>([]);
  const [submittedFarms, setSubmittedFarms] = useState<string[]>([]);
  const lastFarmUuidsRef = useRef<string[] | null>(null);
  const lastRefreshRef = useRef<number>(0);
  const STORAGE_KEY = 'xhf-combinedOut';
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
  } = useData();
  
  // combinedOut の最新値を useRef で追跡し、useCallback の依存関係から外す
  const combinedOutRef = useRef(combinedOut);
  useEffect(() => {
    combinedOutRef.current = combinedOut;
  }, [combinedOut]);
  const requestIdRef = useRef(0);

  const submitSelectedFarms = () => setSubmittedFarms(selectedFarms);
  const clearSelectedFarms = () => {
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
    try {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(slim));
    } catch (err) {
      console.warn('[FarmContext] failed to persist combinedOut to storage', err);
      sessionStorage.removeItem(STORAGE_KEY);
    }
  }, [combinedOut]);

  const fetchCombinedDataIfNeeded = useCallback(async (opts?: { force?: boolean; includeTasks?: boolean }) => {
    if (!auth) return;
    const USE_STREAM = true;
    const LAUNCH_BACKGROUND_FULL_FETCH = true;
    requestIdRef.current += 1;
    const requestId = requestIdRef.current;
    const isActiveRequest = () => requestIdRef.current === requestId;
    const force = opts?.force ?? false;
    const includeTasks = opts?.includeTasks ?? false;

    // 選択中の農場がない場合は、表示をクリア
    if (submittedFarms.length === 0) {
      setCombinedOut(null);
      setCombinedLoading(false);
      setCombinedInProgress(false);
      setCombinedErr(null);
      setCombinedFetchAttempt(0);
      setCombinedFetchMaxAttempts(1);
      setCombinedRetryCountdown(null);
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
      const currentFarmUuids =
        payload.farm_uuids ??
        payload.farmUuids ??
        [];
      if (JSON.stringify([...currentFarmUuids].sort()) === JSON.stringify(target)) {
        return true;
      }
      return false;
    };

    const needsLocationRefresh = () => {
      const current = combinedOutRef.current?.response?.data?.fieldsV2;
      if (!Array.isArray(current)) return true;
      const hasMissing = current.some((f: any) => {
        const loc = f?.location || {};
        return !loc.prefecture || !loc.municipality || loc.latitude === undefined || loc.longitude === undefined;
      });
      return hasMissing;
    };

    const now = Date.now();
    const recentlyRefreshed = now - lastRefreshRef.current < 5 * 60 * 1000; // 5分

    if (!force && isDataMatching() && recentlyRefreshed && !needsLocationRefresh() && (!includeTasks || hasTasksData(combinedOutRef.current))) {
      if (combinedOutRef.current && combinedOutRef.current.source !== 'cache') {
        setCombinedOut({ ...combinedOutRef.current, source: 'cache' });
      }
      setCombinedFetchAttempt(0);
      setCombinedFetchMaxAttempts(1);
      setCombinedRetryCountdown(null);
      return;
    }

    // データが古い、または無ければ、APIを呼び出す
    const MAX_ATTEMPTS = 2;
    const RETRY_DELAY_SECONDS = 5;

    setCombinedFetchMaxAttempts(MAX_ATTEMPTS);
    setCombinedErr(null);
    // UI はそのまま表示し続け、右下トーストのみ
    setCombinedLoading(false);
    setCombinedInProgress(true);
    setCombinedRetryCountdown(null);

    let attempt = 1;
    let lastError: any = null;
    let success = false;

    while (attempt <= MAX_ATTEMPTS) {
      setCombinedFetchAttempt(attempt);
      try {
        if (USE_STREAM) {
          // 背景で非ストリームを並列起動しつつ、stream で即反映
          const fullFetchPromise = LAUNCH_BACKGROUND_FULL_FETCH
            ? fetchCombinedFieldsApi({
                auth,
                farmUuids: submittedFarms,
                stream: false,
                includeTasks,
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
            stream: true,
            includeTasks,
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
          await streamPromise;
          flush(); // 最終状態を反映

          // 背景の一括取得が完了したら最新を反映
          if (fullFetchPromise) {
            try {
              const fullRes = await fullFetchPromise;
              if (isActiveRequest() && fullRes?.response?.data?.fieldsV2) {
                setCombinedOut(fullRes as any);
              }
            } catch (e) {
              console.warn('[FarmContext] background full fetch failed', e);
            }
          }
        } else {
          // 非ストリームの一括取得
          const res = await fetchCombinedFieldsApi({
            auth,
            farmUuids: submittedFarms,
            stream: false,
            includeTasks,
          });
          if (!isActiveRequest()) return;
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
        lastError = error;
        console.warn(`[FarmContext] combined-fields attempt ${attempt} failed`, error);
        if (error?.responseBody) {
          // Surface diagnostics payload from API (includes graphql error summary)
          console.warn("[FarmContext] combined-fields responseBody", error.responseBody);
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
      const baseMessage = lastError?.message || "圃場データ取得に失敗しました";
      const detailedMessage =
        attempt > 1
          ? `${baseMessage}（再試行 ${attempt}/${MAX_ATTEMPTS} も失敗）`
          : baseMessage;
      setCombinedErr(detailedMessage);
    }

    setCombinedLoading(false);
    setCombinedInProgress(false);
    setCombinedFetchAttempt(0);
    setCombinedRetryCountdown(null);
  }, [
    auth,
    submittedFarms,
    setCombinedOut,
    setCombinedLoading,
    setCombinedErr,
    setCombinedFetchAttempt,
    setCombinedFetchMaxAttempts,
    setCombinedRetryCountdown,
    setCombinedInProgress,
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
    <FarmContext.Provider value={{ selectedFarms, setSelectedFarms, submittedFarms, submitSelectedFarms, clearSelectedFarms, fetchCombinedDataIfNeeded }}>
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
