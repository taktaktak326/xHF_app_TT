import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
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

function farmLabel(f: Farm) {
  const owner = [f.owner?.firstName, f.owner?.lastName].filter(Boolean).join(" ") || f.owner?.email || "";
  return `${f.name ?? "(no name)"} — ${owner}`.trim();
}

export function FarmSelector() {
  const { auth } = useAuth();
  const { selectedFarms, setSelectedFarms, submitSelectedFarms } = useFarms();
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

    const supportsGzip = typeof (window as any).DecompressionStream !== 'undefined';
    const datasetPath = supportsGzip ? '/pref_city_p5.topo.json.gz' : '/pref_city_p5.topo.json';
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
              onClick={(e) => {
                e.stopPropagation();
                submitSelectedFarms({ mode: 'replace' }); // ★ 選択を「確定」する
                setDropdownOpen(false);
              }}
		              disabled={selectedFarms.length === 0}
		              className="fields-action-btn fields-action-btn--accent farm-dropdown-submit"
		            >{t('farm_selector.fetch_data')}</button>

              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  submitSelectedFarms({ mode: 'append' });
                  setDropdownOpen(false);
                }}
                disabled={selectedFarms.length === 0}
                className="fields-action-btn farm-dropdown-submit"
              >
                {language === 'en' ? 'Add & fetch' : '追加で取得'}
              </button>
		
		            <div className="farm-dropdown-bulk">
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
