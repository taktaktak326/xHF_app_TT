import { memo, useEffect, useMemo, useState, useRef, useCallback } from 'react';
import type { FC } from 'react';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
  CategoryScale,
  LinearScale,
  BarElement,
  TimeScale,
  TimeSeriesScale,
  type TooltipItem,
} from 'chart.js';
import type { Chart as ChartJSInstance } from 'chart.js';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import zoomPlugin from 'chartjs-plugin-zoom';
import annotationPlugin from 'chartjs-plugin-annotation';
import 'chartjs-adapter-date-fns';
import { enUS, ja } from 'date-fns/locale';
import { format, startOfDay, subDays, addDays, differenceInCalendarDays } from 'date-fns';
import { useData } from '../context/DataContext';
import { useFarms } from '../context/FarmContext';
import { useLanguage } from '../context/LanguageContext';
import { tr } from '../i18n/runtime';
import type { BaseTask, Field, CropSeason, CountryCropGrowthStagePrediction } from '../types/farm';
import { getSessionCache, setSessionCache } from '../utils/sessionCache';
import './FarmsPage.css'; // 共通スタイルをインポート
import './GrowthStagePredictionPage.css';
import LoadingOverlay from '../components/LoadingOverlay';
import { formatCombinedLoadingMessage } from '../utils/loadingMessage';
import { formatInclusiveEndDate, getLocalDateString } from '../utils/formatters';

ChartJS.register(
  ArcElement, Tooltip, Legend, CategoryScale, LinearScale,
  BarElement, TimeScale, TimeSeriesScale, ChartDataLabels, zoomPlugin, annotationPlugin
);

const PREF_CITY_CACHE_KEY = 'pref-city:by-field:v1';

// =============================================================================
// Type Definitions
// =============================================================================

type GroupedPrediction = {
  seasonUuid: string;
  farmName: string;
  fieldName: string;
  cropName: string;
  varietyName: string;
  seasonStartDate: string;
  plantingMethodCode: string | null;
  predictions: CountryCropGrowthStagePrediction[];
  prefecture: string | null;
  municipalityLabel: string | null;
  tasks: Array<{ typeKey: string; task: BaseTask }>;
};

type ChartJSDataset = {
  label: string;
  data: any[];
} & Record<string, any>;

type TimelineData = {
  chartData: { labels: string[]; datasets: ChartJSDataset[] } | null;
  chartHeight: number;
  maxDate: Date | null;
  minDate: Date | null;
};

type UpcomingStageItem = {
  fieldName: string;
  cropName: string;
  varietyName: string;
  stageIndex: string;
  stageName: string;
  start: Date;
  end: Date;
};

type FieldCenter = {
  latitude: number;
  longitude: number;
};

const getFieldCenter = (field: Field): FieldCenter | null => {
  const farmV2 = (field as any).farmV2 ?? (field as any).farm ?? null;
  const farmCenter = farmV2 && typeof farmV2.latitude === 'number' && typeof farmV2.longitude === 'number'
    ? { latitude: farmV2.latitude, longitude: farmV2.longitude }
    : null;
  const candidates = [field.location?.center, (field as any).center, (field as any).centroid, farmCenter];
  for (const candidate of candidates) {
    if (candidate && typeof candidate.latitude === 'number' && typeof candidate.longitude === 'number') {
      return { latitude: candidate.latitude, longitude: candidate.longitude };
    }
  }
  return null;
};

function formatMunicipalityDisplay(location: Field['location']): string {
  if (!location) return '';
  const parts: string[] = [];
  if (location.municipality) {
    parts.push(location.municipality);
  }
  if (location.subMunicipality) {
    parts.push(location.subMunicipality);
  }
  const formatted = parts.join(' ').trim();
  if (!formatted) return '';
  return location.isApproximate ? `${formatted}*` : formatted;
}

function formatPrefectureDisplay(location: Field['location']): string {
  if (!location?.prefecture) return '';
  return location.isApproximate ? `${location.prefecture}*` : location.prefecture;
}

const selectFieldsFromCombinedOut = (combinedOut: any): Field[] => {
  const mergeSeasons = (a: any, b: any) => {
    if (!a && !b) return [];
    const map = new Map<string, any>();
    (Array.isArray(a) ? a : []).forEach((cs: any) => cs?.uuid && map.set(cs.uuid, { ...cs }));
    (Array.isArray(b) ? b : []).forEach((cs: any) => {
      if (!cs?.uuid) return;
      const prev = map.get(cs.uuid) || {};
      map.set(cs.uuid, { ...prev, ...cs });
    });
    return Array.from(map.values());
  };
  const mergeFieldLists = (lists: any[][]) => {
    const map = new Map<string, any>();
    lists.forEach(list => {
      (list ?? []).forEach((f: any) => {
        const uuid = f?.uuid;
        if (!uuid) return;
        const prev = map.get(uuid) || {};
        const merged = { ...prev, ...f };
        merged.cropSeasonsV2 = mergeSeasons(prev.cropSeasonsV2, f.cropSeasonsV2 ?? (f as any).cropSeasons);
        map.set(uuid, merged);
      });
    });
    return Array.from(map.values());
  };

  const primary =
    combinedOut?.response?.data?.fieldsV2 ??
    combinedOut?.response?.data?.fields ??
    combinedOut?.response?.fieldsV2 ??
    combinedOut?.response?.fields;
  if (primary && Array.isArray(primary) && primary.length > 0) return primary as Field[];
  const subs = combinedOut?._sub_responses ?? {};
  const candidateKeys = ['base', 'predictions', 'insights', 'risk1', 'risk2', 'tasks', 'tasks_sprayings'];
  const lists: any[][] = [];
  candidateKeys.forEach((key) => {
    const f =
      subs?.[key]?.response?.data?.fieldsV2 ??
      subs?.[key]?.response?.data?.fields ??
      subs?.[key]?.response?.fieldsV2 ??
      subs?.[key]?.response?.fields;
    if (Array.isArray(f) && f.length > 0) lists.push(f as any[]);
  });
  if (lists.length > 0) return mergeFieldLists(lists) as Field[];
  return [];
};

// Custom Hook for Data Processing
// =============================================================================

const useGroupedPredictions = (): GroupedPrediction[] => {
  const { combinedOut } = useData();
  const [locationByFieldUuid, setLocationByFieldUuid] = useState<Record<string, Partial<NonNullable<Field['location']>>>>(
    () => getSessionCache<Record<string, Partial<NonNullable<Field['location']>>>>(PREF_CITY_CACHE_KEY) ?? {},
  );
  const prefCityWorkerRef = useRef<Worker | null>(null);
  const prefCityPendingRef = useRef<Set<string>>(new Set());
  const prefCityWarmupRetryRef = useRef(0);
  const [prefCityDatasetReady, setPrefCityDatasetReady] = useState(false);
  const [prefCityWarmupError, setPrefCityWarmupError] = useState<string | null>(null);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      setSessionCache(PREF_CITY_CACHE_KEY, locationByFieldUuid);
    }, 250);
    return () => window.clearTimeout(timer);
  }, [locationByFieldUuid]);

  const fields = useMemo(() => selectFieldsFromCombinedOut(combinedOut), [combinedOut]);

  const mergeLocationPreferNonEmpty = useCallback(
    (base: Field['location'], override: Partial<NonNullable<Field['location']>> | undefined) => {
      if (!override) return base;
      const out: any = { ...(base ?? {}) };
      Object.entries(override).forEach(([k, v]) => {
        if (v === null || v === undefined) return;
        if (typeof v === 'string' && v.trim() === '') return;
        out[k] = v;
      });
      return out as Field['location'];
    },
    [],
  );

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (prefCityWorkerRef.current) return;
    const worker = new Worker(new URL('../workers/prefCityReverseGeocode.ts', import.meta.url), { type: 'module' });
    prefCityWorkerRef.current = worker;
    let cancelled = false;
    prefCityWarmupRetryRef.current = 0;

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
          setPrefCityWarmupError(String(data.error ?? 'unknown error'));
          if (!cancelled && prefCityWarmupRetryRef.current < 2) {
            prefCityWarmupRetryRef.current += 1;
            window.setTimeout(() => preloadDataset(1), 800 * prefCityWarmupRetryRef.current);
          }
          return;
        }
        setPrefCityWarmupError(null);
        setPrefCityDatasetReady(true);
        return;
      }
      if (data.type === 'ready') {
        setPrefCityDatasetReady(Boolean(data.loaded));
        return;
      }
      if (data.type !== 'result') return;
      const id = String(data.id ?? '');
      prefCityPendingRef.current.delete(id);
      if (!id) return;
      if (data.error) {
        if (import.meta.env.DEV) {
          console.warn('[pref-city] lookup failed', { id, error: data.error });
        }
        return;
      }
      if (!data.location) return;
	      setLocationByFieldUuid((prev) => ({
	        ...prev,
	        [id]: {
          ...(prev[id] ?? {}),
          prefecture: data.location.prefecture ?? null,
          municipality: data.location.municipality ?? null,
          subMunicipality: data.location.subMunicipality ?? null,
          cityCode: data.location.cityCode ?? null,
        },
	      }));
	    };
	    worker.onerror = (e: any) => {
	      const msg = String(e?.message ?? e ?? 'unknown worker error');
	      setPrefCityDatasetReady(false);
	      setPrefCityWarmupError(msg);
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
    if (!import.meta.env.DEV) return;
    if (!prefCityWarmupError) return;
    console.warn('[pref-city] warmup failed', { error: prefCityWarmupError });
  }, [prefCityWarmupError]);

  useEffect(() => {
    const worker = prefCityWorkerRef.current;
    if (!worker) return;
    if (!prefCityDatasetReady) return;
    fields.forEach((field) => {
      if (!field?.uuid) return;
      const hasPrefCity = Boolean(field.location?.prefecture) && Boolean(field.location?.municipality);
      if (hasPrefCity) return;
      if (locationByFieldUuid[field.uuid]) return;
      if (prefCityPendingRef.current.has(field.uuid)) return;
      const center = getFieldCenter(field);
      const lat = center?.latitude;
      const lon = center?.longitude;
      if (typeof lat !== 'number' || typeof lon !== 'number') return;
      prefCityPendingRef.current.add(field.uuid);
      worker.postMessage({ type: 'lookup', id: field.uuid, lat, lon });
    });
  }, [fields, locationByFieldUuid, prefCityDatasetReady]);

  return useMemo(() => {
    if (!fields || fields.length === 0) return [];

    const allPredictions = fields.flatMap(field => {
      const seasons = field.cropSeasonsV2 ?? [];
      const seasonsWithPred = seasons.filter((season: CropSeason) =>
        Array.isArray(season.countryCropGrowthStagePredictions) && season.countryCropGrowthStagePredictions.length > 0,
      );
      const attachCropEstablishToSeasonUuid = seasonsWithPred.length > 0 ? seasonsWithPred[0].uuid : null;

        return seasons.map((season: CropSeason) => {
          const locationOverride = locationByFieldUuid[field.uuid];
          const effectiveLocation = mergeLocationPreferNonEmpty(field.location, locationOverride);
          const farmName = field.farmV2?.name ?? (field as any)?.farm?.name ?? '';
          const cropEstablishmentTasks =
            attachCropEstablishToSeasonUuid && season.uuid === attachCropEstablishToSeasonUuid
              ? (field.cropEstablishments ?? [])
              : [];
          const sprayingTypeKeyForTask = (task: BaseTask) => {
            const hint = (task.creationFlowHint ?? task.dosedMap?.creationFlowHint ?? '').toUpperCase();
            if (hint === 'CROP_PROTECTION') return 'tasks.type.spraying_crop_protection';
            if (hint === 'WEED_MANAGEMENT') return 'tasks.type.spraying_weed_management';
            if (hint === 'NUTRITION_MANAGEMENT') return 'tasks.type.spraying_nutrition';
            return 'tasks.type.spraying_other';
          };
          return {
            seasonUuid: season.uuid,
            farmName,
            fieldName: field.name,
            cropName: (season as any)?.crop?.name ?? '',
            varietyName: (season as any)?.variety?.name ?? '',
            seasonStartDate: season.startDate ?? '',
          plantingMethodCode: season.cropEstablishmentMethodCode ?? null,
          predictions: Array.isArray(season.countryCropGrowthStagePredictions) ? season.countryCropGrowthStagePredictions : [],
          prefecture: formatPrefectureDisplay(effectiveLocation) || null,
          municipalityLabel: formatMunicipalityDisplay(effectiveLocation) || null,
          tasks: [
            ...((cropEstablishmentTasks ?? []).map(task => ({ typeKey: 'tasks.type.crop_establishment', task }))),
            ...((season.harvests ?? []).map(task => ({ typeKey: 'tasks.type.harvest', task }))),
            ...((season.sprayingsV2 ?? []).map(task => ({ typeKey: sprayingTypeKeyForTask(task), task }))),
            ...((season.waterManagementTasks ?? []).map(task => ({ typeKey: 'tasks.type.water_management', task }))),
            ...((season.scoutingTasks ?? []).map(task => ({ typeKey: 'tasks.type.scouting', task }))),
            ...((season.landPreparations ?? []).map(task => ({ typeKey: 'tasks.type.land_preparation', task }))),
            ...((season.seedTreatmentTasks ?? []).map(task => ({ typeKey: 'tasks.type.seed_treatment', task }))),
            ...((season.seedBoxTreatments ?? []).map(task => ({ typeKey: 'tasks.type.seed_box_treatment', task }))),
          ],
          };
        });
    });

    // 作付開始日でソート
    return allPredictions.sort((a, b) => {
      const aMs = Date.parse(a.seasonStartDate || '') || 0;
      const bMs = Date.parse(b.seasonStartDate || '') || 0;
      return aMs - bMs;
    });
  }, [fields, locationByFieldUuid, mergeLocationPreferNonEmpty]);
};

const useTimelineData = (groupedPredictions: GroupedPrediction[], enabledStages: string[]): TimelineData => {
  return useMemo(() => {
    if (groupedPredictions.length === 0) {
      return { chartData: null, chartHeight: 0, maxDate: null, minDate: null };
    }

    type NormalizedPrediction = CountryCropGrowthStagePrediction & {
      startMs: number;
      endMs: number;
      stageName: string;
    };

    const processedGroups = groupedPredictions
      .map(group => {
        const normalized: NormalizedPrediction[] = group.predictions
          .map(prediction => {
            if (!prediction.index) {
              return null;
            }
            const startMs = Date.parse(prediction.startDate);
            if (!Number.isFinite(startMs)) {
              return null;
            }
            const rawEndMs = prediction.endDate ? Date.parse(prediction.endDate) : NaN;
            let endMs = Number.isFinite(rawEndMs) ? rawEndMs : startMs;
            if (endMs <= startMs) {
              endMs = addDays(new Date(startMs), 1).getTime();
            }
            return {
              ...prediction,
              startMs,
              endMs,
              stageName: prediction.cropGrowthStageV2?.name ?? tr('gsp.stage.unknown'),
            };
          })
          .filter((p): p is NormalizedPrediction => Boolean(p));

        if (normalized.length === 0) {
          return null;
        }

        return { ...group, normalized };
      })
      .filter((group): group is GroupedPrediction & { normalized: NormalizedPrediction[] } => Boolean(group));

    if (processedGroups.length === 0) {
      return { chartData: null, chartHeight: 0, maxDate: null, minDate: null };
    }

    const labels = processedGroups.map(g => `${g.fieldName} (${g.varietyName})`);
    const datasets: ChartJSDataset[] = [];
    let maxDate: Date | null = null;
    let minDate: Date | null = null;

    const stageColors = [
      '#4E79A7', '#F28E2B', '#E15759', '#76B7B2', '#59A14F',
      '#EDC948', '#B07AA1', '#FF9DA7', '#9C755F', '#BAB0AC'
    ];

    const bbchIndices = [...new Set(processedGroups.flatMap(g => g.normalized.map(p => p.index)))].sort(compareBbchIndex);

    bbchIndices.forEach((bbchIndex, i) => {
      if (enabledStages.length > 0 && !enabledStages.includes(bbchIndex)) {
        return;
      }
      const dataset: ChartJSDataset = {
        label: `BBCH ${bbchIndex}`,
        bbchIndex: bbchIndex,
        data: [],
        backgroundColor: stageColors[i % stageColors.length],
        barPercentage: 0.6,
        categoryPercentage: 0.8,
      };

      processedGroups.forEach(group => {
        const prediction = group.normalized.find(p => p.index === bbchIndex);
        if (!prediction) return;

        const startDate = new Date(prediction.startMs);
        const endDate = new Date(prediction.endMs);

        if (!minDate || startDate < minDate) {
          minDate = startDate;
        }

        if (!maxDate || endDate > maxDate) {
          maxDate = endDate;
        }

        dataset.data.push({
          x: [prediction.startMs, prediction.endMs],
          y: `${group.fieldName} (${group.varietyName})`,
          stageName: prediction.stageName,
        });
      });

      // データが1つ以上ある場合のみデータセットを追加
      if (dataset.data.length > 0) {
        datasets.push(dataset);
      }
    });

    const chartData = { labels, datasets };
    const chartHeight = Math.max(200, labels.length * 50 + 100);

    return { chartData, chartHeight, maxDate, minDate };
  }, [groupedPredictions, enabledStages]);
};

const getTextColorForBg = (bgColor: string): string => {
  const color = (bgColor.charAt(0) === '#') ? bgColor.substring(1, 7) : bgColor;
  const r = parseInt(color.substring(0, 2), 16);
  const g = parseInt(color.substring(2, 4), 16);
  const b = parseInt(color.substring(4, 6), 16);
  return (r * 0.299 + g * 0.587 + b * 0.114) > 186 ? '#000000' : '#FFFFFF';
};

const parseBbchIndex = (value: string): number => {
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : Number.POSITIVE_INFINITY;
};

const compareBbchIndex = (a: string, b: string): number => {
  const diff = parseBbchIndex(a) - parseBbchIndex(b);
  return diff !== 0 ? diff : a.localeCompare(b);
};

// =============================================================================
// Main Component
// =============================================================================

export function GrowthStagePredictionPage() {
  const {
    combinedOut,
    combinedLoading,
    combinedInProgress,
    combinedErr,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();
  const { submittedFarms, fetchCombinedDataIfNeeded } = useFarms();
  const { language, t } = useLanguage();
  const collator = useMemo(() => new Intl.Collator(language === 'en' ? 'en' : 'ja'), [language]);
  const groupedPredictions = useGroupedPredictions();
  const [fieldQuery, setFieldQuery] = useState('');
  const [selectedCrop, setSelectedCrop] = useState<string>('ALL');
  const [selectedVariety, setSelectedVariety] = useState<string>('ALL');
  const [selectedPrefecture, setSelectedPrefecture] = useState<string>('ALL');
  const [selectedMunicipality, setSelectedMunicipality] = useState<string>('ALL');
  const [predictionPresence, setPredictionPresence] = useState<'ALL' | 'HAS' | 'NONE'>('ALL');
  const [enabledStages, setEnabledStages] = useState<string[]>([]);
  const [rowsPerPage, setRowsPerPage] = useState<number>(10);
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [tableRowsPerPage, setTableRowsPerPage] = useState<number>(10);
  const [tableCurrentPage, setTableCurrentPage] = useState<number>(1);
  const [isUpcomingExpanded, setIsUpcomingExpanded] = useState<boolean>(false);
  const today = useMemo(() => startOfDay(new Date()), []);

  const formatPlantingMethod = useCallback((methodCode: string | null | undefined) => {
    if (!methodCode) return '-';
    if (methodCode === 'TRANSPLANTING') return t('fmt.crop_method.transplanting');
    if (methodCode === 'DIRECT_SEEDING') return t('fmt.crop_method.direct_seeding');
    if (methodCode === 'MYKOS_DRY_DIRECT_SEEDING') return t('fmt.crop_method.mykos_dry_direct_seeding');
    return methodCode;
  }, [t]);

  const TASK_TYPE_COLUMNS = useMemo(
    () =>
      [
        { typeKey: 'tasks.type.crop_establishment', header: t('tasks.type.crop_establishment') },
        { typeKey: 'tasks.type.land_preparation', header: t('tasks.type.land_preparation') },
        { typeKey: 'tasks.type.seed_treatment', header: t('tasks.type.seed_treatment') },
        { typeKey: 'tasks.type.seed_box_treatment', header: t('tasks.type.seed_box_treatment') },
        { typeKey: 'tasks.type.spraying_crop_protection', header: t('tasks.type.spraying_crop_protection') },
        { typeKey: 'tasks.type.spraying_weed_management', header: t('tasks.type.spraying_weed_management') },
        { typeKey: 'tasks.type.spraying_nutrition', header: t('tasks.type.spraying_nutrition') },
        { typeKey: 'tasks.type.spraying_other', header: t('tasks.type.spraying_other') },
        { typeKey: 'tasks.type.water_management', header: t('tasks.type.water_management') },
        { typeKey: 'tasks.type.scouting', header: t('tasks.type.scouting') },
        { typeKey: 'tasks.type.harvest', header: t('tasks.type.harvest') },
      ] as const,
    [t]
  );

  const cropOptions = useMemo(() => {
    const set = new Set<string>();
    groupedPredictions.forEach(group => set.add(group.cropName));
    return Array.from(set).sort(collator.compare);
  }, [groupedPredictions, collator]);

  const varietyOptions = useMemo(() => {
    const targetGroups = selectedCrop === 'ALL'
      ? groupedPredictions
      : groupedPredictions.filter(group => group.cropName === selectedCrop);
    const set = new Set<string>();
    targetGroups.forEach(group => set.add(group.varietyName));
    return Array.from(set).sort(collator.compare);
  }, [groupedPredictions, selectedCrop, collator]);

  const prefectureOptions = useMemo(() => {
    const set = new Set<string>();
    groupedPredictions.forEach(group => {
      if (group.prefecture) {
        set.add(group.prefecture);
      }
    });
    return Array.from(set).sort(collator.compare);
  }, [groupedPredictions, collator]);

  const municipalityOptions = useMemo(() => {
    const targetGroups = selectedPrefecture === 'ALL'
      ? groupedPredictions
      : groupedPredictions.filter(group => group.prefecture === selectedPrefecture);
    const set = new Set<string>();
    targetGroups.forEach(group => {
      if (group.municipalityLabel) {
        set.add(group.municipalityLabel);
      }
    });
    return Array.from(set).sort(collator.compare);
  }, [groupedPredictions, selectedPrefecture, collator]);

  useEffect(() => {
    if (selectedVariety === 'ALL') return;
    if (!varietyOptions.includes(selectedVariety)) {
      setSelectedVariety('ALL');
    }
  }, [varietyOptions, selectedVariety]);

  useEffect(() => {
    if (selectedMunicipality === 'ALL') return;
    if (!municipalityOptions.includes(selectedMunicipality)) {
      setSelectedMunicipality('ALL');
    }
  }, [municipalityOptions, selectedMunicipality]);

  const filteredPredictions = useMemo(() => {
    const lowerQuery = fieldQuery.trim().toLowerCase();
    return groupedPredictions.filter(group => {
      const cropMatch = selectedCrop === 'ALL' || group.cropName === selectedCrop;
      const varietyMatch = selectedVariety === 'ALL' || group.varietyName === selectedVariety;
      const prefectureMatch = selectedPrefecture === 'ALL' || group.prefecture === selectedPrefecture;
      const municipalityMatch = selectedMunicipality === 'ALL' || group.municipalityLabel === selectedMunicipality;
      const hasNextStagePrediction =
        Array.isArray(group.predictions) &&
        group.predictions.some((pred) => {
          if (!pred?.index) return false;
          const ms = Date.parse(pred.startDate);
          return Number.isFinite(ms) && ms >= today.getTime();
        });
      const predictionMatch =
        predictionPresence === 'ALL' ||
        (predictionPresence === 'HAS' ? hasNextStagePrediction : !hasNextStagePrediction);
      const queryMatch =
        lowerQuery.length === 0 ||
        group.fieldName.toLowerCase().includes(lowerQuery) ||
        group.varietyName.toLowerCase().includes(lowerQuery) ||
        group.cropName.toLowerCase().includes(lowerQuery);
      return cropMatch && varietyMatch && prefectureMatch && municipalityMatch && predictionMatch && queryMatch;
    });
  }, [
    groupedPredictions,
    selectedCrop,
    selectedVariety,
    selectedPrefecture,
    selectedMunicipality,
    predictionPresence,
    fieldQuery,
    today,
  ]);

  const activeTaskTypeColumns = useMemo(() => {
    const used = new Set<string>();
    filteredPredictions.forEach(group => {
      group.tasks.forEach(({ typeKey, task }) => {
        if (task.plannedDate) used.add(typeKey);
      });
    });
    return TASK_TYPE_COLUMNS.filter(col => used.has(col.typeKey));
  }, [TASK_TYPE_COLUMNS, filteredPredictions]);

  useEffect(() => {
    setCurrentPage(1);
    setTableCurrentPage(1);
  }, [selectedCrop, selectedVariety, selectedPrefecture, selectedMunicipality, predictionPresence, fieldQuery]);

  const filteredCount = filteredPredictions.length;
  const totalPages = Math.max(1, Math.ceil(filteredCount / rowsPerPage));

  useEffect(() => {
    setCurrentPage(prev => {
      const next = Math.min(Math.max(prev, 1), totalPages);
      return next;
    });
  }, [totalPages]);

  const paginatedPredictions = useMemo(() => {
    if (filteredCount === 0) return [];
    const start = (currentPage - 1) * rowsPerPage;
    return filteredPredictions.slice(start, start + rowsPerPage);
  }, [filteredPredictions, currentPage, rowsPerPage, filteredCount]);

  const tableTotalPages = Math.max(1, Math.ceil(filteredCount / tableRowsPerPage));

  useEffect(() => {
    setTableCurrentPage(prev => {
      const next = Math.min(Math.max(prev, 1), tableTotalPages);
      return next;
    });
  }, [tableTotalPages]);

  const tablePagePredictions = useMemo(() => {
    if (filteredCount === 0) return [];
    const start = (tableCurrentPage - 1) * tableRowsPerPage;
    return filteredPredictions.slice(start, start + tableRowsPerPage);
  }, [filteredPredictions, tableCurrentPage, tableRowsPerPage, filteredCount]);

  const availableStages = useMemo(() => {
    const indices = new Set<string>();
    filteredPredictions.forEach(group => {
      group.predictions.forEach(pred => {
        if (pred.index) {
          indices.add(pred.index);
        }
      });
    });
    return Array.from(indices).sort(compareBbchIndex);
  }, [filteredPredictions]);

  const stageMeta = useMemo(() => {
    const map = new Map<string, string>();
    filteredPredictions.forEach(group => {
      group.predictions.forEach(pred => {
        if (!pred.index) return;
        if (!map.has(pred.index)) {
          map.set(pred.index, pred.cropGrowthStageV2?.name ?? '');
        } else if (!map.get(pred.index) && pred.cropGrowthStageV2?.name) {
          map.set(pred.index, pred.cropGrowthStageV2.name);
        }
      });
    });
    return map;
  }, [filteredPredictions]);

  const stageColumns = useMemo(
    () =>
      availableStages.map(index => {
        const name = stageMeta.get(index) ?? '';
        return {
          index,
          label: name ? `BBCH${index}(${name})` : `BBCH${index}`,
        };
      }),
    [availableStages, stageMeta]
  );

  useEffect(() => {
    setEnabledStages(prev => {
      if (availableStages.length === 0) {
        return prev.length === 0 ? prev : [];
      }
      if (prev.length === 0) {
        return availableStages;
      }
      const valid = prev.filter(stage => availableStages.includes(stage));
      const missing = availableStages.filter(stage => !valid.includes(stage));
      if (missing.length === 0 && valid.length === prev.length) {
        const unchanged = valid.every((stage, idx) => stage === prev[idx]);
        if (unchanged) {
          return prev;
        }
      }
      return [...valid, ...missing];
    });
  }, [availableStages]);

  const toggleStage = (stage: string) => {
    setEnabledStages(prev => {
      if (prev.includes(stage)) {
        return prev.filter(s => s !== stage);
      }
      return [...prev, stage].sort(compareBbchIndex);
    });
  };

  const UPCOMING_WINDOW_DAYS = 21;
  const upcomingStages = useMemo((): UpcomingStageItem[] => {
    const limit = addDays(today, UPCOMING_WINDOW_DAYS);
    const upcoming = filteredPredictions.flatMap(group =>
      group.predictions
        .map(pred => {
          const startMs = Date.parse(pred.startDate);
          if (!Number.isFinite(startMs)) return null;
          const start = new Date(startMs);
          if (start < today || start > limit) return null;
          const rawEnd = pred.endDate ? Date.parse(pred.endDate) : NaN;
          const end = Number.isFinite(rawEnd) ? new Date(rawEnd) : addDays(start, 1);
          return {
            fieldName: group.fieldName,
            cropName: group.cropName,
            varietyName: group.varietyName,
            stageIndex: pred.index,
            stageName: pred.cropGrowthStageV2?.name ?? '',
            start,
            end,
          };
        })
        .filter((item): item is UpcomingStageItem => Boolean(item))
    );
    return upcoming
      .sort((a, b) => a.start.getTime() - b.start.getTime())
      .slice(0, 15);
  }, [filteredPredictions, today]);

  const deriveTableRow = useCallback((group: GroupedPrediction) => {
    const futureStages = group.predictions
      .map(pred => ({
        ...pred,
        start: Date.parse(pred.startDate),
      }))
      .filter(pred => pred.index && Number.isFinite(pred.start) && pred.start! >= today.getTime())
      .sort((a, b) => (a.start! - b.start!));

    const nextStage = futureStages[0];
    const lastStage = group.predictions[group.predictions.length - 1];
    const stageDates: Record<string, string> = {};
    group.predictions.forEach(pred => {
      if (!pred.index) return;
      const startLabel = getLocalDateString(pred.startDate);
      if (!startLabel) return;
      const endLabel = pred.endDate ? formatInclusiveEndDate(pred.endDate) : '';
      stageDates[pred.index] = endLabel ? `${startLabel}〜${endLabel}` : startLabel;
    });

    const taskDatesByTypeKey = new Map<string, string[]>();
    group.tasks.forEach(({ typeKey, task }) => {
      const iso = task.plannedDate;
      if (!iso) return;
      const ms = Date.parse(iso);
      if (!Number.isFinite(ms)) return;
      const label = format(new Date(ms), 'yyyy/MM/dd');
      const prev = taskDatesByTypeKey.get(typeKey) ?? [];
      prev.push(label);
      taskDatesByTypeKey.set(typeKey, prev);
    });

    TASK_TYPE_COLUMNS.forEach(col => {
      const list = taskDatesByTypeKey.get(col.typeKey) ?? [];
      list.sort();
      const deduped = Array.from(new Set(list));
      taskDatesByTypeKey.set(col.typeKey, deduped);
    });

    return {
      farmName: group.farmName,
      fieldName: group.fieldName,
      prefecture: group.prefecture,
      municipalityLabel: group.municipalityLabel,
      cropName: group.cropName,
      varietyName: group.varietyName,
      seasonStartDate: group.seasonStartDate,
      plantingMethod: formatPlantingMethod(group.plantingMethodCode),
      nextStageIndex: nextStage?.index ?? null,
      nextStageName: nextStage?.cropGrowthStageV2?.name ?? null,
      nextStageStart: nextStage ? new Date(nextStage.start!) : null,
      lastStageIndex: lastStage?.index ?? null,
      lastStageName: lastStage?.cropGrowthStageV2?.name ?? null,
      stageDates,
      taskDatesByTypeKey,
    };
  }, [today, TASK_TYPE_COLUMNS, formatPlantingMethod]);

  const tableRows = useMemo(() => tablePagePredictions.map(deriveTableRow), [tablePagePredictions, deriveTableRow]);
  const csvTableRows = useMemo(() => filteredPredictions.map(deriveTableRow), [filteredPredictions, deriveTableRow]);

  const activeTaskDateColumns = useMemo(() => {
    const columns: Array<{ typeKey: string; occurrence: number; header: string }> = [];
    activeTaskTypeColumns.forEach(typeCol => {
      const maxCount = csvTableRows.reduce((max, row) => {
        const count = row.taskDatesByTypeKey?.get(typeCol.typeKey)?.length ?? 0;
        return Math.max(max, count);
      }, 0);
      for (let i = 0; i < maxCount; i++) {
        columns.push({
          typeKey: typeCol.typeKey,
          occurrence: i,
          header: `${typeCol.header} (${i + 1})`,
        });
      }
    });
    return columns;
  }, [activeTaskTypeColumns, csvTableRows]);

  const handleRowsPerPageChange = (value: number) => {
    setRowsPerPage(value);
    setCurrentPage(1);
  };

  const handlePageChange = (delta: number) => {
    setCurrentPage(prev => {
      const next = Math.min(Math.max(prev + delta, 1), totalPages);
      return next;
    });
  };

  const handleTableRowsPerPageChange = (value: number) => {
    setTableRowsPerPage(value);
    setTableCurrentPage(1);
  };

  const handleTablePageChange = (delta: number) => {
    setTableCurrentPage(prev => {
      const next = Math.min(Math.max(prev + delta, 1), tableTotalPages);
      return next;
    });
  };

  const downloadTableCsv = () => {
    const targetRows = csvTableRows;
    if (targetRows.length === 0) return;

	    const headers = [
	      t('table.farm'),
	      t('table.field'),
	      t('table.prefecture'),
	      t('table.municipality'),
	      t('table.crop'),
	      t('gsp.filter.variety'),
	      t('table.planting_date'),
	      t('table.planting_method'),
	      t('gsp.table.next_stage'),
	      ...activeTaskDateColumns.map(col => col.header),
	      ...stageColumns.map(col => col.label),
	    ];

    const rows = targetRows.map(row => {
      const nextStageLabel = row.nextStageIndex
        ? `BBCH ${row.nextStageIndex}${row.nextStageName ? ` - ${row.nextStageName}` : ''}`
        : t('gsp.value.none');
			      const cells = [
			        row.farmName || '-',
			        row.fieldName,
			        row.prefecture?.trim() ? row.prefecture : '-',
			        row.municipalityLabel?.trim() ? row.municipalityLabel : '-',
			        row.cropName,
			        row.varietyName,
			        row.seasonStartDate ? format(new Date(row.seasonStartDate), 'yyyy/MM/dd') : t('gsp.value.unknown'),
			        row.plantingMethod ?? '-',
			        nextStageLabel,
	            ...activeTaskDateColumns.map(col => {
	              const dates = row.taskDatesByTypeKey?.get(col.typeKey) ?? [];
	              return dates[col.occurrence] ?? '-';
	            }),
			        ...stageColumns.map(col => row.stageDates[col.index] ?? '-'),
			      ];

      return cells
        .map(cell => `"${String(cell ?? '').replace(/"/g, '""')}"`)
        .join(',');
    });

    const bom = new Uint8Array([0xef, 0xbb, 0xbf]);
    const csvContent = [headers.join(','), ...rows].join('\n');
    const blob = new Blob([bom, csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `growth_stage_table_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  useEffect(() => {
    fetchCombinedDataIfNeeded({ includeTasks: true });
  }, [fetchCombinedDataIfNeeded]);

  const isCombinedOutMatchingSelection = useMemo(() => {
    if (!combinedOut) return false;
    const payload: any = (combinedOut as any)?.request?.payload ?? {};
    const farms: string[] = Array.isArray(payload.farm_uuids)
      ? payload.farm_uuids
      : Array.isArray(payload.farmUuids)
        ? payload.farmUuids
        : [];
    const a = [...submittedFarms].slice().sort();
    const b = [...farms].slice().sort();
    return a.length > 0 && JSON.stringify(a) === JSON.stringify(b);
  }, [combinedOut, submittedFarms]);

  if (submittedFarms.length === 0) {
    return (
      <div className="farms-page-container">
        <h2>{t('gsp.title')}</h2>
        <p>{t('risk.select_farm_hint')}</p>
      </div>
    );
  }

  if (combinedLoading) {
    return (
      <div className="farms-page-container">
        <LoadingOverlay
          message={formatCombinedLoadingMessage(
            t('gsp.loading_label'),
            combinedFetchAttempt,
            combinedFetchMaxAttempts,
            combinedRetryCountdown,
          )}
        />
        <h2>{t('gsp.title')}</h2>
      </div>
    );
  }

  // When farm selection changes, FarmContext intentionally keeps the previous data visible while fetching.
  // On this page we prefer not to show stale predictions for a different selection, so show a loading overlay instead.
  if (!isCombinedOutMatchingSelection && combinedInProgress) {
    return (
      <div className="farms-page-container">
        <LoadingOverlay
          message={formatCombinedLoadingMessage(
            t('gsp.loading_label'),
            combinedFetchAttempt,
            combinedFetchMaxAttempts,
            combinedRetryCountdown,
          )}
        />
        <h2>{t('gsp.title')}</h2>
      </div>
    );
  }

  if (combinedErr) {
    return (
      <div className="farms-page-container">
        <h2>{t('gsp.title')}</h2>
        <h3 style={{ color: '#ff6b6b' }}>{t('gsp.load_failed')}</h3>
        <pre style={{ color: '#ff6b6b', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {combinedErr}
        </pre>
      </div>
    );
  }

  return (
    <div className="farms-page-container">
      <h2>{t('gsp.title')}</h2>
      <p>
        {t('gsp.summary', { count: submittedFarms.length })}
        {combinedOut?.source && (
          <span style={{ marginLeft: '1em', color: combinedOut.source === 'cache' ? '#4caf50' : '#2196f3', fontWeight: 'bold' }}>
            ({combinedOut.source === 'cache' ? t('source.cache') : t('source.api')})
          </span>
        )}
      </p>

      <div className="gsp-controls">
        <div className="gsp-filter-grid">
          <label>
            {t('gsp.filter.field_crop')}
            <input
              type="text"
              value={fieldQuery}
              onChange={(e) => setFieldQuery(e.currentTarget.value)}
              placeholder={t('gsp.filter.field_crop_ph')}
            />
          </label>
          <label>
            {t('gsp.filter.crop')}
            <select value={selectedCrop} onChange={(e) => setSelectedCrop(e.currentTarget.value)}>
              <option value="ALL">{t('satellite.option.all')}</option>
              {cropOptions.map(crop => (
                <option key={crop} value={crop}>{crop}</option>
              ))}
            </select>
          </label>
          <label>
            {t('gsp.filter.variety')}
            <select value={selectedVariety} onChange={(e) => setSelectedVariety(e.currentTarget.value)}>
              <option value="ALL">{t('satellite.option.all')}</option>
              {varietyOptions.map(variety => (
                <option key={variety} value={variety}>{variety}</option>
              ))}
            </select>
          </label>
          <label>
            {t('gsp.filter.prefecture')}
            <select
              value={selectedPrefecture}
              onChange={(e) => {
                const next = e.currentTarget.value;
                setSelectedPrefecture(next);
                setSelectedMunicipality('ALL');
              }}
            >
              <option value="ALL">{t('satellite.option.all')}</option>
              {prefectureOptions.map(pref => (
                <option key={pref} value={pref}>{pref}</option>
              ))}
            </select>
          </label>
          <label>
            {t('gsp.filter.municipality')}
            <select
              value={selectedMunicipality}
              onChange={(e) => setSelectedMunicipality(e.currentTarget.value)}
            >
              <option value="ALL">{t('satellite.option.all')}</option>
              {municipalityOptions.map(muni => (
                <option key={muni} value={muni}>{muni}</option>
              ))}
            </select>
          </label>
          <label>
            {language === 'en' ? 'Next-stage prediction' : '次ステージ予測'}
            <select
              value={predictionPresence}
              onChange={(e) => setPredictionPresence(e.currentTarget.value as any)}
            >
              <option value="ALL">{t('satellite.option.all')}</option>
              <option value="HAS">{language === 'en' ? 'Not null' : 'あり'}</option>
              <option value="NONE">{language === 'en' ? 'Null' : 'なし'}</option>
            </select>
          </label>
        </div>

	        <div className="gsp-stage-filters">
	          <div className="gsp-stage-header">
	            <strong>{t('gsp.controls.stage_list_title')}</strong>
	            <div className="gsp-stage-actions">
              <button
                type="button"
                disabled={availableStages.length === 0}
                onClick={() => setEnabledStages(availableStages)}
	              >
	                {t('gsp.controls.select_all')}
	              </button>
              <button
                type="button"
                disabled={enabledStages.length === 0}
                onClick={() => setEnabledStages([])}
	              >
	                {t('gsp.controls.clear_all')}
	              </button>
	            </div>
	          </div>
	          <div className="gsp-stage-checkboxes">
	            {availableStages.length === 0 ? (
	              <span>{t('gsp.controls.no_stages')}</span>
	            ) : (
              availableStages.map(stage => (
                <label key={stage} className="gsp-stage-checkbox">
                  <input
                    type="checkbox"
                    checked={enabledStages.includes(stage)}
                    onChange={() => toggleStage(stage)}
                  />
                  <span>BBCH {stage}</span>
	                </label>
	              ))
	            )}
	          </div>
	        </div>
	      </div>

	      <div className="gsp-instructions">
	        <span>{t('gsp.hint.zoom_pan')}</span>
	      </div>

	      {upcomingStages.length > 0 && (
	        <div className="gsp-upcoming">
	          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '1rem' }}>
	            <h3 style={{ margin: 0 }}>{t('gsp.upcoming.title', { days: UPCOMING_WINDOW_DAYS })}</h3>
	            <button
              type="button"
              onClick={() => setIsUpcomingExpanded(prev => !prev)}
              aria-expanded={isUpcomingExpanded}
              aria-controls="gsp-upcoming-list"
	            >
	              {isUpcomingExpanded ? t('gsp.toggle.collapse') : t('gsp.toggle.open')}
	            </button>
	          </div>
          {isUpcomingExpanded && (
            <ul id="gsp-upcoming-list">
              {upcomingStages.map((stage, idx) => (
                <li key={`${stage.fieldName}-${stage.stageIndex}-${idx}`}>
                  <strong>{stage.fieldName}</strong> / {stage.cropName}（{stage.varietyName}） : BBCH {stage.stageIndex} {stage.stageName ? `- ${stage.stageName}` : ''}
	                  <span style={{ marginLeft: '0.5rem' }}>
	                    {t('gsp.upcoming.start', { date: format(stage.start, 'MM/dd') })}
	                  </span>
	                </li>
	              ))}
	            </ul>
	          )}
	        </div>
	      )}

	      <div className="gsp-summary">
	        {t('gsp.summary.filtered', {
	          filtered: filteredCount,
	          timeline: filteredCount === 0 ? 0 : paginatedPredictions.length,
	          table: filteredCount === 0 ? 0 : tablePagePredictions.length,
	          enabled: enabledStages.length,
	          total: availableStages.length,
	        })}
	      </div>

	      {filteredCount > 0 && (
	        <div className="gsp-pagination-controls">
	          <div className="gsp-pagination-left">
	            <label htmlFor="gsp-rows-per-page">
	              {t('gsp.rows_per_page')}{' '}
              <select
                id="gsp-rows-per-page"
                value={rowsPerPage}
                onChange={(e) => handleRowsPerPageChange(Number(e.currentTarget.value))}
              >
                {[5, 10, 20, 50].map(size => (
                  <option key={size} value={size}>{size}</option>
                ))}
              </select>
            </label>
          </div>
	          <div className="gsp-pagination-right">
            <button
              type="button"
              onClick={() => handlePageChange(-1)}
              disabled={currentPage <= 1}
            >
	              {t('pagination.prev')}
            </button>
            <span style={{ margin: '0 0.75rem' }}>
              {currentPage} / {totalPages}
            </span>
            <button
              type="button"
              onClick={() => handlePageChange(1)}
              disabled={currentPage >= totalPages}
            >
	              {t('pagination.next')}
	            </button>
	          </div>
	        </div>
	      )}

      <div className="timeline-container">
        <GrowthStageTimeline items={paginatedPredictions} enabledStages={enabledStages} />
      </div>

		      {tableRows.length > 0 && (
		        <div className="gsp-table-wrapper">
		          <div className="gsp-table-header">
		            <h3>{t('gsp.section.seasons')}</h3>
		            <button
	              type="button"
	              onClick={downloadTableCsv}
	              disabled={csvTableRows.length === 0}
	              title={language === 'en' ? 'Export all filtered rows' : 'フィルタ後の全行をCSV出力'}
	            >
		              {t('gsp.action.csv_download')}
		            </button>
		          </div>
	          {filteredCount > 0 && (
	            <div className="gsp-pagination-controls gsp-table-pagination">
	              <div className="gsp-pagination-left">
	                <label htmlFor="gsp-table-rows-per-page">
	                  {t('gsp.rows_per_page')}{' '}
                  <select
                    id="gsp-table-rows-per-page"
                    value={tableRowsPerPage}
                    onChange={(e) => handleTableRowsPerPageChange(Number(e.currentTarget.value))}
                  >
                    {[5, 10, 20, 50].map(size => (
                      <option key={`table-size-${size}`} value={size}>{size}</option>
                    ))}
                  </select>
                </label>
              </div>
	              <div className="gsp-pagination-right">
                <button
                  type="button"
                  onClick={() => handleTablePageChange(-1)}
                  disabled={tableCurrentPage <= 1}
                >
	                  {t('pagination.prev')}
                </button>
                <span style={{ margin: '0 0.75rem' }}>
                  {tableCurrentPage} / {tableTotalPages}
                </span>
                <button
                  type="button"
                  onClick={() => handleTablePageChange(1)}
                  disabled={tableCurrentPage >= tableTotalPages}
                >
	                  {t('pagination.next')}
	                </button>
	              </div>
	            </div>
	          )}
		          <table className="gsp-table">
		            <thead>
			              <tr>
			                <th>{t('table.farm')}</th>
			                <th>{t('table.field')}</th>
			                <th>{t('table.prefecture')}</th>
			                <th>{t('table.municipality')}</th>
			                <th>{t('table.crop')}</th>
			                <th>{t('gsp.filter.variety')}</th>
			                <th>{t('table.planting_date')}</th>
			                <th>{t('table.planting_method')}</th>
			                <th>{t('gsp.table.next_stage')}</th>
                    {activeTaskDateColumns.map(col => (
                      <th key={`task-col-${col.typeKey}-${col.occurrence}`}>{col.header}</th>
                    ))}
		                {stageColumns.map(col => (
		                  <th key={`stage-col-${col.index}`}>{col.label}</th>
		                ))}
		              </tr>
	            </thead>
	            <tbody>
	              {tableRows.map(row => (
				                <tr key={`${row.fieldName}-${row.varietyName}-${row.seasonStartDate}`}>
				                  <td>{row.farmName?.trim() ? row.farmName : '-'}</td>
				                  <td>{row.fieldName}</td>
				                  <td>{row.prefecture?.trim() ? row.prefecture : '-'}</td>
				                  <td>{row.municipalityLabel?.trim() ? row.municipalityLabel : '-'}</td>
				                  <td>{row.cropName}</td>
			                  <td>{row.varietyName}</td>
			                  <td>{row.seasonStartDate ? format(new Date(row.seasonStartDate), 'yyyy/MM/dd') : t('gsp.value.unknown')}</td>
			                  <td>{row.plantingMethod ?? '-'}</td>
			                  <td>
			                    {row.nextStageIndex
			                      ? `BBCH ${row.nextStageIndex}${row.nextStageName ? ` - ${row.nextStageName}` : ''}`
			                      : t('gsp.value.none')}
			                  </td>
                      {activeTaskDateColumns.map(col => {
                        const dates = row.taskDatesByTypeKey?.get(col.typeKey) ?? [];
                        const display = dates[col.occurrence] ?? '-';
                        return (
                          <td key={`task-cell-${col.typeKey}-${col.occurrence}`} style={{ minWidth: 140 }}>
                            {display}
                          </td>
                        );
                      })}
	                  {stageColumns.map(col => (
	                    <td key={`stage-date-${col.index}`}>
	                      {row.stageDates[col.index] ?? '-'}
	                    </td>
	                  ))}
	                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

// =============================================================================
// Timeline Chart Component
// =============================================================================

const GrowthStageTimeline: FC<{ items: GroupedPrediction[]; enabledStages: string[] }> = memo(({ items, enabledStages }) => {
  const chartRef = useRef<ChartJSInstance | null>(null);
  const { chartData, chartHeight, maxDate, minDate } = useTimelineData(items, enabledStages);
  const { language, t } = useLanguage();
  const dateLocale = language === 'en' ? enUS : ja;

  if (!chartData || chartData.datasets.length === 0) {
    return <div className="chart-no-data">{t('gsp.chart.no_data')}</div>;
  }

  const today = new Date();
  const todayForTimeline = startOfDay(today);
  const DAY_MS = 24 * 60 * 60 * 1000;

  const limitMin = minDate ? minDate.getTime() : subDays(today, 100).getTime();
  const rawLimitMax = maxDate ? maxDate.getTime() : addDays(today, 180).getTime();
  const limitMax = Math.max(rawLimitMax, limitMin + DAY_MS);

  const halfWindow = 30 * DAY_MS;
  let initialViewMin = Math.max(limitMin, todayForTimeline.getTime() - halfWindow);
  let initialViewMax = Math.min(limitMax, todayForTimeline.getTime() + halfWindow);

  if (initialViewMax <= initialViewMin) {
    if (limitMax - limitMin >= DAY_MS) {
      initialViewMin = limitMin;
      initialViewMax = Math.min(limitMax, limitMin + 2 * halfWindow);
    } else {
      initialViewMin = limitMin;
      initialViewMax = limitMin + DAY_MS;
    }
  } else if (initialViewMax - initialViewMin < 14 * DAY_MS) {
    const needed = 14 * DAY_MS - (initialViewMax - initialViewMin);
    const expandEachSide = needed / 2;
    initialViewMin = Math.max(limitMin, initialViewMin - expandEachSide);
    initialViewMax = Math.min(limitMax, initialViewMax + expandEachSide);
    if (initialViewMax - initialViewMin < 7 * DAY_MS) {
      initialViewMax = Math.min(limitMax, initialViewMin + 7 * DAY_MS);
    }
  }

  const options = {
    indexAxis: 'y' as const,
    responsive: true,
    maintainAspectRatio: false,
    layout: { padding: { top: 30, bottom: 30 } },
    plugins: {
      legend: { display: false },
      tooltip: {
        callbacks: {
          title: (tooltipItems: TooltipItem<'bar'>[]): string => {
            return tooltipItems.length > 0 ? (tooltipItems[0].label || '') : '';
          },
          label: (context: TooltipItem<'bar'>): string => {
            const rawData = context.raw as { x: [number, number], stageName?: string };
            const datasetWithMeta = context.dataset as typeof context.dataset & { bbchIndex?: string };
            const bbchIndex = datasetWithMeta.bbchIndex ?? '';
            const stageName = rawData.stageName || '';
            const label = `BBCH ${bbchIndex}: ${stageName}`;
            if (rawData?.x) {
              const start = format(new Date(rawData.x[0]), 'MM/dd');
              const end = format(new Date(rawData.x[1]), 'MM/dd');
              return `${label} [${start} - ${end}]`;
            }
            return label;
          },
          footer: (tooltipItems: TooltipItem<'bar'>[]): string | string[] => {
            if (tooltipItems.length === 0) return '';
            const rawData = tooltipItems[0].raw as any;
            if (rawData?.x) {
              const start = new Date(rawData.x[0]);
              const end = new Date(rawData.x[1]);
              const duration = differenceInCalendarDays(end, start);
              return t('gsp.duration_days', { days: duration });
            }
            return '';
          }
        }
      },
      datalabels: {
        clip: true,
        display: (context: any) => {
          const value = context.dataset.data[context.dataIndex];
          if (!value?.x) return false;
          const scale = context.chart.scales.x;
          const [start, end] = value.x;
          if (end < scale.min || start > scale.max) return false;
          const visibleStart = Math.max(start, scale.min);
          const visibleEnd = Math.min(end, scale.max);
          const pixelWidth = scale.getPixelForValue(visibleEnd) - scale.getPixelForValue(visibleStart);
          return pixelWidth > 25;
        },
        formatter: (_value: any, context: any) => {
          const datasetWithMeta = context.dataset as typeof context.dataset & { bbchIndex?: string };
          return datasetWithMeta.bbchIndex ?? '';
        },
        color: (context: any) => getTextColorForBg(context.dataset.backgroundColor as string),
        font: { weight: 'bold', size: 12 },
        anchor: 'center' as const, align: 'center' as const,
      },
      zoom: {
        pan: { enabled: true, mode: 'x' as const },
        zoom: {
          wheel: { enabled: true },
          pinch: { enabled: true },
          mode: 'x' as const,
        },
        limits: {
          x: { min: limitMin, max: limitMax, minRange: 7 * 24 * 60 * 60 * 1000 }
        }
      },
      annotation: {
        annotations: {
	          todayLine: {
            type: 'line',
            xMin: todayForTimeline.getTime(),
            xMax: todayForTimeline.getTime(),
            borderColor: 'rgba(255, 99, 132, 0.8)',
            borderWidth: 2,
            borderDash: [6, 6],
	            label: {
	              content: t('chart.today'),
	              display: true,
	              position: 'start',
	            }
	          }
	        }
	      }
	    },
	    scales: {
	      x: {
	        type: 'time' as const, position: 'top' as const,
	        min: initialViewMin, max: initialViewMax,
	        adapters: { date: { locale: dateLocale } },
	        time: {
	          tooltipFormat: language === 'en' ? 'yyyy-MM-dd' : 'yyyy/MM/dd',
	          minUnit: 'day',
	          displayFormats: language === 'en'
	            ? { day: 'M/d', week: 'M/d', month: 'MMM yyyy', year: 'yyyy' }
	            : { day: 'M/d', week: 'M/d', month: 'yyyy年 M月', year: 'yyyy年' }
	        }
	      },
	      y: { stacked: true, ticks: { autoSkip: false } }
	    },
	  };

  return (
    <div>
      <div className="chart-controls">
        <button
          type="button"
          onClick={() => {
            if (chartRef.current) {
              // chartjs-plugin-zoom で追加されるメソッド
              (chartRef.current as any)?.resetZoom?.();
            }
          }}
	        >
	          {t('gsp.chart.reset_view')}
	        </button>
	      </div>
      <div className="gantt-chart-wrapper" style={{ height: `${chartHeight}px` }}>
        <Bar
          ref={(instance) => {
            chartRef.current = instance ?? null;
          }}
          options={options as any}
          data={chartData}
        />
      </div>
    </div>
  );
});
