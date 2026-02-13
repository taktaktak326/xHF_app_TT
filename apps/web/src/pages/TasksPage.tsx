import { useEffect, useMemo, useState, useRef, useCallback } from 'react';
import type { FC } from 'react';
import { useNavigate } from 'react-router-dom';
import { useData } from '../context/DataContext';
import { useFarms } from '../context/FarmContext';
import { useAuth } from '../context/AuthContext';
import { formatInclusiveEndDate, getLocalDateString } from '../utils/formatters'; // このファイルで直接使われなくなりますが、サブコンポーネントで必要になる可能性があります
import { postJsonCached } from '../utils/cachedJsonFetch';
import type {
  Field,
  AggregatedTask,
  // 各タスクの型をインポート
  CropSeason,
  BaseTask,
  SubstanceApplicationRate,
  CountryCropGrowthStagePrediction,
} from '../types/farm';
import {
  format,
  startOfDay,
  subDays,
  differenceInCalendarDays,
  addDays,
  startOfMonth,
  endOfMonth,
  startOfWeek,
  endOfWeek,
  eachDayOfInterval,
  addMonths,
  subMonths,
  isSameMonth,
  isSameDay,
} from 'date-fns';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend,
  type ChartDataset,
} from 'chart.js';
import type { Chart as ChartJSInstance } from 'chart.js';
import { Bar } from 'react-chartjs-2';
import zoomPlugin from 'chartjs-plugin-zoom';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import './TasksPage.css';
import LoadingOverlay from '../components/LoadingOverlay';
import LoadingSpinner from '../components/LoadingSpinner';
import { formatCombinedLoadingMessage } from '../utils/loadingMessage';
import { withApiBase } from '../utils/apiBase';
import { useLanguage } from '../context/LanguageContext';
import { getCurrentLanguage, tr } from '../i18n/runtime';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend, zoomPlugin, ChartDataLabels);

const TASK_COLOR_MAP: Record<string, string> = {
  Harvest: '#F6A04D', // 収穫: オレンジ
  Spraying: '#81D4FA', // フォールバック
  Spraying_CROP_PROTECTION: '#4DB6AC', // 防除: ティール
  Spraying_NUTRITION: '#A8E063', // 施肥: イエローグリーン
  Spraying_WEED_MANAGEMENT: '#2E7D32', // 雑草管理: 濃いグリーン
  Spraying_OTHER: '#7E57C2', // その他: パープル系
  WaterManagement: '#42A5F5', // 水管理: ブルー
  Scouting: '#EF5350', // 圃場巡回: コーラル
  CropEstablishment: '#AB47BC', // 作付: パープル
  LandPreparation: '#8D6E63', // 土づくり: ブラウン
  SeedTreatment: '#26C6DA', // 種子処理: シアン
  SeedBoxTreatment: '#FFD54F', // 育苗箱処理: イエロー
};

// =============================================================================
// Type Definitions for Tasks
// =============================================================================

const TASK_TYPE_LABEL_KEYS: Record<string, string> = {
  Harvest: 'tasks.type.harvest',
  Spraying: 'tasks.type.spraying',
  WaterManagement: 'tasks.type.water_management',
  Scouting: 'tasks.type.scouting',
  CropEstablishment: 'tasks.type.crop_establishment',
  LandPreparation: 'tasks.type.land_preparation',
  SeedTreatment: 'tasks.type.seed_treatment',
  SeedBoxTreatment: 'tasks.type.seed_box_treatment',
};

type SprayingChartKey =
  | 'Spraying_CROP_PROTECTION'
  | 'Spraying_NUTRITION'
  | 'Spraying_WEED_MANAGEMENT'
  | 'Spraying_OTHER';

type CropProtectionProduct = {
  uuid?: string;
  name?: string | null;
  categories?: Array<{ name?: string | null }> | null;
};

const SPRAYING_LABEL_KEYS: Record<SprayingChartKey, string> = {
  Spraying_CROP_PROTECTION: 'tasks.type.spraying_crop_protection',
  Spraying_NUTRITION: 'tasks.type.spraying_nutrition',
  Spraying_WEED_MANAGEMENT: 'tasks.type.spraying_weed_management',
  Spraying_OTHER: 'tasks.type.spraying_other',
};

const CHART_TYPE_LABEL_KEYS: Record<string, string> = {
  ...TASK_TYPE_LABEL_KEYS,
  ...SPRAYING_LABEL_KEYS,
};

const TASK_STATE_LABEL_KEYS: Record<string, string> = {
  PLANNED: 'tasks.state.planned',
  IN_PROGRESS: 'tasks.state.in_progress',
  DONE: 'tasks.state.done',
  CANCELLED: 'tasks.state.cancelled',
  PENDING: 'tasks.state.pending',
  NOT_STARTED: 'tasks.state.not_started',
  READY: 'tasks.state.ready',
  SCHEDULED: 'tasks.state.scheduled',
  OVERDUE: 'tasks.state.overdue',
  AUTO_EXECUTED: 'tasks.state.auto_executed',
  AUTO_COMPLETED: 'tasks.state.auto_completed',
  AUTO_CANCELLED: 'tasks.state.auto_cancelled',
  FAILED: 'tasks.state.failed',
  SKIPPED: 'tasks.state.skipped',
  MISSED: 'tasks.state.missed',
};

const COUNTRY_UUID_JP = '0f59ff55-c86b-4b7b-4eaa-eb003d47dcd3';
const HERBICIDE_CATEGORY_NAME = 'HERBICIDE';

const STATUS_TOKEN_LABEL_KEYS: Record<string, string> = {
  AUTO: 'tasks.status_token.auto',
  EXECUTED: 'tasks.status_token.executed',
  EXECUTE: 'tasks.status_token.execute',
  EXECUTION: 'tasks.status_token.execute',
  COMPLETED: 'tasks.status_token.completed',
  COMPLETE: 'tasks.status_token.completed',
  CANCELLED: 'tasks.status_token.cancelled',
  CANCEL: 'tasks.status_token.cancelled',
  OVERDUE: 'tasks.status_token.overdue',
  PENDING: 'tasks.status_token.pending',
  PLANNED: 'tasks.status_token.planned',
  IN: '',
  PROGRESS: 'tasks.status_token.progress',
  DONE: 'tasks.status_token.done',
  READY: 'tasks.status_token.ready',
  SCHEDULED: 'tasks.status_token.scheduled',
  STARTED: 'tasks.status_token.started',
  START: 'tasks.status_token.start',
  ACTIVE: 'tasks.status_token.active',
  MISSED: 'tasks.status_token.missed',
  FAILED: 'tasks.status_token.failed',
  SKIPPED: 'tasks.status_token.skipped',
  TODAY: 'tasks.status_token.today',
};

const STATUS_CLASS_ENTRIES = [
  ['PLANNED', 'status-planned'],
  ['IN_PROGRESS', 'status-in_progress'],
  ['DONE', 'status-done'],
  ['PENDING', 'status-planned'],
  ['NOT_STARTED', 'status-planned'],
  ['READY', 'status-planned'],
  ['SCHEDULED', 'status-planned'],
  ['OVERDUE', 'status-overdue'],
  ['AUTO_EXECUTED', 'status-done'],
  ['AUTO_COMPLETED', 'status-done'],
  ['AUTO_CANCELLED', 'status-cancelled'],
  ['FAILED', 'status-overdue'],
  ['SKIPPED', 'status-overdue'],
  ['MISSED', 'status-overdue'],
  ['AUTO', 'status-done'],
  ['EXECUTED', 'status-done'],
  ['COMPLETED', 'status-done'],
  ['CANCEL', 'status-cancelled'],
  ['CANCELLED', 'status-cancelled'],
  ['ACTIVE', 'status-in_progress'],
  ['PROGRESS', 'status-in_progress'],
] as const;

const STATUS_CLASS_MAP: Record<string, string> = Object.fromEntries(STATUS_CLASS_ENTRIES);


function getStatusClass(state: string | null | undefined): string {
  if (!state) return 'status-default';
  const key = state.toUpperCase();
  if (STATUS_CLASS_MAP[key]) {
    return STATUS_CLASS_MAP[key];
  }
  const normalized = key.replace(/[\s-]+/g, '_');
  const tokens = normalized.split('_').filter(Boolean);
  if (tokens.length === 0) {
    return 'status-default';
  }
  for (const token of tokens) {
    const mapped = STATUS_CLASS_MAP[token];
    if (mapped) {
      return mapped;
    }
  }
  return 'status-default';
}

function getStatusLabel(state: string | null | undefined): string {
  if (!state) return '-';
  const key = state.toUpperCase();
  if (TASK_STATE_LABEL_KEYS[key]) {
    return tr(TASK_STATE_LABEL_KEYS[key]);
  }

  const normalized = key.replace(/[\s-]+/g, '_');
  const tokens = normalized.split('_').filter(Boolean);
  if (tokens.length === 0) {
    return state;
  }

  const translatedTokens = tokens.map((token, index) => {
    const wordKey = STATUS_TOKEN_LABEL_KEYS[token];
    if (wordKey === '') return '';
    const word = wordKey ? tr(wordKey) : undefined;
    if (word !== undefined) {
      return word;
    }
    if (index === 0) {
      return token.charAt(0) + token.slice(1).toLowerCase();
    }
    return token.toLowerCase();
  }).filter(Boolean);

  if (translatedTokens.length > 0) {
    return translatedTokens.join('');
  }

  return state;
}

function getSprayingChartKey(task: AggregatedTask): SprayingChartKey {
  const hint = (task.creationFlowHint || '').toUpperCase();
  if (hint === 'CROP_PROTECTION') return 'Spraying_CROP_PROTECTION';
  if (hint === 'NUTRITION_MANAGEMENT') return 'Spraying_NUTRITION';
  if (hint === 'WEED_MANAGEMENT') return 'Spraying_WEED_MANAGEMENT';
  return 'Spraying_OTHER';
}

function getChartTypeKey(task: AggregatedTask): string {
  return task.type === 'Spraying' ? getSprayingChartKey(task) : task.type;
}

function getFilterTypeKey(task: AggregatedTask): string {
  if (task.type === 'Spraying') {
    return getSprayingChartKey(task);
  }
  return task.type;
}

function getTaskLabel(task: AggregatedTask): string {
  if (task.type === 'Spraying') {
    const key = getSprayingChartKey(task);
    const labelKey = SPRAYING_LABEL_KEYS[key];
    if (labelKey) return tr(labelKey);
    const baseKey = TASK_TYPE_LABEL_KEYS[task.type];
    return baseKey ? tr(baseKey) : task.type;
  }
  const labelKey = TASK_TYPE_LABEL_KEYS[task.type];
  return labelKey ? tr(labelKey) : task.type;
}

function normalizeProductUuid(value?: string | null): string {
  return (value ?? '').trim().toLowerCase();
}

function getJstDateInputValue(rawDate?: string | null): string {
  if (!rawDate) return '';
  const date = new Date(rawDate);
  if (!Number.isFinite(date.getTime())) return '';
  return date
    .toLocaleDateString('ja-JP', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      timeZone: 'Asia/Tokyo',
    })
    .replace(/\//g, '-');
}

function toJstPlannedDateIso(dateInput: string): string | null {
  if (!dateInput) return null;
  const [y, m, d] = dateInput.split('-').map(Number);
  if (!y || !m || !d) return null;
  const utcMs = Date.UTC(y, m - 1, d, 0, 0, 0) - 9 * 60 * 60 * 1000;
  if (!Number.isFinite(utcMs)) return null;
  return new Date(utcMs).toISOString().replace('.000Z', 'Z');
}

function getJstDateInputValueFromDate(date: Date): string {
  return date
    .toLocaleDateString('ja-JP', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      timeZone: 'Asia/Tokyo',
    })
    .replace(/\//g, '-');
}

function toJstBoundaryIsoFromDate(date: Date, endOfDay: boolean): string {
  const dateInput = getJstDateInputValueFromDate(date);
  const [y, m, d] = dateInput.split('-').map(Number);
  const utcMs = Date.UTC(
    y,
    m - 1,
    d,
    endOfDay ? 23 : 0,
    endOfDay ? 59 : 0,
    endOfDay ? 59 : 0,
    endOfDay ? 999 : 0
  ) - 9 * 60 * 60 * 1000;
  return new Date(utcMs).toISOString().replace('.000Z', 'Z');
}

function getBbchRangeLabel(prediction: CountryCropGrowthStagePrediction): string {
  const start = getLocalDateString(prediction.startDate);
  if (!start) return '';
  const end = prediction.endDate ? formatInclusiveEndDate(prediction.endDate) : '';
  return end ? `${start}〜${end}` : start;
}

function parseBbchIndex(index?: string | null): number {
  if (!index) return Number.POSITIVE_INFINITY;
  const parsed = Number.parseInt(index, 10);
  return Number.isFinite(parsed) ? parsed : Number.POSITIVE_INFINITY;
}

function normalizeBbchIndex(index?: string | null): string {
  if (!index) return '';
  const trimmed = String(index).trim();
  return trimmed.toUpperCase().startsWith('BBCH') ? trimmed.slice(4) : trimmed;
}

function getBbchBadgeStyle(index: string): { background: string; borderColor: string; color: string } {
  const numeric = Number.parseInt(index, 10);
  if (!Number.isFinite(numeric)) {
    return { background: 'rgba(160, 160, 170, 0.2)', borderColor: 'rgba(160, 160, 170, 0.5)', color: '#dfe3ff' };
  }
  const clamp = (value: number) => Math.min(99, Math.max(0, value));
  const value = clamp(numeric);
  const startHue = 210; // blue
  const endHue = 10; // red
  const hue = startHue + ((endHue - startHue) * (value / 99));
  const background = `hsla(${hue}, 85%, 65%, 0.2)`;
  const borderColor = `hsla(${hue}, 85%, 65%, 0.6)`;
  const color = `hsl(${hue}, 90%, 85%)`;
  return { background, borderColor, color };
}

function findBbchIndexForDate(
  predictions: CountryCropGrowthStagePrediction[],
  dateInput: string
): string {
  if (!dateInput) return '';
  for (const pred of predictions) {
    if (!pred?.startDate) continue;
    const start = getJstDateInputValue(pred.startDate);
    if (!start) continue;
    let end = '';
    if (pred.endDate) {
      const endDate = new Date(pred.endDate);
      endDate.setDate(endDate.getDate() - 1);
      end = getJstDateInputValueFromDate(endDate);
    }
    const inRange = end ? start <= dateInput && dateInput <= end : start <= dateInput;
    if (!inRange) continue;
    return normalizeBbchIndex(pred.index);
  }
  return '';
}

type ChartMode = 'count' | 'area';

const MODE_LABEL_KEYS: Record<ChartMode, string> = {
  count: 'tasks.mode.count',
  area: 'tasks.mode.area',
};

const TABLE_PAGE_SIZE = 50;
const WEATHER_RANGE_DAYS = 10;
const WEATHER_CLUSTER_RADIUS_KM = 2;

type RangeKey = '7d' | '30d' | '90d' | 'all';
type DaysRangeKey = Exclude<RangeKey, 'all'>;
type RangeOption =
  | { key: DaysRangeKey; labelKey: string; days: number }
  | { key: 'all'; labelKey: string };

const RANGE_OPTIONS: RangeOption[] = [
  { key: '7d', labelKey: 'tasks.range.7d', days: 7 },
  { key: '30d', labelKey: 'tasks.range.30d', days: 30 },
  { key: '90d', labelKey: 'tasks.range.90d', days: 90 },
  { key: 'all', labelKey: 'tasks.range.all' },
];

const SPRAYING_FILTER_OPTIONS = [
  { key: 'Spraying', labelKey: 'tasks.filter.spraying_all' },
  { key: 'Spraying_CROP_PROTECTION', labelKey: SPRAYING_LABEL_KEYS.Spraying_CROP_PROTECTION },
  { key: 'Spraying_NUTRITION', labelKey: SPRAYING_LABEL_KEYS.Spraying_NUTRITION },
  { key: 'Spraying_WEED_MANAGEMENT', labelKey: SPRAYING_LABEL_KEYS.Spraying_WEED_MANAGEMENT },
  { key: 'Spraying_OTHER', labelKey: SPRAYING_LABEL_KEYS.Spraying_OTHER },
];

const TASK_TYPE_FILTER_OPTIONS = [
  { key: 'all', labelKey: 'tasks.filter.all_tasks' },
  ...SPRAYING_FILTER_OPTIONS,
  ...Object.entries(TASK_TYPE_LABEL_KEYS)
    .filter(([key]) => key !== 'Spraying')
    .map(([key, labelKey]) => ({ key, labelKey })),
];

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

type FieldCenter = { latitude: number; longitude: number };
type FieldEntry = { uuid: string; name: string; center: FieldCenter };
type FieldCluster = { id: string; fields: FieldEntry[] };

const getFieldCenter = (field: Field): FieldCenter | null => {
  const candidates = [field.location?.center, (field as any).center, (field as any).centroid];
  for (const candidate of candidates) {
    if (candidate && typeof candidate.latitude === 'number' && typeof candidate.longitude === 'number') {
      return { latitude: candidate.latitude, longitude: candidate.longitude };
    }
  }
  return null;
};

const toRadians = (deg: number): number => (deg * Math.PI) / 180;
const distanceKm = (a: FieldCenter, b: FieldCenter): number => {
  const dLat = toRadians(b.latitude - a.latitude);
  const dLon = toRadians(b.longitude - a.longitude);
  const lat1 = toRadians(a.latitude);
  const lat2 = toRadians(b.latitude);
  const sinLat = Math.sin(dLat / 2);
  const sinLon = Math.sin(dLon / 2);
  const h = sinLat * sinLat + Math.cos(lat1) * Math.cos(lat2) * sinLon * sinLon;
  const c = 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
  return 6371 * c;
};

const clusterFieldsByDistance = (fields: FieldEntry[], radiusKm: number): FieldCluster[] => {
  const unassigned = new Set(fields.map(f => f.uuid));
  const lookup = new Map(fields.map(f => [f.uuid, f]));
  const clusters: FieldCluster[] = [];
  let counter = 1;

  for (const field of fields) {
    if (!unassigned.has(field.uuid)) continue;
    const members: FieldEntry[] = [];
    const queue: FieldEntry[] = [field];
    unassigned.delete(field.uuid);

    while (queue.length > 0) {
      const current = queue.shift();
      if (!current) continue;
      members.push(current);
      for (const candidateUuid of Array.from(unassigned)) {
        const candidate = lookup.get(candidateUuid);
        if (!candidate) {
          unassigned.delete(candidateUuid);
          continue;
        }
        const within = members.some(member => distanceKm(member.center, candidate.center) <= radiusKm);
        if (within) {
          unassigned.delete(candidateUuid);
          queue.push(candidate);
        }
      }
    }

    clusters.push({ id: `cluster-${counter}`, fields: members });
    counter += 1;
  }

  return clusters;
};

/**
 * combinedOut データから全てのタスクを集約し、ソートして返すカスタムフック
 */
const useAggregatedTasks = (): AggregatedTask[] => {
  const { combinedOut } = useData();

  return useMemo((): AggregatedTask[] => {
    const fields = selectFieldsFromCombinedOut(combinedOut);
    if (!fields || fields.length === 0) return [];

    const allTasks = fields.flatMap(field => {
      const tasks: AggregatedTask[] = [];

      const fieldArea = field.area ?? 0;
      const farmName = field.farmV2?.name ?? (field as any).farm?.name ?? null;
      const farmUuid = field.farmV2?.uuid ?? (field as any).farm?.uuid ?? null;

      // `cropSeasonsV2` の外にあるタスク (例: cropEstablishments)
      if (field.cropEstablishments && field.cropSeasonsV2 && field.cropSeasonsV2.length > 0) {
        const primarySeason = field.cropSeasonsV2[0]; // 最初の作期に紐付ける（仮）
        const commonProps = {
          fieldUuid: field.uuid,
          fieldName: field.name,
          cropName: primarySeason.crop.name,
          seasonStartDate: primarySeason.startDate,
          seasonUuid: primarySeason.uuid,
          fieldArea,
          farmName,
          farmUuid,
        };
        field.cropEstablishments.forEach((task: BaseTask) => tasks.push({ ...task, ...commonProps, type: 'CropEstablishment' as const }));
      }

      // `cropSeasonsV2` の中にあるタスク
      const seasonTasks = field.cropSeasonsV2?.flatMap((season: CropSeason) => {
        const commonProps = {
          fieldUuid: field.uuid,
          fieldName: field.name,
          cropName: season.crop.name,
          cropUuid: season.crop.uuid ?? null,
          seasonStartDate: season.startDate,
          seasonUuid: season.uuid,
          fieldArea,
          farmName,
          farmUuid,
        };
        return [
          ...(season.harvests?.map((task: BaseTask) => ({ ...task, ...commonProps, type: 'Harvest' as const })) ?? []),
          ...(season.sprayingsV2?.map((task: BaseTask) => {
            const creationFlowHint = task.creationFlowHint ?? task.dosedMap?.creationFlowHint ?? null;
            return {
              ...task,
              ...commonProps,
              type: 'Spraying' as const,
              creationFlowHint,
              dosedMap: task.dosedMap ?? null,
            };
          }) ?? []),
          ...(season.waterManagementTasks?.map((task: BaseTask) => ({ ...task, ...commonProps, type: 'WaterManagement' as const })) ?? []),
          ...(season.scoutingTasks?.map((task: BaseTask) => ({ ...task, ...commonProps, type: 'Scouting' as const })) ?? []),
          ...(season.landPreparations?.map((task: BaseTask) => ({ ...task, ...commonProps, type: 'LandPreparation' as const })) ?? []),
          ...(season.seedTreatmentTasks?.map((task: BaseTask) => ({ ...task, ...commonProps, type: 'SeedTreatment' as const })) ?? []),
          ...(season.seedBoxTreatments?.map((task: BaseTask) => ({ ...task, ...commonProps, type: 'SeedBoxTreatment' as const })) ?? []),
        ];
      }) ?? [];

      return [...tasks, ...seasonTasks];
    });

    // 計画日 or 実行日でソート（新しいものが上）
    return allTasks.sort((a, b) => {
      const dateA = a.executionDate || a.plannedDate;
      const dateB = b.executionDate || b.plannedDate;
      if (!dateA) return 1;
      if (!dateB) return -1;
      return new Date(dateB).getTime() - new Date(dateA).getTime();
    });
  }, [combinedOut]);
};

// =============================================================================
// Main Component
// =============================================================================

export function TasksPage() {
  const {
    combinedOut,
    combinedLoading,
    combinedErr,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();
  const { auth } = useAuth();
  const { language, t } = useLanguage();
  const { submittedFarms, fetchCombinedDataIfNeeded, clearCombinedCache } = useFarms(); // fetchCombinedDataIfNeeded を useFarms から取得
  const navigate = useNavigate();
  const baseTasks = useAggregatedTasks();
  const [plannedDateOverrides, setPlannedDateOverrides] = useState<Record<string, string | null>>({});
  const [updateStateByTask, setUpdateStateByTask] = useState<Record<string, { loading: boolean; error?: string }>>({});
  const [selectedTaskUuids, setSelectedTaskUuids] = useState<Set<string>>(new Set());
  const [isBulkModalOpen, setIsBulkModalOpen] = useState(false);
  const [isBulkSaving, setIsBulkSaving] = useState(false);
  const allTasks = useMemo(() => {
    if (!baseTasks.length) return baseTasks;
    return baseTasks.map(task => {
      const override = plannedDateOverrides[task.uuid];
      if (override === undefined) return task;
      return { ...task, plannedDate: override };
    });
  }, [baseTasks, plannedDateOverrides]);
  const [herbicideProductUuidsByCrop, setHerbicideProductUuidsByCrop] = useState<Map<string, Set<string>>>(new Map());
  const herbicideFetchInFlight = useRef<Set<string>>(new Set());
  const herbicideCacheKeyRef = useRef<string>('');
  const bbchBySeason = useMemo(() => {
    const fields = selectFieldsFromCombinedOut(combinedOut);
    const map = new Map<string, CountryCropGrowthStagePrediction[]>();
    fields.forEach(field => {
      field.cropSeasonsV2?.forEach(season => {
        if (!season.uuid) return;
        map.set(season.uuid, season.countryCropGrowthStagePredictions ?? []);
      });
    });
    return map;
  }, [combinedOut]);
  useEffect(() => {
    if (!auth) return;
    const sprayingTasks = allTasks.filter(task => task.type === 'Spraying' && task.cropUuid);
    const cropUuids = Array.from(new Set(sprayingTasks.map(task => task.cropUuid).filter(Boolean))) as string[];
    if (cropUuids.length === 0) return;
    const farmUuids = submittedFarms.length > 0
      ? submittedFarms
      : Array.from(new Set(sprayingTasks.map(task => task.farmUuid).filter(Boolean))) as string[];
    if (farmUuids.length === 0) return;
    const cacheKey = `${farmUuids.slice().sort().join(',')}|${cropUuids.slice().sort().join(',')}`;
    if (herbicideCacheKeyRef.current === cacheKey) return;
    if (herbicideFetchInFlight.current.has(cacheKey)) return;
    herbicideFetchInFlight.current.add(cacheKey);

    let cancelled = false;
    const fetchProducts = async () => {
      let items: Record<string, CropProtectionProduct[] | { ok?: boolean }> = {};
      try {
        const { ok, json } = await postJsonCached<any>(
          withApiBase('/crop-protection-products/bulk'),
          {
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            farm_uuids: farmUuids,
            country_uuid: COUNTRY_UUID_JP,
            crop_uuids: cropUuids,
            task_type_code: 'FIELDTREATMENT',
          },
          undefined,
          { cacheKey: `crop-protection-products:bulk:${cacheKey}`, cache: 'session' },
        );
        if (!ok || !json?.ok) return;
        items = json.items ?? {};
        if (cancelled) return;
        setHerbicideProductUuidsByCrop(prev => {
          const next = new Map(prev);
          Object.entries(items).forEach(([cropUuid, productList]) => {
            if (!Array.isArray(productList)) return;
            const herbicideUuids = new Set<string>();
            productList.forEach((product) => {
              if (!product?.categories?.some((category) => category?.name === HERBICIDE_CATEGORY_NAME)) return;
              const normalized = normalizeProductUuid(product?.uuid);
              if (normalized) herbicideUuids.add(normalized);
            });
            next.set(cropUuid, herbicideUuids);
          });
          herbicideCacheKeyRef.current = cacheKey;
          return next;
        });
      } finally {
        herbicideFetchInFlight.current.delete(cacheKey);
      }
    };

    fetchProducts();
    return () => {
      cancelled = true;
    };
  }, [auth, allTasks, herbicideProductUuidsByCrop, submittedFarms]);
  const fieldOptions = useMemo(() => {
    const fields = selectFieldsFromCombinedOut(combinedOut);
    return fields
      .filter(field => field?.uuid)
      .map(field => ({ uuid: field.uuid, name: field.name ?? '(no name)', seasons: field.cropSeasonsV2 ?? [] }));
  }, [combinedOut]);
  const herbicideOrdersByTask = useMemo(() => {
    const orders = new Map<string, number>();
    const isHerbicideTask = (task: AggregatedTask): boolean => {
      if (task.type !== 'Spraying') return false;
      const recipe = task.dosedMap?.recipeV2 ?? [];
      const entries = Array.isArray(recipe) ? recipe : [recipe];
      if (task.cropUuid) {
        const herbicideUuids = herbicideProductUuidsByCrop.get(task.cropUuid);
        if (herbicideUuids && herbicideUuids.size > 0) {
          return entries.some((entry) => {
            const normalized = normalizeProductUuid(entry?.uuid);
            return normalized ? herbicideUuids.has(normalized) : false;
          });
        }
      }
      const hint = (task.creationFlowHint || '').toUpperCase();
      return hint === 'WEED_MANAGEMENT';
    };

    const byGroup = allTasks.reduce((acc, task) => {
      if (!isHerbicideTask(task)) return acc;
      const groupKey = `${task.fieldUuid}:${task.seasonUuid ?? 'none'}`;
      if (!acc[groupKey]) acc[groupKey] = [];
      acc[groupKey].push(task);
      return acc;
    }, {} as Record<string, AggregatedTask[]>);

    Object.values(byGroup).forEach((tasks) => {
      const sorted = [...tasks].sort((a, b) => {
        const dateA = a.executionDate || a.plannedDate;
        const dateB = b.executionDate || b.plannedDate;
        if (!dateA) return 1;
        if (!dateB) return -1;
        return new Date(dateA).getTime() - new Date(dateB).getTime();
      });
      sorted.forEach((task, index) => {
        orders.set(task.uuid, index + 1);
      });
    });

    return orders;
  }, [allTasks, herbicideProductUuidsByCrop]);
  const herbicideIntervalAlertsByTask = useMemo(() => {
    const alerts = new Map<string, boolean>();
    const isHerbicideTask = (task: AggregatedTask): boolean => {
      if (task.type !== 'Spraying') return false;
      const recipe = task.dosedMap?.recipeV2 ?? [];
      const entries = Array.isArray(recipe) ? recipe : [recipe];
      if (task.cropUuid) {
        const herbicideUuids = herbicideProductUuidsByCrop.get(task.cropUuid);
        if (herbicideUuids && herbicideUuids.size > 0) {
          return entries.some((entry) => {
            const normalized = normalizeProductUuid(entry?.uuid);
            return normalized ? herbicideUuids.has(normalized) : false;
          });
        }
      }
      const hint = (task.creationFlowHint || '').toUpperCase();
      return hint === 'WEED_MANAGEMENT';
    };

    const byGroup = allTasks.reduce((acc, task) => {
      if (!isHerbicideTask(task)) return acc;
      const groupKey = `${task.fieldUuid}:${task.seasonUuid ?? 'none'}`;
      if (!acc[groupKey]) acc[groupKey] = [];
      acc[groupKey].push(task);
      return acc;
    }, {} as Record<string, AggregatedTask[]>);

    Object.values(byGroup).forEach((tasks) => {
      const sorted = [...tasks].sort((a, b) => {
        const dateA = a.executionDate || a.plannedDate;
        const dateB = b.executionDate || b.plannedDate;
        if (!dateA) return 1;
        if (!dateB) return -1;
        return new Date(dateA).getTime() - new Date(dateB).getTime();
      });
      sorted.forEach((task, index) => {
        if (index === 0) return;
        const prev = sorted[index - 1];
        const currentDate = task.executionDate || task.plannedDate;
        const prevDate = prev.executionDate || prev.plannedDate;
        if (!currentDate || !prevDate) return;
        const diff = differenceInCalendarDays(new Date(currentDate), new Date(prevDate));
        if (diff < 20) {
          alerts.set(task.uuid, true);
        }
      });
    });

    return alerts;
  }, [allTasks, herbicideProductUuidsByCrop]);
  const bbchPredictionsByField = useMemo(() => {
    const map = new Map<string, CountryCropGrowthStagePrediction[]>();
    fieldOptions.forEach(field => {
      const seasons = field.seasons ?? [];
      const active = seasons.find(season => season.activeGrowthStage) ?? seasons[0] ?? null;
      if (!active?.uuid) return;
      map.set(field.uuid, bbchBySeason.get(active.uuid) ?? []);
    });
    return map;
  }, [bbchBySeason, fieldOptions]);
  const weatherClusterByField = useMemo(() => {
    const fields = selectFieldsFromCombinedOut(combinedOut);
    const withLocation: FieldEntry[] = [];
    const withoutLocation: FieldEntry[] = [];
    fields.forEach(field => {
      const center = getFieldCenter(field);
      if (!center) {
        withoutLocation.push({ uuid: field.uuid, name: field.name, center: { latitude: 0, longitude: 0 } });
        return;
      }
      withLocation.push({ uuid: field.uuid, name: field.name, center });
    });
    const clusters = clusterFieldsByDistance(withLocation, WEATHER_CLUSTER_RADIUS_KM);
    const map = new Map<string, { id: string; representative: FieldEntry; fields: FieldEntry[] }>();
    clusters.forEach(cluster => {
      const representative = cluster.fields[0];
      cluster.fields.forEach(field => {
        map.set(field.uuid, { id: cluster.id, representative, fields: cluster.fields });
      });
    });
    withoutLocation.forEach(field => {
      map.set(field.uuid, { id: `cluster-${field.uuid}`, representative: field, fields: [field] });
    });
    return map;
  }, [combinedOut]);
  const [range, setRange] = useState<RangeKey>('all');
  const [farmFilter, setFarmFilter] = useState<string>('all');
  const [typeFilter, setTypeFilter] = useState<string>('all');
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [bbchFilter, setBbchFilter] = useState<string>('all');
  const [planDateFrom, setPlanDateFrom] = useState<string>('');
  const [planDateTo, setPlanDateTo] = useState<string>('');
  const [noteFilter, setNoteFilter] = useState<string>('all');
  const [viewMode, setViewMode] = useState<'list' | 'calendar'>('list');
  const [calendarMonth, setCalendarMonth] = useState<Date>(() => startOfMonth(new Date()));
  const planDateBounds = useMemo(() => {
    let min: Date | null = null;
    let max: Date | null = null;
    allTasks.forEach(task => {
      if (!task.plannedDate) return;
      const d = startOfDay(new Date(task.plannedDate));
      if (!min || d < min) min = d;
      if (!max || d > max) max = d;
    });
    const fmt = (d: Date | null) => d ? d.toISOString().slice(0, 10) : '';
    return { min: fmt(min), max: fmt(max) };
  }, [allTasks]);
  const farmFilterOptions = useMemo(() => {
    const map = new Map<string, string>();
    let hasUnknown = false;
    allTasks.forEach(task => {
      const key = task.farmUuid || task.farmName || '';
      const label = task.farmName || '';
      if (key) {
        if (!map.has(key)) {
          map.set(key, label || t('tasks.farm.no_name'));
        }
      } else {
        hasUnknown = true;
      }
    });
    const options = Array.from(map.entries()).map(([key, label]) => ({ key, label }));
    options.sort((a, b) => a.label.localeCompare(b.label, language === 'ja' ? 'ja' : 'en'));
    if (hasUnknown) options.push({ key: '__unknown__', label: t('tasks.farm.no_info') });
    return [{ key: 'all', label: t('tasks.farm.all') }, ...options];
  }, [allTasks, language, t]);
  const statusFilterOptions = useMemo(() => {
    const map = new Map<string, string>();
    allTasks.forEach(task => {
      const key = (task.state || '').toUpperCase();
      if (!key) return;
      if (!map.has(key)) {
        map.set(key, getStatusLabel(task.state));
      }
    });
    const dynamicOptions = Array.from(map.entries()).map(([key, label]) => ({ key, label }));
    // キー順ではなくラベル順で見やすく
    dynamicOptions.sort((a, b) => a.label.localeCompare(b.label, language === 'ja' ? 'ja' : 'en'));
    return [{ key: 'all', label: t('tasks.filter.status_all') }, ...dynamicOptions];
  }, [allTasks, language, t]);
  const bbchFilterOptions = useMemo(() => {
    const indices = new Set<string>();
    let hasUnknown = false;
    allTasks.forEach(task => {
      const dateInput = getJstDateInputValue(task.plannedDate || task.executionDate);
      if (!dateInput) {
        hasUnknown = true;
        return;
      }
      const predictions = bbchBySeason.get(task.seasonUuid ?? '') ?? [];
      const index = findBbchIndexForDate(predictions, dateInput);
      if (!index) {
        hasUnknown = true;
        return;
      }
      indices.add(index);
    });
    const sorted = Array.from(indices).sort((a, b) => parseBbchIndex(a) - parseBbchIndex(b));
    const options = sorted.map(index => ({ key: index, label: `BBCH ${index}` }));
    if (hasUnknown) options.push({ key: '__unknown__', label: t('tasks.bbch.unknown') });
    return [{ key: 'all', label: t('tasks.bbch.all') }, ...options];
  }, [allTasks, bbchBySeason, language, t]);
  const noteFilterOptions = useMemo(() => {
    const notes = new Set<string>();
    let hasEmpty = false;
    allTasks.forEach(task => {
      const note = (task.note || '').trim();
      if (!note) {
        hasEmpty = true;
        return;
      }
      notes.add(note);
    });
    const options = Array.from(notes)
      .sort((a, b) => a.localeCompare(b, language === 'ja' ? 'ja' : 'en'))
      .map(note => ({
        key: note,
        label: note.length > 40 ? `${note.slice(0, 40)}…` : note,
      }));
    return [
      { key: 'all', label: t('tasks.note.all') },
      ...(hasEmpty ? [{ key: '__empty__', label: t('tasks.note.none') }] : []),
      ...options,
    ];
  }, [allTasks, language, t]);

  useEffect(() => {
    if (farmFilter !== 'all' && !farmFilterOptions.some(opt => opt.key === farmFilter)) {
      setFarmFilter('all');
    }
  }, [farmFilter, farmFilterOptions]);
  useEffect(() => {
    if (statusFilter !== 'all' && !statusFilterOptions.some(opt => opt.key === statusFilter)) {
      setStatusFilter('all');
    }
  }, [statusFilter, statusFilterOptions]);
  useEffect(() => {
    if (bbchFilter !== 'all' && !bbchFilterOptions.some(opt => opt.key === bbchFilter)) {
      setBbchFilter('all');
    }
  }, [bbchFilter, bbchFilterOptions]);
  useEffect(() => {
    if (noteFilter !== 'all' && !noteFilterOptions.some(opt => opt.key === noteFilter)) {
      setNoteFilter('all');
    }
  }, [noteFilter, noteFilterOptions]);
  useEffect(() => {
    if (!planDateFrom && planDateBounds.min) {
      setPlanDateFrom(planDateBounds.min);
    }
    if (!planDateTo && planDateBounds.max) {
      setPlanDateTo(planDateBounds.max);
    }
  }, [planDateBounds, planDateFrom, planDateTo]);
  const filteredTasks = useMemo(() => {
    const byRange = filterTasksByRange(allTasks, range);
    return byRange.filter(task => {
      const filterTypeKey = getFilterTypeKey(task);
      const matchesFarm =
        farmFilter === 'all' ||
        (farmFilter === '__unknown__' && !task.farmUuid && !task.farmName) ||
        task.farmUuid === farmFilter ||
        (!task.farmUuid && task.farmName === farmFilter);
      const matchesType =
        typeFilter === 'all' ||
        (typeFilter === 'Spraying' && task.type === 'Spraying') ||
        filterTypeKey === typeFilter;
      const normalizedState = (task.state || '').toUpperCase();
      const matchesStatus = statusFilter === 'all' || normalizedState === statusFilter;
      const planned = task.plannedDate ? startOfDay(new Date(task.plannedDate)) : null;
      const fromBoundary = planDateFrom ? startOfDay(new Date(planDateFrom)) : null;
      const toBoundary = planDateTo ? startOfDay(new Date(planDateTo)) : null;
      if ((fromBoundary || toBoundary) && !planned) return false;
      if (fromBoundary && planned && planned < fromBoundary) return false;
      if (toBoundary && planned && planned > toBoundary) return false;
      const trimmedNote = (task.note || '').trim();
      const matchesNote =
        noteFilter === 'all' ||
        (noteFilter === '__empty__' && !trimmedNote) ||
        trimmedNote === noteFilter;
      let matchesBbch = true;
      if (bbchFilter !== 'all') {
        const dateInput = getJstDateInputValue(task.plannedDate || task.executionDate);
        const predictions = bbchBySeason.get(task.seasonUuid ?? '') ?? [];
        const bbchIndex = dateInput ? findBbchIndexForDate(predictions, dateInput) : '';
        matchesBbch =
          (bbchFilter === '__unknown__' && !bbchIndex) ||
          (bbchFilter !== '__unknown__' && bbchIndex === bbchFilter);
      }
      return matchesFarm && matchesType && matchesStatus && matchesNote && matchesBbch;
    });
  }, [allTasks, range, farmFilter, typeFilter, statusFilter, planDateFrom, planDateTo, noteFilter, bbchFilter, bbchBySeason]);

  const tasksById = useMemo(() => {
    const map = new Map<string, AggregatedTask>();
    filteredTasks.forEach(task => {
      if (!task.uuid) return;
      map.set(task.uuid, task);
    });
    return map;
  }, [filteredTasks]);

  const tasksByDate = useMemo(() => {
    const map: Record<string, AggregatedTask[]> = {};
    filteredTasks.forEach(task => {
      const rawDate = task.plannedDate || task.executionDate;
      if (!rawDate) return;
      const dateKey = format(startOfDay(new Date(rawDate)), 'yyyy-MM-dd');
      if (!map[dateKey]) map[dateKey] = [];
      map[dateKey].push(task);
    });
    return map;
  }, [filteredTasks]);
  const selectedTasks = useMemo(
    () => filteredTasks.filter(task => selectedTaskUuids.has(task.uuid) && task.type === 'Spraying'),
    [filteredTasks, selectedTaskUuids]
  );
  const selectedTaskCount = selectedTasks.length;

  const handleDownloadTasksCsv = useCallback(() => {
    if (filteredTasks.length === 0) return;

    const csvEscape = (value: unknown) => {
      const text = String(value ?? '')
        .replace(/\r\n/g, '\n')
        .replace(/\r/g, '\n')
        .replace(/\n/g, ' ');
      return `"${text.replace(/"/g, '""')}"`;
    };
    const buildCsvRow = (cells: unknown[]) => cells.map(csvEscape).join(',');

    const getAssigneeName = (task: AggregatedTask) =>
      task.assignee
        ? `${task.assignee.lastName || ''} ${task.assignee.firstName || ''}`.trim() ||
          t('tasks.assignee.unassigned')
        : t('tasks.assignee.unassigned');

    const getBbchIndex = (task: AggregatedTask) => {
      const dateInput = getJstDateInputValue(task.plannedDate || task.executionDate);
      const predictions = bbchBySeason.get(task.seasonUuid ?? '') ?? [];
      return dateInput ? (findBbchIndexForDate(predictions, dateInput) || '') : '';
    };

    const buildRecipeText = (task: AggregatedTask) => {
      const recipes = buildRecipeDisplay(task);
      if (recipes.length === 0) return '';
      return recipes
        .map(entry => {
          const parts = [
            entry.name,
            entry.formLabel ? `(${entry.formLabel})` : '',
            entry.per10a ? entry.per10a : '',
            entry.total ? t('tasks.recipe.total', { total: entry.total }) : '',
          ].filter(Boolean);
          return parts.join(' ');
        })
        .join(' / ');
    };

    const headers = [
      t('table.farm'),
      t('table.field'),
      t('tasks.csv.area_ha'),
      t('tasks.csv.task_type'),
      t('table.crop'),
      t('tasks.csv.planned_date'),
      t('tasks.csv.execution_date'),
      'BBCH',
      t('table.status'),
      t('tasks.csv.assignee'),
      t('tasks.csv.recipe'),
      t('tasks.csv.note'),
    ];

    const sorted = [...filteredTasks].sort((a, b) => {
      const dateA = a.plannedDate || a.executionDate || '';
      const dateB = b.plannedDate || b.executionDate || '';
      if (dateA !== dateB) return dateA.localeCompare(dateB);
      const farmA = a.farmName || '';
      const farmB = b.farmName || '';
      if (farmA !== farmB) return farmA.localeCompare(farmB, language === 'ja' ? 'ja' : 'en');
      return (a.fieldName || '').localeCompare(b.fieldName || '', language === 'ja' ? 'ja' : 'en');
    });

    const rows = sorted.map(task => {
      const areaHa = task.fieldArea ? (task.fieldArea / 10000) : null;
      return buildCsvRow([
        task.farmName ?? '',
        task.fieldName ?? '',
        areaHa && Number.isFinite(areaHa) ? areaHa.toFixed(2) : '',
        getTaskLabel(task),
        task.cropName ?? '',
        getLocalDateString(task.plannedDate) || '',
        getLocalDateString(task.executionDate) || '',
        getBbchIndex(task),
        getStatusLabel(task.state),
        getAssigneeName(task),
        buildRecipeText(task),
        (task.note ?? '').trim(),
      ]);
    });

    const bom = new Uint8Array([0xef, 0xbb, 0xbf]);
    const csvContent = [buildCsvRow(headers), ...rows].join('\r\n');
    const blob = new Blob([bom, csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `tasks_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  }, [bbchBySeason, filteredTasks]);

  useEffect(() => {
    const allowed = new Set(filteredTasks.filter(task => task.type === 'Spraying').map(task => task.uuid));
    setSelectedTaskUuids(prev => {
      const next = new Set<string>();
      prev.forEach(uuid => {
        if (allowed.has(uuid)) next.add(uuid);
      });
      if (next.size === prev.size) return prev;
      return next;
    });
  }, [filteredTasks]);

  const toggleTaskSelection = useCallback((uuid: string, checked: boolean) => {
    setSelectedTaskUuids(prev => {
      const next = new Set(prev);
      if (checked) {
        next.add(uuid);
      } else {
        next.delete(uuid);
      }
      return next;
    });
  }, []);

  const toggleAllSelections = useCallback((uuids: string[], checked: boolean) => {
    setSelectedTaskUuids(prev => {
      const next = new Set(prev);
      uuids.forEach(uuid => {
        if (checked) {
          next.add(uuid);
        } else {
          next.delete(uuid);
        }
      });
      return next;
    });
  }, []);

  const isWeatherEnabled = useCallback((task: AggregatedTask) => {
    if (task.type !== 'Spraying') return false;
    if (!task.plannedDate) return false;
    if (!task.fieldUuid) return false;
    const planned = startOfDay(new Date(task.plannedDate));
    if (!Number.isFinite(planned.getTime())) return false;
    const today = startOfDay(new Date());
    const end = addDays(today, WEATHER_RANGE_DAYS);
    return planned >= today && planned <= end;
  }, []);

  const handleOpenWeather = useCallback((task: AggregatedTask) => {
    if (!isWeatherEnabled(task)) return;
    const cluster = weatherClusterByField.get(task.fieldUuid);
    if (!cluster) return;
    const representative = cluster.representative;
    navigate(`/weather/${representative.uuid}`, {
      state: {
        clusterId: cluster.id,
        representativeUuid: representative.uuid,
        fieldUuids: cluster.fields.map(field => field.uuid),
        fieldNames: cluster.fields.map(field => field.name),
        radiusKm: WEATHER_CLUSTER_RADIUS_KM,
      },
    });
  }, [isWeatherEnabled, weatherClusterByField, navigate]);

  const updateSprayingPlannedDate = useCallback(
    async (task: AggregatedTask, nextDateInput: string, opts?: { skipInvalidate?: boolean }) => {
      if (task.type !== 'Spraying') return;
      if (!auth) {
        throw new Error(t('tasks.error.no_auth_relogin'));
      }
      const plannedDate = toJstPlannedDateIso(nextDateInput);
      if (!plannedDate) {
        throw new Error(t('tasks.error.invalid_planned_date'));
      }
      setUpdateStateByTask(prev => ({ ...prev, [task.uuid]: { loading: true } }));
      try {
        const res = await fetch(withApiBase(`/tasks/v2/sprayings/${task.uuid}`), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            plannedDate,
            executionDate: task.executionDate ?? null,
          }),
        });
        if (!res.ok) {
          const text = await res.text();
          let detail = text || `HTTP ${res.status}`;
          try {
            const data = text ? JSON.parse(text) : null;
            detail = (data && (data.error || data.message || data.detail)) || detail;
          } catch {
            // ignore parse error
          }
          throw new Error(detail);
        }
        setPlannedDateOverrides(prev => ({ ...prev, [task.uuid]: plannedDate }));
        setUpdateStateByTask(prev => ({ ...prev, [task.uuid]: { loading: false } }));
        if (!opts?.skipInvalidate) {
          clearCombinedCache();
          fetchCombinedDataIfNeeded({ includeTasks: true, force: true });
        }
      } catch (err) {
        const message = err instanceof Error ? err.message : t('tasks.error.update_failed');
        setUpdateStateByTask(prev => ({ ...prev, [task.uuid]: { loading: false, error: message } }));
        throw err;
      }
    },
    [auth, clearCombinedCache, fetchCombinedDataIfNeeded]
  );
  const canEditPlannedDate = Boolean(auth);

  const applyBulkPlannedDates = useCallback(
    async (updates: { task: AggregatedTask; dateInput: string }[]) => {
      if (updates.length === 0) return;
      setIsBulkSaving(true);
      try {
        for (const entry of updates) {
          await updateSprayingPlannedDate(entry.task, entry.dateInput, { skipInvalidate: true });
        }
        clearCombinedCache();
        fetchCombinedDataIfNeeded({ includeTasks: true, force: true });
      } finally {
        setIsBulkSaving(false);
      }
    },
    [clearCombinedCache, fetchCombinedDataIfNeeded, updateSprayingPlannedDate]
  );

  // ===========================================================================
  // Data Fetching
  // ===========================================================================
  useEffect(() => {
    fetchCombinedDataIfNeeded({ includeTasks: true });
  }, [fetchCombinedDataIfNeeded]);

  // ===========================================================================
  // Render
  // ===========================================================================

  if (submittedFarms.length === 0) {
    return (
      <div className="tasks-page-container">
        <h2>{t('tasks.title')}</h2>
        <p>{t('tasks.select_farms_hint')}</p>
      </div>
    );
  }

  if (combinedLoading) {
    return (
      <div className="tasks-page-container">
        <LoadingOverlay
          message={formatCombinedLoadingMessage(
            t('label.tasks_data'),
            combinedFetchAttempt,
            combinedFetchMaxAttempts,
            combinedRetryCountdown,
          )}
        />
        <h2>{t('tasks.title')}</h2>
      </div>
    );
  }

  if (combinedErr) {
    return (
      <div className="tasks-page-container">
        <h2>{t('tasks.title')}</h2>
        <h3 style={{ color: '#ff6b6b' }}>{t('tasks.load_failed')}</h3>
        <pre style={{ color: '#ff6b6b', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {combinedErr}
        </pre>
      </div>
    );
  }

  return (
    <div className="tasks-page-container">
      <h2>{t('tasks.title')}</h2>
      <p>
        {t('tasks.summary', { count: submittedFarms.length })}
        {combinedOut?.source && (
          <span style={{ marginLeft: '1em', color: combinedOut.source === 'cache' ? '#4caf50' : '#2196f3', fontWeight: 'bold' }}>
            ({combinedOut.source === 'cache' ? t('source.cache') : t('source.api')})
          </span>
        )}
      </p>

      <div className="tasks-view-tabs" role="tablist" aria-label={t('tasks.view.aria')}>
        <button
          type="button"
          role="tab"
          aria-selected={viewMode === 'list'}
          className={`tasks-view-tab ${viewMode === 'list' ? 'active' : ''}`}
          onClick={() => setViewMode('list')}
        >
          {t('tasks.view.list')}
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={viewMode === 'calendar'}
          className={`tasks-view-tab ${viewMode === 'calendar' ? 'active' : ''}`}
          onClick={() => setViewMode('calendar')}
        >
          {t('tasks.view.calendar')}
        </button>
      </div>

      {viewMode === 'list' && (
        <div className="tasks-filter-bar">
          <div className="filter-control">
            <label htmlFor="task-farm-filter">{t('tasks.filter.farm')}</label>
            <select
              id="task-farm-filter"
              value={farmFilter}
              onChange={(e) => setFarmFilter(e.target.value)}
            >
              {farmFilterOptions.map(opt => (
                <option key={opt.key} value={opt.key}>{opt.label}</option>
              ))}
            </select>
          </div>
          <div className="filter-control">
            <label htmlFor="task-type-filter">{t('tasks.filter.type')}</label>
            <select
              id="task-type-filter"
              value={typeFilter}
              onChange={(e) => setTypeFilter(e.target.value)}
            >
              {TASK_TYPE_FILTER_OPTIONS.map(opt => (
                <option key={opt.key} value={opt.key}>{t(opt.labelKey)}</option>
              ))}
            </select>
          </div>
          <div className="filter-control">
            <label htmlFor="task-status-filter">{t('tasks.filter.status')}</label>
            <select
              id="task-status-filter"
              value={statusFilter}
              onChange={(e) => setStatusFilter(e.target.value)}
            >
              {statusFilterOptions.map(opt => (
                <option key={opt.key} value={opt.key}>{opt.label}</option>
              ))}
            </select>
          </div>
          <div className="filter-control">
            <label htmlFor="task-bbch-filter">BBCH</label>
            <select
              id="task-bbch-filter"
              value={bbchFilter}
              onChange={(e) => setBbchFilter(e.target.value)}
            >
              {bbchFilterOptions.map(opt => (
                <option key={opt.key} value={opt.key}>{opt.label}</option>
              ))}
            </select>
          </div>
          <div className="filter-control">
            <label htmlFor="task-plan-from">{t('tasks.filter.plan_from')}</label>
            <input
              id="task-plan-from"
              type="date"
              value={planDateFrom}
              onChange={(e) => setPlanDateFrom(e.target.value)}
            />
          </div>
          <div className="filter-control">
            <label htmlFor="task-plan-to">{t('tasks.filter.plan_to')}</label>
            <input
              id="task-plan-to"
              type="date"
              value={planDateTo}
              onChange={(e) => setPlanDateTo(e.target.value)}
            />
          </div>
          <div className="filter-control">
            <label htmlFor="task-note-filter">{t('tasks.filter.note')}</label>
            <select
              id="task-note-filter"
              value={noteFilter}
              onChange={(e) => setNoteFilter(e.target.value)}
            >
              {noteFilterOptions.map(opt => (
                <option key={opt.key} value={opt.key}>{opt.label}</option>
              ))}
            </select>
          </div>
        </div>
      )}

      {viewMode === 'list' && (
        <>
          <TasksChart tasks={filteredTasks} range={range} onRangeChange={setRange} />
          <div className="tasks-bulk-actions">
            <div className="tasks-bulk-summary">
              {t('tasks.bulk.selected_spraying', { count: selectedTaskCount })}
            </div>
            <div className="tasks-bulk-action-buttons">
              <button type="button" onClick={handleDownloadTasksCsv} disabled={filteredTasks.length === 0}>
                {t('action.csv_download')}
              </button>
              <button
                type="button"
                onClick={() => setIsBulkModalOpen(true)}
                disabled={!canEditPlannedDate || selectedTaskCount === 0 || isBulkSaving}
              >
                {t('tasks.bulk.change_planned_dates')}
              </button>
            </div>
          </div>
          <TasksTable
            tasks={filteredTasks}
            canEditPlannedDate={canEditPlannedDate}
            onUpdatePlannedDate={updateSprayingPlannedDate}
            updateStateByTask={updateStateByTask}
            selectedTaskUuids={selectedTaskUuids}
            onToggleSelect={toggleTaskSelection}
            onToggleSelectAll={toggleAllSelections}
            onOpenWeather={handleOpenWeather}
            isWeatherEnabled={isWeatherEnabled}
          />
          {isBulkModalOpen && (
            <BulkPlannedDateModal
              tasks={selectedTasks}
              bbchBySeason={bbchBySeason}
              isSaving={isBulkSaving}
              onClose={() => setIsBulkModalOpen(false)}
              onApply={applyBulkPlannedDates}
            />
          )}
        </>
      )}

      {viewMode === 'calendar' && (
        <TasksCalendar
          tasksByDate={tasksByDate}
          tasksById={tasksById}
          currentMonth={calendarMonth}
          onChangeMonth={setCalendarMonth}
          onMoveTask={updateSprayingPlannedDate}
          fieldOptions={fieldOptions.map(({ uuid, name }) => ({ uuid, name }))}
          bbchPredictionsByField={bbchPredictionsByField}
          herbicideOrdersByTask={herbicideOrdersByTask}
          herbicideIntervalAlertsByTask={herbicideIntervalAlertsByTask}
        />
      )}
    </div>
  );
}

// =============================================================================
// Sub Components
// =============================================================================

function filterTasksByRange(tasks: AggregatedTask[], range: RangeKey): AggregatedTask[] {
  const option = RANGE_OPTIONS.find(opt => opt.key === range);
  const today = startOfDay(new Date());
  let startBoundary: Date | null = null;
  let endBoundary: Date | null = null;

  if (option && option.key !== 'all') {
    const days = option.days;
    startBoundary = subDays(today, days - 1);
    endBoundary = today;
  }

  return tasks.filter(task => {
    const rawDate = task.plannedDate || task.executionDate;
    if (!rawDate) return false;
    const taskDate = startOfDay(new Date(rawDate));
    if (startBoundary && taskDate < startBoundary) return false;
    if (endBoundary && taskDate > endBoundary) return false;
    return true;
  });
}

function TasksCalendar({
  tasksByDate,
  tasksById,
  currentMonth,
  onChangeMonth,
  onMoveTask,
  fieldOptions,
  bbchPredictionsByField,
  herbicideOrdersByTask,
  herbicideIntervalAlertsByTask,
}: {
  tasksByDate: Record<string, AggregatedTask[]>;
  tasksById: Map<string, AggregatedTask>;
  currentMonth: Date;
  onChangeMonth: (next: Date) => void;
  onMoveTask: (task: AggregatedTask, nextDateInput: string) => Promise<void> | void;
  fieldOptions: Array<{ uuid: string; name: string }>;
  bbchPredictionsByField: Map<string, CountryCropGrowthStagePrediction[]>;
  herbicideOrdersByTask: Map<string, number>;
  herbicideIntervalAlertsByTask: Map<string, boolean>;
}) {
  const { language, t } = useLanguage();
  const locale = language === 'ja' ? 'ja-JP' : 'en-US';

  const monthStart = startOfMonth(currentMonth);
  const monthEnd = endOfMonth(monthStart);
  const gridStart = startOfWeek(monthStart);
  const gridEnd = endOfWeek(monthEnd);
  const days = eachDayOfInterval({ start: gridStart, end: gridEnd });
  const today = new Date();
  const monthLabel = useMemo(
    () =>
      new Intl.DateTimeFormat(locale, { year: 'numeric', month: 'long', timeZone: 'Asia/Tokyo' }).format(
        monthStart,
      ),
    [locale, monthStart],
  );
  const weekdayLabels = useMemo(() => {
    const formatter = new Intl.DateTimeFormat(locale, { weekday: 'short', timeZone: 'Asia/Tokyo' });
    const base = startOfWeek(new Date(2020, 0, 5));
    return Array.from({ length: 7 }, (_value, index) => formatter.format(addDays(base, index)));
  }, [locale]);
  const tasksLimit = 3;
  const [hoverDateKey, setHoverDateKey] = useState<string | null>(null);
  const [movingTaskId, setMovingTaskId] = useState<string | null>(null);
  const [movingDateKey, setMovingDateKey] = useState<string | null>(null);
  const [selectedTask, setSelectedTask] = useState<AggregatedTask | null>(null);
  const [selectedDateInput, setSelectedDateInput] = useState<string>('');
  const [selectedFieldUuid, setSelectedFieldUuid] = useState<string>('');
  const [selectedDateTasks, setSelectedDateTasks] = useState<{ dateKey: string; tasks: AggregatedTask[] } | null>(null);
  const [inlineEditTaskId, setInlineEditTaskId] = useState<string | null>(null);
  const [inlineEditDateInput, setInlineEditDateInput] = useState<string>('');
  const [weatherByDate, setWeatherByDate] = useState<Record<string, {
    precipAvg: number;
    humidityAvg: number | null;
    tempAvg: number | null;
    windAvg: number | null;
    sunshineAvg: number | null;
    tone: 'good' | 'ok' | 'bad';
    years: number;
    soil: 'dry' | 'ok' | 'wet';
    soilBalance: number | null;
    dryingIndex: number | null;
  }>>({});
  const weatherCacheRef = useRef<Map<string, Record<string, {
    precipAvg: number;
    humidityAvg: number | null;
    tempAvg: number | null;
    windAvg: number | null;
    sunshineAvg: number | null;
    tone: 'good' | 'ok' | 'bad';
    years: number;
    soil: 'dry' | 'ok' | 'wet';
    soilBalance: number | null;
    dryingIndex: number | null;
  }>>>(new Map());
  const weatherFetchInFlight = useRef<Set<string>>(new Set());
  const { auth } = useAuth();

  useEffect(() => {
    if (selectedFieldUuid) return;
    if (fieldOptions.length === 0) return;
    setSelectedFieldUuid(fieldOptions[0].uuid);
  }, [fieldOptions, selectedFieldUuid]);

  useEffect(() => {
    if (!auth || !selectedFieldUuid) return;
    const cacheKey = `${selectedFieldUuid}:${format(currentMonth, 'yyyy')}`;
    const cached = weatherCacheRef.current.get(cacheKey);
    if (cached) {
      setWeatherByDate(cached);
      return;
    }
    if (weatherFetchInFlight.current.has(cacheKey)) return;
    weatherFetchInFlight.current.add(cacheKey);
    let cancelled = false;
    const fetchWeather = async () => {
      const yearStart = new Date(currentMonth.getFullYear(), 0, 1);
      const yearEnd = new Date(currentMonth.getFullYear(), 11, 31);
      const fromDate = toJstBoundaryIsoFromDate(new Date(currentMonth.getFullYear() - 4, 0, 1), false);
      const tillDate = toJstBoundaryIsoFromDate(yearEnd, true);
      try {
        const { ok, json } = await postJsonCached<any>(
          withApiBase('/weather-by-field'),
          {
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            field_uuid: selectedFieldUuid,
            from_date: fromDate,
            till_date: tillDate,
          },
          undefined,
          { cacheKey: `weather-by-field:${selectedFieldUuid}|${fromDate}|${tillDate}`, cache: 'session' },
        );
        if (!ok || !json?.ok) return;
        const daily = json?.response?.data?.fieldV2?.weatherHistoricForecastDaily ?? [];
        const monthDayStats = new Map<string, {
          precipSum: number;
          humiditySum: number;
          tempSum: number;
          windSum: number;
          sunshineSum: number;
          count: number;
        }>();
        daily.forEach((entry: any) => {
          const dateKey = getLocalDateString(entry?.date ?? '').replace(/\//g, '-');
          const parts = dateKey.split('-');
          if (parts.length !== 3) return;
          const monthDay = `${parts[1]}-${parts[2]}`;
          const precipRaw = entry?.precipitationBestMm;
          const humidityRaw = entry?.relativeHumidityPctAvg;
          const tempRaw = entry?.airTempCAvg;
          const windRaw = entry?.windSpeedMSAvg;
          const sunshineRaw = entry?.sunshineDurationH;
          const precip = typeof precipRaw === 'number' ? precipRaw : Number(precipRaw);
          const humidity = typeof humidityRaw === 'number' ? humidityRaw : Number(humidityRaw);
          const temp = typeof tempRaw === 'number' ? tempRaw : Number(tempRaw);
          const wind = typeof windRaw === 'number' ? windRaw : Number(windRaw);
          const sunshine = typeof sunshineRaw === 'number' ? sunshineRaw : Number(sunshineRaw);
          if (!Number.isFinite(precip)) return;
          const prev = monthDayStats.get(monthDay);
          if (prev) {
            prev.precipSum += precip;
            if (Number.isFinite(humidity)) prev.humiditySum += humidity;
            if (Number.isFinite(temp)) prev.tempSum += temp;
            if (Number.isFinite(wind)) prev.windSum += wind;
            if (Number.isFinite(sunshine)) prev.sunshineSum += sunshine;
            prev.count += 1;
          } else {
            monthDayStats.set(monthDay, {
              precipSum: precip,
              humiditySum: Number.isFinite(humidity) ? humidity : 0,
              tempSum: Number.isFinite(temp) ? temp : 0,
              windSum: Number.isFinite(wind) ? wind : 0,
              sunshineSum: Number.isFinite(sunshine) ? sunshine : 0,
              count: 1,
            });
          }
        });
        const map: Record<string, {
          precipAvg: number;
          humidityAvg: number | null;
          tempAvg: number | null;
          windAvg: number | null;
          sunshineAvg: number | null;
          tone: 'good' | 'ok' | 'bad';
          years: number;
          soil: 'dry' | 'ok' | 'wet';
          soilBalance: number | null;
          dryingIndex: number | null;
        }> = {};
        const targetDays = eachDayOfInterval({ start: yearStart, end: yearEnd });
        targetDays.forEach((day) => {
          const dateKey = format(day, 'yyyy-MM-dd');
          const monthDay = format(day, 'MM-dd');
          const stats = monthDayStats.get(monthDay);
          if (!stats || stats.count === 0) return;
          const precipAvg = stats.precipSum / stats.count;
          const humidityAvg = stats.humiditySum ? stats.humiditySum / stats.count : null;
          const tempAvg = stats.tempSum ? stats.tempSum / stats.count : null;
          const windAvg = stats.windSum ? stats.windSum / stats.count : null;
          const sunshineAvg = stats.sunshineSum ? stats.sunshineSum / stats.count : null;
          const tone = precipAvg <= 1 ? 'good' : precipAvg <= 5 ? 'ok' : 'bad';
          const dryingIndex = (() => {
            if (tempAvg === null || humidityAvg === null || windAvg === null || sunshineAvg === null) return null;
            return Math.max(0, tempAvg * 0.3 + windAvg * 1.5 + sunshineAvg * 0.8 - humidityAvg * 0.2);
          })();
          const soilBalance = dryingIndex === null ? null : precipAvg - dryingIndex;
          const soil = soilBalance === null
            ? 'ok'
            : soilBalance <= -2
              ? 'dry'
              : soilBalance >= 2
                ? 'wet'
                : 'ok';
          map[dateKey] = {
            precipAvg,
            humidityAvg,
            tempAvg,
            windAvg,
            sunshineAvg,
            tone,
            years: stats.count,
            soil,
            soilBalance,
            dryingIndex,
          };
        });
        if (!cancelled) {
          weatherCacheRef.current.set(cacheKey, map);
          setWeatherByDate(map);
        }
      } catch {
        if (!cancelled) setWeatherByDate({});
      } finally {
        weatherFetchInFlight.current.delete(cacheKey);
      }
    };
    fetchWeather();
    return () => {
      cancelled = true;
    };
  }, [auth, currentMonth, selectedFieldUuid]);

  const handleDrop = useCallback(
    async (event: React.DragEvent<HTMLDivElement>, dateKey: string) => {
      event.preventDefault();
      setHoverDateKey(null);
      const payload = event.dataTransfer.getData('application/x-task');
      if (!payload) return;
      try {
        const data = JSON.parse(payload) as { uuid?: string };
        if (!data.uuid) return;
        const task = tasksById.get(data.uuid);
        if (!task || task.type !== 'Spraying') return;
        const currentKey = format(startOfDay(new Date(task.plannedDate || task.executionDate || '')), 'yyyy-MM-dd');
        if (currentKey === dateKey) return;
        setMovingTaskId(task.uuid);
        setMovingDateKey(dateKey);
        await onMoveTask(task, dateKey);
      } catch {
        // ignore malformed payload
      } finally {
        setMovingTaskId(null);
        setMovingDateKey(null);
      }
    },
    [onMoveTask, tasksById]
  );

  const openDatePicker = useCallback((task: AggregatedTask) => {
    if (task.type !== 'Spraying') return;
    const current = getJstDateInputValue(task.plannedDate || task.executionDate);
    setSelectedTask(task);
    setSelectedDateInput(current);
  }, []);

  const closeDatePicker = useCallback(() => {
    setSelectedTask(null);
    setSelectedDateInput('');
  }, []);

  const handleApplyDate = useCallback(async () => {
    if (!selectedTask || !selectedDateInput) return;
    setMovingTaskId(selectedTask.uuid);
    setMovingDateKey(selectedDateInput);
    try {
      await onMoveTask(selectedTask, selectedDateInput);
    } finally {
      setMovingTaskId(null);
      setMovingDateKey(null);
      closeDatePicker();
    }
  }, [closeDatePicker, onMoveTask, selectedDateInput, selectedTask]);

  return (
    <div className="tasks-calendar">
      <div className="tasks-calendar-header">
        <button type="button" onClick={() => onChangeMonth(subMonths(monthStart, 1))}>
          {t('tasks.calendar.prev_month')}
        </button>
        <h3>{monthLabel}</h3>
        <button type="button" onClick={() => onChangeMonth(addMonths(monthStart, 1))}>
          {t('tasks.calendar.next_month')}
        </button>
        <button type="button" onClick={() => onChangeMonth(startOfMonth(new Date()))}>
          {t('tasks.calendar.today')}
        </button>
        <div className="tasks-calendar-field">
          <label htmlFor="tasks-calendar-field-select">{t('table.field')}</label>
          <select
            id="tasks-calendar-field-select"
            value={selectedFieldUuid}
            onChange={(event) => setSelectedFieldUuid(event.target.value)}
          >
            {fieldOptions.length === 0 && <option value="">{t('tasks.calendar.no_fields')}</option>}
            {fieldOptions.map(option => (
              <option key={option.uuid} value={option.uuid}>
                {option.name}
              </option>
            ))}
          </select>
        </div>
      </div>
      <div className="tasks-calendar-weekdays">
        {weekdayLabels.map(label => (
          <div key={label} className="tasks-calendar-weekday">
            {label}
          </div>
        ))}
      </div>
      <div className="tasks-calendar-grid">
        {days.map(day => {
          const dateKey = format(day, 'yyyy-MM-dd');
          const tasks = tasksByDate[dateKey] ?? [];
          const visible = tasks.slice(0, tasksLimit);
          const extra = tasks.length - visible.length;
          const bbchPredictions = selectedFieldUuid ? bbchPredictionsByField.get(selectedFieldUuid) ?? [] : [];
          const bbchIndex = selectedFieldUuid ? findBbchIndexForDate(bbchPredictions, dateKey) : '';
          const bbchStyle = bbchIndex ? getBbchBadgeStyle(bbchIndex) : null;
          const weatherInfo = weatherByDate[dateKey];
          const weatherLabel = weatherInfo
            ? weatherInfo.tone === 'good'
              ? t('tasks.weather.rain.low')
              : weatherInfo.tone === 'ok'
                ? t('tasks.weather.rain.medium')
                : t('tasks.weather.rain.high')
            : null;
          const soilLabel = weatherInfo
            ? weatherInfo.soil === 'dry'
              ? t('tasks.weather.soil.dry')
              : weatherInfo.soil === 'ok'
                ? t('tasks.weather.soil.ok')
                : t('tasks.weather.soil.wet')
            : null;
          return (
            <div
              key={dateKey}
              className={[
                'tasks-calendar-cell',
                isSameMonth(day, monthStart) ? 'is-current' : 'is-outside',
                isSameDay(day, today) ? 'is-today' : '',
                hoverDateKey === dateKey ? 'is-drop-target' : '',
                movingDateKey === dateKey ? 'is-loading' : '',
              ].join(' ')}
              onClick={() => {
                if (!isSameMonth(day, monthStart)) {
                  onChangeMonth(startOfMonth(day));
                }
              }}
              onDragOver={(event) => {
                event.preventDefault();
                setHoverDateKey(dateKey);
              }}
              onDragLeave={() => setHoverDateKey((prev) => (prev === dateKey ? null : prev))}
              onDrop={(event) => handleDrop(event, dateKey)}
            >
              <div className="tasks-calendar-date">{format(day, 'd')}</div>
              {weatherInfo && weatherLabel && (
                <div
                  className={`tasks-calendar-weather tasks-calendar-weather--${weatherInfo.tone}`}
                  title={t('tasks.weather.rain.tooltip', {
                    years: weatherInfo.years,
                    mm: weatherInfo.precipAvg.toFixed(1),
                  })}
                >
                  {weatherLabel}
                </div>
              )}
              {weatherInfo && soilLabel && (
                <div
                  className={`tasks-calendar-soil tasks-calendar-soil--${weatherInfo.soil}`}
                  title={t('tasks.weather.soil.tooltip', {
                    years: weatherInfo.years,
                    precip: weatherInfo.precipAvg.toFixed(1),
                    humidity:
                      weatherInfo.humidityAvg !== null ? weatherInfo.humidityAvg.toFixed(0) : '-',
                    temp: weatherInfo.tempAvg !== null ? weatherInfo.tempAvg.toFixed(1) : '-',
                    wind: weatherInfo.windAvg !== null ? weatherInfo.windAvg.toFixed(1) : '-',
                    sunshine: weatherInfo.sunshineAvg !== null ? weatherInfo.sunshineAvg.toFixed(1) : '-',
                    dryingIndex:
                      weatherInfo.dryingIndex !== null ? weatherInfo.dryingIndex.toFixed(1) : '-',
                    soilBalance:
                      weatherInfo.soilBalance !== null ? weatherInfo.soilBalance.toFixed(1) : '-',
                  })}
                >
                  {soilLabel}
                </div>
              )}
              {bbchIndex && (
                <div
                  className="tasks-calendar-bbch"
                  title={`BBCH ${bbchIndex}`}
                  style={bbchStyle ?? undefined}
                >
                  BBCH {bbchIndex}
                </div>
              )}
              <div className="tasks-calendar-items">
                {visible.map(task => {
                  const typeKey = getChartTypeKey(task);
                  const color = TASK_COLOR_MAP[typeKey] ?? '#9e9e9e';
                  const herbicideOrder = herbicideOrdersByTask.get(task.uuid);
                  const intervalAlert = herbicideIntervalAlertsByTask.get(task.uuid);
                  const baseLabel = getTaskLabel(task);
                  const badgeText = herbicideOrder
                    ? t('tasks.herbicide.badge_paren', { order: herbicideOrder })
                    : '';
                  const title = `${baseLabel}${badgeText} / ${task.fieldName}`;
                  const canDrag = task.type === 'Spraying';
                  const isMoving = movingTaskId === task.uuid;
                  return (
                    <div
                      key={task.uuid}
                      className={`tasks-calendar-item ${canDrag ? 'is-draggable' : 'is-locked'} ${isMoving ? 'is-moving' : ''}`}
                      title={
                        canDrag
                          ? `${title} ${t('tasks.calendar.drag_hint')}`
                          : `${title} ${t('tasks.calendar.locked_hint')}`
                      }
                      draggable={canDrag}
                      onClick={() => canDrag && openDatePicker(task)}
                      onDragStart={(event) => {
                        if (!canDrag) {
                          event.preventDefault();
                          return;
                        }
                        event.dataTransfer.setData('application/x-task', JSON.stringify({ uuid: task.uuid }));
                        event.dataTransfer.effectAllowed = 'move';
                      }}
                    >
                      <span className="tasks-calendar-dot" style={{ backgroundColor: color }} />
                      <span className="tasks-calendar-item__text">
                        <span className="tasks-calendar-item__label">
                          {baseLabel}
                          {herbicideOrder && (
                            <span className="tasks-calendar-badge tasks-calendar-badge--herbicide">
                              {t('tasks.herbicide.badge', { order: herbicideOrder })}
                            </span>
                          )}
                          {intervalAlert && (
                            <span className="tasks-calendar-badge tasks-calendar-badge--alert">
                              {t('tasks.herbicide.interval_lt_20d')}
                            </span>
                          )}
                        </span>
                        <span className="tasks-calendar-item__field">{task.fieldName}</span>
                        {isMoving && (
                          <span className="tasks-calendar-item__loading">
                            <LoadingSpinner size={12} />
                            {t('action.updating')}
                          </span>
                        )}
                      </span>
                    </div>
                  );
                })}
                {extra > 0 && (
                  <button
                    type="button"
                    className="tasks-calendar-more"
                    onClick={() => setSelectedDateTasks({ dateKey, tasks })}
                  >
                    {t('tasks.calendar.more', { count: extra })}
                  </button>
                )}
              </div>
            </div>
          );
        })}
      </div>
      {selectedTask && (
        <div className="tasks-calendar-modal-backdrop" onClick={closeDatePicker}>
          <div className="tasks-calendar-modal" onClick={(event) => event.stopPropagation()}>
            <div className="tasks-calendar-modal-header">
              <h4>{t('tasks.calendar.edit_spraying_title')}</h4>
              <button type="button" onClick={closeDatePicker}>{t('action.close')}</button>
            </div>
            <div className="tasks-calendar-modal-body">
              <p className="tasks-calendar-modal-title">
                {getTaskLabel(selectedTask)} / {selectedTask.fieldName}
              </p>
              <label>
                {t('tasks.calendar.new_date')}
                <input
                  type="date"
                  value={selectedDateInput}
                  onChange={(event) => setSelectedDateInput(event.target.value)}
                />
              </label>
            </div>
            <div className="tasks-calendar-modal-footer">
              <button
                type="button"
                onClick={handleApplyDate}
                disabled={!selectedDateInput || Boolean(movingTaskId)}
              >
                {t('tasks.calendar.save_changes')}
              </button>
            </div>
          </div>
        </div>
      )}
      {selectedDateTasks && (
        <div
          className="tasks-calendar-modal-backdrop"
          onClick={() => setSelectedDateTasks(null)}
        >
          <div className="tasks-calendar-modal tasks-calendar-modal--wide" onClick={(event) => event.stopPropagation()}>
            <div className="tasks-calendar-modal-header">
              <h4>
                {t('tasks.calendar.tasks_on_date', {
                  date: format(new Date(selectedDateTasks.dateKey), 'yyyy/MM/dd'),
                })}
              </h4>
              <button type="button" onClick={() => setSelectedDateTasks(null)}>{t('action.close')}</button>
            </div>
            <div className="tasks-calendar-modal-body">
              <div className="tasks-calendar-task-list">
                {selectedDateTasks.tasks.map(task => {
                  const herbicideOrder = herbicideOrdersByTask.get(task.uuid);
                  const intervalAlert = herbicideIntervalAlertsByTask.get(task.uuid);
                  const baseLabel = getTaskLabel(task);
                  return (
                    <div key={task.uuid} className="tasks-calendar-task-row">
                      <span className="tasks-calendar-task-type">
                        {baseLabel}
                        {herbicideOrder && (
                          <span className="tasks-calendar-badge tasks-calendar-badge--herbicide">
                            {t('tasks.herbicide.badge', { order: herbicideOrder })}
                          </span>
                        )}
                        {intervalAlert && (
                          <span className="tasks-calendar-badge tasks-calendar-badge--alert">
                            {t('tasks.herbicide.interval_lt_20d')}
                          </span>
                        )}
                      </span>
                      <span className="tasks-calendar-task-field">{task.fieldName}</span>
                      <span className="tasks-calendar-task-crop">{task.cropName}</span>
                      {task.type === 'Spraying' && (
                        <div className="tasks-calendar-task-actions">
                          {inlineEditTaskId === task.uuid ? (
                            <>
                              <input
                                type="date"
                                value={inlineEditDateInput}
                                onChange={(event) => setInlineEditDateInput(event.target.value)}
                              />
                              <button
                                type="button"
                                onClick={async () => {
                                  if (!inlineEditDateInput) return;
                                  setMovingTaskId(task.uuid);
                                  setMovingDateKey(inlineEditDateInput);
                                  try {
                                    await onMoveTask(task, inlineEditDateInput);
                                    setInlineEditTaskId(null);
                                    setInlineEditDateInput('');
                                  } finally {
                                    setMovingTaskId(null);
                                    setMovingDateKey(null);
                                  }
                                }}
                                disabled={!inlineEditDateInput || Boolean(movingTaskId)}
                              >
                                {t('action.save')}
                              </button>
                              <button
                                type="button"
                                onClick={() => {
                                  setInlineEditTaskId(null);
                                  setInlineEditDateInput('');
                                }}
                              >
                                {t('action.cancel')}
                              </button>
                            </>
                          ) : (
                            <button
                              type="button"
                              onClick={() => {
                                setInlineEditTaskId(task.uuid);
                                setInlineEditDateInput(getJstDateInputValue(task.plannedDate || task.executionDate));
                              }}
                            >
                              {t('tasks.calendar.change_date')}
                            </button>
                          )}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function TasksChart({
  tasks,
  range,
  onRangeChange,
}: {
  tasks: AggregatedTask[];
  range: RangeKey;
  onRangeChange: (key: RangeKey) => void;
}) {
  const { language, t } = useLanguage();
  const [mode, setMode] = useState<ChartMode>('count');
  const [selectedDate, setSelectedDate] = useState<string | null>(null);
  const chartRef = useRef<ChartJSInstance | null>(null);
  const usageSummary = useMemo(() => aggregateUsageSummary(tasks), [tasks]);
  const usageCsvRows = useMemo(() => aggregateUsageCsvRows(tasks), [tasks]);

  const { labels, datasets, totalsByDate } = useMemo(() => {
    const countByDate: Record<string, Record<string, number>> = {};
    const areaByDate: Record<string, Record<string, number>> = {};
    const totalsByDate: Record<string, { count: number; area: number }> = {};
    const dateSet = new Set<string>();
    const round2 = (num: number) => Math.round(num * 100) / 100;

    tasks.forEach(task => {
      const rawDate = task.plannedDate || task.executionDate;
      if (!rawDate) return;
      const taskDate = startOfDay(new Date(rawDate));
      const dateKey = format(taskDate, 'yyyy-MM-dd');
      dateSet.add(dateKey);
      if (!countByDate[dateKey]) {
        countByDate[dateKey] = {};
        areaByDate[dateKey] = {};
      }
      const typeKey = getChartTypeKey(task);
      countByDate[dateKey][typeKey] = (countByDate[dateKey][typeKey] ?? 0) + 1;
      // 面積は ha 換算し小数第2位で丸める
      const areaValue = round2((task.fieldArea ?? 0) / 10000);
      areaByDate[dateKey][typeKey] = (areaByDate[dateKey][typeKey] ?? 0) + areaValue;
    });

    const labelsAll = Array.from(dateSet).sort();
    const orderedTypes = Array.from(
      new Set([
        ...Object.keys(CHART_TYPE_LABEL_KEYS),
        ...tasks.map(task => getChartTypeKey(task)),
      ])
    );

    const datasetsAll = orderedTypes.reduce<ChartDataset<'bar', number[]>[]>((acc, typeKey) => {
      const labelKey = CHART_TYPE_LABEL_KEYS[typeKey] ?? TASK_TYPE_LABEL_KEYS[typeKey];
      const label = labelKey ? t(labelKey) : typeKey;
      const color = TASK_COLOR_MAP[typeKey] ?? '#9e9e9e';
      const data = labelsAll.map(date => {
        const byType = mode === 'area' ? areaByDate : countByDate;
        const value = byType[date]?.[typeKey] ?? 0;
        const numeric = typeof value === 'number' ? value : Number(value) || 0;
        return round2(numeric);
      });
      const total = data.reduce((sum, value) => (Number.isFinite(value) ? sum + value : sum), 0);
      if (total <= 0) {
        return acc;
      }
      const dataset: ChartDataset<'bar', number[]> = {
        label,
        data,
        backgroundColor: color,
        borderColor: color,
        borderWidth: 1,
        stack: 'tasks',
      };
      acc.push(dataset);
      return acc;
    }, []);

    labelsAll.forEach(date => {
      const countTotal = Object.values(countByDate[date] ?? {}).reduce((sum, v) => sum + v, 0);
      const areaTotal = Object.values(areaByDate[date] ?? {}).reduce((sum, v) => sum + v, 0);
      totalsByDate[date] = { count: countTotal, area: areaTotal };
    });

    let labels = labelsAll.filter(date => {
      const totals = totalsByDate[date] || { count: 0, area: 0 };
      if (totals.count <= 0 && totals.area <= 0) return false;
      return mode === 'area' ? totals.area > 0 : totals.count > 0;
    });

    let datasets = datasetsAll;
    return { labels, datasets, totalsByDate };
  }, [tasks, mode, language, t]);

  useEffect(() => {
    if (selectedDate && !labels.includes(selectedDate)) {
      setSelectedDate(null);
    }
  }, [labels, selectedDate]);

  const selectedTasks = useMemo(() => {
    if (!selectedDate) return [];
    return tasks
      .filter(task => {
        const rawDate = task.plannedDate || task.executionDate;
        if (!rawDate) return false;
        return format(startOfDay(new Date(rawDate)), 'yyyy-MM-dd') === selectedDate;
      })
      .sort((a, b) => {
        const dateA = a.executionDate || a.plannedDate || '';
        const dateB = b.executionDate || b.plannedDate || '';
        return dateA.localeCompare(dateB);
      });
  }, [selectedDate, tasks]);

  const totalsForSelected = selectedDate ? totalsByDate[selectedDate] : undefined;
  const selectedUsageSummary = useMemo(() => aggregateUsageSummary(selectedTasks), [selectedTasks]);

  const handleResetZoom = () => {
    chartRef.current?.resetZoom?.();
  };

  const handleDownloadUsageCsv = () => {
    if (!usageCsvRows.length) return;
    const csvEscape = (value: unknown) => {
      const text = String(value ?? '')
        .replace(/\r\n/g, '\n')
        .replace(/\r/g, '\n')
        .replace(/\n/g, ' ');
      return `"${text.replace(/"/g, '""')}"`;
    };
    const header = [
      t('tasks.usage_csv.date'),
      t('tasks.usage_csv.name'),
      t('tasks.usage_csv.form'),
      t('tasks.usage_csv.total'),
      t('tasks.usage_csv.unit'),
    ];
    const rows = usageCsvRows.map(item => [
      item.date,
      item.name,
      item.formLabel ?? '',
      formatAmountUpTo2(item.totalValue),
      item.unitLabel,
    ]);
    const csv = [header, ...rows]
      .map(cols => cols.map(csvEscape).join(','))
      .join('\r\n');
    const bom = new Uint8Array([0xef, 0xbb, 0xbf]);
    const blob = new Blob([bom, csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'spray_usage.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  const options = useMemo(() => {
    const isArea = mode === 'area';
    return {
      responsive: true,
      maintainAspectRatio: false,
      layout: {
        padding: {
          top: 12,
        },
      },
      interaction: { intersect: false, mode: 'nearest' as const },
      onClick: (_event: any, elements: any[]) => {
        if (!elements?.length) return;
        const index = elements[0].index;
        const clicked = labels[index];
        if (clicked) {
          setSelectedDate(clicked);
        }
      },
      plugins: {
        legend: {
          position: 'bottom' as const,
          labels: {
            color: '#e0e0e0',
          },
        },
        tooltip: {
          filter: (item: any) => {
            const value = typeof item.parsed?.y === 'number' ? item.parsed.y : item.raw;
            return typeof value === 'number' && value !== 0;
          },
          callbacks: {
            label: (context: any) => {
              const rawValue = typeof context.parsed?.y === 'number' ? context.parsed.y : context.raw;
              if (!rawValue) return undefined;
              const datasetLabel = context.dataset.label || '';
              if (isArea) {
                return t('tasks.chart.tooltip.area', {
                  label: datasetLabel,
                  value: rawValue.toFixed(2),
                });
              }
              return t('tasks.chart.tooltip.count', { label: datasetLabel, value: rawValue });
            },
            footer: (items: any[]) => {
              if (!items?.length) return '';
              const index = items[0].dataIndex;
              const date = labels[index];
              const totals = date ? totalsByDate[date] : undefined;
              const totalValue = totals ? (isArea ? totals.area : totals.count) : 0;
              if (!totalValue) return '';
              return isArea
                ? t('tasks.chart.tooltip.total_area', { value: totalValue.toFixed(2) })
                : t('tasks.chart.tooltip.total_count', { value: totalValue });
            },
          },
        },
        datalabels: { // データラベルプラグインの設定
          display: (context: any) => { // ラベルを表示する条件
            const dataIndex = context.dataIndex;
            const datasets = context.chart.data.datasets || [];
            let topIndex = -1;
            datasets.forEach((ds: any, idx: number) => {
              const value = Number((ds.data?.[dataIndex] ?? 0));
              if (Number.isFinite(value) && value > 0) {
                topIndex = idx;
              }
            });
            if (topIndex < 0) return false;
            return context.datasetIndex === topIndex;
          },
          formatter: (_value: any, context: any) => { // ラベルのフォーマット
            const date = labels[context.dataIndex];
            if (!date) return '';
            const total = totalsByDate[date];
            if (!total) return '';
            // モードに応じて合計件数または合計面積を表示
            const value = isArea ? total.area : total.count;
            if (value === 0) return '';
            return isArea ? value.toFixed(2) : value;
          },
          color: '#e0e0e0', // ラベルの色
          font: {
            weight: 'bold', // フォントの太さ
          },
          anchor: 'end', // ラベルの表示位置（バーの上端）
          align: 'end', // ラベルの整列位置
          offset: -6, // バーからのオフセット
        },
        zoom: {
          pan: {
            enabled: true,
            mode: 'x' as const,
            modifierKey: 'shift' as const,
          },
          zoom: {
            wheel: { enabled: true },
            pinch: { enabled: true },
            drag: {
              enabled: true,
              backgroundColor: 'rgba(100,108,255,0.15)',
              borderColor: 'rgba(100,108,255,0.3)',
              borderWidth: 1,
            },
            mode: 'x' as const,
          },
          limits: {
            x: { minRange: 1 },
          },
        },
      },
      scales: {
        x: {
          stacked: true,
          ticks: {
            color: '#e0e0e0',
          },
          grid: {
            color: 'rgba(255,255,255,0.1)',
          },
        },
        y: {
          stacked: true,
          ticks: {
            color: '#e0e0e0',
            callback: (value: string | number) => {
              if (isArea && typeof value === 'number') {
                return value.toFixed(0);
              }
              return value;
            },
          },
          grid: {
            color: 'rgba(255,255,255,0.1)',
          },
        },
      },
    };
  }, [mode, labels, totalsByDate, setSelectedDate]);

  const noData = labels.length === 0 || datasets.length === 0;

  return (
    <div className="tasks-chart-card">
      <div className="tasks-chart-header">
        <h3>{t('tasks.chart.title')}</h3>
      </div>
      {usageSummary.length > 0 && (
        <div className="usage-summary-card">
          <div className="usage-summary-head">
            <div>
              <div className="usage-summary-title">{t('tasks.usage.title')}</div>
              <div className="usage-summary-sub">{t('tasks.usage.subtitle')}</div>
            </div>
            <button type="button" className="usage-download-btn" onClick={handleDownloadUsageCsv}>
              {t('tasks.usage.export')}
            </button>
          </div>
          <div className="usage-summary-list">
            {usageSummary.map((item, idx) => (
              <div key={`usage-${idx}`} className="usage-pill">
                <div className="usage-pill-name">{item.name}</div>
                {item.formLabel && <span className="usage-pill-form">{item.formLabel}</span>}
                <span className="usage-pill-total">{formatAmountUpTo2(item.totalValue)} {item.unitLabel}</span>
              </div>
            ))}
          </div>
        </div>
      )}
      <div className="tasks-chart-controls">
        <div className="tasks-chart-range">
          {RANGE_OPTIONS.map(option => (
            <button
              key={option.key}
              type="button"
              className={range === option.key ? 'active' : ''}
              onClick={() => onRangeChange(option.key)}
            >
              {t(option.labelKey)}
            </button>
          ))}
        </div>
        <div className="tasks-chart-toggle">
          <button type="button" className="tasks-chart-reset" onClick={handleResetZoom}>
            {t('tasks.chart.reset_zoom')}
          </button>
          {(Object.keys(MODE_LABEL_KEYS) as ChartMode[]).map(modeKey => (
            <button
              key={modeKey}
              type="button"
              className={mode === modeKey ? 'active' : ''}
              onClick={() => setMode(modeKey)}
            >
              {t(MODE_LABEL_KEYS[modeKey])}
            </button>
          ))}
        </div>
      </div>
      <div className="tasks-chart-wrapper">
        {noData ? (
          <div className="tasks-chart-empty" style={{ textAlign: 'center', paddingTop: '4rem' }}>
            {t('tasks.chart.empty_range')}
          </div>
        ) : (
          <Bar
            plugins={[ChartDataLabels]}
            ref={(instance) => {
              chartRef.current = instance ?? null;
            }}
            data={{ labels, datasets }}
            options={options as any}
          />
        )}
      </div>
      <div className="tasks-chart-footnote">
        {mode === 'area'
          ? t('tasks.chart.footnote_area')
          : t('tasks.chart.footnote_count')}
      </div>
      {selectedDate && (
        <div className="tasks-detail-card">
          <div className="tasks-detail-header">
            <h4>{t('tasks.chart.tasks_on_date', { date: selectedDate })}</h4>
            <button type="button" onClick={() => setSelectedDate(null)}>{t('action.close')}</button>
          </div>
          {totalsForSelected && (
            <div className="tasks-detail-summary">
              <span className="summary-chip">
                {t('tasks.chart.total_tasks', { count: totalsForSelected.count })}
              </span>
              <span className="summary-chip">
                {totalsForSelected.area.toFixed(2)} ha
              </span>
              {selectedUsageSummary.map((item, idx) => (
                <span key={`selected-usage-${idx}`} className="summary-chip summary-chip-usage">
                  {item.name} {formatAmountUpTo2(item.totalValue)} {item.unitLabel}
                </span>
              ))}
            </div>
          )}
          {selectedTasks.length === 0 ? (
            <p className="tasks-chart-empty">{t('tasks.chart.no_details')}</p>
          ) : (
            <ul className="tasks-detail-list">
              {selectedTasks.map(task => {
                const lead = calculateLeadTimeInfo(task);
                const statusLabel = getStatusLabel(task.state);
                const statusClass = getStatusClass(task.state);
                const assigneeName = task.assignee
                  ? `${task.assignee.lastName || ''} ${task.assignee.firstName || ''}`.trim() ||
                    t('tasks.assignee.unassigned')
                  : t('tasks.assignee.unassigned');
                const recipes = buildRecipeDisplay(task);
                return (
                  <li key={task.uuid}>
                    <div className="tasks-detail-main">
                      <div className="tasks-detail-title">
                        <span className={`task-type-label type-${task.type.toLowerCase()}`}>
                          {getTaskLabel(task)}
                        </span>
                        <span className="tasks-detail-field">{task.fieldName}</span>
                        <span className="tasks-detail-area">{formatAreaHa(task.fieldArea)}</span>
                      </div>
                      <div className="tasks-detail-subtitle">
                        <span>{task.cropName || t('tasks.crop.no_info')}</span>
                      </div>
                    </div>
                    <div className="tasks-detail-grid">
                      <div>
                        <p className="tasks-detail-label">{t('tasks.detail.planned_execution')}</p>
                        <p className="tasks-detail-value">
                          {getLocalDateString(task.plannedDate) || '-'} / {getLocalDateString(task.executionDate) || '-'}
                        </p>
                      </div>
                      <div>
                        <p className="tasks-detail-label">{t('tasks.detail.status')}</p>
                        <p className="tasks-detail-value">
                          <span className={`task-status-badge ${statusClass}`}>
                            {statusLabel}
                          </span>
                        </p>
                      </div>
                      <div>
                        <p className="tasks-detail-label">{t('tasks.detail.lead_time')}</p>
                        <p className={`tasks-detail-value ${lead.className ?? ''}`}>
                          {lead.text}
                        </p>
                      </div>
                      <div>
                        <p className="tasks-detail-label">{t('tasks.detail.assignee')}</p>
                        <p className={`tasks-detail-value ${task.assignee ? '' : 'unassigned'}`}>
                          {task.assignee ? assigneeName : t('tasks.assignee.unassigned')}
                        </p>
                      </div>
                    </div>
                    {recipes.length > 0 && (
                      <div className="tasks-detail-recipes">
                        <p className="tasks-detail-label">{t('tasks.detail.recipe')}</p>
                        <div className="recipe-list">
                          {recipes.map((entry, idx) => (
                            <div key={`${task.uuid}-detail-recipe-${idx}`} className="recipe-item">
                              <div className="recipe-name">{entry.name}</div>
                              <div className="recipe-meta">
                                {entry.formLabel && <span className="recipe-chip">{entry.formLabel}</span>}
                                {entry.per10a && <span className="recipe-chip recipe-chip-10a">{entry.per10a}</span>}
                                {entry.total && (
                                  <span className="recipe-chip recipe-chip-total">
                                    {t('tasks.recipe.total', { total: entry.total })}
                                  </span>
                                )}
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}
                    {task.note && <p className="tasks-detail-note">{task.note}</p>}
                  </li>
                );
              })}
            </ul>
          )}
        </div>
      )}
    </div>
  );
}

type LeadTimeInfo = {
  text: string;
  className?: string;
};

type RecipeDisplay = {
  name: string;
  formLabel?: string;
  per10a?: string;
  total?: string;
};

type UsageSummary = {
  name: string;
  formLabel?: string;
  totalValue: number;
  unitLabel: 'kg' | 'L';
};

type UsageCsvRow = {
  date: string;
  name: string;
  formLabel?: string;
  totalValue: number;
  unitLabel: 'kg' | 'L';
};

function calculateLeadTimeInfo(task: AggregatedTask): LeadTimeInfo {
  const { plannedDate, executionDate } = task;
  const today = startOfDay(new Date());

  if (plannedDate && executionDate) {
    const diff = differenceInCalendarDays(new Date(executionDate), new Date(plannedDate));
    if (diff === 0) {
      return { text: tr('tasks.lead_time.on_time'), className: 'leadtime-on-time' };
    }
    if (diff > 0) {
      return { text: tr('tasks.lead_time.late', { days: diff }), className: 'leadtime-late' };
    }
    return { text: tr('tasks.lead_time.early', { days: Math.abs(diff) }), className: 'leadtime-early' };
  }

  if (plannedDate) {
    const diff = differenceInCalendarDays(new Date(plannedDate), today);
    if (diff > 0) {
      return { text: tr('tasks.lead_time.in_days', { days: diff }) };
    }
    if (diff === 0) {
      return { text: tr('tasks.lead_time.today'), className: 'leadtime-today' };
    }
    return { text: tr('tasks.lead_time.overdue', { days: Math.abs(diff) }), className: 'leadtime-late' };
  }

  return { text: '-' };
}

function formatAreaHa(areaM2?: number | null): string {
  if (areaM2 === undefined || areaM2 === null) return '-';
  const ha = areaM2 / 10000;
  if (!Number.isFinite(ha)) return '-';
  return `${ha.toFixed(2)} ha`;
}

function formatAmount(value: number): string {
  const hasFraction = Math.abs(value - Math.round(value)) > 1e-6;
  const locale = getCurrentLanguage() === 'ja' ? 'ja-JP' : 'en-US';
  return new Intl.NumberFormat(locale, {
    maximumFractionDigits: 1,
    minimumFractionDigits: hasFraction ? 1 : 0,
  }).format(value);
}

function formatAmountUpTo2(value: number): string {
  const hasFraction = Math.abs(value - Math.round(value)) > 1e-6;
  const needsTwo = Math.abs(value * 10 - Math.round(value * 10)) > 1e-6;
  const locale = getCurrentLanguage() === 'ja' ? 'ja-JP' : 'en-US';
  return new Intl.NumberFormat(locale, {
    maximumFractionDigits: 2,
    minimumFractionDigits: hasFraction ? (needsTwo ? 2 : 1) : 0,
  }).format(value);
}

function formatPer10a(rawRate: number | string | null | undefined, unit?: string | null): string | null {
  if (rawRate === null || rawRate === undefined || rawRate === '') return null;
  if (!unit) return null;
  const rate = Number(rawRate);
  if (!Number.isFinite(rate)) return null;
  const upper = unit.toUpperCase();
  if (upper === 'KGPM2') {
    return `${formatAmount(rate * 1000)} kg/10a`;
  }
  if (upper === 'M3PM2') {
    return `${formatAmount(rate * 1000 * 1000)} L/10a`;
  }
  return null;
}

function convertTotalValue(total: number | null | undefined, unit?: string | null): { value: number; unitLabel: 'kg' | 'L' } | null {
  if (total === null || total === undefined || !unit) return null;
  const upper = unit.toUpperCase();
  if (upper === 'KGPM2') {
    return { value: total, unitLabel: 'kg' };
  }
  if (upper === 'M3PM2') {
    return { value: total * 1000, unitLabel: 'L' };
  }
  return null;
}

function formatTotalAmount(total: number | null | undefined, unit?: string | null): string | null {
  if (total === null || total === undefined || !unit) return null;
  const upper = unit.toUpperCase();
  if (upper === 'KGPM2') {
    return `${formatAmountUpTo2(total)} kg`;
  }
  if (upper === 'M3PM2') {
    return `${formatAmountUpTo2(total * 1000)} L`;
  }
  return null;
}

function mapFormulationLabel(formulation?: string | null): string | undefined {
  if (!formulation) return undefined;
  const upper = formulation.toUpperCase();
  if (upper === 'SOLID') return tr('tasks.formulation.solid');
  if (upper === 'LIQUID') return tr('tasks.formulation.liquid');
  if (upper === 'GRANULAR') return tr('tasks.formulation.granular');
  return formulation;
}

function mapIngredientName(item: SubstanceApplicationRate): string {
  if (item.name) return item.name;
  if ((item.type || '').toUpperCase() === 'WATER') return tr('tasks.ingredient.water');
  return tr('tasks.ingredient.unknown');
}

function getRateForPer10a(item: SubstanceApplicationRate, taskAreaM2?: number | null): number | null {
  if (item.averageApplicationRate !== null && item.averageApplicationRate !== undefined) {
    return item.averageApplicationRate;
  }
  if (item.ratesPerZone?.length) {
    const rate = item.ratesPerZone[0];
    if (rate !== null && rate !== undefined) return rate;
  }
  if (item.ratesByZone?.length) {
    const rate = item.ratesByZone[0]?.rate;
    if (rate !== null && rate !== undefined) return rate;
  }
  // フォールバック: 合計量と圃場面積から算出
  if (
    item.totalApplication !== null &&
    item.totalApplication !== undefined &&
    taskAreaM2 &&
    taskAreaM2 > 0
  ) {
    const ratePerM2 = item.totalApplication / taskAreaM2; // unitがXXPM2の場合を想定
    if (Number.isFinite(ratePerM2)) return ratePerM2;
  }
  return null;
}

function buildRecipeDisplay(task: AggregatedTask): RecipeDisplay[] {
  if (task.type !== 'Spraying') return [];
  const recipe = task.dosedMap?.recipeV2;
  const entries: SubstanceApplicationRate[] = Array.isArray(recipe)
    ? recipe
    : recipe
      ? [recipe as SubstanceApplicationRate]
      : [];
  if (entries.length === 0) return [];

  return entries.map((item) => {
    const name = mapIngredientName(item);
    const formLabel = mapFormulationLabel(item.formulation);
    const per10aRaw = formatPer10a(getRateForPer10a(item, task.fieldArea), item.unit ?? null);
    const totalRaw = formatTotalAmount(item.totalApplication ?? null, item.unit ?? null);
    const per10a = per10aRaw ?? undefined;
    const total = totalRaw ?? undefined;
    return { name, formLabel, per10a, total };
  });
}

function aggregateUsageSummary(tasks: AggregatedTask[]): UsageSummary[] {
  const map = new Map<string, UsageSummary>();
  tasks
    .filter(task => task.type === 'Spraying')
    .forEach(task => {
      const recipe = task.dosedMap?.recipeV2;
      const entries: SubstanceApplicationRate[] = Array.isArray(recipe)
        ? recipe
        : recipe
          ? [recipe as SubstanceApplicationRate]
          : [];
      entries.forEach(item => {
        const totalInfo = convertTotalValue(item.totalApplication ?? null, item.unit ?? null);
        if (!totalInfo) return;
        const name = mapIngredientName(item);
        const formLabel = mapFormulationLabel(item.formulation);
        const key = `${name}-${formLabel ?? ''}-${totalInfo.unitLabel}`;
        const prev = map.get(key);
        const nextValue = (prev?.totalValue ?? 0) + totalInfo.value;
        map.set(key, {
          name,
          formLabel,
          unitLabel: totalInfo.unitLabel,
          totalValue: nextValue,
        });
      });
    });

  return Array.from(map.values()).sort((a, b) => b.totalValue - a.totalValue);
}

function aggregateUsageCsvRows(tasks: AggregatedTask[]): UsageCsvRow[] {
  const map = new Map<string, UsageCsvRow>();
  tasks
    .filter(task => task.type === 'Spraying')
    .forEach(task => {
      const rawDate = task.executionDate || task.plannedDate;
      if (!rawDate) return;
      const dateKey = format(new Date(rawDate), 'yyyy-MM-dd');
      const recipe = task.dosedMap?.recipeV2;
      const entries: SubstanceApplicationRate[] = Array.isArray(recipe)
        ? recipe
        : recipe
          ? [recipe as SubstanceApplicationRate]
          : [];
      entries.forEach(item => {
        const totalInfo = convertTotalValue(item.totalApplication ?? null, item.unit ?? null);
        if (!totalInfo) return;
        const name = mapIngredientName(item);
        const formLabel = mapFormulationLabel(item.formulation);
        const key = `${dateKey}-${name}-${formLabel ?? ''}-${totalInfo.unitLabel}`;
        const prev = map.get(key);
        const nextValue = (prev?.totalValue ?? 0) + totalInfo.value;
        map.set(key, {
          date: dateKey,
          name,
          formLabel,
          unitLabel: totalInfo.unitLabel,
          totalValue: nextValue,
        });
      });
    });

  return Array.from(map.values()).sort((a, b) => a.date.localeCompare(b.date) || b.totalValue - a.totalValue);
}

const TaskRow: FC<{
  task: AggregatedTask;
  canEditPlannedDate: boolean;
  onUpdatePlannedDate?: (task: AggregatedTask, nextDateInput: string) => Promise<void>;
  updateState?: { loading: boolean; error?: string };
  isSelected?: boolean;
  onToggleSelect?: (uuid: string, checked: boolean) => void;
  onOpenWeather?: (task: AggregatedTask) => void;
  isWeatherEnabled?: boolean;
}> = ({ task, canEditPlannedDate, onUpdatePlannedDate, updateState, isSelected, onToggleSelect, onOpenWeather, isWeatherEnabled }) => {
  const { t } = useLanguage();
  const assigneeName = task.assignee
    ? `${task.assignee.lastName || ''} ${task.assignee.firstName || ''}`.trim() || t('tasks.assignee.unassigned')
    : t('tasks.assignee.unassigned');
  const leadInfo = calculateLeadTimeInfo(task);
  const statusLabel = getStatusLabel(task.state);
  const statusClass = getStatusClass(task.state);
  const [isEditing, setIsEditing] = useState(false);
  const [draftDate, setDraftDate] = useState(() => getJstDateInputValue(task.plannedDate));
  const [localError, setLocalError] = useState<string | null>(null);
  const isEditable = task.type === 'Spraying' && canEditPlannedDate;
  const isSelectable = task.type === 'Spraying';
  const isSaving = Boolean(updateState?.loading);

  useEffect(() => {
    if (!isEditing) {
      setDraftDate(getJstDateInputValue(task.plannedDate));
    }
  }, [task.plannedDate, isEditing]);

  useEffect(() => {
    if (updateState?.error && isEditing) {
      setLocalError(updateState.error);
    }
  }, [updateState?.error, isEditing]);

  const handleSave = async () => {
    if (!onUpdatePlannedDate) return;
    setLocalError(null);
    try {
      await onUpdatePlannedDate(task, draftDate);
      setIsEditing(false);
    } catch (err) {
      const message = err instanceof Error ? err.message : t('tasks.error.update_failed');
      setLocalError(message);
    }
  };

  const handleCancel = () => {
    setIsEditing(false);
    setDraftDate(getJstDateInputValue(task.plannedDate));
    setLocalError(null);
  };

  return (
    <tr key={task.uuid}>
      <td>
        {isSelectable ? (
          <input
            type="checkbox"
            checked={Boolean(isSelected)}
            onChange={(e) => onToggleSelect?.(task.uuid, e.currentTarget.checked)}
            aria-label={t('tasks.table.select_task_aria')}
          />
        ) : (
          <span className="tasks-select-placeholder">-</span>
        )}
      </td>
      <td>
        <span className={`task-type-label type-${task.type.toLowerCase()}`}>
          {getTaskLabel(task)}
        </span>
      </td>
      <td>{task.fieldName}</td>
      <td>{task.cropName}</td>
      <td>
        <div className="planned-date-cell">
          {!isEditing ? (
            <div className="planned-date-display">
              <span>{getLocalDateString(task.plannedDate) || '-'}</span>
              {isEditable && (
                <button
                  type="button"
                  className="planned-date-btn"
                  onClick={() => setIsEditing(true)}
                >
                  {t('action.edit')}
                </button>
              )}
            </div>
          ) : (
            <div className="planned-date-edit">
              <input
                type="date"
                value={draftDate}
                onChange={(e) => setDraftDate(e.currentTarget.value)}
                disabled={isSaving}
              />
              <div className="planned-date-edit-controls">
                <button
                  type="button"
                  className="planned-date-btn"
                  onClick={handleSave}
                  disabled={isSaving || !draftDate}
                >
                  {t('action.save')}
                </button>
                <button
                  type="button"
                  className="planned-date-btn secondary"
                  onClick={handleCancel}
                  disabled={isSaving}
                >
                  {t('action.cancel')}
                </button>
              </div>
            </div>
          )}
          {localError && <span className="planned-date-error">{localError}</span>}
        </div>
      </td>
      <td>{getLocalDateString(task.executionDate) || '-'}</td>
      <td className={leadInfo.className ?? ''}>
        {leadInfo.text}
      </td>
      <td>
        <span className={`task-status-badge ${statusClass}`}>
          {statusLabel}
        </span>
      </td>
      <td className={!task.assignee ? 'unassigned' : ''}>
        {assigneeName}
      </td>
      <td className="recipe-cell">
        {(() => {
          const recipes = buildRecipeDisplay(task);
          if (recipes.length === 0) {
            return <span className="no-note">-</span>;
          }
          return (
            <div className="recipe-list">
              {recipes.map((entry, idx) => (
                <div key={`${task.uuid}-recipe-${idx}`} className="recipe-item">
                  <div className="recipe-name">{entry.name}</div>
                  <div className="recipe-meta">
                    {entry.formLabel && <span className="recipe-chip">{entry.formLabel}</span>}
                    {entry.per10a && <span className="recipe-chip recipe-chip-10a">{entry.per10a}</span>}
                    {entry.total && (
                      <span className="recipe-chip recipe-chip-total">
                        {t('tasks.recipe.total', { total: entry.total })}
                      </span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          );
        })()}
      </td>
      <td className="note-cell">
        {task.note ? (
          <div title={task.note} className="note-content">
            {task.note}
          </div>
        ) : (
          <span className="no-note">-</span>
        )}
      </td>
      <td className="weather-cell">
        <button
          type="button"
          className="weather-link-button"
          onClick={() => onOpenWeather?.(task)}
          disabled={!isWeatherEnabled}
        >
          {t('tasks.weather.link')}
        </button>
      </td>
    </tr>
  );
};

const TasksTable: FC<{
  tasks: AggregatedTask[];
  canEditPlannedDate: boolean;
  onUpdatePlannedDate?: (task: AggregatedTask, nextDateInput: string) => Promise<void>;
  updateStateByTask?: Record<string, { loading: boolean; error?: string }>;
  selectedTaskUuids?: Set<string>;
  onToggleSelect?: (uuid: string, checked: boolean) => void;
  onToggleSelectAll?: (uuids: string[], checked: boolean) => void;
  onOpenWeather?: (task: AggregatedTask) => void;
  isWeatherEnabled?: (task: AggregatedTask) => boolean;
}> = ({ tasks, canEditPlannedDate, onUpdatePlannedDate, updateStateByTask, selectedTaskUuids, onToggleSelect, onToggleSelectAll, onOpenWeather, isWeatherEnabled }) => {
  const { language, t } = useLanguage();
  const [page, setPage] = useState(1);
  type SortKey =
    | 'type'
    | 'field'
    | 'crop'
    | 'plannedDate'
    | 'executionDate'
    | 'leadTime'
    | 'status'
    | 'assignee'
    | 'recipe'
    | 'note';
  const [sortState, setSortState] = useState<{
    key: SortKey | null;
    direction: 'asc' | 'desc';
  }>({ key: null, direction: 'asc' });
  useEffect(() => setPage(1), [tasks]);

  const handleSort = (key: SortKey) => {
    setSortState(prev => ({
      key,
      direction: prev.key === key && prev.direction === 'asc' ? 'desc' : 'asc',
    }));
  };

  const sortedTasks = useMemo(() => {
    if (!sortState.key) return tasks;
    const getLeadValue = (task: AggregatedTask): number | null => {
      const { plannedDate, executionDate } = task;
      const today = startOfDay(new Date());
      if (plannedDate && executionDate) {
        return differenceInCalendarDays(new Date(executionDate), new Date(plannedDate));
      }
      if (plannedDate) {
        return differenceInCalendarDays(new Date(plannedDate), today);
      }
      return null;
    };
    const getSortValue = (task: AggregatedTask) => {
      switch (sortState.key) {
        case 'type':
          return getTaskLabel(task);
        case 'field':
          return task.fieldName || '';
        case 'crop':
          return task.cropName || '';
        case 'plannedDate':
          return task.plannedDate ? new Date(task.plannedDate).getTime() : null;
        case 'executionDate':
          return task.executionDate ? new Date(task.executionDate).getTime() : null;
        case 'leadTime':
          return getLeadValue(task);
        case 'status':
          return getStatusLabel(task.state);
        case 'assignee':
          return task.assignee
            ? `${task.assignee.lastName || ''} ${task.assignee.firstName || ''}`.trim()
            : '';
        case 'recipe': {
          const recipes = buildRecipeDisplay(task);
          return recipes.map(item => item.name).join(', ');
        }
        case 'note':
          return (task.note || '').trim();
        default:
          return '';
      }
    };
    const compare = (a: AggregatedTask, b: AggregatedTask) => {
      const valA = getSortValue(a);
      const valB = getSortValue(b);
      if (valA === null && valB === null) return 0;
      if (valA === null) return 1;
      if (valB === null) return -1;
      let result = 0;
      if (typeof valA === 'number' && typeof valB === 'number') {
        result = valA - valB;
      } else {
        result = String(valA).localeCompare(String(valB), language === 'ja' ? 'ja' : 'en');
      }
      return sortState.direction === 'asc' ? result : -result;
    };
    return [...tasks].sort(compare);
  }, [tasks, sortState, language]);

  const totalPages = Math.max(1, Math.ceil(tasks.length / TABLE_PAGE_SIZE));
  const currentPage = Math.min(page, totalPages);
  const startIndex = (currentPage - 1) * TABLE_PAGE_SIZE;
  const endIndex = startIndex + TABLE_PAGE_SIZE;
  const visibleTasks = sortedTasks.slice(startIndex, endIndex);
  const selectableUuids = visibleTasks.filter(task => task.type === 'Spraying').map(task => task.uuid);
  const allSelected = selectableUuids.length > 0 && selectableUuids.every(uuid => selectedTaskUuids?.has(uuid));

  const renderSortIndicator = (key: SortKey) => {
    if (sortState.key !== key) return '↕';
    return sortState.direction === 'asc' ? '▲' : '▼';
  };

  const handlePageChange = (next: number) => {
    const clamped = Math.min(Math.max(next, 1), totalPages);
    setPage(clamped);
  };

  return (
    <div className="table-container">
      <table className="tasks-table">
        <thead>
          <tr>
            <th>
              <input
                type="checkbox"
                checked={allSelected}
                onChange={(e) => onToggleSelectAll?.(selectableUuids, e.currentTarget.checked)}
                disabled={selectableUuids.length === 0}
                aria-label={t('tasks.table.select_all_visible_spraying_aria')}
              />
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('type')} className="tasks-table-sort">
                {t('tasks.table.task_type')}{' '}
                <span className="sort-indicator">{renderSortIndicator('type')}</span>
              </button>
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('field')} className="tasks-table-sort">
                {t('table.field')} <span className="sort-indicator">{renderSortIndicator('field')}</span>
              </button>
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('crop')} className="tasks-table-sort">
                {t('table.crop')} <span className="sort-indicator">{renderSortIndicator('crop')}</span>
              </button>
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('plannedDate')} className="tasks-table-sort">
                {t('tasks.table.planned_date')}{' '}
                <span className="sort-indicator">{renderSortIndicator('plannedDate')}</span>
              </button>
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('executionDate')} className="tasks-table-sort">
                {t('tasks.table.execution_date')}{' '}
                <span className="sort-indicator">{renderSortIndicator('executionDate')}</span>
              </button>
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('leadTime')} className="tasks-table-sort">
                {t('tasks.table.lead_time')}{' '}
                <span className="sort-indicator">{renderSortIndicator('leadTime')}</span>
              </button>
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('status')} className="tasks-table-sort">
                {t('table.status')} <span className="sort-indicator">{renderSortIndicator('status')}</span>
              </button>
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('assignee')} className="tasks-table-sort">
                {t('tasks.table.assignee')}{' '}
                <span className="sort-indicator">{renderSortIndicator('assignee')}</span>
              </button>
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('recipe')} className="tasks-table-sort">
                {t('tasks.table.recipe')}{' '}
                <span className="sort-indicator">{renderSortIndicator('recipe')}</span>
              </button>
            </th>
            <th className="sortable">
              <button type="button" onClick={() => handleSort('note')} className="tasks-table-sort">
                {t('tasks.table.note')} <span className="sort-indicator">{renderSortIndicator('note')}</span>
              </button>
            </th>
            <th>{t('tasks.table.spraying_weather')}</th>
          </tr>
        </thead>
        <tbody>
          {visibleTasks.map(task => (
            <TaskRow
              key={task.uuid}
              task={task}
              canEditPlannedDate={canEditPlannedDate}
              onUpdatePlannedDate={onUpdatePlannedDate}
              updateState={updateStateByTask?.[task.uuid]}
              isSelected={selectedTaskUuids?.has(task.uuid)}
              onToggleSelect={onToggleSelect}
              onOpenWeather={onOpenWeather}
              isWeatherEnabled={isWeatherEnabled?.(task)}
            />
          ))}
        </tbody>
      </table>
      {tasks.length === 0 && <p style={{ padding: '1rem' }}>{t('tasks.table.empty')}</p>}
      {tasks.length > 0 && (
        <div className="tasks-pagination">
          <div className="tasks-pagination-info">
            {t('tasks.pagination.info', {
              from: startIndex + 1,
              to: Math.min(endIndex, tasks.length),
              total: tasks.length,
            })}
          </div>
          <div className="tasks-pagination-buttons">
            <button type="button" onClick={() => handlePageChange(currentPage - 1)} disabled={currentPage === 1}>
              {t('pagination.prev')}
            </button>
            <span className="tasks-pagination-page">
              {currentPage} / {totalPages}
            </span>
            <button type="button" onClick={() => handlePageChange(currentPage + 1)} disabled={currentPage === totalPages}>
              {t('pagination.next')}
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

const BulkPlannedDateModal: FC<{
  tasks: AggregatedTask[];
  bbchBySeason: Map<string, CountryCropGrowthStagePrediction[]>;
  isSaving: boolean;
  onClose: () => void;
  onApply: (updates: { task: AggregatedTask; dateInput: string }[]) => Promise<void>;
}> = ({ tasks, bbchBySeason, isSaving, onClose, onApply }) => {
  const { t } = useLanguage();
  const [bulkDate, setBulkDate] = useState('');
  const [bulkBbchIndex, setBulkBbchIndex] = useState('');
  const [taskDateMap, setTaskDateMap] = useState<Record<string, string>>({});
  const [taskBbchMap, setTaskBbchMap] = useState<Record<string, string>>({});
  const [error, setError] = useState<string | null>(null);

  const findBbchLabelForDate = useCallback(
    (task: AggregatedTask, dateInput: string) => {
      if (!dateInput) return '';
      const predictions = bbchBySeason.get(task.seasonUuid ?? '') ?? [];
      for (const pred of predictions) {
        if (!pred?.startDate) continue;
        const start = getJstDateInputValue(pred.startDate);
        if (!start) continue;
        let end = '';
        if (pred.endDate) {
          const endDate = new Date(pred.endDate);
          endDate.setDate(endDate.getDate() - 1);
          end = getJstDateInputValueFromDate(endDate);
        }
        const inRange = end ? start <= dateInput && dateInput <= end : start <= dateInput;
        if (!inRange) continue;
        const stageName = pred.cropGrowthStageV2?.name ?? '';
        return stageName ? `BBCH ${pred.index} - ${stageName}` : `BBCH ${pred.index}`;
      }
      return '';
    },
    [bbchBySeason]
  );

  const availableBbchIndices = useMemo(() => {
    const seen = new Set<string>();
    tasks.forEach(task => {
      const predictions = bbchBySeason.get(task.seasonUuid ?? '') ?? [];
      predictions.forEach(pred => {
        const normalized = normalizeBbchIndex(pred.index);
        if (!normalized || !pred.startDate) return;
        seen.add(normalized);
      });
    });
    return Array.from(seen).sort((a, b) => parseBbchIndex(a) - parseBbchIndex(b));
  }, [tasks, bbchBySeason]);

  useEffect(() => {
    const next: Record<string, string> = {};
    tasks.forEach(task => {
      next[task.uuid] = getJstDateInputValue(task.plannedDate);
    });
    setTaskDateMap(next);
    setTaskBbchMap({});
    setBulkDate('');
    setBulkBbchIndex('');
    setError(null);
  }, [tasks]);

  const applyBulkDate = () => {
    if (!bulkDate) return;
    setTaskDateMap(prev => {
      const next = { ...prev };
      tasks.forEach(task => {
        next[task.uuid] = bulkDate;
      });
      return next;
    });
    setTaskBbchMap(prev => {
      const next = { ...prev };
      tasks.forEach(task => {
        const label = findBbchLabelForDate(task, bulkDate);
        if (label) {
          next[task.uuid] = label;
        } else if (next[task.uuid]) {
          delete next[task.uuid];
        }
      });
      return next;
    });
  };

  const applyBulkBbch = () => {
    if (!bulkBbchIndex) return;
    let missing = 0;
    setTaskDateMap(prev => {
      const next = { ...prev };
      tasks.forEach(task => {
        const predictions = bbchBySeason.get(task.seasonUuid ?? '') ?? [];
        const match = predictions.find(
          pred => normalizeBbchIndex(pred.index) === bulkBbchIndex && pred.startDate
        );
        if (match?.startDate) {
          next[task.uuid] = getJstDateInputValue(match.startDate);
        } else {
          missing += 1;
        }
      });
      return next;
    });
    setTaskBbchMap(prev => {
      const next = { ...prev };
      tasks.forEach(task => {
        const predictions = bbchBySeason.get(task.seasonUuid ?? '') ?? [];
        const match = predictions.find(
          pred => normalizeBbchIndex(pred.index) === bulkBbchIndex && pred.startDate
        );
        if (match?.startDate) {
          const stageName = match.cropGrowthStageV2?.name ?? '';
          next[task.uuid] = stageName ? `BBCH ${bulkBbchIndex} - ${stageName}` : `BBCH ${bulkBbchIndex}`;
        } else if (next[task.uuid]) {
          delete next[task.uuid];
        }
      });
      return next;
    });
    if (missing > 0) {
      setError(t('tasks.bulk.bbch_missing', { index: bulkBbchIndex, missing }));
    } else {
      setError(null);
    }
  };

  const handleSave = async () => {
    setError(null);
    const updates = tasks
      .map(task => ({ task, dateInput: taskDateMap[task.uuid] || '' }))
      .filter(entry => entry.dateInput);
    if (updates.length !== tasks.length) {
      setError(t('tasks.bulk.require_all_dates'));
      return;
    }
    try {
      await onApply(updates);
      onClose();
    } catch (err) {
      const message = err instanceof Error ? err.message : t('tasks.error.update_failed');
      setError(message);
    }
  };

  return (
    <div className="tasks-modal-backdrop" onClick={onClose}>
      <div className="tasks-modal" onClick={(e) => e.stopPropagation()}>
        <div className="tasks-modal-header">
          <h3>{t('tasks.bulk.title')}</h3>
          <button type="button" onClick={onClose}>
            {t('action.close')}
          </button>
        </div>
        <div className="tasks-modal-body">
          <div className="tasks-modal-bulk">
            <div className="tasks-modal-bulk-input">
              <label>{t('tasks.bulk.bulk_date')}</label>
              <input
                type="date"
                value={bulkDate}
                onChange={(e) => setBulkDate(e.currentTarget.value)}
                disabled={isSaving}
              />
            </div>
            <button
              type="button"
              onClick={applyBulkDate}
              disabled={!bulkDate || isSaving || tasks.length === 0}
            >
              {t('tasks.bulk.apply_all')}
            </button>
            <div className="tasks-modal-bulk-input">
              <label>{t('tasks.bulk.bbch_bulk')}</label>
              <select
                value={bulkBbchIndex}
                onChange={(e) => setBulkBbchIndex(e.currentTarget.value)}
                disabled={isSaving || availableBbchIndices.length === 0}
              >
                <option value="">{t('tasks.bulk.bbch_select')}</option>
                {availableBbchIndices.map(index => (
                  <option key={index} value={index}>BBCH {index}</option>
                ))}
              </select>
            </div>
            <button
              type="button"
              onClick={applyBulkBbch}
              disabled={!bulkBbchIndex || isSaving || tasks.length === 0}
            >
              {t('tasks.bulk.apply_bbch')}
            </button>
          </div>
          {tasks.length === 0 ? (
            <p className="tasks-modal-empty">{t('tasks.bulk.empty')}</p>
          ) : (
            <div className="tasks-modal-list">
              {tasks.map(task => {
                const predictions = (bbchBySeason.get(task.seasonUuid ?? '') ?? [])
                  .filter(pred => pred.index && pred.startDate);
                const sortedPredictions = [...predictions].sort(
                  (a, b) => parseBbchIndex(a.index) - parseBbchIndex(b.index)
                );
                return (
                  <div key={task.uuid} className="tasks-modal-item">
                    <div className="tasks-modal-item-head">
                      <div className="tasks-modal-item-title">
                        {task.fieldName} / {task.cropName} - {getTaskLabel(task)}
                      </div>
                      <div className="tasks-modal-item-meta">
                        {t('tasks.bulk.current', { date: getLocalDateString(task.plannedDate) || '-' })}
                      </div>
                    </div>
                    <div className="tasks-modal-item-body">
                      <div className="tasks-modal-date">
                        <label>{t('tasks.table.planned_date')}</label>
                        <input
                          type="date"
                          value={taskDateMap[task.uuid] || ''}
                          onChange={(e) => {
                            const nextValue = e.currentTarget.value;
                            setTaskDateMap(prev => ({ ...prev, [task.uuid]: nextValue }));
                            setTaskBbchMap(prev => {
                              const label = findBbchLabelForDate(task, nextValue);
                              if (label) {
                                return { ...prev, [task.uuid]: label };
                              }
                              if (!prev[task.uuid]) return prev;
                              const next = { ...prev };
                              delete next[task.uuid];
                              return next;
                            });
                          }}
                          disabled={isSaving}
                        />
                        <div className="tasks-modal-selected-bbch">
                          {t('tasks.bulk.selected_bbch', { value: taskBbchMap[task.uuid] || '-' })}
                        </div>
                      </div>
                      <div className="tasks-modal-bbch">
                        <div className="tasks-modal-bbch-title">{t('tasks.bulk.bbch_predictions_title')}</div>
                        {sortedPredictions.length === 0 ? (
                          <div className="tasks-modal-bbch-empty">{t('tasks.bulk.bbch_predictions_empty')}</div>
                        ) : (
                          <ul>
                            {sortedPredictions.map(pred => {
                              const rangeLabel = getBbchRangeLabel(pred);
                              const stageName = pred.cropGrowthStageV2?.name ?? '';
                              const startDateInput = pred.startDate
                                ? getJstDateInputValue(pred.startDate)
                                : '';
                              const selectedLabel = stageName
                                ? `BBCH ${pred.index} - ${stageName}`
                                : `BBCH ${pred.index}`;
                              return (
                                <li key={`${task.uuid}-${pred.index}-${pred.startDate}`}>
                                  <button
                                    type="button"
                                    className="bbch-item"
                                    onClick={() => {
                                      if (!startDateInput || isSaving) return;
                                      setTaskDateMap(prev => ({ ...prev, [task.uuid]: startDateInput }));
                                      setTaskBbchMap(prev => ({ ...prev, [task.uuid]: selectedLabel }));
                                    }}
                                    disabled={!startDateInput || isSaving}
                                  >
                                  <span className="bbch-label">
                                    BBCH {pred.index}{stageName ? ` - ${stageName}` : ''}
                                  </span>
                                  {rangeLabel && <span className="bbch-range">{rangeLabel}</span>}
                                  </button>
                                </li>
                              );
                            })}
                          </ul>
                        )}
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          )}
          {error && <div className="tasks-modal-error">{error}</div>}
        </div>
        <div className="tasks-modal-footer">
          <button type="button" onClick={onClose} disabled={isSaving}>
            {t('action.cancel')}
          </button>
          <button
            type="button"
            onClick={handleSave}
            disabled={isSaving || tasks.length === 0}
          >
            {t('action.save_changes')}
          </button>
        </div>
      </div>
    </div>
  );
};
