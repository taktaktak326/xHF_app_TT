import { memo, useCallback, useEffect, useMemo, useRef, useState, useTransition, type KeyboardEvent as ReactKeyboardEvent } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  Legend,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { useAuth } from '../context/AuthContext';
import { withApiBase } from '../utils/apiBase';
import LoadingOverlay from '../components/LoadingOverlay';
import LoadingSpinner from '../components/LoadingSpinner';
import './TaskProgressDashboardPage.css';

type SortKey = 'name' | 'field_count' | 'due_task_count' | 'overdue_count' | 'delay_rate' | 'completion_rate';
type SortDir = 'asc' | 'desc';

type DashboardTask = {
  uuid: string;
  farmUuid: string;
  farmName: string;
  fieldUuid: string;
  fieldName: string;
  seasonKey: string;
  typeFamily: string;
  taskName: string;
  taskType: string;
  userName: string;
  assigneeName: string;
  product: string;
  dosage: string;
  creationFlowHint: string;
  scheduledDay: string;
  executionDay: string;
  completed: boolean;
  taskDate: string | null;
  plannedDate: string | null;
  executionDate: string | null;
  state: string | null;
  occurrence: number;
};

type ActionFilterKey = 'none' | 'overdue' | 'due_today' | 'upcoming_3days' | 'incomplete' | 'future' | 'completed';

type SnapshotRun = {
  run_id: string;
  snapshot_date: string;
  status: string;
  message?: string | null;
  farms_scanned?: number;
  farms_matched?: number;
  fields_saved?: number;
  tasks_saved?: number;
  started_at?: string;
  finished_at?: string | null;
};

type SnapshotTask = {
  snapshot_date: string;
  run_id: string;
  task_uuid: string;
  farm_uuid: string;
  farm_name: string;
  field_uuid: string;
  field_name: string;
  user_name: string | null;
  season_uuid: string;
  crop_uuid: string;
  task_name: string;
  task_type: string;
  task_date: string | null;
  planned_date: string | null;
  execution_date: string | null;
  status: string | null;
  product: string | null;
  dosage: string | null;
  spray_category: string | null;
  creation_flow_hint: string | null;
  assignee_name: string | null;
  bbch_index: string | null;
  bbch_scale: string | null;
  occurrence: number | null;
  fetched_at: string | null;
};

const REQUIRED_SNAPSHOT_TASK_KEYS: Array<keyof SnapshotTask> = [
  'snapshot_date',
  'run_id',
  'task_uuid',
  'farm_uuid',
  'farm_name',
  'field_uuid',
  'field_name',
  'season_uuid',
  'crop_uuid',
  'task_name',
  'task_type',
  'task_date',
  'planned_date',
  'execution_date',
  'status',
  'product',
  'dosage',
  'spray_category',
  'creation_flow_hint',
  'assignee_name',
  'bbch_index',
  'bbch_scale',
  'occurrence',
  'fetched_at',
];

function hasRequiredSnapshotTaskKeys(task: unknown): task is SnapshotTask {
  if (!task || typeof task !== 'object') return false;
  const row = task as Record<string, unknown>;
  for (const key of REQUIRED_SNAPSHOT_TASK_KEYS) {
    if (!(key in row)) return false;
  }
  return true;
}

type SnapshotField = {
  snapshot_date: string;
  run_id: string;
  field_uuid: string;
  season_uuid: string;
  field_name: string;
  farm_uuid: string;
  farm_name: string;
  user_name: string | null;
  crop_name: string | null;
  variety_name: string | null;
  area_m2: number | null;
  bbch_index: string | null;
  bbch_scale: string | null;
  fetched_at: string | null;
};

type FieldDeltaRow = {
  diff_type: '増加' | '減少';
  field_uuid: string;
  field_name: string;
  area_m2: number | null;
};

type FieldTableSortKey =
  | 'snapshot_date'
  | 'farm_name'
  | 'field_name'
  | 'user_name'
  | 'area_m2'
  | 'bbch_index'
  | 'fetched_at';

type TaskTableSortKey =
  | 'snapshot_date'
  | 'farm_name'
  | 'field_name'
  | 'task_display'
  | 'task_name'
  | 'task_type'
  | 'occurrence'
  | 'task_date'
  | 'planned_date'
  | 'execution_date'
  | 'status'
  | 'user_name'
  | 'product'
  | 'spray_subtype'
  | 'fetched_at';

type TaskTableRow = SnapshotTask & {
  _family: string;
  _subtypeLabel: string;
  _taskDisplay: string;
};

type FarmerRow = {
  id: string;
  name: string;
  field_count: number;
  no_task_field_count?: number;
  is_unranked?: boolean;
  due_task_count: number;
  completed_count: number;
  overdue_count: number;
  due_today_count: number;
  upcoming_3days_count: number;
  future_task_count: number;
  delay_rate: number;
  completion_rate: number;
  delay_status: 'good' | 'warn' | 'bad';
  trend_direction: 'worsening' | 'stable' | 'improving';
};

type TaskTypeRow = {
  task_type_name: string;
  display_order: number;
  due_count: number;
  completed_count: number;
  overdue_count: number;
  pending_count: number;
  completion_rate: number;
  delay_rate: number;
};

type DistributionRow = {
  bucket: string;
  count: number;
  color: string;
};

type TrendPoint = {
  date: string;
  completion_rate: number;
  delay_rate: number;
};

type DailyFarmBar = {
  date: string;
  _dateKey: string; // YYYY-MM-DD for filtering
  [farmName: string]: string | number;
};

type FarmerDetail = {
  id: string;
  name: string;
  field_count: number;
  summary: {
    due: number;
    completed: number;
    overdue: number;
    pending: number;
    delay_rate: number;
    completion_rate: number;
  };
  task_types: Array<{
    name: string;
    display_order: number;
    due_count: number;
    completed_count: number;
    overdue_count: number;
    pending_count: number;
    completion_rate: number;
    delay_rate: number;
  }>;
};

type DashboardBundle = {
  farmer_count?: number;
  field_count?: number;
  total_area_m2?: number;
  kpi: {
    completion_rate: number;
    completed_count: number;
    due_count: number;
    overdue_count: number;
    delay_rate: number;
    due_today_count: number;
    upcoming_3days_count: number;
    future_count: number;
    total_task_count: number;
    as_of: string;
  };
  farmers: FarmerRow[];
  task_types: TaskTypeRow[];
  distribution: DistributionRow[];
  trend: TrendPoint[];
  daily_farm_bars: DailyFarmBar[];
  daily_farm_names: string[];
  farmer_details: Record<string, FarmerDetail>;
  no_task_farmers?: Array<{ id: string; name: string; field_count: number; no_task_field_count: number }>;
  as_of: string;
};

function emptyDashboardBundle(asOf: string): DashboardBundle {
  return {
    farmer_count: 0,
    field_count: 0,
    total_area_m2: 0,
    kpi: {
      completion_rate: 0,
      completed_count: 0,
      due_count: 0,
      overdue_count: 0,
      delay_rate: 0,
      due_today_count: 0,
      upcoming_3days_count: 0,
      future_count: 0,
      total_task_count: 0,
      as_of: asOf,
    },
    farmers: [],
    task_types: [],
    distribution: [],
    trend: [],
    daily_farm_bars: [],
    daily_farm_names: [],
    farmer_details: {},
    no_task_farmers: [],
    as_of: asOf,
  };
}

const TYPE_FAMILY_ORDER: string[] = [
  '播種タスク',
  '防除タスク（除草剤）',
  '防除タスク（殺菌剤）',
  '防除タスク（殺虫剤）',
  '防除タスク（その他）',
  '施肥タスク',
  '雑草管理タスク',
  '水管理タスク',
  '観察記録タスク',
  '収穫タスク',
  '土壌管理タスク',
  '種子処理タスク',
  '育苗箱処理タスク',
];
const ALL_FAMILY_OPTION = '全部';
const PROTECTION_FILTER_OPTIONS = [
  '防除タスク（除草剤）',
  '防除タスク（殺菌剤）',
  '防除タスク（殺虫剤）',
  '防除タスク（その他）',
] as const;
const COUNTRY_UUID_JP = '0f59ff55-c86b-4b7b-4eaa-eb003d47dcd3';
const RICE_CROP_UUID = 'e54c5e22-94a0-a5ff-34a6-4fe0f8ad1ccc';
const SNAPSHOT_CLIENT_CACHE_TTL_MS = 60 * 60 * 1000;
const SNAPSHOT_TASK_LIMIT = 50000;
const SNAPSHOT_FIELD_LIMIT = 50000;
const INITIAL_FIELD_ROWS = 50;
const INITIAL_TASK_ROWS = 50;
const FIELD_ROWS_STEP = 50;
const TASK_ROWS_STEP = 50;
const FARM_BAR_COLORS = [
  '#7986ff', '#64b5f6', '#4db6ac', '#81c784', '#aed581',
  '#dce775', '#ffd54f', '#ffb74d', '#ff8a65', '#e57373',
  '#ba68c8', '#f06292', '#4dd0e1', '#a1887f', '#90a4ae',
];

const snapshotPageCache = new Map<string, { expiresAt: number; data: any }>();
const snapshotDatesCache = new Map<string, { expiresAt: number; data: any }>();
const jstDayKeyCache = new Map<string, string>();
const sprayMasterCache = new Map<string, { expiresAt: number; data: Record<string, string> }>();
/** Client-side cache for computeDashboardBundle results keyed by filter combination */
const bundleCache = new Map<string, { tasksLen: number; bundle: DashboardBundle }>();
const BUNDLE_CACHE_MAX = 20;
const WORKER_BUNDLE_THRESHOLD = 4000;
const SNAPSHOT_SS_PREFIX = 'hfr:ss:v1';
function summaryFilterCacheKey(snapshotDate: string, family: string, action: ActionFilterKey): string {
  return `${snapshotDate}:summary:${family}:${action}`;
}

function readSessionCache<T>(key: string): { data: T; expiresAt: number } | null {
  try {
    const raw = window.sessionStorage.getItem(`${SNAPSHOT_SS_PREFIX}:${key}`);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const expiresAt = Number(parsed?.expiresAt || 0);
    if (!expiresAt || expiresAt <= Date.now()) {
      window.sessionStorage.removeItem(`${SNAPSHOT_SS_PREFIX}:${key}`);
      return null;
    }
    return { data: parsed.data as T, expiresAt };
  } catch {
    return null;
  }
}

function writeSessionCache<T>(key: string, data: T, expiresAt: number): void {
  try {
    window.sessionStorage.setItem(
      `${SNAPSHOT_SS_PREFIX}:${key}`,
      JSON.stringify({ data, expiresAt }),
    );
  } catch {
    // ignore quota errors
  }
}

function clearSnapshotSessionCache(): void {
  try {
    const prefix = `${SNAPSHOT_SS_PREFIX}:`;
    const keys: string[] = [];
    for (let i = 0; i < window.sessionStorage.length; i += 1) {
      const key = window.sessionStorage.key(i);
      if (!key) continue;
      if (key.startsWith(prefix)) keys.push(key);
    }
    keys.forEach((key) => window.sessionStorage.removeItem(key));
  } catch {
    // ignore
  }
}

const TYPE_FAMILY_BY_TASK_TYPE: Record<string, string> = {
  Harvest: '収穫タスク',
  Spraying: '散布タスク',
  WaterManagement: '水管理タスク',
  Scouting: '観察記録タスク',
  CropEstablishment: '播種タスク',
  LandPreparation: '土壌管理タスク',
  SeedTreatment: '種子処理タスク',
  SeedBoxTreatment: '育苗箱処理タスク',
};

function normalizeTaskFamilyLabel(value: string): string {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (raw === '生育調査') return '観察記録タスク';
  if (raw === '圃場調査') return '観察記録タスク';
  if (raw === '観察記録') return '観察記録タスク';
  if (raw === '土づくり') return '土壌管理タスク';
  if (raw === '土壌管理') return '土壌管理タスク';
  if (raw === '収穫') return '収穫タスク';
  if (raw === '水管理') return '水管理タスク';
  if (raw === '播種') return '播種タスク';
  if (raw === '種子処理') return '種子処理タスク';
  if (raw === '育苗箱処理') return '育苗箱処理タスク';
  if (raw === '防除タスク') return '防除タスク（その他）';
  return raw;
}


function normalizeSprayCategory(categoryRaw: string): string {
  const key = categoryRaw.trim().toUpperCase();
  if (key === 'H') return '除草剤';
  if (key === 'F') return '殺菌剤';
  if (key === 'I') return '殺虫剤';
  if (key === 'HERBICIDE') return '除草剤';
  if (key === 'FUNGICIDE') return '殺菌剤';
  if (key === 'INSECTICIDE') return '殺虫剤';
  if (key === 'FERTILIZER') return '肥料';
  if (key === 'PRODUCT') return '分類不明';
  return categoryRaw.trim();
}

type SprayFlowType = 'weed' | 'fertilization' | 'protection' | 'unknown';

function sprayFlowFromHint(hintRaw: string): SprayFlowType | null {
  const hint = hintRaw.trim().toUpperCase();
  if (!hint) return null;
  if (hint.includes('WEED_MANAGEMENT') || hint === 'WEEDMANAGEMENT') return 'weed';
  if (hint.includes('NUTRITION')) return 'fertilization';
  if (hint.includes('CROP_PROTECTION') || hint === 'CROPPROTECTION') return 'protection';
  return 'unknown';
}

function spraySubtypeFromCategory(categoryRaw: string): string | null {
  const normalized = normalizeSprayCategory(categoryRaw).trim();
  if (!normalized || normalized === '分類不明') return null;
  return normalized;
}

function spraySubtypeFromProductName(productRaw: string | null | undefined): string | null {
  const text = normalizeProductToken(productRaw);
  if (!text) return null;
  if (text.includes('fungicide') || text.includes('殺菌')) return '殺菌剤';
  if (text.includes('insecticide') || text.includes('殺虫')) return '殺虫剤';
  if (text.includes('herbicide') || text.includes('除草')) return '除草剤';
  if (text.includes('fertilizer') || text.includes('肥料') || text.includes('りん酸') || text.includes('窒素')) return '肥料';
  return null;
}

function normalizeProductToken(value: string | null | undefined): string {
  const raw = String(value || '').trim();
  if (!raw) return '';
  return raw
    .normalize('NFKC')
    .toLowerCase()
    .replace(/[ \t\r\n\u3000]/g, '')
    .replace(/[･・]/g, '');
}

function splitProductNames(productRaw: string | null | undefined): string[] {
  if (!productRaw) return [];
  return productRaw
    .split(/[\/／|｜\n\r]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

function lookupSprayCategoryCode(
  sprayCategoryMap: Record<string, string>,
  cropUuid: string,
  productName: string,
): string {
  const keyName = String(productName || '').toLowerCase();
  const normalizedKeyName = normalizeProductToken(productName);
  return (
    sprayCategoryMap[`${cropUuid}:${keyName}`] ||
    sprayCategoryMap[`${cropUuid}:${normalizedKeyName}`] ||
    sprayCategoryMap[`${RICE_CROP_UUID}:${keyName}`] ||
    sprayCategoryMap[`${RICE_CROP_UUID}:${normalizedKeyName}`] ||
    sprayCategoryMap[`*:${keyName}`] ||
    sprayCategoryMap[`*:${normalizedKeyName}`] ||
    ''
  );
}

function resolveTaskFamilyFromSnapshot(
  task: SnapshotTask,
  sprayCategoryMap: Record<string, string>,
): string {
  if (task.task_type === 'Spraying') {
    const hintRaw = String(task.creation_flow_hint || '');
    const flowByHint = sprayFlowFromHint(hintRaw);
    if (flowByHint === 'weed') return '雑草管理タスク';
    if (flowByHint === 'fertilization') return '施肥タスク';
    if (flowByHint === 'protection') {
      const cropUuid = (task.crop_uuid || '').trim();
      const products = splitProductNames(task.product);
      const found = new Set<string>();
      const bySprayCategory = spraySubtypeFromCategory(String(task.spray_category || ''));
      if (bySprayCategory) found.add(bySprayCategory);
      for (const name of products) {
        const byMaster = spraySubtypeFromCategory(lookupSprayCategoryCode(sprayCategoryMap, cropUuid, name));
        if (byMaster) found.add(byMaster);
        const byName = spraySubtypeFromProductName(name);
        if (byName) found.add(byName);
      }
      if (found.size === 1) {
        const subtype = Array.from(found)[0];
        if (subtype === '除草剤' || subtype === '殺菌剤' || subtype === '殺虫剤') {
          return `防除タスク（${subtype}）`;
        }
      }
      return '防除タスク（その他）';
    }
    return '防除タスク（その他）';
  }
  const byName = normalizeTaskFamilyLabel(task.task_name || '');
  if (byName) return byName;
  return normalizeTaskFamilyLabel(TYPE_FAMILY_BY_TASK_TYPE[task.task_type] || task.task_type || 'その他');
}

function detectSpraySubtype(
  task: SnapshotTask,
  sprayCategoryMap: Record<string, string>,
): { subtype: string; source: string } {
  if (task.task_type !== 'Spraying') return { subtype: '-', source: '-' };

  const flow = sprayFlowFromHint(String(task.creation_flow_hint || ''));
  if (flow === 'weed') return { subtype: '雑草管理タスク', source: 'creation_flow_hint' };
  if (flow === 'fertilization') return { subtype: '施肥タスク', source: 'creation_flow_hint' };

  if (flow === 'protection') {
    const cropUuid = (task.crop_uuid || '').trim();
    const products = splitProductNames(task.product);
    const fromMaster = new Set<string>();
    const fromName = new Set<string>();
    const bySprayCategory = spraySubtypeFromCategory(String(task.spray_category || ''));
    if (bySprayCategory) fromMaster.add(bySprayCategory);
    for (const name of products) {
      const byMaster = spraySubtypeFromCategory(lookupSprayCategoryCode(sprayCategoryMap, cropUuid, name));
      if (byMaster) fromMaster.add(byMaster);
      const byName = spraySubtypeFromProductName(name);
      if (byName) fromName.add(byName);
    }
    const merged = new Set<string>([...fromMaster, ...fromName]);
    if (merged.size === 1) {
      const subtype = Array.from(merged)[0];
      if (subtype === '除草剤' || subtype === '殺菌剤' || subtype === '殺虫剤') {
        const source = fromMaster.size > 0 ? 'product_master' : 'product_name';
        return { subtype, source };
      }
    }
    if (merged.size > 1) return { subtype: 'その他', source: 'multi_subtype' };
    return { subtype: 'その他', source: 'no_product_match' };
  }

  return { subtype: '未判定', source: 'creation_flow_hint_missing' };
}

function tasksFromSnapshot(snapshotTasks: SnapshotTask[], sprayCategoryMap: Record<string, string>): DashboardTask[] {
  const mapped = snapshotTasks.map((task, idx) => ({
    uuid: task.task_uuid || `task-${idx + 1}`,
    farmUuid: task.farm_uuid || '',
    farmName: task.farm_name || '不明農場',
    fieldUuid: task.field_uuid || '',
    fieldName: task.field_name || '不明圃場',
    seasonKey: task.season_uuid || 'field',
    typeFamily: resolveTaskFamilyFromSnapshot(task, sprayCategoryMap),
    taskName: task.task_name || '',
    taskType: task.task_type || '',
    userName: task.user_name || '',
    assigneeName: task.assignee_name || '',
    product: task.product || '',
    dosage: task.dosage || '',
    creationFlowHint: task.creation_flow_hint || '',
    scheduledDay: getJstDayKey(task.planned_date) || getJstDayKey(task.task_date),
    executionDay: getJstDayKey(task.execution_date),
    completed: Boolean(task.execution_date) || String(task.status || '').toUpperCase().includes('DONE')
      || String(task.status || '').toUpperCase().includes('COMPLETED')
      || String(task.status || '').toUpperCase().includes('EXECUTED'),
    taskDate: task.task_date,
    plannedDate: task.planned_date,
    executionDate: task.execution_date,
    state: task.status,
    occurrence: Number(task.occurrence || 1),
  }));

  const sprayingTasks = mapped
    .filter((row) => row.taskType === 'Spraying')
    .sort((a, b) => {
      const ka = `${a.farmUuid}|${a.fieldUuid}|${a.seasonKey}|${a.scheduledDay}|${a.taskName}|${a.uuid}`;
      const kb = `${b.farmUuid}|${b.fieldUuid}|${b.seasonKey}|${b.scheduledDay}|${b.taskName}|${b.uuid}`;
      return ka.localeCompare(kb, 'ja');
    });
  const seqByKey = new Map<string, number>();
  for (const row of sprayingTasks) {
    // Sprayings は「雑草管理/施肥/防除(サブタイプ)」ごとに回数を採番する。
    const key = `${row.fieldUuid}|${row.seasonKey}|${row.typeFamily}`;
    const next = (seqByKey.get(key) || 0) + 1;
    seqByKey.set(key, next);
    row.occurrence = next;
  }

  return mapped;
}

function getScheduledDay(task: DashboardTask): string {
  return task.scheduledDay;
}

function getJstDayKey(value: string | Date | null | undefined): string {
  if (!value) return '';
  if (typeof value === 'string') {
    const cached = jstDayKeyCache.get(value);
    if (cached !== undefined) return cached;
  }
  const date = value instanceof Date ? value : new Date(value);
  if (!Number.isFinite(date.getTime())) return '';
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const y = parts.find((p) => p.type === 'year')?.value;
  const m = parts.find((p) => p.type === 'month')?.value;
  const d = parts.find((p) => p.type === 'day')?.value;
  const result = y && m && d ? `${y}-${m}-${d}` : '';
  if (typeof value === 'string') {
    jstDayKeyCache.set(value, result);
  }
  return result;
}

function addDaysToKey(dayKey: string, offset: number): string {
  const [y, m, d] = dayKey.split('-').map(Number);
  if (!y || !m || !d) return dayKey;
  const utc = Date.UTC(y, m - 1, d + offset, 0, 0, 0);
  return getJstDayKey(new Date(utc));
}

function trendLabel(direction: FarmerRow['trend_direction']) {
  if (direction === 'worsening') return { symbol: '↗', text: '悪化', className: 'trend-bad' };
  if (direction === 'improving') return { symbol: '↘', text: '改善', className: 'trend-good' };
  return { symbol: '→', text: '横ばい', className: 'trend-stable' };
}

function formatDateTime(value: string | null | undefined): string {
  if (!value) return '-';
  const dt = new Date(value);
  if (!Number.isFinite(dt.getTime())) return value;
  return dt.toLocaleString('ja-JP');
}

function formatDate(value: string | null | undefined): string {
  if (!value) return '-';
  const dt = new Date(value);
  if (!Number.isFinite(dt.getTime())) return value;
  return dt.toLocaleDateString('ja-JP');
}

function formatAreaHa(areaM2: number | null | undefined): string {
  if (typeof areaM2 !== 'number' || !Number.isFinite(areaM2)) return '-';
  const ha = areaM2 / 10000;
  return `${ha.toLocaleString('ja-JP', { minimumFractionDigits: 2, maximumFractionDigits: 4 })} ha`;
}

function triggerBlobDownload(filename: string, blob: Blob): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function bucketContainsDelayRate(bucketLabel: string, delayRate: number): boolean {
  const label = String(bucketLabel || '').trim();
  const rate = Number(delayRate);
  if (!label || !Number.isFinite(rate)) return false;
  if (label.endsWith('%+')) {
    const lo = Number(label.replace('%+', '').trim());
    return Number.isFinite(lo) ? rate >= lo : false;
  }
  const m = label.match(/^(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)%$/);
  if (!m) return false;
  const lo = Number(m[1]);
  const hi = Number(m[2]);
  if (!Number.isFinite(lo) || !Number.isFinite(hi)) return false;
  return rate >= lo && rate < hi;
}

/**
 * Client-side DashboardBundle computation from filtered tasks.
 * Mirrors the server-side summary logic so filter changes are instant.
 */
function computeDashboardBundle(
  filteredTasks: DashboardTask[],
  snapshotDate: string,
  asOf: string,
): DashboardBundle {
  const today = getJstDayKey(new Date());
  const in3days = addDaysToKey(today, 3);

  function rate(a: number, b: number): number {
    return b > 0 ? Math.round((a * 1000) / b) / 10 : 0;
  }

  function counts(tasks: DashboardTask[]) {
    let due = 0, completed = 0, overdue = 0, dueToday = 0, upcoming3 = 0, future = 0;
    for (const t of tasks) {
      const planned = getScheduledDay(t);
      const done = t.completed;
      if (!planned) {
        if (!done) future++;
        continue;
      }
      if (planned <= today) {
        due++;
        if (done) completed++;
      }
      if (planned < today && !done) overdue++;
      if (planned === today && !done) dueToday++;
      if (planned > today && planned <= in3days && !done) upcoming3++;
      if (planned > today && !done) future++;
    }
    if (due === 0 && tasks.length > 0) {
      due = tasks.length;
      completed = tasks.filter((t) => t.completed).length;
    }
    return { due, completed, overdue, dueToday, upcoming3, future };
  }

  // Group by farmer
  const farmersMap = new Map<string, { name: string; fieldSet: Set<string>; tasks: DashboardTask[] }>();
  for (const task of filteredTasks) {
    const farmerId = task.farmUuid || `name:${task.farmName}`;
    let entry = farmersMap.get(farmerId);
    if (!entry) {
      entry = { name: task.farmName || '', fieldSet: new Set(), tasks: [] };
      farmersMap.set(farmerId, entry);
    }
    if (task.fieldUuid) entry.fieldSet.add(task.fieldUuid);
    entry.tasks.push(task);
  }

  const farmers: FarmerRow[] = [];
  const farmerDetails: Record<string, FarmerDetail> = {};
  let idx = 0;
  for (const [farmerId, entry] of farmersMap) {
    idx++;
    const c = counts(entry.tasks);
    const delayRate = rate(c.overdue, c.due);
    const completionRate = rate(c.completed, c.due);
    farmers.push({
      id: farmerId,
      name: entry.name || `農業者${idx}`,
      field_count: entry.fieldSet.size,
      due_task_count: c.due,
      completed_count: c.completed,
      overdue_count: c.overdue,
      due_today_count: c.dueToday,
      upcoming_3days_count: c.upcoming3,
      future_task_count: c.future,
      delay_rate: delayRate,
      completion_rate: completionRate,
      delay_status: delayRate < 15 ? 'good' : delayRate < 30 ? 'warn' : 'bad',
      trend_direction: 'stable',
    });

    // Per-farmer task type breakdown
    const typeMap = new Map<string, DashboardTask[]>();
    for (const t of entry.tasks) {
      const label = `${t.typeFamily} ${t.occurrence}回目`;
      if (!typeMap.has(label)) typeMap.set(label, []);
      typeMap.get(label)!.push(t);
    }
    const typeRows: FarmerDetail['task_types'] = [];
    let order = 0;
    for (const name of [...typeMap.keys()].sort((a, b) => a.localeCompare(b, 'ja'))) {
      order++;
      const tc = counts(typeMap.get(name)!);
      typeRows.push({
        name,
        display_order: order,
        due_count: tc.due,
        completed_count: tc.completed,
        overdue_count: tc.overdue,
        pending_count: tc.future,
        completion_rate: rate(tc.completed, tc.due),
        delay_rate: rate(tc.overdue, tc.due),
      });
    }
    farmerDetails[farmerId] = {
      id: farmerId,
      name: entry.name || `農業者${idx}`,
      field_count: entry.fieldSet.size,
      summary: { due: c.due, completed: c.completed, overdue: c.overdue, pending: c.future, delay_rate: delayRate, completion_rate: completionRate },
      task_types: typeRows,
    };
  }

  // Global task type breakdown
  const typeMapAll = new Map<string, DashboardTask[]>();
  for (const t of filteredTasks) {
    const label = `${t.typeFamily} ${t.occurrence}回目`;
    if (!typeMapAll.has(label)) typeMapAll.set(label, []);
    typeMapAll.get(label)!.push(t);
  }
  const taskTypes: TaskTypeRow[] = [];
  let tOrder = 0;
  for (const name of [...typeMapAll.keys()].sort((a, b) => a.localeCompare(b, 'ja'))) {
    tOrder++;
    const tc = counts(typeMapAll.get(name)!);
    taskTypes.push({
      task_type_name: name,
      display_order: tOrder,
      due_count: tc.due,
      completed_count: tc.completed,
      overdue_count: tc.overdue,
      pending_count: tc.future,
      completion_rate: rate(tc.completed, tc.due),
      delay_rate: rate(tc.overdue, tc.due),
    });
  }

  // KPI totals
  const totalDue = farmers.reduce((s, f) => s + f.due_task_count, 0);
  const totalCompleted = farmers.reduce((s, f) => s + f.completed_count, 0);
  const totalOverdue = farmers.reduce((s, f) => s + f.overdue_count, 0);
  const totalDueToday = farmers.reduce((s, f) => s + f.due_today_count, 0);
  const totalUpcoming = farmers.reduce((s, f) => s + f.upcoming_3days_count, 0);
  const totalFuture = farmers.reduce((s, f) => s + f.future_task_count, 0);

  // Distribution buckets
  const buckets: Array<[number, number, string, string]> = [
    [0, 5, '#22c55e', '0-5%'], [5, 10, '#22c55e', '5-10%'], [10, 15, '#22c55e', '10-15%'],
    [15, 20, '#f59e0b', '15-20%'], [20, 25, '#f59e0b', '20-25%'], [25, 30, '#f59e0b', '25-30%'],
    [30, 1000, '#ef4444', '30%+'],
  ];
  const distribution: DistributionRow[] = buckets.map(([lo, hi, color, label]) => ({
    bucket: label,
    count: farmers.filter((f) => f.delay_rate >= lo && f.delay_rate < hi).length,
    color,
  }));

  // 30-day trend (1-pass O(n+30))
  const dateObj = snapshotDate ? new Date(snapshotDate + 'T00:00:00+09:00') : new Date();
  const trendDays: string[] = [];
  for (let i = 29; i >= 0; i--) {
    const d = new Date(dateObj.getTime() - i * 86400000);
    trendDays.push(getJstDayKey(d));
  }
  const plannedOnAll = new Map<string, number>();
  const plannedOnNotDone = new Map<string, number>();
  const completedOn = new Map<string, number>();
  const totalDoneCount = filteredTasks.filter((t) => t.completed).length;
  for (const t of filteredTasks) {
    const planned = getScheduledDay(t);
    if (!planned) continue;
    plannedOnAll.set(planned, (plannedOnAll.get(planned) || 0) + 1);
    if (!t.completed) {
      plannedOnNotDone.set(planned, (plannedOnNotDone.get(planned) || 0) + 1);
    } else {
      const cDay = t.executionDay || planned;
      completedOn.set(cDay, (completedOn.get(cDay) || 0) + 1);
    }
  }
  // Pre-accumulate before trend window
  const trendStart = trendDays[0];
  let cumDue = 0, cumCompleted = 0, cumOverdue = 0;
  for (const t of filteredTasks) {
    const planned = getScheduledDay(t);
    if (!planned || planned >= trendStart) continue;
    cumDue++;
    if (!t.completed) {
      cumOverdue++;
    } else {
      const cDay = t.executionDay || planned;
      if (cDay < trendStart) cumCompleted++;
    }
  }
  const trend: TrendPoint[] = [];
  for (const dayKey of trendDays) {
    cumDue += plannedOnAll.get(dayKey) || 0;
    cumCompleted += completedOn.get(dayKey) || 0;
    let effDue = cumDue, effCompleted = cumCompleted, effOverdue = cumOverdue;
    if (effDue === 0 && filteredTasks.length > 0) {
      effDue = filteredTasks.length;
      effCompleted = totalDoneCount;
      effOverdue = 0;
    }
    const mm = parseInt(dayKey.slice(5, 7), 10);
    const dd = parseInt(dayKey.slice(8, 10), 10);
    trend.push({
      date: `${mm}/${dd}`,
      completion_rate: rate(effCompleted, effDue),
      delay_rate: rate(effOverdue, effDue),
    });
    cumOverdue += plannedOnNotDone.get(dayKey) || 0;
  }

  // Daily per-farm task count (for stacked bar chart)
  // Group tasks by date × farm
  const dailyFarmMap = new Map<string, Map<string, number>>();
  const farmNameSet = new Set<string>();
  for (const t of filteredTasks) {
    const planned = getScheduledDay(t);
    if (!planned) continue;
    const farmName = t.farmName || '不明';
    farmNameSet.add(farmName);
    let dayMap = dailyFarmMap.get(planned);
    if (!dayMap) {
      dayMap = new Map();
      dailyFarmMap.set(planned, dayMap);
    }
    dayMap.set(farmName, (dayMap.get(farmName) || 0) + 1);
  }
  const dailyFarmNames = [...farmNameSet].sort((a, b) => a.localeCompare(b, 'ja'));
  // Build continuous date range covering all task dates
  const taskDates = [...dailyFarmMap.keys()].sort();
  const rangeStart = taskDates.length > 0 && taskDates[0] < trendDays[0] ? taskDates[0] : trendDays[0];
  const rangeEnd = taskDates.length > 0 && taskDates[taskDates.length - 1] > trendDays[trendDays.length - 1]
    ? taskDates[taskDates.length - 1]
    : trendDays[trendDays.length - 1];
  const allBarDays: string[] = [];
  {
    const startMs = new Date(rangeStart + 'T00:00:00+09:00').getTime();
    const endMs = new Date(rangeEnd + 'T00:00:00+09:00').getTime();
    for (let ms = startMs; ms <= endMs; ms += 86400000) {
      allBarDays.push(getJstDayKey(new Date(ms)));
    }
  }
  // Only populate days that have task data; skip zero-fill for empty days
  // (Recharts treats missing keys as 0 for stacked bars)
  const dailyFarmBars: DailyFarmBar[] = allBarDays.map((dayKey) => {
    const mm = parseInt(dayKey.slice(5, 7), 10);
    const dd = parseInt(dayKey.slice(8, 10), 10);
    const row: DailyFarmBar = { date: `${mm}/${dd}`, _dateKey: dayKey };
    const dayMap = dailyFarmMap.get(dayKey);
    if (dayMap) {
      dayMap.forEach((count, name) => { row[name] = count; });
    }
    return row;
  });

  return {
    kpi: {
      completion_rate: rate(totalCompleted, totalDue),
      completed_count: totalCompleted,
      due_count: totalDue,
      overdue_count: totalOverdue,
      delay_rate: rate(totalOverdue, totalDue),
      due_today_count: totalDueToday,
      upcoming_3days_count: totalUpcoming,
      future_count: totalFuture,
      total_task_count: filteredTasks.length,
      as_of: asOf,
    },
    farmers,
    task_types: taskTypes,
    distribution,
    trend,
    daily_farm_bars: dailyFarmBars,
    daily_farm_names: dailyFarmNames,
    farmer_details: farmerDetails,
    as_of: asOf,
  };
}

type TaskFamilyFilterPanelProps = {
  familyOptions: string[];
  selectedFamily: string;
  isFilterPending: boolean;
  dashboardPending: boolean;
  actionFilter: ActionFilterKey;
  onSelectFamily: (family: string) => void;
  onClearActionFilter: () => void;
};

const TaskFamilyFilterPanel = memo(function TaskFamilyFilterPanel({
  familyOptions,
  selectedFamily,
  isFilterPending,
  dashboardPending,
  actionFilter,
  onSelectFamily,
  onClearActionFilter,
}: TaskFamilyFilterPanelProps) {
  const options = [ALL_FAMILY_OPTION, ...familyOptions];
  const segmentOptions = options;
  return (
    <div className="task-family-filter">
      <span className="task-family-filter__label">タスクを選択</span>
      <div className="task-family-filter__hybrid">
        <div className="task-family-segments" role="radiogroup" aria-label="表示タスク選択">
          {segmentOptions.map((family) => {
            const checked = selectedFamily === family;
            return (
              <button
                key={family}
                type="button"
                className={`task-family-segment ${checked ? 'active' : ''}`}
                onClick={() => onSelectFamily(family)}
                aria-pressed={checked}
              >
                {family}
              </button>
            );
          })}
        </div>
      </div>
      {(isFilterPending || dashboardPending || actionFilter !== 'none') && (
        <span className="task-family-filter__label">
          {(isFilterPending || dashboardPending) && 'フィルター反映中...'}
          {actionFilter !== 'none' && (
            <>
              {(isFilterPending || dashboardPending) ? ' ' : ''}
              <button type="button" onClick={onClearActionFilter}>解除</button>
            </>
          )}
        </span>
      )}
    </div>
  );
});

// --------------- Custom Tooltip Components ---------------

const tooltipLabelMap: Record<string, string> = {
  completed_count: '完了',
  overdue_count: '遅延',
  pending_count: '未着手',
  count: '農業者数',
  x: '到来済みタスク',
  y: '遅延率',
  delay_rate: '遅延率',
  completion_rate: '完了率',
};

function ChartTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  return (
    <div className="custom-tooltip">
      {label != null && <div className="custom-tooltip__title">{label}</div>}
      <ul className="custom-tooltip__list">
        {payload.map((entry: any, i: number) => (
          <li key={i} className="custom-tooltip__row">
            <span className="custom-tooltip__dot" style={{ background: entry.color || entry.fill }} />
            <span className="custom-tooltip__label">{tooltipLabelMap[entry.dataKey] ?? entry.name ?? entry.dataKey}</span>
            <span className="custom-tooltip__value">
              {typeof entry.value === 'number' ? entry.value.toLocaleString() : entry.value}
              {entry.unit ?? ''}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function ScatterTooltip({ active, payload }: any) {
  if (!active || !payload?.length) return null;
  const data = payload[0]?.payload as (FarmerRow & { x: number; y: number; z: number }) | undefined;
  if (!data) return null;
  return (
    <div className="custom-tooltip">
      <div className="custom-tooltip__title">{data.name}</div>
      <ul className="custom-tooltip__list">
        <li className="custom-tooltip__row">
          <span className="custom-tooltip__dot" style={{ background: '#64b5f6' }} />
          <span className="custom-tooltip__label">到来済みタスク</span>
          <span className="custom-tooltip__value">{data.x}</span>
        </li>
        <li className="custom-tooltip__row">
          <span className="custom-tooltip__dot" style={{ background: data.delay_rate >= 30 ? '#ef4444' : data.delay_rate >= 15 ? '#f59e0b' : '#22c55e' }} />
          <span className="custom-tooltip__label">遅延率</span>
          <span className="custom-tooltip__value">{data.delay_rate}%</span>
        </li>
        <li className="custom-tooltip__row">
          <span className="custom-tooltip__dot" style={{ background: '#22c55e' }} />
          <span className="custom-tooltip__label">完了率</span>
          <span className="custom-tooltip__value">{data.completion_rate}%</span>
        </li>
        <li className="custom-tooltip__row">
          <span className="custom-tooltip__dot" style={{ background: '#94a3b8' }} />
          <span className="custom-tooltip__label">圃場数</span>
          <span className="custom-tooltip__value">{data.field_count}</span>
        </li>
      </ul>
    </div>
  );
}

function DailyFarmTooltip({ active, payload, label }: any) {
  if (!active || !payload?.length) return null;
  const nonZero = payload.filter((e: any) => e.value > 0);
  if (nonZero.length === 0) return null;
  const total = nonZero.reduce((s: number, e: any) => s + (e.value || 0), 0);
  return (
    <div className="custom-tooltip">
      <div className="custom-tooltip__title">{label}  <span className="custom-tooltip__total">合計 {total}</span></div>
      <ul className="custom-tooltip__list">
        {nonZero.map((entry: any, i: number) => (
          <li key={i} className="custom-tooltip__row">
            <span className="custom-tooltip__dot" style={{ background: entry.color || entry.fill }} />
            <span className="custom-tooltip__label">{entry.dataKey}</span>
            <span className="custom-tooltip__value">{entry.value}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

// --------------- End Custom Tooltip Components ---------------

type DashboardChartsPanelProps = {
  dashboard: DashboardBundle;
  bubbleRows: Array<FarmerRow & { x: number; y: number; z: number }>;
  sortedFarmers: FarmerRow[];
  selectedFarmer: FarmerRow | null;
  selectedDetail: FarmerDetail | null;
  linkedTasks: DashboardTask[];
  selectedDelayBucket: string | null;
  onSelectFarmer: (id: string | null) => void;
  onSelectDelayBucket: (bucket: string | null) => void;
  onSort: (key: SortKey) => void;
};

const DashboardChartsPanel = memo(function DashboardChartsPanel({
  dashboard,
  bubbleRows,
  sortedFarmers,
  selectedFarmer,
  selectedDetail,
  linkedTasks,
  selectedDelayBucket,
  onSelectFarmer,
  onSelectDelayBucket,
  onSort,
}: DashboardChartsPanelProps) {
  const [chartMonthOffset, setChartMonthOffset] = useState(0);
  const [dailyChartMode, setDailyChartMode] = useState<'scheduled' | 'completed'>('scheduled');

  const completedDailyBars = useMemo(() => {
    const byDay = new Map<string, Map<string, number>>();
    const farmSet = new Set<string>();
    for (const t of linkedTasks) {
      const dayKey = String(t.executionDay || '').trim();
      if (!dayKey) continue;
      const farm = t.farmName || '不明';
      farmSet.add(farm);
      if (!byDay.has(dayKey)) byDay.set(dayKey, new Map());
      const m = byDay.get(dayKey)!;
      m.set(farm, (m.get(farm) || 0) + 1);
    }
    const farmNames = Array.from(farmSet).sort((a, b) => a.localeCompare(b, 'ja'));
    const dayKeys = Array.from(byDay.keys()).sort();
    const bars: DailyFarmBar[] = dayKeys.map((dayKey) => {
      const dt = new Date(`${dayKey}T00:00:00+09:00`);
      const label = Number.isFinite(dt.getTime())
        ? dt.toLocaleDateString('ja-JP', { month: 'numeric', day: 'numeric' })
        : dayKey;
      const row: DailyFarmBar = { date: label, _dateKey: dayKey };
      farmNames.forEach((farm) => {
        row[farm] = byDay.get(dayKey)?.get(farm) || 0;
      });
      return row;
    });
    return { bars, farmNames };
  }, [linkedTasks]);

  const activeDailyBars = dailyChartMode === 'scheduled'
    ? (dashboard.daily_farm_bars ?? [])
    : completedDailyBars.bars;
  const activeDailyFarmNames = dailyChartMode === 'scheduled'
    ? (dashboard.daily_farm_names ?? [])
    : completedDailyBars.farmNames;

  // Filter daily_farm_bars to the selected month window
  const chartBarsFiltered = useMemo(() => {
    const bars = activeDailyBars;
    if (bars.length === 0) return bars;
    const now = new Date();
    const baseMonth = new Date(now.getFullYear(), now.getMonth() + chartMonthOffset, 1);
    const prefix = `${baseMonth.getFullYear()}-${String(baseMonth.getMonth() + 1).padStart(2, '0')}`;
    return bars.filter((b) => typeof b._dateKey === 'string' && b._dateKey.startsWith(prefix));
  }, [activeDailyBars, chartMonthOffset]);

  const chartMonthLabel = useMemo(() => {
    const now = new Date();
    const d = new Date(now.getFullYear(), now.getMonth() + chartMonthOffset, 1);
    return `${d.getFullYear()}年${d.getMonth() + 1}月`;
  }, [chartMonthOffset]);
  const distributionTotal = useMemo(
    () => (dashboard.distribution || []).reduce((sum, row) => sum + Number(row.count || 0), 0),
    [dashboard.distribution],
  );
  const distributionPeak = useMemo(() => {
    const rows = dashboard.distribution || [];
    if (rows.length === 0) return '';
    const sorted = [...rows].sort((a, b) => Number(b.count || 0) - Number(a.count || 0));
    return String(sorted[0]?.bucket || '');
  }, [dashboard.distribution]);

  return (
    <>
      <section className="task-progress-layer2">
        <article className="card chart-card">
          <h3>農業者遅延マップ</h3>
          <div className="chart-wrap">
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart margin={{ top: 10, right: 14, left: 4, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#203047" />
                <XAxis type="number" dataKey="x" stroke="#94a3b8" name="到来済みタスク" />
                <YAxis type="number" dataKey="y" stroke="#94a3b8" unit="%" name="遅延率" />
                <Tooltip cursor={{ strokeDasharray: '3 3' }} content={<ScatterTooltip />} />
                <Scatter data={bubbleRows} onClick={(entry) => onSelectFarmer(String((entry as any).id))}>
                  {bubbleRows.map((row) => (
                    <Cell
                      key={row.id}
                      fill={row.delay_rate >= 30 ? '#ef4444' : row.delay_rate >= 15 ? '#f59e0b' : '#22c55e'}
                    />
                  ))}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="card ranking-card">
          {!selectedFarmer && (
            <>
              <h3>農業者ランキング</h3>
              <div className="table-wrap">
                <table>
                  <thead>
                    <tr>
                      <th>#</th>
                      <th><button onClick={() => onSort('name')}>農業者名</button></th>
                      <th><button onClick={() => onSort('field_count')}>圃場数</button></th>
                      <th>未登録圃場</th>
                      <th><button onClick={() => onSort('due_task_count')}>タスク</button></th>
                      <th><button onClick={() => onSort('overdue_count')}>遅延</button></th>
                      <th><button onClick={() => onSort('delay_rate')}>遅延率</button></th>
                      <th><button onClick={() => onSort('completion_rate')}>完了率</button></th>
                      <th>傾向</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(() => {
                      let rank = 0;
                      return sortedFarmers.map((f) => {
                        const isUnranked = Boolean(f.is_unranked);
                        if (!isUnranked) rank += 1;
                      const trend = trendLabel(f.trend_direction);
                      return (
                        <tr key={f.id} onClick={() => onSelectFarmer(f.id)}>
                          <td>{isUnranked ? '-' : rank}</td>
                          <td>
                            <span className={`status-dot ${f.delay_status}`} />
                            {f.name}
                          </td>
                          <td>{f.field_count}</td>
                          <td>{typeof f.no_task_field_count === 'number' ? f.no_task_field_count : '-'}</td>
                          <td>{f.due_task_count}</td>
                          <td className="danger">{f.overdue_count}</td>
                          <td><span className={`badge ${f.delay_status}`}>{f.delay_rate}%</span></td>
                          <td>{f.completion_rate}%</td>
                          <td className={trend.className}>{trend.symbol} {trend.text}</td>
                        </tr>
                      );
                      });
                    })()}
                  </tbody>
                </table>
              </div>
            </>
          )}

          {selectedFarmer && (
            <div className="farmer-detail">
              <div className="farmer-detail-head">
                <div>
                  <h3>{selectedFarmer.name}</h3>
                  <p>{selectedFarmer.field_count} 圃場 / 到来済み {selectedFarmer.due_task_count} タスク</p>
                </div>
                <button onClick={() => onSelectFarmer(null)} aria-label="閉じる">✕</button>
              </div>

              <div className="farmer-mini-kpi">
                <div className="mini green"><span>完了</span><strong>{selectedFarmer.completed_count}</strong></div>
                <div className="mini red"><span>遅延</span><strong>{selectedFarmer.overdue_count}</strong></div>
                <div className="mini blue"><span>予定</span><strong>{selectedFarmer.future_task_count}</strong></div>
                <div className={`mini ${selectedFarmer.delay_status}`}><span>遅延率</span><strong>{selectedFarmer.delay_rate}%</strong></div>
              </div>

              {selectedDetail && (
                <div className="farmer-task-breakdown">
                  {selectedDetail.task_types.map((row) => {
                  const total = row.completed_count + row.overdue_count + row.pending_count;
                  const completedWidth = total ? (row.completed_count / total) * 100 : 0;
                  const overdueWidth = total ? (row.overdue_count / total) * 100 : 0;
                  const pendingWidth = total ? (row.pending_count / total) * 100 : 0;

                  return (
                    <div key={row.name} className="breakdown-row">
                      <div className="breakdown-name">{row.name}</div>
                      <div className="breakdown-bar" role="img" aria-label={`${row.name} progress`}>
                        <span style={{ width: `${completedWidth}%` }} className="seg-completed" />
                        <span style={{ width: `${overdueWidth}%` }} className="seg-overdue" />
                        <span style={{ width: `${pendingWidth}%` }} className="seg-pending" />
                      </div>
                      <div className="breakdown-text">
                        {row.completed_count}/{row.due_count} 完了 · {row.overdue_count} 遅延 · 完了 {row.completion_rate}% / 遅延 {row.delay_rate}%
                      </div>
                    </div>
                  );
                  })}
                </div>
              )}
            </div>
          )}
        </article>
      </section>

      <section className="task-progress-layer3">
        <article className="card chart-card">
          <h3>タスクタイプ別進捗（回数別）</h3>
          <div className="chart-wrap">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={dashboard.task_types} layout="vertical" margin={{ top: 10, right: 16, left: 44, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#203047" />
                <XAxis type="number" stroke="#94a3b8" />
                <YAxis dataKey="task_type_name" type="category" width={170} stroke="#94a3b8" />
                <Tooltip content={<ChartTooltip />} />
                <Bar dataKey="completed_count" stackId="a" fill="#22c55e" name="完了" />
                <Bar dataKey="overdue_count" stackId="a" fill="#ef4444" name="遅延" />
                <Bar dataKey="pending_count" stackId="a" fill="rgba(255,255,255,0.2)" name="未着手" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="card chart-card">
          <h3>遅延率分布</h3>
          <p className="manual-update-msg">
            母数 n={distributionTotal}
            {distributionPeak ? ` / 最多帯: ${distributionPeak}` : ''}
            {selectedDelayBucket ? ` / フィルター: ${selectedDelayBucket}` : ''}
          </p>
          {selectedDelayBucket && (
            <div className="snapshot-table-controls">
              <button type="button" onClick={() => onSelectDelayBucket(null)}>
                遅延率分布フィルター解除
              </button>
            </div>
          )}
          <div className="chart-wrap">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={dashboard.distribution} margin={{ top: 10, right: 16, left: 8, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#203047" />
                <XAxis dataKey="bucket" stroke="#94a3b8" />
                <YAxis stroke="#94a3b8" />
                <Tooltip content={<ChartTooltip />} />
                <Bar dataKey="count" name="農業者数">
                  {dashboard.distribution.map((row) => (
                    <Cell
                      key={row.bucket}
                      fill={row.color}
                      fillOpacity={row.bucket === distributionPeak || row.bucket === selectedDelayBucket ? 1 : 0.75}
                      stroke={row.bucket === distributionPeak || row.bucket === selectedDelayBucket ? '#ffffff' : 'none'}
                      strokeWidth={row.bucket === distributionPeak || row.bucket === selectedDelayBucket ? 1 : 0}
                      style={{ cursor: 'pointer' }}
                      onClick={() => onSelectDelayBucket(row.bucket)}
                    />
                  ))}
                  <LabelList
                    dataKey="count"
                    position="top"
                    formatter={(value: unknown) => {
                      const count = Number(value || 0);
                      if (!Number.isFinite(count) || count <= 0) return '';
                      const ratio = distributionTotal > 0 ? Math.round((count / distributionTotal) * 1000) / 10 : 0;
                      return `${count} (${ratio}%)`;
                    }}
                    style={{ fill: '#cbd5e1', fontSize: 11, fontWeight: 700 }}
                  />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>
      </section>

      <section className="task-progress-layer4 card chart-card">
        <div className="chart-month-nav">
          <div className="chart-mode-toggle" role="group" aria-label="日別集計モード">
            <button
              type="button"
              className={dailyChartMode === 'scheduled' ? 'active' : ''}
              onClick={() => setDailyChartMode('scheduled')}
            >
              予定日ベース
            </button>
            <button
              type="button"
              className={dailyChartMode === 'completed' ? 'active' : ''}
              onClick={() => setDailyChartMode('completed')}
            >
              完了日ベース
            </button>
          </div>
          <h3>{dailyChartMode === 'scheduled' ? '日別タスク数（農場別）' : '日別完了タスク数（農場別）'} — {chartMonthLabel}</h3>
          <div className="chart-month-nav-actions">
            <button type="button" onClick={() => setChartMonthOffset((v) => v - 1)}>&lt; 前月</button>
            <button type="button" onClick={() => setChartMonthOffset((v) => v + 1)}>翌月 &gt;</button>
          </div>
        </div>
        <div className="chart-wrap trend">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartBarsFiltered} margin={{ top: 10, right: 16, left: 8, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#203047" />
              <XAxis dataKey="date" stroke="#94a3b8" interval={Math.max(0, Math.floor((chartBarsFiltered.length - 1) / 8))} />
              <YAxis stroke="#94a3b8" allowDecimals={false} />
              <Tooltip content={<DailyFarmTooltip />} />
              {activeDailyFarmNames.length <= 12 && (
                <Legend wrapperStyle={{ fontSize: '0.75rem' }} />
              )}
              {activeDailyFarmNames.map((name, i) => (
                <Bar key={name} dataKey={name} stackId="farm" fill={FARM_BAR_COLORS[i % FARM_BAR_COLORS.length]} />
              ))}
            </BarChart>
          </ResponsiveContainer>
        </div>
      </section>
    </>
  );
});

export function TaskProgressDashboardPage() {
  const { auth } = useAuth();
  const [query, setQuery] = useState('');
  const [selectedFarmerId, setSelectedFarmerId] = useState<string | null>(null);
  const [selectedDelayBucket, setSelectedDelayBucket] = useState<string | null>(null);
  const [actionFilter, setActionFilter] = useState<ActionFilterKey>('none');
  const [sortKey, setSortKey] = useState<SortKey>('delay_rate');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const [selectedFamily, setSelectedFamily] = useState<string>(ALL_FAMILY_OPTION);
  const [snapshotDate, setSnapshotDate] = useState<string>(getJstDayKey(new Date()));
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [snapshotLoading, setSnapshotLoading] = useState(false);
  const [snapshotErr, setSnapshotErr] = useState<string | null>(null);
  const [snapshotRun, setSnapshotRun] = useState<SnapshotRun | null>(null);
  const [snapshotFields, setSnapshotFields] = useState<SnapshotField[]>([]);
  const [snapshotTasks, setSnapshotTasks] = useState<SnapshotTask[]>([]);
  const [snapshotFieldsLoaded, setSnapshotFieldsLoaded] = useState(false);
  const [snapshotFieldsLoading, setSnapshotFieldsLoading] = useState(false);
  const [latestFieldCount, setLatestFieldCount] = useState<number | null>(null);
  const [fieldDeltaModalOpen, setFieldDeltaModalOpen] = useState(false);
  const [fieldDeltaRows, setFieldDeltaRows] = useState<FieldDeltaRow[]>([]);
  const [fieldDeltaLoading, setFieldDeltaLoading] = useState(false);
  const [fieldDeltaErr, setFieldDeltaErr] = useState<string | null>(null);
  const [fieldDeltaComparedDate, setFieldDeltaComparedDate] = useState<string>('');
  const [tasksHydrating, setTasksHydrating] = useState(false);
  const [snapshotTasksLoaded, setSnapshotTasksLoaded] = useState(false);
  const [visibleFieldRows, setVisibleFieldRows] = useState(INITIAL_FIELD_ROWS);
  const [visibleTaskRows, setVisibleTaskRows] = useState(INITIAL_TASK_ROWS);
  const [fieldTableQuery, setFieldTableQuery] = useState('');
  const [taskTableQuery, setTaskTableQuery] = useState('');
  const [taskStatusFilter, setTaskStatusFilter] = useState('all');
  const [taskTypeFilter, setTaskTypeFilter] = useState('all');
  const [fieldSortKey, setFieldSortKey] = useState<FieldTableSortKey>('farm_name');
  const [fieldSortDir, setFieldSortDir] = useState<SortDir>('asc');
  const [taskSortKey, setTaskSortKey] = useState<TaskTableSortKey>('planned_date');
  const [taskSortDir, setTaskSortDir] = useState<SortDir>('asc');
  const [showNoTaskFieldsOnly, setShowNoTaskFieldsOnly] = useState(false);
  const [refreshToken, setRefreshToken] = useState(0);
  const [manualUpdateLoading, setManualUpdateLoading] = useState(false);
  const [manualUpdateMsg, setManualUpdateMsg] = useState<string | null>(null);
  const [snapshotLoadingElapsedSec, setSnapshotLoadingElapsedSec] = useState(0);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchActiveIndex, setSearchActiveIndex] = useState(-1);
  const [loadingSteps, setLoadingSteps] = useState<Record<string, 'pending' | 'loading' | 'done' | 'cached'>>({});
  const [sprayCategoryMap, setSprayCategoryMap] = useState<Record<string, string>>({});
  const sprayCategoryMapRef = useRef<Record<string, string>>({});
  const [sprayMapReady, setSprayMapReady] = useState(false);
  const [isFilterPending, startFilterTransition] = useTransition();
  const [workerPending, setWorkerPending] = useState(false);
  const [workerDashboard, setWorkerDashboard] = useState<DashboardBundle | null>(null);
  const workerRef = useRef<Worker | null>(null);
  const workerReqIdRef = useRef(0);
  const workerReqKeyRef = useRef<string>('');
  const workerReqTasksLenRef = useRef(0);
  const rankingFarmerNameByIdRef = useRef<Record<string, string>>({});
  const loadingOverlayStartedAtRef = useRef<number | null>(null);
  const [dashboardState, setDashboardState] = useState<DashboardBundle>(emptyDashboardBundle(getJstDayKey(new Date())));
  const dashboardStateRef = useRef(dashboardState);
  dashboardStateRef.current = dashboardState;
  const didPickInitialSnapshotDate = useRef(false);
  const snapshotFieldTableRef = useRef<HTMLElement | null>(null);
  const searchBoxRef = useRef<HTMLDivElement | null>(null);

  const fetchSnapshotMeta = useCallback(async (targetDate: string): Promise<any> => {
    const metaKey = `${targetDate}:meta`;
    const now = Date.now();
    const cached = snapshotPageCache.get(metaKey);
    const ss = !cached || cached.expiresAt <= now ? readSessionCache<any>(metaKey) : null;
    const active = (cached && cached.expiresAt > now) ? cached : ss;
    if (active) return active.data;

    const res = await fetch(
      withApiBase(`/hfr-snapshots?snapshot_date=${encodeURIComponent(targetDate)}&include_fields=false&include_tasks=false`),
    );
    const json = await res.json();
    if (!res.ok || json?.ok === false) {
      const reason = json?.detail?.reason || json?.reason || `HTTP ${res.status}`;
      throw new Error(`スナップショット取得失敗: ${reason}`);
    }
    const expiresAt = Date.now() + SNAPSHOT_CLIENT_CACHE_TTL_MS;
    snapshotPageCache.set(metaKey, { data: json, expiresAt });
    writeSessionCache(metaKey, json, expiresAt);
    return json;
  }, []);

  const fetchSnapshotFieldsForDate = useCallback(async (targetDate: string): Promise<SnapshotField[]> => {
    const key = `${targetDate}:fields:${SNAPSHOT_FIELD_LIMIT}`;
    const now = Date.now();
    const cached = snapshotPageCache.get(key);
    const ss = !cached || cached.expiresAt <= now ? readSessionCache<any>(key) : null;
    const active = (cached && cached.expiresAt > now) ? cached : ss;
    if (active?.data?.fields && Array.isArray(active.data.fields)) {
      return active.data.fields as SnapshotField[];
    }
    const res = await fetch(
      withApiBase(
        `/hfr-snapshots?snapshot_date=${encodeURIComponent(targetDate)}`
        + `&include_fields=true&include_tasks=false&field_limit=${SNAPSHOT_FIELD_LIMIT}&limit=${SNAPSHOT_FIELD_LIMIT}`,
      ),
    );
    const json = await res.json();
    if (!res.ok || json?.ok === false) {
      const reason = json?.detail?.reason || json?.reason || `HTTP ${res.status}`;
      throw new Error(`圃場一覧取得失敗: ${reason}`);
    }
    const expiresAt = Date.now() + SNAPSHOT_CLIENT_CACHE_TTL_MS;
    snapshotPageCache.set(key, { data: json, expiresAt });
    writeSessionCache(key, json, expiresAt);
    return (json?.fields ?? []) as SnapshotField[];
  }, []);

  const applySnapshotDate = useCallback((nextDate: string) => {
    const normalized = String(nextDate || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) return;
    setSnapshotDate((prev) => (prev === normalized ? prev : normalized));
  }, []);

  useEffect(() => {
    if (typeof Worker === 'undefined') return;
    const worker = new Worker(new URL('../workers/dashboardBundleWorker.ts', import.meta.url), { type: 'module' });
    workerRef.current = worker;
    worker.onmessage = (event: MessageEvent<{ id: number; bundle?: DashboardBundle }>) => {
      const { id, bundle } = event.data || {};
      if (id !== workerReqIdRef.current) return;
      if (bundle) {
        setWorkerDashboard(bundle);
        const key = workerReqKeyRef.current;
        if (key) {
          if (bundleCache.size >= BUNDLE_CACHE_MAX) {
            const firstKey = bundleCache.keys().next().value;
            if (firstKey !== undefined) bundleCache.delete(firstKey);
          }
          bundleCache.set(key, { tasksLen: workerReqTasksLenRef.current, bundle });
        }
      }
      setWorkerPending(false);
    };
    return () => {
      worker.terminate();
      workerRef.current = null;
    };
  }, []);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    const load = async () => {
      setTasksHydrating(false);
      setSnapshotErr(null);
      try {
        const now = Date.now();
        const metaKey = `${snapshotDate}:meta`;
        const summaryKey = `${snapshotDate}:summary`;
        const summaryDefaultKey = summaryFilterCacheKey(snapshotDate, ALL_FAMILY_OPTION, 'none');
        const snapKey = `${snapshotDate}:tasks:${SNAPSHOT_TASK_LIMIT}`;
        const datesKey = 'dates:365';
        const cachedMeta = snapshotPageCache.get(metaKey);
        const cachedSummary = snapshotPageCache.get(summaryKey);
        const cachedSnap = snapshotPageCache.get(snapKey);
        const cachedDates = snapshotDatesCache.get(datesKey);
        const ssMeta = !cachedMeta || cachedMeta.expiresAt <= now ? readSessionCache<any>(metaKey) : null;
        const ssSummary = !cachedSummary || cachedSummary.expiresAt <= now ? readSessionCache<any>(summaryKey) : null;
        const ssSnap = !cachedSnap || cachedSnap.expiresAt <= now ? readSessionCache<any>(snapKey) : null;
        const ssDates = !cachedDates || cachedDates.expiresAt <= now ? readSessionCache<any>(datesKey) : null;
        const activeMeta = (cachedMeta && cachedMeta.expiresAt > now) ? cachedMeta : ssMeta;
        const activeSummary = (cachedSummary && cachedSummary.expiresAt > now) ? cachedSummary : ssSummary;
        const activeSnap = (cachedSnap && cachedSnap.expiresAt > now) ? cachedSnap : ssSnap;
        const activeDates = (cachedDates && cachedDates.expiresAt > now) ? cachedDates : ssDates;
        const useCachedMeta = Boolean(activeMeta);
        const useCachedSummary = Boolean(activeSummary);
        const useCachedSnap = Boolean(activeSnap);
        const useCachedDates = Boolean(activeDates);
        const hasInstantHydrate = useCachedMeta && useCachedSummary && useCachedSnap;

        setSnapshotLoading(true);
        if (hasInstantHydrate) {
          const cachedAsOf = (activeSummary?.data?.as_of || snapshotDate) as string;
          setSnapshotRun((activeMeta?.data?.run ?? null) as SnapshotRun | null);
          setSnapshotFields([]);
          setSnapshotFieldsLoaded(false);
          setSnapshotTasks((activeSnap?.data?.tasks ?? []) as SnapshotTask[]);
          setSnapshotTasksLoaded(true);
          setDashboardState({
            farmer_count: Number(activeSummary?.data?.farmer_count ?? 0),
            field_count: Number(activeSummary?.data?.field_count ?? 0),
            total_area_m2: Number(activeSummary?.data?.total_area_m2 ?? 0),
            kpi: activeSummary?.data?.kpi ?? emptyDashboardBundle(snapshotDate).kpi,
            farmers: activeSummary?.data?.farmers ?? [],
            task_types: activeSummary?.data?.task_types ?? [],
            distribution: activeSummary?.data?.distribution ?? [],
            trend: activeSummary?.data?.trend ?? [],
            daily_farm_bars: activeSummary?.data?.daily_farm_bars ?? [],
            daily_farm_names: activeSummary?.data?.daily_farm_names ?? [],
            farmer_details: activeSummary?.data?.farmer_details ?? {},
            no_task_farmers: activeSummary?.data?.no_task_farmers ?? [],
            as_of: cachedAsOf,
          });
          snapshotPageCache.set(summaryDefaultKey, { data: activeSummary?.data, expiresAt: now + SNAPSHOT_CLIENT_CACHE_TTL_MS });
          if (useCachedDates) {
            const cached = activeDates?.data;
            const dates = Array.isArray(cached?.dates) ? cached.dates.filter((d: any) => typeof d === 'string') : [];
            setAvailableDates(dates);
          }
          setLoadingSteps({
            meta: 'cached',
            summary: 'cached',
            tasks: 'cached',
            dates: useCachedDates ? 'cached' : 'loading',
          });
          if (!useCachedDates) {
            void (async () => {
              try {
                const datesRes = await fetch(withApiBase('/hfr-snapshots/dates?limit=365'), {
                  signal: controller.signal,
                });
                if (!datesRes.ok || !active) return;
                const datesJson = await datesRes.json();
                const expiresAt = Date.now() + SNAPSHOT_CLIENT_CACHE_TTL_MS;
                snapshotDatesCache.set(datesKey, { data: datesJson, expiresAt });
                writeSessionCache(datesKey, datesJson, expiresAt);
                const nextDates = Array.isArray(datesJson?.dates)
                  ? datesJson.dates.filter((d: any) => typeof d === 'string')
                  : [];
                setAvailableDates(nextDates);
                setLoadingSteps((prev) => ({ ...prev, dates: 'done' }));
              } catch {
                if (active) setLoadingSteps((prev) => ({ ...prev, dates: 'done' }));
              }
            })();
          }
          // キャッシュ即時復元でも、再入場時はローディングメニューを短時間表示する
          await new Promise((resolve) => window.setTimeout(resolve, 220));
          if (active) setSnapshotLoading(false);
          return;
        }

        setLoadingSteps({ meta: 'pending', summary: 'pending', tasks: 'pending', dates: 'pending' });

        let metaJson: any;
        let summaryJson: any = null;
        let dates: string[] = [];

        if (useCachedMeta) {
          metaJson = activeMeta?.data;
          setLoadingSteps((prev) => ({ ...prev, meta: 'cached' }));
        } else {
          setLoadingSteps((prev) => ({ ...prev, meta: 'loading' }));
          const metaRes = await fetch(
            withApiBase(`/hfr-snapshots?snapshot_date=${encodeURIComponent(snapshotDate)}&include_fields=false&include_tasks=false`),
            { signal: controller.signal },
          );
          metaJson = await metaRes.json();
          if (!metaRes.ok || metaJson?.ok === false) {
            const reason = metaJson?.detail?.reason || metaJson?.reason || `HTTP ${metaRes.status}`;
            throw new Error(`スナップショット取得失敗: ${reason}`);
          }
          const expiresAt = now + SNAPSHOT_CLIENT_CACHE_TTL_MS;
          snapshotPageCache.set(metaKey, { data: metaJson, expiresAt });
          writeSessionCache(metaKey, metaJson, expiresAt);
          if (active) setLoadingSteps((prev) => ({ ...prev, meta: 'done' }));
        }

        if (useCachedSummary) {
          summaryJson = activeSummary?.data;
          setLoadingSteps((prev) => ({ ...prev, summary: 'cached' }));
        } else {
          const empty = emptyDashboardBundle(snapshotDate);
          summaryJson = {
            farmer_count: Number(metaJson?.farmer_count ?? 0),
            field_count: Number(metaJson?.field_count ?? 0),
            total_area_m2: Number(metaJson?.total_area_m2 ?? 0),
            kpi: empty.kpi,
            farmers: [],
            task_types: [],
            distribution: [],
            trend: [],
            daily_farm_bars: [],
            daily_farm_names: [],
            farmer_details: {},
            no_task_farmers: Array.isArray(metaJson?.no_task_farmers) ? metaJson.no_task_farmers : [],
            as_of: snapshotDate,
          };
          setLoadingSteps((prev) => ({ ...prev, summary: 'done' }));
        }

        if (useCachedDates) {
          const cached = activeDates?.data;
          dates = Array.isArray(cached?.dates) ? cached.dates.filter((d: any) => typeof d === 'string') : [];
          setLoadingSteps((prev) => ({ ...prev, dates: 'cached' }));
        } else {
          // 日付一覧は表示に必須ではないため、バックグラウンドで更新する。
          setLoadingSteps((prev) => ({ ...prev, dates: 'loading' }));
          void (async () => {
            try {
              const datesRes = await fetch(withApiBase('/hfr-snapshots/dates?limit=365'), {
                signal: controller.signal,
              });
              if (!datesRes.ok) return;
              const datesJson = await datesRes.json();
              const expiresAt = Date.now() + SNAPSHOT_CLIENT_CACHE_TTL_MS;
              snapshotDatesCache.set(datesKey, {
                data: datesJson,
                expiresAt,
              });
              writeSessionCache(datesKey, datesJson, expiresAt);
              if (!active) return;
              const nextDates = Array.isArray(datesJson?.dates)
                ? datesJson.dates.filter((d: any) => typeof d === 'string')
                : [];
              setAvailableDates(nextDates);
              setLoadingSteps((prev) => ({ ...prev, dates: 'done' }));
            } catch {
              if (active) setLoadingSteps((prev) => ({ ...prev, dates: 'done' }));
            }
          })();
        }

        if (!active) return;
        setSnapshotRun((metaJson?.run ?? null) as SnapshotRun | null);
        setSnapshotFields([]);
        setSnapshotFieldsLoaded(false);
        if (useCachedSnap) {
          setSnapshotTasks((activeSnap?.data?.tasks ?? []) as SnapshotTask[]);
          setSnapshotTasksLoaded(true);
        } else {
          setSnapshotTasks([]);
          setSnapshotTasksLoaded(false);
        }
        setDashboardState((prev) => ({
          farmer_count: Number(summaryJson?.farmer_count ?? prev.farmer_count ?? 0),
          field_count: Number(summaryJson?.field_count ?? prev.field_count ?? 0),
          total_area_m2: Number(summaryJson?.total_area_m2 ?? prev.total_area_m2 ?? 0),
          kpi: summaryJson?.kpi ?? emptyDashboardBundle(snapshotDate).kpi,
          farmers: summaryJson?.farmers ?? [],
          task_types: summaryJson?.task_types ?? [],
          distribution: summaryJson?.distribution ?? [],
          trend: summaryJson?.trend ?? [],
          daily_farm_bars: summaryJson?.daily_farm_bars ?? [],
          daily_farm_names: summaryJson?.daily_farm_names ?? [],
          farmer_details: summaryJson?.farmer_details ?? {},
          no_task_farmers: summaryJson?.no_task_farmers ?? prev.no_task_farmers ?? [],
          as_of: summaryJson?.as_of ?? snapshotDate,
        }));
        setAvailableDates(dates);
        if (summaryJson) {
          const expiresAt = Date.now() + SNAPSHOT_CLIENT_CACHE_TTL_MS;
          snapshotPageCache.set(summaryDefaultKey, { data: summaryJson, expiresAt });
        }

        if (useCachedSnap) {
          setSnapshotTasks((activeSnap?.data?.tasks ?? []) as SnapshotTask[]);
          setSnapshotTasksLoaded(true);
          setLoadingSteps((prev) => ({ ...prev, tasks: 'cached' }));
        } else {
          setTasksHydrating(true);
          setLoadingSteps((prev) => ({ ...prev, tasks: 'loading' }));
          void (async () => {
            try {
              let taskRes = await fetch(
                withApiBase(
                  `/hfr-snapshots/tasks-lite?snapshot_date=${encodeURIComponent(snapshotDate)}`
                  + `&task_limit=${SNAPSHOT_TASK_LIMIT}&limit=${SNAPSHOT_TASK_LIMIT}`,
                ),
                { signal: controller.signal },
              );
              let taskJson = await taskRes.json();
              let tasks = Array.isArray(taskJson?.tasks) ? taskJson.tasks : [];
              let valid = tasks.length === 0 || hasRequiredSnapshotTaskKeys(tasks[0]);

              if ((!taskRes.ok || taskJson?.ok === false || !valid) && active) {
                // Safety fallback: keep dashboard complete even if lite endpoint/schema mismatches.
                taskRes = await fetch(
                  withApiBase(
                    `/hfr-snapshots?snapshot_date=${encodeURIComponent(snapshotDate)}`
                    + `&include_fields=false&include_tasks=true&task_limit=${SNAPSHOT_TASK_LIMIT}&limit=${SNAPSHOT_TASK_LIMIT}`,
                  ),
                  { signal: controller.signal },
                );
                taskJson = await taskRes.json();
                tasks = Array.isArray(taskJson?.tasks) ? taskJson.tasks : [];
                valid = tasks.length === 0 || hasRequiredSnapshotTaskKeys(tasks[0]);
              }
              if (!active || !taskRes.ok || taskJson?.ok === false || !valid) return;
              const expiresAt = Date.now() + SNAPSHOT_CLIENT_CACHE_TTL_MS;
              snapshotPageCache.set(snapKey, {
                data: taskJson,
                expiresAt,
              });
              writeSessionCache(snapKey, taskJson, expiresAt);
              setSnapshotTasks(tasks as SnapshotTask[]);
              setSnapshotTasksLoaded(true);
              setLoadingSteps((prev) => ({ ...prev, tasks: 'done' }));
            } catch {
              // 背景ロード失敗は画面全体エラーにしない
              if (active) setLoadingSteps((prev) => ({ ...prev, tasks: 'done' }));
            } finally {
              if (active) setTasksHydrating(false);
            }
          })();
        }

      } catch (error: any) {
        if (!active || error?.name === 'AbortError') return;
        setSnapshotErr(error?.message || 'スナップショットの取得に失敗しました');
        setSnapshotRun(null);
        setSnapshotFields([]);
        setSnapshotFieldsLoaded(false);
        setSnapshotTasks([]);
        setSnapshotTasksLoaded(false);
        setTasksHydrating(false);
      } finally {
        if (active) setSnapshotLoading(false);
      }
    };

    void load();
    return () => {
      active = false;
      controller.abort();
    };
  }, [snapshotDate, refreshToken]);

  useEffect(() => {
    setVisibleFieldRows(INITIAL_FIELD_ROWS);
    setVisibleTaskRows(INITIAL_TASK_ROWS);
  }, [snapshotDate, snapshotFields.length, snapshotTasks.length, fieldTableQuery, taskTableQuery, taskStatusFilter, taskTypeFilter]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    const loadSprayMaster = async () => {
      const hasSpraying = snapshotTasks.some((t) => t.task_type === 'Spraying');
      if (!hasSpraying) {
        sprayCategoryMapRef.current = {};
        setSprayCategoryMap({});
        setSprayMapReady(true);
        return;
      }
      const cacheKey = `${COUNTRY_UUID_JP}:${RICE_CROP_UUID}:FIELDTREATMENT`;
      const now = Date.now();
      const cached = sprayMasterCache.get(cacheKey);
      if (cached && cached.expiresAt > now) {
        sprayCategoryMapRef.current = cached.data;
        setSprayCategoryMap(cached.data);
        setSprayMapReady(true);
        return;
      }
      setSprayMapReady(false);
      try {
        const body: Record<string, unknown> = {
          country_uuid: COUNTRY_UUID_JP,
          crop_uuid: RICE_CROP_UUID,
          task_type_code: 'FIELDTREATMENT',
        };
        if (auth?.login?.login_token && auth?.api_token) {
          body.login_token = auth.login.login_token;
          body.api_token = auth.api_token;
        }
        const res = await fetch(withApiBase('/crop-protection-products/cached'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          signal: controller.signal,
          body: JSON.stringify(body),
        });
        const json = await res.json();
        if (!active || !res.ok || json?.ok === false) return;
        const items = Array.isArray(json?.items) ? json.items : [];
        const next: Record<string, string> = {};
        items.forEach((p: any) => {
          const name = String(p?.product_name || p?.name || '').trim();
          if (!name) return;
          const codeOrName = String(p?.category_name || p?.category_code || '').trim();
          if (!codeOrName) return;
          const keyRaw = name.toLowerCase();
          const keyNormalized = normalizeProductToken(name);
          next[`${RICE_CROP_UUID}:${keyRaw}`] = codeOrName;
          next[`*:${keyRaw}`] = codeOrName;
          if (keyNormalized) {
            next[`${RICE_CROP_UUID}:${keyNormalized}`] = codeOrName;
            next[`*:${keyNormalized}`] = codeOrName;
          }
        });
        sprayMasterCache.set(cacheKey, {
          data: next,
          expiresAt: Date.now() + (12 * 60 * 60 * 1000),
        });
        sprayCategoryMapRef.current = next;
        setSprayCategoryMap(next);
      } catch {
        if (!active) return;
        sprayCategoryMapRef.current = {};
        setSprayCategoryMap({});
      } finally {
        if (active) setSprayMapReady(true);
      }
    };
    void loadSprayMaster();
    return () => {
      active = false;
      controller.abort();
    };
  }, [auth?.login?.login_token, auth?.api_token, snapshotTasks]);

  const loadingOverlayActive = snapshotLoading || tasksHydrating || !sprayMapReady;
  useEffect(() => {
    if (!loadingOverlayActive) {
      loadingOverlayStartedAtRef.current = null;
      setSnapshotLoadingElapsedSec(0);
      return;
    }
    if (!loadingOverlayStartedAtRef.current) {
      loadingOverlayStartedAtRef.current = Date.now();
    }
    const timer = window.setInterval(() => {
      const startedAt = loadingOverlayStartedAtRef.current || Date.now();
      const elapsed = Math.floor((Date.now() - startedAt) / 1000);
      setSnapshotLoadingElapsedSec(elapsed);
    }, 500);
    return () => window.clearInterval(timer);
  }, [loadingOverlayActive]);

  // Only compute allTasks once spray master is ready (avoids double-processing 50k tasks)
  const allTasks = useMemo(
    () => sprayMapReady ? tasksFromSnapshot(snapshotTasks, sprayCategoryMapRef.current) : [],
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [snapshotTasks, sprayMapReady],
  );
  const normalizedGlobalQuery = useMemo(() => query.trim().toLowerCase(), [query]);
  const baseGloballyFilteredTasks = useMemo(() => {
    let tasks = allTasks;
    if (selectedFarmerId) {
      tasks = tasks.filter((task) => {
        const farmerId = task.farmUuid || `name:${task.farmName || ''}`;
        return farmerId === selectedFarmerId;
      });
    }
    if (normalizedGlobalQuery) {
      tasks = tasks.filter((task) => {
        const text = [
          task.farmName,
          task.userName,
          task.fieldName,
          task.taskName,
          task.assigneeName,
        ].map((v) => String(v || '').toLowerCase()).join(' ');
        return text.includes(normalizedGlobalQuery);
      });
    }
    return tasks;
  }, [allTasks, selectedFarmerId, normalizedGlobalQuery]);
  const globallyFilteredTasks = useMemo(() => {
    if (!selectedDelayBucket) return baseGloballyFilteredTasks;
    const today = getJstDayKey(new Date());
    const byFarmer = new Map<string, { tasks: DashboardTask[]; due: number; overdue: number }>();
    for (const task of baseGloballyFilteredTasks) {
      const farmerId = task.farmUuid || `name:${task.farmName || ''}`;
      if (!byFarmer.has(farmerId)) byFarmer.set(farmerId, { tasks: [], due: 0, overdue: 0 });
      const e = byFarmer.get(farmerId)!;
      e.tasks.push(task);
      const planned = getScheduledDay(task);
      if (planned && planned <= today) {
        e.due += 1;
      }
      if (planned && planned < today && !task.completed) {
        e.overdue += 1;
      }
    }
    const keepFarmerIds = new Set<string>();
    byFarmer.forEach((v, farmerId) => {
      const due = v.due === 0 && v.tasks.length > 0 ? v.tasks.length : v.due;
      const delayRate = due > 0 ? Math.round((v.overdue * 1000) / due) / 10 : 0;
      if (bucketContainsDelayRate(selectedDelayBucket, delayRate)) {
        keepFarmerIds.add(farmerId);
      }
    });
    return baseGloballyFilteredTasks.filter((task) => {
      const farmerId = task.farmUuid || `name:${task.farmName || ''}`;
      return keepFarmerIds.has(farmerId);
    });
  }, [baseGloballyFilteredTasks, selectedDelayBucket]);
  const hasGlobalTaskFilter = Boolean(selectedFarmerId || normalizedGlobalQuery || selectedDelayBucket);
  const familyOptions = useMemo(() => {
    const set = new Set<string>();
    allTasks.forEach((task) => set.add(task.typeFamily));
    if (set.size === 0) {
      (dashboardState.task_types || []).forEach((row) => {
        const raw = String(row.task_type_name || '').trim();
        if (!raw) return;
        const m = raw.match(/^(.*)\s\d+回目$/);
        const family = normalizeTaskFamilyLabel(m ? m[1] : raw);
        if (family) set.add(family);
      });
    }
    const hasSprayingTask = allTasks.some((task) => task.taskType === 'Spraying');
    if (hasSprayingTask) {
      PROTECTION_FILTER_OPTIONS.forEach((name) => set.add(name));
    }
    return Array.from(set).sort((a, b) => {
      const ai = TYPE_FAMILY_ORDER.indexOf(a);
      const bi = TYPE_FAMILY_ORDER.indexOf(b);
      if (ai === -1 && bi === -1) return a.localeCompare(b, 'ja');
      if (ai === -1) return 1;
      if (bi === -1) return -1;
      return ai - bi;
    });
  }, [allTasks, dashboardState.task_types]);

  const taskFilterIndex = useMemo(() => {
    const today = getJstDayKey(new Date());
    const in7days = addDaysToKey(today, 7);
    const all = globallyFilteredTasks;
    const allByAction: Record<Exclude<ActionFilterKey, 'none'>, DashboardTask[]> = {
      overdue: [],
      due_today: [],
      upcoming_3days: [],
      incomplete: [],
      future: [],
      completed: [],
    };
    const byFamily = new Map<string, DashboardTask[]>();
    const byFamilyAction = new Map<string, Record<Exclude<ActionFilterKey, 'none'>, DashboardTask[]>>();

    const ensureFamily = (family: string) => {
      if (!byFamily.has(family)) byFamily.set(family, []);
      if (!byFamilyAction.has(family)) {
        byFamilyAction.set(family, {
          overdue: [],
          due_today: [],
          upcoming_3days: [],
          incomplete: [],
          future: [],
          completed: [],
        });
      }
    };

    for (const task of all) {
      const planned = getScheduledDay(task);
      const done = task.completed;
      const family = task.typeFamily;
      ensureFamily(family);
      byFamily.get(family)!.push(task);
      const famAction = byFamilyAction.get(family)!;

      if (!done) {
        allByAction.incomplete.push(task);
        famAction.incomplete.push(task);
      } else {
        allByAction.completed.push(task);
        famAction.completed.push(task);
      }
      if (planned && planned < today && !done) {
        allByAction.overdue.push(task);
        famAction.overdue.push(task);
      }
      if (planned && planned === today && !done) {
        allByAction.due_today.push(task);
        famAction.due_today.push(task);
      }
      if (planned && planned > today && planned <= in7days && !done) {
        allByAction.upcoming_3days.push(task);
        famAction.upcoming_3days.push(task);
      }
      if (planned && planned > today && !done) {
        allByAction.future.push(task);
        famAction.future.push(task);
      }
    }
    return { all, allByAction, byFamily, byFamilyAction };
  }, [globallyFilteredTasks]);

  // Shared filtered tasks — computed once, used by both dashboard bundle and linkedTasks
  const filteredTasksForDashboard = useMemo((): DashboardTask[] => {
    if (snapshotLoading || (allTasks.length === 0 && !sprayMapReady)) return [];
    const isDefault = selectedFamily === ALL_FAMILY_OPTION && actionFilter === 'none';
    if (isDefault) return taskFilterIndex.all;

    const selectedIsValid = selectedFamily === ALL_FAMILY_OPTION || familyOptions.includes(selectedFamily);
    const base =
      selectedFamily === ALL_FAMILY_OPTION
        ? taskFilterIndex.all
        : (taskFilterIndex.byFamily.get(selectedFamily) ?? []);
    const filtered =
      actionFilter === 'none'
        ? base
        : (selectedFamily === ALL_FAMILY_OPTION
          ? taskFilterIndex.allByAction[actionFilter]
          : (taskFilterIndex.byFamilyAction.get(selectedFamily)?.[actionFilter] ?? []));
    if (filtered.length > 0 || actionFilter !== 'none') return filtered;

    // 選択値が古い/無効な場合だけ全タスクへフォールバック
    if (actionFilter === 'none' && !selectedIsValid) {
      return taskFilterIndex.all;
    }
    return filtered;
  }, [snapshotLoading, allTasks.length, sprayMapReady, selectedFamily, actionFilter, familyOptions, taskFilterIndex]);

  const useWorkerBundle = filteredTasksForDashboard.length >= WORKER_BUNDLE_THRESHOLD;
  const dashboardFilterCacheKey = useMemo(
    () => `${snapshotDate}:${selectedFamily}:${actionFilter}:q=${normalizedGlobalQuery}:farmer=${selectedFarmerId || ''}:delayBucket=${selectedDelayBucket || ''}`,
    [snapshotDate, selectedFamily, actionFilter, normalizedGlobalQuery, selectedFarmerId, selectedDelayBucket],
  );

  // Client-side dashboard recomputation — useMemo for instant response
  const computedDashboard = useMemo((): DashboardBundle | null => {
    if (useWorkerBundle) return null;
    if (snapshotLoading || (allTasks.length === 0 && !sprayMapReady)) return null;

    const isDefaultFilter = selectedFamily === ALL_FAMILY_OPTION && actionFilter === 'none' && !hasGlobalTaskFilter;
    if (isDefaultFilter) {
      const summaryDefaultKey = summaryFilterCacheKey(snapshotDate, ALL_FAMILY_OPTION, 'none');
      const cached = snapshotPageCache.get(summaryDefaultKey);
      if (cached && cached.expiresAt > Date.now()) {
        const serverData = cached.data as DashboardBundle;
        if (!serverData.daily_farm_bars || serverData.daily_farm_bars.length === 0) {
          const supplement = computeDashboardBundle(allTasks, snapshotDate, serverData.as_of || snapshotDate);
          return { ...serverData, daily_farm_bars: supplement.daily_farm_bars, daily_farm_names: supplement.daily_farm_names };
        }
        return serverData;
      }
    }

    // Check bundle cache for instant filter switching
    const cacheKey = dashboardFilterCacheKey;
    const hit = bundleCache.get(cacheKey);
    if (hit && hit.tasksLen === allTasks.length) {
      return hit.bundle;
    }

    const asOf = dashboardStateRef.current.as_of || snapshotDate;
    const bundle = computeDashboardBundle(filteredTasksForDashboard, snapshotDate, asOf);

    // Store in cache (evict oldest if over limit)
    if (bundleCache.size >= BUNDLE_CACHE_MAX) {
      const firstKey = bundleCache.keys().next().value;
      if (firstKey !== undefined) bundleCache.delete(firstKey);
    }
    bundleCache.set(cacheKey, { tasksLen: allTasks.length, bundle });
    return bundle;
  }, [
    snapshotDate,
    selectedFamily,
    actionFilter,
    snapshotLoading,
    allTasks,
    sprayMapReady,
    filteredTasksForDashboard,
    useWorkerBundle,
    hasGlobalTaskFilter,
    dashboardFilterCacheKey,
  ]);

  useEffect(() => {
    if (!useWorkerBundle) {
      setWorkerPending(false);
      setWorkerDashboard(null);
      return;
    }
    if (snapshotLoading || (allTasks.length === 0 && !sprayMapReady)) {
      setWorkerPending(false);
      setWorkerDashboard(null);
      return;
    }
    const cacheKey = dashboardFilterCacheKey;
    const hit = bundleCache.get(cacheKey);
    if (hit && hit.tasksLen === allTasks.length) {
      setWorkerDashboard(hit.bundle);
      setWorkerPending(false);
      return;
    }
    if (!workerRef.current) return;
    const reqId = workerReqIdRef.current + 1;
    workerReqIdRef.current = reqId;
    workerReqKeyRef.current = cacheKey;
    workerReqTasksLenRef.current = allTasks.length;
    setWorkerPending(true);
    workerRef.current.postMessage({
      id: reqId,
      tasks: filteredTasksForDashboard,
      snapshotDate,
      asOf: dashboardStateRef.current.as_of || snapshotDate,
    });
  }, [
    useWorkerBundle,
    snapshotLoading,
    allTasks.length,
    sprayMapReady,
    filteredTasksForDashboard,
    snapshotDate,
    selectedFamily,
    actionFilter,
    dashboardFilterCacheKey,
  ]);

  useEffect(() => {
    if (familyOptions.length === 0) return;
    if (selectedFamily === ALL_FAMILY_OPTION) return;
    if (familyOptions.includes(selectedFamily)) return;
    const sprayDefault = familyOptions.find((family) => family.startsWith('防除タスク'));
    if (sprayDefault) {
      setSelectedFamily(sprayDefault);
      return;
    }
    setSelectedFamily(familyOptions[0]);
  }, [familyOptions, selectedFamily]);

  // linkedTasks = filteredTasksForDashboard (already computed, no re-filtering needed)
  const linkedTasks = filteredTasksForDashboard;
  const linkedTaskFieldUuids = useMemo(() => {
    const set = new Set<string>();
    for (const task of linkedTasks) {
      const fuid = String(task.fieldUuid || '');
      if (fuid) set.add(fuid);
    }
    return set;
  }, [linkedTasks]);

  const dashboard = { ...dashboardState, ...(workerDashboard ?? computedDashboard ?? {}) } as DashboardBundle;
  const chartTaskTypes = useMemo(() => {
    const today = getJstDayKey(new Date());
    const grouped = new Map<string, { due: number; completed: number; overdue: number; pending: number }>();
    for (const t of linkedTasks) {
      const label = `${t.typeFamily} ${t.occurrence}回目`;
      if (!grouped.has(label)) grouped.set(label, { due: 0, completed: 0, overdue: 0, pending: 0 });
      const bucket = grouped.get(label)!;
      const planned = getScheduledDay(t);
      const done = t.completed;
      if (!planned) {
        if (!done) bucket.pending += 1;
        continue;
      }
      if (planned <= today) {
        bucket.due += 1;
        if (done) bucket.completed += 1;
      } else if (!done) {
        bucket.pending += 1;
      }
      if (planned < today && !done) bucket.overdue += 1;
    }

    const toDisplayOrder = (name: string): number => {
      const m = name.match(/^(.*)\s(\d+)回目$/);
      const family = m ? m[1] : name;
      const occurrence = m ? Number(m[2]) : 99;
      const idx = TYPE_FAMILY_ORDER.indexOf(family);
      const base = idx >= 0 ? idx : 99;
      return base * 100 + (Number.isFinite(occurrence) ? occurrence : 99);
    };
    const pct = (n: number, d: number): number => (d > 0 ? Number(((n * 100) / d).toFixed(1)) : 0);

    return Array.from(grouped.entries())
      .sort((a, b) => toDisplayOrder(a[0]) - toDisplayOrder(b[0]))
      .map(([task_type_name, c], idx) => ({
        task_type_name,
        display_order: idx + 1,
        due_count: c.due,
        completed_count: c.completed,
        overdue_count: c.overdue,
        pending_count: c.pending,
        completion_rate: pct(c.completed, c.due),
        delay_rate: pct(c.overdue, c.due),
      }));
  }, [linkedTasks]);

  const farmerSearchIndex = useMemo(() => {
    const map = new Map<string, string[]>();
    const add = (id: string, value: string) => {
      const key = String(id || '').trim();
      const v = String(value || '').trim();
      if (!key || !v) return;
      if (!map.has(key)) map.set(key, []);
      const arr = map.get(key)!;
      if (!arr.includes(v)) arr.push(v);
    };

    dashboard.farmers.forEach((row) => add(String(row.id || ''), String(row.name || '')));
    (dashboard.no_task_farmers || []).forEach((row: any) => {
      add(String(row?.id || ''), String(row?.name || ''));
    });
    snapshotTasks.forEach((row) => {
      const id = String(row.farm_uuid || `name:${row.farm_name || ''}`);
      add(id, String(row.farm_name || ''));
      add(id, String(row.user_name || ''));
    });
    snapshotFields.forEach((row) => {
      const id = String(row.farm_uuid || `name:${row.farm_name || ''}`);
      add(id, String(row.farm_name || ''));
      add(id, String(row.user_name || ''));
    });

    return map;
  }, [dashboard.farmers, dashboard.no_task_farmers, snapshotTasks, snapshotFields]);

  const searchOptions = useMemo(() => {
    const farmSet = new Set<string>();
    const userSet = new Set<string>();
    dashboard.farmers.forEach((row) => {
      const name = String(row.name || '').trim();
      if (name) farmSet.add(name);
    });
    (dashboard.no_task_farmers || []).forEach((row: any) => {
      const name = String(row?.name || '').trim();
      if (name) farmSet.add(name);
    });
    snapshotTasks.forEach((row) => {
      const farm = String(row.farm_name || '').trim();
      const user = String(row.user_name || '').trim();
      if (farm) farmSet.add(farm);
      if (user) userSet.add(user);
    });
    snapshotFields.forEach((row) => {
      const farm = String(row.farm_name || '').trim();
      const user = String(row.user_name || '').trim();
      if (farm) farmSet.add(farm);
      if (user) userSet.add(user);
    });
    const farms = Array.from(farmSet).sort((a, b) => a.localeCompare(b, 'ja')).map((label) => ({ label, type: '農場' as const }));
    const users = Array.from(userSet).sort((a, b) => a.localeCompare(b, 'ja')).map((label) => ({ label, type: 'ユーザー' as const }));
    return [...farms, ...users];
  }, [dashboard.farmers, dashboard.no_task_farmers, snapshotTasks, snapshotFields]);

  const searchOptionRows = useMemo(() => {
    const q = query.trim().toLowerCase();
    const base = q
      ? searchOptions.filter((opt) => opt.label.toLowerCase().includes(q))
      : searchOptions;
    return base.slice(0, 12);
  }, [searchOptions, query]);

  const filteredFarmers = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return dashboard.farmers;
    return dashboard.farmers.filter((row) => {
      const id = String(row.id || '');
      const labels = farmerSearchIndex.get(id) || [String(row.name || '')];
      return labels.some((v) => String(v || '').toLowerCase().includes(q));
    });
  }, [dashboard.farmers, query, farmerSearchIndex]);

  const handleSelectSearchOption = useCallback((value: string) => {
    setQuery(value);
    setSearchOpen(false);
    setSearchActiveIndex(-1);
  }, []);

  const handleSearchKeyDown = useCallback((e: ReactKeyboardEvent<HTMLInputElement>) => {
    if (!searchOpen) {
      if (e.key === 'ArrowDown' && searchOptionRows.length > 0) {
        setSearchOpen(true);
        setSearchActiveIndex(0);
        e.preventDefault();
      }
      return;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setSearchActiveIndex((prev) => {
        const next = prev + 1;
        return next >= searchOptionRows.length ? 0 : next;
      });
      return;
    }
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      setSearchActiveIndex((prev) => {
        const next = prev <= 0 ? searchOptionRows.length - 1 : prev - 1;
        return Math.max(0, next);
      });
      return;
    }
    if (e.key === 'Enter') {
      if (searchActiveIndex >= 0 && searchActiveIndex < searchOptionRows.length) {
        e.preventDefault();
        handleSelectSearchOption(searchOptionRows[searchActiveIndex].label);
      }
      return;
    }
    if (e.key === 'Escape') {
      setSearchOpen(false);
      setSearchActiveIndex(-1);
    }
  }, [searchOpen, searchOptionRows, searchActiveIndex, handleSelectSearchOption]);

  useEffect(() => {
    const onDocMouseDown = (event: MouseEvent) => {
      const el = searchBoxRef.current;
      if (!el) return;
      if (event.target instanceof Node && el.contains(event.target)) return;
      setSearchOpen(false);
      setSearchActiveIndex(-1);
    };
    document.addEventListener('mousedown', onDocMouseDown);
    return () => document.removeEventListener('mousedown', onDocMouseDown);
  }, []);

  const sortedFarmers = useMemo(() => {
    const rows = [...filteredFarmers];
    rows.sort((a, b) => {
      const av = a[sortKey];
      const bv = b[sortKey];
      if (typeof av === 'string' && typeof bv === 'string') {
        return sortDir === 'asc' ? av.localeCompare(bv, 'ja') : bv.localeCompare(av, 'ja');
      }
      const diff = Number(av) - Number(bv);
      return sortDir === 'asc' ? diff : -diff;
    });
    return rows;
  }, [filteredFarmers, sortDir, sortKey]);

  const selectedDetail = useMemo(
    () => (selectedFarmerId ? dashboard.farmer_details[selectedFarmerId] ?? null : null),
    [dashboard.farmer_details, selectedFarmerId]
  );

  const bubbleRows = useMemo(
    () => sortedFarmers.map((f) => ({ ...f, x: f.due_task_count, y: f.delay_rate, z: Math.max(16, f.overdue_count) })),
    [sortedFarmers]
  );

  const snapshotTaskByUuid = useMemo(() => {
    const map = new Map<string, SnapshotTask>();
    for (const row of snapshotTasks) {
      const key = String(row.task_uuid || '');
      if (!key) continue;
      map.set(key, row);
    }
    return map;
  }, [snapshotTasks]);
  const snapshotFieldsByFieldUuid = useMemo(() => {
    const map = new Map<string, SnapshotField[]>();
    for (const row of snapshotFields) {
      const key = String(row.field_uuid || '');
      if (!key) continue;
      if (!map.has(key)) map.set(key, []);
      map.get(key)?.push(row);
    }
    return map;
  }, [snapshotFields]);
  const fieldStats = useMemo(() => {
    const farmerCountFromSummary = Number(dashboard.farmer_count ?? 0);
    const farmerCountFromTasks = dashboard.farmers.length;
    const farmerCount = farmerCountFromSummary > 0 ? farmerCountFromSummary : farmerCountFromTasks;
    const fieldCountFromFarmers = dashboard.farmers.reduce((s, f) => s + f.field_count, 0);
    const summaryFieldCount = Number(dashboard.field_count ?? 0);
    const summaryAreaM2 = Number(dashboard.total_area_m2 ?? 0);
    const taskFieldCount = linkedTaskFieldUuids.size;
    const summaryTaskFieldCount = summaryFieldCount > 0 ? Math.max(0, Math.min(summaryFieldCount, taskFieldCount)) : taskFieldCount;
    const summaryNoTaskFieldCount = summaryFieldCount > 0 ? Math.max(0, summaryFieldCount - summaryTaskFieldCount) : 0;

    if (snapshotFieldsLoaded) {
      const uniqueFields = new Map<string, number>();
      const farmerSet = new Set<string>();
      let fieldsWithTask = 0;
      let fieldsWithoutTask = 0;
      for (const f of snapshotFields) {
        const fuid = String(f.field_uuid || '');
        if (hasGlobalTaskFilter && fuid && !linkedTaskFieldUuids.has(fuid)) continue;
        if (fuid && !uniqueFields.has(fuid)) {
          uniqueFields.set(fuid, Number(f.area_m2 ?? 0) || 0);
          if (linkedTaskFieldUuids.has(fuid)) fieldsWithTask += 1;
          else fieldsWithoutTask += 1;
        }
        const farmKey = String(f.farm_uuid || f.farm_name || '').trim();
        if (farmKey) farmerSet.add(farmKey);
      }
      const totalAreaM2 = [...uniqueFields.values()].reduce((s, v) => s + v, 0);
      return {
        farmerCount: farmerSet.size || farmerCount,
        fieldCount: uniqueFields.size || fieldCountFromFarmers,
        totalAreaHa: totalAreaM2 / 10000,
        fieldsWithTask,
        fieldsWithoutTask,
      };
    }

    if (summaryFieldCount > 0 || summaryAreaM2 > 0) {
      return {
        farmerCount,
        fieldCount: summaryFieldCount || fieldCountFromFarmers,
        totalAreaHa: summaryAreaM2 / 10000,
        fieldsWithTask: summaryTaskFieldCount,
        fieldsWithoutTask: summaryNoTaskFieldCount,
      };
    }
    return {
      farmerCount,
      fieldCount: fieldCountFromFarmers,
      totalAreaHa: null as number | null,
      fieldsWithTask: summaryTaskFieldCount,
      fieldsWithoutTask: summaryNoTaskFieldCount,
    };
  }, [dashboard.farmers, dashboard.farmer_count, dashboard.field_count, dashboard.total_area_m2, snapshotFields, snapshotFieldsLoaded, linkedTaskFieldUuids, hasGlobalTaskFilter]);

  const noTaskFieldCountByFarmer = useMemo(() => {
    const map = new Map<string, number>();
    if (!snapshotFieldsLoaded) return map;
    for (const row of snapshotFields) {
      const farmerId = String(row.farm_uuid || `name:${row.farm_name || ''}`);
      if (!farmerId) continue;
      const fieldUuid = String(row.field_uuid || '');
      if (!fieldUuid || linkedTaskFieldUuids.has(fieldUuid)) continue;
      map.set(farmerId, (map.get(farmerId) || 0) + 1);
    }
    return map;
  }, [snapshotFieldsLoaded, snapshotFields, linkedTaskFieldUuids]);

  const rankingFarmers = useMemo(() => {
    const base = sortedFarmers.map((f) => ({
      ...f,
      no_task_field_count: noTaskFieldCountByFarmer.get(String(f.id || '')) || 0,
      is_unranked: false,
    }));

    const existing = new Set(base.map((r) => String(r.id || '')));
    const q = query.trim();
    const extras: FarmerRow[] = [];
    const noTaskFarmers = Array.isArray(dashboard.no_task_farmers) ? dashboard.no_task_farmers : [];
    noTaskFarmers.forEach((meta: any) => {
      const farmerId = String(meta?.id || '');
      if (!farmerId) return;
      if (existing.has(farmerId)) return;
      const noTaskCount = Number(meta?.no_task_field_count || 0);
      if (noTaskCount <= 0) return;
      const name = String(meta?.name || '').trim() || farmerId.replace(/^name:/, '');
      if (q && !name.includes(q)) return;
      extras.push({
        id: farmerId,
        name,
        field_count: Number(meta?.field_count || noTaskCount),
        no_task_field_count: noTaskCount,
        is_unranked: true,
        due_task_count: 0,
        completed_count: 0,
        overdue_count: 0,
        due_today_count: 0,
        upcoming_3days_count: 0,
        future_task_count: 0,
        delay_rate: 0,
        completion_rate: 0,
        delay_status: 'good',
        trend_direction: 'stable',
      });
    });
    extras.sort((a, b) => a.name.localeCompare(b.name, 'ja'));
    return [...base, ...extras];
  }, [sortedFarmers, noTaskFieldCountByFarmer, query, dashboard.no_task_farmers]);
  const selectedRankingFarmer = useMemo(
    () => (selectedFarmerId ? rankingFarmers.find((f) => f.id === selectedFarmerId) ?? null : null),
    [rankingFarmers, selectedFarmerId],
  );
  useEffect(() => {
    const map: Record<string, string> = {};
    rankingFarmers.forEach((f) => {
      const id = String(f.id || '').trim();
      if (!id) return;
      map[id] = String(f.name || '').trim();
    });
    rankingFarmerNameByIdRef.current = map;
  }, [rankingFarmers]);

  const canManualUpdate =
    (auth?.email || '').trim().toLowerCase() === 'am@shonai.inc' &&
    Boolean(auth?.login?.login_token) &&
    Boolean(auth?.api_token);

  const allTaskRows = useMemo(() => {
    const out: TaskTableRow[] = [];
    for (const t of linkedTasks) {
      const row = snapshotTaskByUuid.get(t.uuid);
      if (!row) continue;
      const family = resolveTaskFamilyFromSnapshot(row, sprayCategoryMap);
      const subtypeInfo = detectSpraySubtype(row, sprayCategoryMap);
      const occurrence = Number(t.occurrence || row.occurrence || 1);
      out.push({
        ...row,
        occurrence,
        _family: family,
        _subtypeLabel: subtypeInfo.subtype !== '-' ? `${subtypeInfo.subtype} (${subtypeInfo.source})` : '-',
        _taskDisplay: `${family} ${occurrence}回目`,
      });
    }
    return out;
  }, [linkedTasks, snapshotTaskByUuid, sprayCategoryMap]);

  const sortedSnapshotFields = useMemo(
    () => [...snapshotFields].sort((a, b) => {
      const fa = String(a.farm_name || '');
      const fb = String(b.farm_name || '');
      if (fa !== fb) return fa.localeCompare(fb, 'ja');
      const na = String(a.field_name || '');
      const nb = String(b.field_name || '');
      if (na !== nb) return na.localeCompare(nb, 'ja');
      return String(a.season_uuid || '').localeCompare(String(b.season_uuid || ''), 'ja');
    }),
    [snapshotFields],
  );
  const baseSnapshotFields = useMemo(() => {
    if (!snapshotFieldsLoaded) return [] as SnapshotField[];
    const showAllFields = selectedFamily === ALL_FAMILY_OPTION && actionFilter === 'none' && !hasGlobalTaskFilter;
    if (showAllFields && !showNoTaskFieldsOnly) {
      return sortedSnapshotFields;
    }
    if (showAllFields && showNoTaskFieldsOnly) {
      return sortedSnapshotFields.filter((row) => !linkedTaskFieldUuids.has(String(row.field_uuid || '')));
    }
    const fieldUuidSet = new Set<string>();
    const out: SnapshotField[] = [];
    for (const t of linkedTasks) {
      const fuid = String(t.fieldUuid || '');
      if (!fuid || fieldUuidSet.has(fuid)) continue;
      fieldUuidSet.add(fuid);
      const rows = snapshotFieldsByFieldUuid.get(fuid);
      if (rows && rows.length > 0) out.push(...rows);
    }
    if (showNoTaskFieldsOnly) {
      return out.filter((row) => !linkedTaskFieldUuids.has(String(row.field_uuid || '')));
    }
    return out;
  }, [linkedTasks, snapshotFieldsByFieldUuid, snapshotFieldsLoaded, selectedFamily, actionFilter, sortedSnapshotFields, showNoTaskFieldsOnly, linkedTaskFieldUuids, hasGlobalTaskFilter]);

  const filteredSnapshotFields = useMemo(() => {
    const q = fieldTableQuery.trim().toLowerCase();
    if (!q) return baseSnapshotFields;
    return baseSnapshotFields.filter((row) => {
      const text = [
        row.farm_name,
        row.field_name,
        row.user_name,
        row.field_uuid,
        row.crop_name,
        row.variety_name,
      ].map((v) => String(v || '').toLowerCase()).join(' ');
      return text.includes(q);
    });
  }, [baseSnapshotFields, fieldTableQuery]);

  const sortedFilteredSnapshotFields = useMemo(() => {
    const rows = [...filteredSnapshotFields];
    const toDay = (v: string | null | undefined) => String(v || '');
    rows.sort((a, b) => {
      let av: any;
      let bv: any;
      switch (fieldSortKey) {
        case 'snapshot_date':
          av = toDay(a.snapshot_date); bv = toDay(b.snapshot_date); break;
        case 'farm_name':
          av = a.farm_name || ''; bv = b.farm_name || ''; break;
        case 'field_name':
          av = a.field_name || ''; bv = b.field_name || ''; break;
        case 'user_name':
          av = a.user_name || ''; bv = b.user_name || ''; break;
        case 'area_m2':
          av = Number(a.area_m2 || 0); bv = Number(b.area_m2 || 0); break;
        case 'bbch_index':
          av = a.bbch_index || ''; bv = b.bbch_index || ''; break;
        case 'fetched_at':
          av = Date.parse(String(a.fetched_at || '')) || 0;
          bv = Date.parse(String(b.fetched_at || '')) || 0;
          break;
        default:
          av = ''; bv = '';
      }
      if (typeof av === 'number' && typeof bv === 'number') {
        const diff = av - bv;
        return fieldSortDir === 'asc' ? diff : -diff;
      }
      const diff = String(av).localeCompare(String(bv), 'ja');
      return fieldSortDir === 'asc' ? diff : -diff;
    });
    return rows;
  }, [filteredSnapshotFields, fieldSortKey, fieldSortDir]);

  const taskStatusOptions = useMemo(() => {
    const set = new Set<string>();
    allTaskRows.forEach((row) => {
      const s = String(row.status || '').trim();
      if (s) set.add(s);
    });
    return Array.from(set).sort((a, b) => a.localeCompare(b, 'ja'));
  }, [allTaskRows]);

  const taskTypeOptions = useMemo(() => {
    const set = new Set<string>();
    allTaskRows.forEach((row) => {
      const s = String(row._family || row.task_type || '').trim();
      if (s) set.add(s);
    });
    return Array.from(set).sort((a, b) => a.localeCompare(b, 'ja'));
  }, [allTaskRows]);

  const filteredTaskRows = useMemo(() => {
    const q = taskTableQuery.trim().toLowerCase();
    return allTaskRows.filter((row) => {
      if (taskStatusFilter !== 'all' && String(row.status || '') !== taskStatusFilter) return false;
      if (taskTypeFilter !== 'all' && String(row._family || row.task_type || '') !== taskTypeFilter) return false;
      if (!q) return true;
      const text = [
        row.farm_name,
        row.field_name,
        row.task_name,
        row.task_type,
        row._family,
        row._subtypeLabel,
        row.user_name,
        row.assignee_name,
        row.product,
        row.task_uuid,
      ].map((v) => String(v || '').toLowerCase()).join(' ');
      return text.includes(q);
    });
  }, [allTaskRows, taskTableQuery, taskStatusFilter, taskTypeFilter]);

  const sortedFilteredTaskRows = useMemo(() => {
    const rows = [...filteredTaskRows];
    const parseTs = (v: string | null | undefined): number => Date.parse(String(v || '')) || 0;
    rows.sort((a, b) => {
      let av: any;
      let bv: any;
      switch (taskSortKey) {
        case 'snapshot_date': av = a.snapshot_date || ''; bv = b.snapshot_date || ''; break;
        case 'farm_name': av = a.farm_name || ''; bv = b.farm_name || ''; break;
        case 'field_name': av = a.field_name || ''; bv = b.field_name || ''; break;
        case 'task_display': av = a._taskDisplay || ''; bv = b._taskDisplay || ''; break;
        case 'task_name': av = a.task_name || ''; bv = b.task_name || ''; break;
        case 'task_type': av = a.task_type || ''; bv = b.task_type || ''; break;
        case 'occurrence': av = Number(a.occurrence || 0); bv = Number(b.occurrence || 0); break;
        case 'task_date': av = parseTs(a.task_date); bv = parseTs(b.task_date); break;
        case 'planned_date': av = parseTs(a.planned_date); bv = parseTs(b.planned_date); break;
        case 'execution_date': av = parseTs(a.execution_date); bv = parseTs(b.execution_date); break;
        case 'status': av = a.status || ''; bv = b.status || ''; break;
        case 'user_name': av = a.user_name || ''; bv = b.user_name || ''; break;
        case 'product': av = a.product || ''; bv = b.product || ''; break;
        case 'spray_subtype': av = a._subtypeLabel || ''; bv = b._subtypeLabel || ''; break;
        case 'fetched_at': av = parseTs(a.fetched_at); bv = parseTs(b.fetched_at); break;
        default: av = ''; bv = '';
      }
      if (typeof av === 'number' && typeof bv === 'number') {
        const diff = av - bv;
        return taskSortDir === 'asc' ? diff : -diff;
      }
      const diff = String(av).localeCompare(String(bv), 'ja');
      return taskSortDir === 'asc' ? diff : -diff;
    });
    return rows;
  }, [filteredTaskRows, taskSortKey, taskSortDir]);

  const filteredSnapshotTaskCount = sortedFilteredTaskRows.length;
  const visibleTasks = useMemo(
    () => sortedFilteredTaskRows.slice(0, Math.min(visibleTaskRows, sortedFilteredTaskRows.length)),
    [sortedFilteredTaskRows, visibleTaskRows],
  );

  const visibleFields = useMemo(
    () => sortedFilteredSnapshotFields.slice(0, Math.min(visibleFieldRows, sortedFilteredSnapshotFields.length)),
    [sortedFilteredSnapshotFields, visibleFieldRows],
  );

  const handleRefresh = () => {
    snapshotPageCache.clear();
    snapshotDatesCache.clear();
    clearSnapshotSessionCache();
    setRefreshToken((v) => v + 1);
  };

  const loadSnapshotFields = useCallback(async () => {
    if (snapshotFieldsLoading) return;
    setSnapshotFieldsLoading(true);
    try {
      const rows = await fetchSnapshotFieldsForDate(snapshotDate);
      setSnapshotFields(rows);
      setSnapshotFieldsLoaded(true);
    } catch (err: any) {
      setManualUpdateMsg(err?.message || '圃場一覧の取得に失敗しました');
    } finally {
      setSnapshotFieldsLoading(false);
    }
  }, [snapshotDate, snapshotFieldsLoading, fetchSnapshotFieldsForDate]);

  const latestSnapshotDate = useMemo(() => {
    const latest = String(availableDates[0] || '').trim();
    return /^\d{4}-\d{2}-\d{2}$/.test(latest) ? latest : '';
  }, [availableDates]);

  useEffect(() => {
    let active = true;
    const run = async () => {
      if (!latestSnapshotDate) {
        setLatestFieldCount(null);
        return;
      }
      if (latestSnapshotDate === snapshotDate) {
        setLatestFieldCount(Number(dashboardState.field_count ?? 0));
        return;
      }
      try {
        const meta = await fetchSnapshotMeta(latestSnapshotDate);
        if (!active) return;
        setLatestFieldCount(Number(meta?.field_count ?? 0));
      } catch {
        if (active) setLatestFieldCount(null);
      }
    };
    void run();
    return () => {
      active = false;
    };
  }, [latestSnapshotDate, snapshotDate, dashboardState.field_count, fetchSnapshotMeta]);

  const fieldCountDeltaCard = useMemo(() => {
    const baseCount = Number(dashboardState.field_count ?? 0);
    const latestCount =
      latestSnapshotDate === snapshotDate
        ? baseCount
        : (typeof latestFieldCount === 'number' ? latestFieldCount : null);
    const delta = latestCount === null ? null : latestCount - baseCount;
    return {
      baseCount,
      latestCount,
      delta,
    };
  }, [dashboardState.field_count, latestSnapshotDate, snapshotDate, latestFieldCount]);

  const handleOpenFieldDeltaModal = useCallback(async () => {
    setFieldDeltaModalOpen(true);
    setFieldDeltaErr(null);
    setFieldDeltaLoading(true);
    try {
      if (!latestSnapshotDate) {
        setFieldDeltaRows([]);
        setFieldDeltaComparedDate('');
        return;
      }
      const baseDate = snapshotDate;
      const latestDate = latestSnapshotDate;
      setFieldDeltaComparedDate(latestDate);

      if (baseDate === latestDate) {
        setFieldDeltaRows([]);
        return;
      }

      const [baseFields, latestFields] = await Promise.all([
        fetchSnapshotFieldsForDate(baseDate),
        fetchSnapshotFieldsForDate(latestDate),
      ]);

      const toMap = (rows: SnapshotField[]) => {
        const map = new Map<string, { field_uuid: string; field_name: string; area_m2: number | null }>();
        for (const row of rows) {
          const fieldUuid = String(row.field_uuid || '').trim();
          if (!fieldUuid || map.has(fieldUuid)) continue;
          map.set(fieldUuid, {
            field_uuid: fieldUuid,
            field_name: String(row.field_name || ''),
            area_m2: typeof row.area_m2 === 'number' ? row.area_m2 : null,
          });
        }
        return map;
      };

      const baseMap = toMap(baseFields);
      const latestMap = toMap(latestFields);
      const rows: FieldDeltaRow[] = [];

      latestMap.forEach((row, uuid) => {
        if (!baseMap.has(uuid)) {
          rows.push({ diff_type: '増加', ...row });
        }
      });
      baseMap.forEach((row, uuid) => {
        if (!latestMap.has(uuid)) {
          rows.push({ diff_type: '減少', ...row });
        }
      });

      rows.sort((a, b) => {
        if (a.diff_type !== b.diff_type) return a.diff_type === '増加' ? -1 : 1;
        return a.field_name.localeCompare(b.field_name, 'ja');
      });
      setFieldDeltaRows(rows);
    } catch (error: any) {
      setFieldDeltaErr(error?.message || '差分圃場の取得に失敗しました');
    } finally {
      setFieldDeltaLoading(false);
    }
  }, [latestSnapshotDate, snapshotDate, fetchSnapshotFieldsForDate]);

  const handleDownloadFieldDeltaCsv = useCallback(() => {
    if (fieldDeltaRows.length === 0) return;
    const header = ['diff_type', 'field_uuid', 'field_name', 'area_ha'];
    const csvEscape = (value: unknown): string => {
      const raw = String(value ?? '');
      if (/[",\n]/.test(raw)) return `"${raw.replace(/"/g, '""')}"`;
      return raw;
    };
    const lines: string[] = [header.join(',')];
    fieldDeltaRows.forEach((row) => {
      const areaHa =
        typeof row.area_m2 === 'number' && Number.isFinite(row.area_m2)
          ? (row.area_m2 / 10000).toFixed(4)
          : '';
      lines.push([
        csvEscape(row.diff_type),
        csvEscape(row.field_uuid),
        csvEscape(row.field_name),
        csvEscape(areaHa),
      ].join(','));
    });
    const csv = lines.join('\n');
    const blob = new Blob([`\uFEFF${csv}`], { type: 'text/csv;charset=utf-8;' });
    const compareDate = fieldDeltaComparedDate || latestSnapshotDate || 'latest';
    triggerBlobDownload(`hfr_field_delta_${snapshotDate}_vs_${compareDate}.csv`, blob);
  }, [fieldDeltaRows, snapshotDate, fieldDeltaComparedDate, latestSnapshotDate]);

  const handleManualUpdate = async () => {
    if (!canManualUpdate || !auth?.login?.login_token || !auth?.api_token) return;
    const ok = window.confirm('最新スナップショットを取得してDBを更新します。実行しますか？');
    if (!ok) return;
    setManualUpdateLoading(true);
    setManualUpdateMsg(null);
    try {
      const res = await fetch(withApiBase('/jobs/hfr-snapshot'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          email: auth.email,
          login_token: auth.login.login_token,
          api_token: auth.api_token,
          dryRun: false,
          suffix: 'HFR',
          languageCode: 'ja',
        }),
      });
      const json = await res.json();
      if (!res.ok || json?.ok === false) {
        const reason = json?.detail?.reason || json?.reason || `HTTP ${res.status}`;
        if (reason === 'snapshot_job_running') {
          throw new Error('現在スナップショット更新が実行中です。完了後に再実行してください。');
        }
        throw new Error(`HFR更新に失敗: ${reason}`);
      }
      setManualUpdateMsg(
        `更新完了: fields=${json?.fields_saved ?? '-'} tasks=${json?.tasks_saved ?? '-'} run=${json?.run_id ?? '-'}`,
      );
      snapshotPageCache.clear();
      snapshotDatesCache.clear();
      clearSnapshotSessionCache();
      setSnapshotFields([]);
      setSnapshotFieldsLoaded(false);
      setRefreshToken((v) => v + 1);
    } catch (error: any) {
      setManualUpdateMsg(error?.message || 'HFR更新に失敗しました');
    } finally {
      setManualUpdateLoading(false);
    }
  };

  const handleDownloadFieldsCsv = useCallback(async () => {
    if (!snapshotFieldsLoaded) return;
    try {
      const params = new URLSearchParams({
        snapshot_date: snapshotDate,
        action_filter: actionFilter,
        today: getJstDayKey(new Date()),
      });
      if (selectedFamily !== ALL_FAMILY_OPTION) params.set('families', selectedFamily);
      const res = await fetch(withApiBase(`/hfr-snapshots/fields-csv?${params.toString()}`));
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const blob = await res.blob();
      const filename = `hfr_fields_${snapshotDate}.csv`;
      triggerBlobDownload(filename, blob);
    } catch (error: any) {
      setManualUpdateMsg(`圃場CSV出力に失敗: ${error?.message || 'unknown'}`);
    }
  }, [snapshotFieldsLoaded, snapshotDate, actionFilter, selectedFamily]);

  const handleDownloadTasksCsv = useCallback(async () => {
    try {
      const params = new URLSearchParams({
        snapshot_date: snapshotDate,
        action_filter: actionFilter,
        today: getJstDayKey(new Date()),
      });
      if (selectedFamily !== ALL_FAMILY_OPTION) params.set('families', selectedFamily);
      const res = await fetch(withApiBase(`/hfr-snapshots/tasks-csv?${params.toString()}`));
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const blob = await res.blob();
      const filename = `hfr_tasks_${snapshotDate}.csv`;
      triggerBlobDownload(filename, blob);
    } catch (error: any) {
      setManualUpdateMsg(`タスクCSV出力に失敗: ${error?.message || 'unknown'}`);
    }
  }, [snapshotDate, actionFilter, selectedFamily]);

  useEffect(() => {
    setSelectedFarmerId(null);
    setSelectedDelayBucket(null);
    setActionFilter('none');
    setShowNoTaskFieldsOnly(false);
  }, [snapshotDate]);

  useEffect(() => {
    if (didPickInitialSnapshotDate.current) return;
    if (availableDates.length === 0) return;
    const latest = String(availableDates[0] || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(latest)) return;
    didPickInitialSnapshotDate.current = true;
    setSnapshotDate((prev) => (prev === latest ? prev : latest));
  }, [availableDates]);

  const handleSort = useCallback((nextKey: SortKey) => {
    if (sortKey === nextKey) {
      setSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSortKey(nextKey);
    setSortDir(nextKey === 'name' ? 'asc' : 'desc');
  }, [sortKey]);

  const handleFieldTableSort = useCallback((nextKey: FieldTableSortKey) => {
    if (fieldSortKey === nextKey) {
      setFieldSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setFieldSortKey(nextKey);
    setFieldSortDir(nextKey === 'area_m2' || nextKey === 'fetched_at' ? 'desc' : 'asc');
  }, [fieldSortKey]);

  const handleTaskTableSort = useCallback((nextKey: TaskTableSortKey) => {
    if (taskSortKey === nextKey) {
      setTaskSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setTaskSortKey(nextKey);
    setTaskSortDir(
      nextKey === 'planned_date' || nextKey === 'task_date' || nextKey === 'execution_date' || nextKey === 'occurrence'
        ? 'desc'
        : 'asc',
    );
  }, [taskSortKey]);

  const selectFamily = useCallback((family: string) => {
    startFilterTransition(() => {
      setSelectedFamily(family);
    });
  }, []);

  const clearActionFilter = useCallback(() => {
    startFilterTransition(() => setActionFilter('none'));
  }, []);

  const selectFarmer = useCallback((id: string | null) => {
    setSelectedFarmerId((prev) => {
      const next = prev === id ? null : id;
      if (next) {
        const name = rankingFarmerNameByIdRef.current[next];
        if (name) setQuery(name);
      } else {
        setQuery('');
      }
      return next;
    });
  }, []);
  const selectDelayBucket = useCallback((bucket: string | null) => {
    setSelectedDelayBucket((prev) => (prev === bucket ? null : bucket));
  }, []);

  const handleShowNoTaskFields = useCallback(async () => {
    if (!snapshotFieldsLoaded) {
      await loadSnapshotFields();
    }
    setShowNoTaskFieldsOnly(true);
    setTimeout(() => {
      snapshotFieldTableRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
    }, 0);
  }, [snapshotFieldsLoaded, loadSnapshotFields]);

  const isFullyReady = !snapshotLoading && !tasksHydrating && sprayMapReady;
  if (!isFullyReady) {
    const sprayStatus: 'pending' | 'loading' | 'done' | 'cached' = sprayMapReady
      ? 'done'
      : snapshotTasksLoaded
        ? 'loading'
        : 'pending';
    const allSteps = { ...loadingSteps, spray: sprayStatus };
    const doneCount = Object.values(allSteps).filter((s) => s !== 'pending' && s !== 'loading').length;
    const totalSteps = Math.max(1, Object.keys(allSteps).length);
    const progress = Math.min(95, (doneCount / totalSteps) * 100);
    const stepLabels: Record<string, string> = {
      meta: 'メタデータ',
      summary: 'サマリー集計',
      tasks: 'タスク一覧',
      dates: '日付一覧',
      spray: '薬剤マスタ',
    };
    const stepIcon = (status: string) => {
      switch (status) {
        case 'done': return '\u2714';
        case 'cached': return '\u2714 (cache)';
        case 'loading': return '\u25CB';
        default: return '\u2500';
      }
    };
    return (
      <div className="task-progress-page">
        <LoadingOverlay
          message="スナップショットを読み込んでいます..."
          progress={progress}
          details={[
            `基準日: ${snapshotDate}`,
            `経過: ${snapshotLoadingElapsedSec}秒`,
          ]}
        >
          <ul className="loading-overlay__steps">
            {Object.entries(allSteps).map(([key, status]) => (
              <li key={key} className={`loading-step loading-step--${status}`}>
                <span className="loading-step__icon">{stepIcon(status)}</span>
                <span>{stepLabels[key] || key}</span>
              </li>
            ))}
          </ul>
        </LoadingOverlay>
      </div>
    );
  }

  if (snapshotErr) {
    return (
      <div className="task-progress-page">
        <section className="task-progress-header card">
          <h2>xHF for Rita 26タスク管理ダッシュボード</h2>
          <p style={{ color: '#ef4444' }}>{snapshotErr}</p>
        </section>
      </div>
    );
  }

  return (
    <div className="task-progress-page">
      <section className="task-progress-header card">
        <div>
          <h2>xHF for Rita 26タスク管理ダッシュボード</h2>
        </div>
        <div className="task-progress-header-tools">
          <div className="toolbar-group toolbar-group--search" ref={searchBoxRef}>
            <input
              value={query}
              onChange={(e) => {
                setQuery(e.target.value);
                setSearchOpen(true);
                setSearchActiveIndex(-1);
              }}
              onFocus={() => setSearchOpen(true)}
              onKeyDown={handleSearchKeyDown}
              placeholder="農場名 / ユーザー名で検索"
              aria-label="農場名またはユーザー名で検索"
              aria-expanded={searchOpen}
              aria-autocomplete="list"
              role="combobox"
            />
            {searchOpen && searchOptionRows.length > 0 && (
              <div className="toolbar-search-dropdown" role="listbox">
                {searchOptionRows.map((opt, idx) => (
                  <button
                    type="button"
                    key={`${opt.type}:${opt.label}`}
                    className={`toolbar-search-option ${idx === searchActiveIndex ? 'active' : ''}`}
                    onMouseEnter={() => setSearchActiveIndex(idx)}
                    onClick={() => handleSelectSearchOption(opt.label)}
                  >
                    <span>{opt.label}</span>
                    <small>{opt.type}</small>
                  </button>
                ))}
              </div>
            )}
          </div>
          {availableDates.length > 0 && (
            <div className="toolbar-group toolbar-group--date">
              <label className="snapshot-date-label">
                基準日
                <input
                  className="snapshot-date-input"
                  type="date"
                  value={availableDates.includes(snapshotDate) ? snapshotDate : availableDates[0]}
                  min={availableDates[availableDates.length - 1]}
                  max={availableDates[0]}
                  onChange={(e) => {
                    const v = e.target.value;
                    if (!v) return;
                    if (!availableDates.includes(v)) return;
                    applySnapshotDate(v);
                  }}
                />
              </label>
            </div>
          )}
          <div className="toolbar-actions">
            <button type="button" onClick={handleRefresh}>再読込</button>
            {canManualUpdate && (
              <button type="button" onClick={handleManualUpdate} disabled={manualUpdateLoading}>
                {manualUpdateLoading ? (
                  <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6 }}>
                    <LoadingSpinner size={12} />
                    <span>最新スナップショット取得中...</span>
                  </span>
                ) : '最新スナップショット取得（全体）'}
              </button>
            )}
          </div>
          <div className="toolbar-meta">
            {tasksHydrating && <span>詳細タスク読込中...</span>}
            <span>{new Date(dashboard.as_of).toLocaleString('ja-JP')}</span>
            <span className="source-tag">snapshot: {snapshotRun?.run_id ?? '-'}</span>
          </div>
        </div>
        {manualUpdateMsg && <p className="manual-update-msg">{manualUpdateMsg}</p>}
        <TaskFamilyFilterPanel
          familyOptions={familyOptions}
          selectedFamily={selectedFamily}
          isFilterPending={isFilterPending}
          dashboardPending={workerPending}
          actionFilter={actionFilter}
          onSelectFamily={selectFamily}
          onClearActionFilter={clearActionFilter}
        />
      </section>

      <section className="task-progress-kpis">
        <article className={`card kpi-card kpi-green ${actionFilter === 'completed' ? 'kpi-active' : ''}`} onClick={() => startFilterTransition(() => setActionFilter('completed'))}>
          <h3>全体完了率</h3>
          <strong>{dashboard.kpi.completion_rate}%</strong>
          <p>{dashboard.kpi.completed_count} / {dashboard.kpi.due_count} タスク完了</p>
        </article>
        <article className={`card kpi-card kpi-red ${actionFilter === 'overdue' ? 'kpi-active' : ''}`} onClick={() => startFilterTransition(() => setActionFilter('overdue'))}>
          <h3>遅延中タスク</h3>
          <strong>{dashboard.kpi.overdue_count}</strong>
          <p>遅延率 {dashboard.kpi.delay_rate}%</p>
        </article>
        <article className={`card kpi-card kpi-yellow ${actionFilter === 'due_today' ? 'kpi-active' : ''}`} onClick={() => startFilterTransition(() => setActionFilter('due_today'))}>
          <h3>本日期限</h3>
          <strong>{dashboard.kpi.due_today_count}</strong>
          <p>要確認タスク</p>
        </article>
        <article className={`card kpi-card kpi-blue ${actionFilter === 'upcoming_3days' ? 'kpi-active' : ''}`} onClick={() => startFilterTransition(() => setActionFilter('upcoming_3days'))}>
          <h3>今後7日以内</h3>
          <strong>{dashboard.kpi.upcoming_3days_count}</strong>
          <p>7日以内の予定タスク</p>
        </article>
        <article className={`card kpi-card kpi-gray ${actionFilter === 'future' ? 'kpi-active' : ''}`} onClick={() => startFilterTransition(() => setActionFilter('future'))}>
          <h3>未来タスク合計</h3>
          <strong>{dashboard.kpi.future_count}</strong>
          <p>未着手タスク</p>
        </article>
        <article className="card kpi-card kpi-teal">
          <h3>農業者 / 圃場数</h3>
          <strong>{fieldStats.farmerCount} / {fieldStats.fieldCount}<small style={{ fontSize: '0.5em', fontWeight: 400, color: 'var(--text-muted)' }}> 圃場</small></strong>
          <p>合計面積 {fieldStats.totalAreaHa !== null ? `${fieldStats.totalAreaHa.toFixed(1)} ha` : '圃場ロード後表示'}</p>
        </article>
      </section>

      <section className="task-progress-subkpis">
        <article className="card subkpi-card subkpi-indigo">
          <h4>総圃場数差分（最新 - 基準日）</h4>
          <strong>
            {fieldCountDeltaCard.delta === null
              ? '-'
              : `${fieldCountDeltaCard.delta > 0 ? '+' : ''}${fieldCountDeltaCard.delta}`}
          </strong>
          <p>
            基準日 {snapshotDate} / 最新 {latestSnapshotDate || '-'}
          </p>
          <button type="button" onClick={handleOpenFieldDeltaModal}>
            差分圃場を表示
          </button>
        </article>
        <article className="card subkpi-card subkpi-emerald">
          <h4>タスクあり圃場数</h4>
          <strong>{fieldStats.fieldsWithTask}</strong>
          <p>登録済み圃場</p>
        </article>
        <article className="card subkpi-card subkpi-slate">
          <h4>タスクなし圃場数</h4>
          <strong>{fieldStats.fieldsWithoutTask}</strong>
          <p>未登録圃場</p>
          <button type="button" onClick={handleShowNoTaskFields}>
            タスクなし圃場を表示
          </button>
        </article>
      </section>

      <DashboardChartsPanel
        dashboard={{ ...dashboard, task_types: chartTaskTypes }}
        bubbleRows={bubbleRows}
        sortedFarmers={rankingFarmers}
        selectedFarmer={selectedRankingFarmer}
        selectedDetail={selectedDetail}
        linkedTasks={linkedTasks}
        selectedDelayBucket={selectedDelayBucket}
        onSelectFarmer={selectFarmer}
        onSelectDelayBucket={selectDelayBucket}
        onSort={handleSort}
      />

      <section className="card ranking-card" ref={snapshotFieldTableRef}>
        <h3>圃場スナップショット一覧（表示 {visibleFields.length} / 全 {sortedFilteredSnapshotFields.length}件）</h3>
        <p className="manual-update-msg">画面は軽量表示。詳細確認はCSV出力を推奨します。</p>
        <div className="snapshot-table-controls">
          <button type="button" onClick={loadSnapshotFields} disabled={snapshotFieldsLoaded || snapshotFieldsLoading}>
            {snapshotFieldsLoading ? '圃場一覧を読込中...' : snapshotFieldsLoaded ? '圃場一覧ロード済み' : '圃場一覧を読み込む'}
          </button>
          <button type="button" onClick={handleDownloadFieldsCsv} disabled={!snapshotFieldsLoaded || sortedFilteredSnapshotFields.length === 0}>
            圃場CSVダウンロード
          </button>
          <button type="button" onClick={() => setShowNoTaskFieldsOnly((v) => !v)} disabled={!snapshotFieldsLoaded}>
            {showNoTaskFieldsOnly ? 'タスクなし圃場フィルター解除' : 'タスクなし圃場のみ'}
          </button>
        </div>
        <div className="snapshot-table-controls">
          <input
            className="snapshot-table-filter-input"
            value={fieldTableQuery}
            onChange={(e) => setFieldTableQuery(e.target.value)}
            placeholder="圃場テーブル絞り込み（農場名/圃場名/担当者/UUID）"
            aria-label="圃場テーブル絞り込み"
          />
        </div>
        <div className="snapshot-table-controls">
          <button
            type="button"
            onClick={() => setVisibleFieldRows((v) => Math.min(sortedFilteredSnapshotFields.length, v + FIELD_ROWS_STEP))}
            disabled={visibleFields.length >= sortedFilteredSnapshotFields.length}
          >
            さらに{FIELD_ROWS_STEP}件表示
          </button>
          <button
            type="button"
            onClick={() => setVisibleFieldRows(sortedFilteredSnapshotFields.length)}
            disabled={visibleFields.length >= sortedFilteredSnapshotFields.length}
          >
            すべて表示
          </button>
        </div>
        {!snapshotFieldsLoaded ? (
          <p className="manual-update-msg">圃場テーブルは未ロードです。必要な時だけ「圃場一覧を読み込む」を押してください。</p>
        ) : (
          <div className="table-wrap table-wrap-wide">
          <table>
            <thead>
              <tr>
                <th><button type="button" onClick={() => handleFieldTableSort('snapshot_date')}>snapshot_date</button></th>
                <th>run_id</th>
                <th><button type="button" onClick={() => handleFieldTableSort('farm_name')}>farm_name</button></th>
                <th>farm_uuid</th>
                <th><button type="button" onClick={() => handleFieldTableSort('field_name')}>field_name</button></th>
                <th>field_uuid</th>
                <th>season_uuid</th>
                <th><button type="button" onClick={() => handleFieldTableSort('user_name')}>user_name</button></th>
                <th>crop_name</th>
                <th>variety_name</th>
                <th><button type="button" onClick={() => handleFieldTableSort('area_m2')}>area_ha</button></th>
                <th><button type="button" onClick={() => handleFieldTableSort('bbch_index')}>bbch_index</button></th>
                <th>bbch_scale</th>
                <th><button type="button" onClick={() => handleFieldTableSort('fetched_at')}>fetched_at</button></th>
              </tr>
            </thead>
            <tbody>
              {visibleFields.map((row, idx) => (
                <tr key={`${row.snapshot_date}-${row.field_uuid}-${row.season_uuid}-${idx}`}>
                  <td>{row.snapshot_date || '-'}</td>
                  <td>{row.run_id || '-'}</td>
                  <td>{row.farm_name || '-'}</td>
                  <td>{row.farm_uuid || '-'}</td>
                  <td>{row.field_name || '-'}</td>
                  <td>{row.field_uuid || '-'}</td>
                  <td>{row.season_uuid || '-'}</td>
                  <td>{row.user_name || '-'}</td>
                  <td>{row.crop_name || '-'}</td>
                  <td>{row.variety_name || '-'}</td>
                  <td>{formatAreaHa(row.area_m2)}</td>
                  <td>{row.bbch_index || '-'}</td>
                  <td>{row.bbch_scale || '-'}</td>
                  <td>{formatDateTime(row.fetched_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          </div>
        )}
      </section>

      <section className="card ranking-card">
        <h3>タスクスナップショット一覧（表示 {visibleTasks.length} / 全 {filteredSnapshotTaskCount}件）</h3>
        <p className="manual-update-msg">画面は軽量表示。大量データはCSV出力で確認してください。</p>
        <div className="snapshot-table-controls">
          <input
            className="snapshot-table-filter-input"
            value={taskTableQuery}
            onChange={(e) => setTaskTableQuery(e.target.value)}
            placeholder="タスクテーブル絞り込み（農場/圃場/タスク/担当者/薬剤/UUID）"
            aria-label="タスクテーブル絞り込み"
          />
          <select value={taskTypeFilter} onChange={(e) => setTaskTypeFilter(e.target.value)} aria-label="タスク種別絞り込み">
            <option value="all">タスク種別: すべて</option>
            {taskTypeOptions.map((v) => (
              <option key={v} value={v}>{v}</option>
            ))}
          </select>
          <select value={taskStatusFilter} onChange={(e) => setTaskStatusFilter(e.target.value)} aria-label="ステータス絞り込み">
            <option value="all">ステータス: すべて</option>
            {taskStatusOptions.map((v) => (
              <option key={v} value={v}>{v}</option>
            ))}
          </select>
        </div>
        <div className="snapshot-table-controls">
          <button type="button" onClick={handleDownloadTasksCsv} disabled={filteredSnapshotTaskCount === 0}>
            タスクCSVダウンロード
          </button>
          <button
            type="button"
            onClick={() => setVisibleTaskRows((v) => Math.min(filteredSnapshotTaskCount, v + TASK_ROWS_STEP))}
            disabled={visibleTasks.length >= filteredSnapshotTaskCount}
          >
            さらに{TASK_ROWS_STEP}件表示
          </button>
          <button
            type="button"
            onClick={() => setVisibleTaskRows(filteredSnapshotTaskCount)}
            disabled={visibleTasks.length >= filteredSnapshotTaskCount}
          >
            すべて表示
          </button>
        </div>
        {!snapshotTasksLoaded && (
          <p className="manual-update-msg">タスク一覧を自動取得しています...</p>
        )}
        <div className="table-wrap table-wrap-wide">
          <table>
            <thead>
              <tr>
                <th><button type="button" onClick={() => handleTaskTableSort('snapshot_date')}>snapshot_date</button></th>
                <th>run_id</th>
                <th><button type="button" onClick={() => handleTaskTableSort('farm_name')}>farm_name</button></th>
                <th>farm_uuid</th>
                <th><button type="button" onClick={() => handleTaskTableSort('field_name')}>field_name</button></th>
                <th>field_uuid</th>
                <th>season_uuid</th>
                <th>crop_uuid</th>
                <th><button type="button" onClick={() => handleTaskTableSort('task_display')}>task_display</button></th>
                <th><button type="button" onClick={() => handleTaskTableSort('task_name')}>task_name</button></th>
                <th><button type="button" onClick={() => handleTaskTableSort('task_type')}>task_type</button></th>
                <th><button type="button" onClick={() => handleTaskTableSort('occurrence')}>occurrence</button></th>
                <th>task_uuid</th>
                <th><button type="button" onClick={() => handleTaskTableSort('task_date')}>task_date</button></th>
                <th><button type="button" onClick={() => handleTaskTableSort('planned_date')}>planned_date</button></th>
                <th><button type="button" onClick={() => handleTaskTableSort('execution_date')}>execution_date</button></th>
                <th><button type="button" onClick={() => handleTaskTableSort('status')}>status</button></th>
                <th><button type="button" onClick={() => handleTaskTableSort('user_name')}>user_name</button></th>
                <th>assignee_name</th>
                <th><button type="button" onClick={() => handleTaskTableSort('product')}>product</button></th>
                <th>dosage</th>
                <th>spray_category</th>
                <th><button type="button" onClick={() => handleTaskTableSort('spray_subtype')}>spray_subtype</button></th>
                <th>creation_flow_hint</th>
                <th>bbch_index</th>
                <th>bbch_scale</th>
                <th><button type="button" onClick={() => handleTaskTableSort('fetched_at')}>fetched_at</button></th>
              </tr>
            </thead>
            <tbody>
              {visibleTasks.map((row, idx) => {
                const meta = row as SnapshotTask & { _family?: string; _subtypeLabel?: string; _taskDisplay?: string };
                const occurrence = Number(row.occurrence || 1);
                return (
                  <tr key={`${row.snapshot_date}-${row.task_uuid}-${idx}`}>
                    <td>{row.snapshot_date || '-'}</td>
                    <td>{row.run_id || '-'}</td>
                    <td>{row.farm_name || '-'}</td>
                    <td>{row.farm_uuid || '-'}</td>
                    <td>{row.field_name || '-'}</td>
                    <td>{row.field_uuid || '-'}</td>
                    <td>{row.season_uuid || '-'}</td>
                    <td>{row.crop_uuid || '-'}</td>
                    <td>{meta._taskDisplay || '-'}</td>
                    <td>{row.task_name || '-'}</td>
                    <td>{row.task_type || '-'}</td>
                    <td>{Number.isFinite(occurrence) ? occurrence : '-'}</td>
                    <td>{row.task_uuid || '-'}</td>
                    <td>{formatDate(row.task_date)}</td>
                    <td>{formatDateTime(row.planned_date)}</td>
                    <td>{formatDateTime(row.execution_date)}</td>
                    <td>{row.status || '-'}</td>
                    <td>{row.user_name || '-'}</td>
                    <td>{row.assignee_name || '-'}</td>
                    <td>{row.product || '-'}</td>
                    <td>{row.dosage || '-'}</td>
                    <td>{row.spray_category || '-'}</td>
                    <td>{meta._subtypeLabel || '-'}</td>
                    <td>{row.creation_flow_hint || '-'}</td>
                    <td>{row.bbch_index || '-'}</td>
                    <td>{row.bbch_scale || '-'}</td>
                    <td>{formatDateTime(row.fetched_at)}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </section>

      {fieldDeltaModalOpen && (
        <div className="task-progress-modal-backdrop" onClick={() => setFieldDeltaModalOpen(false)}>
          <div className="task-progress-modal" onClick={(e) => e.stopPropagation()}>
            <div className="task-progress-modal-header">
              <h3>総圃場数の差分圃場一覧</h3>
              <div className="task-progress-modal-actions">
                <button type="button" onClick={handleDownloadFieldDeltaCsv} disabled={fieldDeltaLoading || fieldDeltaRows.length === 0}>
                  CSVダウンロード
                </button>
                <button type="button" onClick={() => setFieldDeltaModalOpen(false)}>閉じる</button>
              </div>
            </div>
            <p className="manual-update-msg">
              基準日 {snapshotDate} と 最新 {fieldDeltaComparedDate || latestSnapshotDate || '-'} の比較
            </p>
            {fieldDeltaErr && <p className="manual-update-msg" style={{ color: '#ef4444' }}>{fieldDeltaErr}</p>}
            {fieldDeltaLoading ? (
              <div className="task-progress-modal-loading">
                <LoadingSpinner size={14} />
                <p className="manual-update-msg">差分圃場を読み込み中...</p>
              </div>
            ) : (
              <div className="table-wrap task-progress-modal-table">
                <table>
                  <thead>
                    <tr>
                      <th>差分</th>
                      <th>field_uuid</th>
                      <th>name</th>
                      <th>面積 (ha)</th>
                    </tr>
                  </thead>
                  <tbody>
                    {fieldDeltaRows.length === 0 ? (
                      <tr>
                        <td colSpan={4}>差分はありません</td>
                      </tr>
                    ) : (
                      fieldDeltaRows.map((row) => (
                        <tr key={`${row.diff_type}-${row.field_uuid}`}>
                          <td>{row.diff_type}</td>
                          <td>{row.field_uuid}</td>
                          <td>{row.field_name || '-'}</td>
                          <td>{formatAreaHa(row.area_m2)}</td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
