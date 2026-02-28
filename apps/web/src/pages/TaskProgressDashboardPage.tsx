import { memo, useCallback, useEffect, useMemo, useRef, useState, useTransition } from 'react';
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { useAuth } from '../context/AuthContext';
import { useFarms } from '../context/FarmContext';
import { withApiBase } from '../utils/apiBase';
import LoadingOverlay from '../components/LoadingOverlay';
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

type ActionFilterKey = 'none' | 'overdue' | 'due_today' | 'upcoming_3days' | 'incomplete' | 'future';

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

type FarmerRow = {
  id: string;
  name: string;
  field_count: number;
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
  farmer_details: Record<string, FarmerDetail>;
  as_of: string;
};

function emptyDashboardBundle(asOf: string): DashboardBundle {
  return {
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
    farmer_details: {},
    as_of: asOf,
  };
}

const TYPE_FAMILY_ORDER: string[] = [
  '防除タスク（除草剤）',
  '防除タスク（殺菌剤）',
  '防除タスク（殺虫剤）',
  '防除タスク（その他）',
  '防除タスク',
  '雑草管理タスク',
  '施肥タスク',
  '播種',
  '生育調査',
  '収穫',
  '水管理',
  '土づくり',
  '種子処理',
  '育苗箱処理',
];
const ALL_FAMILY_OPTION = '全部';
const PROTECTION_FILTER_OPTIONS = [
  '防除タスク',
  '防除タスク（除草剤）',
  '防除タスク（殺菌剤）',
  '防除タスク（殺虫剤）',
  '防除タスク（その他）',
] as const;
const SNAPSHOT_LOAD_ESTIMATE_SEC = 20;
const COUNTRY_UUID_JP = '0f59ff55-c86b-4b7b-4eaa-eb003d47dcd3';
const RICE_CROP_UUID = 'e54c5e22-94a0-a5ff-34a6-4fe0f8ad1ccc';
const SNAPSHOT_CLIENT_CACHE_TTL_MS = 2 * 60 * 1000;
const SNAPSHOT_TASK_LIMIT = 50000;
const SNAPSHOT_FIELD_LIMIT = 50000;
const INITIAL_FIELD_ROWS = 50;
const INITIAL_TASK_ROWS = 50;
const FIELD_ROWS_STEP = 50;
const TASK_ROWS_STEP = 50;

const snapshotPageCache = new Map<string, { expiresAt: number; data: any }>();
const snapshotDatesCache = new Map<string, { expiresAt: number; data: any }>();
const jstDayKeyCache = new Map<string, string>();
const sprayMasterCache = new Map<string, { expiresAt: number; data: Record<string, string> }>();
const SNAPSHOT_SS_PREFIX = 'hfr:ss:v1';

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
  Harvest: '収穫',
  Spraying: '防除タスク',
  WaterManagement: '水管理',
  Scouting: '生育調査',
  CropEstablishment: '播種',
  LandPreparation: '土づくり',
  SeedTreatment: '種子処理',
  SeedBoxTreatment: '育苗箱処理',
};

function matchesActionFilter(task: DashboardTask, filter: ActionFilterKey, today: string): boolean {
  if (filter === 'none') return true;
  const planned = getScheduledDay(task);
  const done = isCompleted(task);
  const in7days = addDaysToKey(today, 7);
  if (filter === 'incomplete') return !done;
  if (filter === 'overdue') return Boolean(planned && planned < today && !done);
  if (filter === 'due_today') return Boolean(planned && planned === today && !done);
  if (filter === 'upcoming_3days') return Boolean(planned && planned > today && planned <= in7days && !done);
  if (filter === 'future') return Boolean(planned && planned > today && !done);
  return true;
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
    return '防除タスク';
  }
  const byName = (task.task_name || '').trim();
  if (byName) return byName;
  return TYPE_FAMILY_BY_TASK_TYPE[task.task_type] || task.task_type || 'その他';
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

function isProtectionFamily(family: string): boolean {
  return family === '防除タスク' || family.startsWith('防除タスク（');
}

function matchesFamilySelection(task: DashboardTask, selectedFamily: string): boolean {
  if (selectedFamily === ALL_FAMILY_OPTION) return true;
  if (selectedFamily === '防除タスク') {
    return isProtectionFamily(task.typeFamily);
  }
  return task.typeFamily === selectedFamily;
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

  const protectionTasks = mapped
    .filter((row) => isProtectionFamily(row.typeFamily))
    .sort((a, b) => {
      const ka = `${a.farmUuid}|${a.fieldUuid}|${a.seasonKey}|${a.scheduledDay}|${a.taskName}|${a.uuid}`;
      const kb = `${b.farmUuid}|${b.fieldUuid}|${b.seasonKey}|${b.scheduledDay}|${b.taskName}|${b.uuid}`;
      return ka.localeCompare(kb, 'ja');
    });
  const seqByKey = new Map<string, number>();
  for (const row of protectionTasks) {
    const key = `${row.fieldUuid}|${row.seasonKey}|防除タスク`;
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

function statusClass(rate: number): 'good' | 'warn' | 'bad' {
  if (rate < 15) return 'good';
  if (rate < 30) return 'warn';
  return 'bad';
}

function rate(numerator: number, denominator: number): number {
  if (denominator <= 0) return 0;
  return Number(((numerator / denominator) * 100).toFixed(1));
}

function isCompleted(task: DashboardTask): boolean {
  return task.completed;
}


function getTaskTypeLabel(task: DashboardTask): string {
  return `${task.typeFamily} ${task.occurrence}回目`;
}

function getTypeDisplayOrder(taskTypeLabel: string): number {
  const match = taskTypeLabel.match(/^(.*)\s(\d+)回目$/);
  if (!match) return 99999;
  const family = match[1];
  const occurrence = Number(match[2]);
  const idx = TYPE_FAMILY_ORDER.indexOf(family);
  const base = idx >= 0 ? idx : 99;
  return base * 100 + (Number.isFinite(occurrence) ? occurrence : 99);
}

function buildDashboardBundle(
  tasksAll: DashboardTask[],
  selectedFamilies: Set<string>,
  referenceDay: string,
  asOfValue?: string,
): DashboardBundle {
  void selectedFamilies;
  const tasks = tasksAll;
  const asOf = asOfValue || new Date().toISOString();
  const today = referenceDay || getJstDayKey(new Date());
  const in7days = addDaysToKey(today, 7);

  const farmersMap = new Map<string, { name: string; fieldSet: Set<string>; tasks: DashboardTask[] }>();
  tasks.forEach((task) => {
    const farmerId = task.farmUuid || `name:${task.farmName}`;
    const prev = farmersMap.get(farmerId) ?? { name: task.farmName, fieldSet: new Set<string>(), tasks: [] };
    prev.name = task.farmName || prev.name;
    if (task.fieldUuid) prev.fieldSet.add(task.fieldUuid);
    prev.tasks.push(task);
    farmersMap.set(farmerId, prev);
  });

  const calcCounts = (rows: DashboardTask[]) => {
    let due = 0;
    let completed = 0;
    let overdue = 0;
    let dueToday = 0;
    let upcoming3 = 0;
    let future = 0;
    let doneCount = 0;

    rows.forEach((task) => {
      const planned = getScheduledDay(task);
      const done = isCompleted(task);
      if (done) doneCount += 1;
      if (!planned) {
        // 日付未設定タスクは遅延判定はできないため、未完了分のみ「予定（未着手）」に寄せる。
        if (!done) future += 1;
        return;
      }

      if (planned <= today) {
        due += 1;
        if (done) completed += 1;
      }
      if (planned < today && !done) overdue += 1;
      if (planned === today && !done) dueToday += 1;
      if (planned > today && planned <= in7days && !done) upcoming3 += 1;
      if (planned > today && !done) future += 1;
    });

    // 到来済みがゼロだと KPI/グラフが全ゼロ化しやすいため、
    // 全件が未来予定の期間では「全タスク基準」で表示する。
    if (due === 0 && rows.length > 0) {
      due = rows.length;
      completed = doneCount;
      overdue = rows.reduce((acc, task) => {
        const planned = getScheduledDay(task);
        if (!planned) return acc;
        return planned < today && !isCompleted(task) ? acc + 1 : acc;
      }, 0);
      future = rows.reduce((acc, task) => {
        const planned = getScheduledDay(task);
        if (!planned) return !isCompleted(task) ? acc + 1 : acc;
        return planned > today && !isCompleted(task) ? acc + 1 : acc;
      }, 0);
    }

    return { due, completed, overdue, dueToday, upcoming3, future };
  };

  const makeTypeRows = (rows: DashboardTask[]) => {
    const typeMap = new Map<string, DashboardTask[]>();
    rows.forEach((task) => {
      const label = getTaskTypeLabel(task);
      if (!typeMap.has(label)) typeMap.set(label, []);
      typeMap.get(label)?.push(task);
    });

    return Array.from(typeMap.entries())
      .sort((a, b) => getTypeDisplayOrder(a[0]) - getTypeDisplayOrder(b[0]))
      .map(([name, list], idx) => {
        const c = calcCounts(list);
        return {
          name,
          display_order: idx + 1,
          due_count: c.due,
          completed_count: c.completed,
          overdue_count: c.overdue,
          pending_count: c.future,
          completion_rate: rate(c.completed, c.due),
          delay_rate: rate(c.overdue, c.due),
        };
      });
  };

  const farmer_details: Record<string, FarmerDetail> = {};
  const farmers: FarmerRow[] = [];

  Array.from(farmersMap.entries()).forEach(([id, item], idx) => {
    const c = calcCounts(item.tasks);
    const delayRate = rate(c.overdue, c.due);
    const completionRate = rate(c.completed, c.due);

    const recent14From = addDaysToKey(today, -13);
    const prev14From = addDaysToKey(today, -27);
    const prev14To = addDaysToKey(today, -14);
    const recentOverdue = item.tasks.filter((task) => {
      const planned = getScheduledDay(task);
      return planned >= recent14From && planned <= today && planned < today && !isCompleted(task);
    }).length;
    const prevOverdue = item.tasks.filter((task) => {
      const planned = getScheduledDay(task);
      return planned >= prev14From && planned <= prev14To && planned < today && !isCompleted(task);
    }).length;
    const trend = recentOverdue - prevOverdue;
    const trend_direction: FarmerRow['trend_direction'] = trend > 2 ? 'worsening' : trend < -2 ? 'improving' : 'stable';

    farmers.push({
      id,
      name: item.name || `農業者${idx + 1}`,
      field_count: item.fieldSet.size,
      due_task_count: c.due,
      completed_count: c.completed,
      overdue_count: c.overdue,
      due_today_count: c.dueToday,
      upcoming_3days_count: c.upcoming3,
      future_task_count: c.future,
      delay_rate: delayRate,
      completion_rate: completionRate,
      delay_status: statusClass(delayRate),
      trend_direction,
    });

    farmer_details[id] = {
      id,
      name: item.name || `農業者${idx + 1}`,
      field_count: item.fieldSet.size,
      summary: {
        due: c.due,
        completed: c.completed,
        overdue: c.overdue,
        pending: c.future,
        delay_rate: delayRate,
        completion_rate: completionRate,
      },
      task_types: makeTypeRows(item.tasks),
    };
  });

  const allCounts = farmers.reduce(
    (acc, f) => {
      acc.due += f.due_task_count;
      acc.completed += f.completed_count;
      acc.overdue += f.overdue_count;
      acc.dueToday += f.due_today_count;
      acc.upcoming3 += f.upcoming_3days_count;
      acc.future += f.future_task_count;
      return acc;
    },
    { due: 0, completed: 0, overdue: 0, dueToday: 0, upcoming3: 0, future: 0 }
  );

  const kpi = {
    completion_rate: rate(allCounts.completed, allCounts.due),
    completed_count: allCounts.completed,
    due_count: allCounts.due,
    overdue_count: allCounts.overdue,
    delay_rate: rate(allCounts.overdue, allCounts.due),
    due_today_count: allCounts.dueToday,
    upcoming_3days_count: allCounts.upcoming3,
    future_count: allCounts.future,
    total_task_count: tasks.length,
    as_of: asOf,
  };

  const allTypeRows = makeTypeRows(tasks).map((row) => ({
    task_type_name: row.name,
    display_order: row.display_order,
    due_count: row.due_count,
    completed_count: row.completed_count,
    overdue_count: row.overdue_count,
    pending_count: row.pending_count,
    completion_rate: row.completion_rate,
    delay_rate: row.delay_rate,
  }));

  const buckets: Array<{ bucket: string; min: number; max: number; color: string }> = [
    { bucket: '0-5%', min: 0, max: 5, color: '#22c55e' },
    { bucket: '5-10%', min: 5, max: 10, color: '#22c55e' },
    { bucket: '10-15%', min: 10, max: 15, color: '#22c55e' },
    { bucket: '15-20%', min: 15, max: 20, color: '#f59e0b' },
    { bucket: '20-25%', min: 20, max: 25, color: '#f59e0b' },
    { bucket: '25-30%', min: 25, max: 30, color: '#f59e0b' },
    { bucket: '30%+', min: 30, max: 1000, color: '#ef4444' },
  ];

  const distribution = buckets.map((bucket) => ({
    bucket: bucket.bucket,
    count: farmers.filter((f) => f.delay_rate >= bucket.min && f.delay_rate < bucket.max).length,
    color: bucket.color,
  }));

  const trend: TrendPoint[] = Array.from({ length: 30 }).map((_, i) => {
    const day = addDaysToKey(today, -(29 - i));
    let due = 0;
    let completed = 0;
    let overdue = 0;

    tasks.forEach((task) => {
      const planned = getScheduledDay(task);
      if (!planned || planned > day) return;
      due += 1;
      const completedDay = task.executionDay;
      if (completedDay && completedDay <= day) {
        completed += 1;
      } else if (planned < day) {
        overdue += 1;
      }
    });

    if (due === 0 && tasks.length > 0) {
      due = tasks.length;
      completed = tasks.filter((task) => {
        if (!isCompleted(task)) return false;
        const completedDay = task.executionDay;
        if (!completedDay) return true;
        return completedDay <= day;
      }).length;
      overdue = tasks.filter((task) => {
        const planned = getScheduledDay(task);
        return Boolean(planned && planned < day && !isCompleted(task));
      }).length;
    }

    return {
      date: `${Number(day.slice(5, 7))}/${Number(day.slice(8, 10))}`,
      completion_rate: rate(completed, due),
      delay_rate: rate(overdue, due),
    };
  });

  return {
    kpi,
    farmers,
    task_types: allTypeRows,
    distribution,
    trend,
    farmer_details,
    as_of: asOf,
  };
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

function quickFilterLabel(filterKey: ActionFilterKey): string {
  if (filterKey === 'overdue') return '遅延中タスク';
  if (filterKey === 'due_today') return '本日期限タスク';
  if (filterKey === 'upcoming_3days') return '今後7日以内タスク';
  if (filterKey === 'incomplete') return '未完了タスク';
  if (filterKey === 'future') return '未来タスク';
  return '全タスク';
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
  return (
    <div className="task-family-filter">
      <span className="task-family-filter__label">表示タスク（単一選択）</span>
      <div className="task-family-filter__options">
        {options.map((family) => {
          const checked = selectedFamily === family;
          return (
            <label key={family} className={`task-family-chip ${checked ? 'checked' : ''}`}>
              <input
                type="radio"
                name="task-family-single"
                checked={checked}
                onChange={() => onSelectFamily(family)}
              />
              <span>{family}</span>
            </label>
          );
        })}
      </div>
      <span className="task-family-filter__label">
        KPI・グラフは選択中のタスクで集計しています（{selectedFamily || '-'}）
      </span>
      <span className="task-family-filter__label">
        KPI連動: {quickFilterLabel(actionFilter)}
        {(isFilterPending || dashboardPending) && '（反映中...）'}
        {actionFilter !== 'none' && (
          <>
            {' '}
            <button type="button" onClick={onClearActionFilter}>解除</button>
          </>
        )}
      </span>
      <span className="task-family-filter__label">
        到来済みタスクが0件の日は、全タスク基準に自動フォールバックして表示します。
      </span>
    </div>
  );
});

type DashboardChartsPanelProps = {
  dashboard: DashboardBundle;
  bubbleRows: Array<FarmerRow & { x: number; y: number; z: number }>;
  sortedFarmers: FarmerRow[];
  selectedFarmer: FarmerRow | null;
  selectedDetail: FarmerDetail | null;
  onSelectFarmer: (id: string | null) => void;
  onSort: (key: SortKey) => void;
};

const DashboardChartsPanel = memo(function DashboardChartsPanel({
  dashboard,
  bubbleRows,
  sortedFarmers,
  selectedFarmer,
  selectedDetail,
  onSelectFarmer,
  onSort,
}: DashboardChartsPanelProps) {
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
                <Tooltip cursor={{ strokeDasharray: '3 3' }} />
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
                      <th><button onClick={() => onSort('due_task_count')}>タスク</button></th>
                      <th><button onClick={() => onSort('overdue_count')}>遅延</button></th>
                      <th><button onClick={() => onSort('delay_rate')}>遅延率</button></th>
                      <th><button onClick={() => onSort('completion_rate')}>完了率</button></th>
                      <th>傾向</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedFarmers.map((f, idx) => {
                      const trend = trendLabel(f.trend_direction);
                      return (
                        <tr key={f.id} onClick={() => onSelectFarmer(f.id)}>
                          <td>{idx + 1}</td>
                          <td>
                            <span className={`status-dot ${f.delay_status}`} />
                            {f.name}
                          </td>
                          <td>{f.field_count}</td>
                          <td>{f.due_task_count}</td>
                          <td className="danger">{f.overdue_count}</td>
                          <td><span className={`badge ${f.delay_status}`}>{f.delay_rate}%</span></td>
                          <td>{f.completion_rate}%</td>
                          <td className={trend.className}>{trend.symbol} {trend.text}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </>
          )}

          {selectedFarmer && selectedDetail && (
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
            </div>
          )}
        </article>
      </section>

      <section className="task-progress-layer3">
        <article className="card chart-card">
          <h3>タスクタイプ別進捗（回数別）</h3>
          <div className="chart-wrap">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={dashboard.task_types} layout="vertical" margin={{ top: 10, right: 16, left: 20, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#203047" />
                <XAxis type="number" stroke="#94a3b8" />
                <YAxis dataKey="task_type_name" type="category" width={120} stroke="#94a3b8" />
                <Tooltip />
                <Bar dataKey="completed_count" stackId="a" fill="#22c55e" />
                <Bar dataKey="overdue_count" stackId="a" fill="#ef4444" />
                <Bar dataKey="pending_count" stackId="a" fill="rgba(255,255,255,0.2)" />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>

        <article className="card chart-card">
          <h3>遅延率分布</h3>
          <div className="chart-wrap">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={dashboard.distribution} margin={{ top: 10, right: 16, left: 8, bottom: 8 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#203047" />
                <XAxis dataKey="bucket" stroke="#94a3b8" />
                <YAxis stroke="#94a3b8" />
                <Tooltip />
                <Bar dataKey="count">
                  {dashboard.distribution.map((row) => (
                    <Cell key={row.bucket} fill={row.color} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </article>
      </section>

      <section className="task-progress-layer4 card chart-card">
        <h3>遅延率トレンド（過去30日）</h3>
        <div className="chart-wrap trend">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={dashboard.trend} margin={{ top: 10, right: 16, left: 8, bottom: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#203047" />
              <XAxis dataKey="date" stroke="#94a3b8" interval={4} />
              <YAxis stroke="#94a3b8" domain={[0, 100]} unit="%" />
              <Tooltip />
              <Line dataKey="delay_rate" stroke="#ef4444" strokeWidth={2.5} dot={false} name="遅延率" />
              <Line dataKey="completion_rate" stroke="#22c55e" strokeWidth={2.5} dot={false} name="完了率" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </section>
    </>
  );
});

export function TaskProgressDashboardPage() {
  const { auth } = useAuth();
  const { submittedFarms } = useFarms();
  const [query, setQuery] = useState('');
  const [selectedFarmerId, setSelectedFarmerId] = useState<string | null>(null);
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
  const [tasksHydrating, setTasksHydrating] = useState(false);
  const [visibleFieldRows, setVisibleFieldRows] = useState(INITIAL_FIELD_ROWS);
  const [visibleTaskRows, setVisibleTaskRows] = useState(INITIAL_TASK_ROWS);
  const [refreshToken, setRefreshToken] = useState(0);
  const [manualUpdateLoading, setManualUpdateLoading] = useState(false);
  const [manualUpdateMsg, setManualUpdateMsg] = useState<string | null>(null);
  const [snapshotLoadingElapsedSec, setSnapshotLoadingElapsedSec] = useState(0);
  const [sprayCategoryMap, setSprayCategoryMap] = useState<Record<string, string>>({});
  const [summaryReady, setSummaryReady] = useState(false);
  const [isFilterPending, startFilterTransition] = useTransition();
  const [dashboardPending, setDashboardPending] = useState(false);
  const [dashboardState, setDashboardState] = useState<DashboardBundle>(emptyDashboardBundle(getJstDayKey(new Date())));
  const didPickInitialSnapshotDate = useRef(false);

  const applySnapshotDate = useCallback((nextDate: string) => {
    const normalized = String(nextDate || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(normalized)) return;
    setSnapshotDate((prev) => (prev === normalized ? prev : normalized));
  }, []);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    const load = async () => {
      setSnapshotLoading(true);
      setTasksHydrating(false);
      setSnapshotErr(null);
      setSummaryReady(false);
      try {
        const now = Date.now();
        const metaKey = `${snapshotDate}:meta`;
        const summaryKey = `${snapshotDate}:summary`;
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

        if (useCachedMeta && useCachedSummary) {
          const cachedAsOf = (activeSummary?.data?.as_of || snapshotDate) as string;
          setSnapshotRun((activeMeta?.data?.run ?? null) as SnapshotRun | null);
          setSnapshotFields([]);
          setSnapshotFieldsLoaded(false);
          setDashboardState({
            kpi: activeSummary?.data?.kpi ?? emptyDashboardBundle(snapshotDate).kpi,
            farmers: activeSummary?.data?.farmers ?? [],
            task_types: activeSummary?.data?.task_types ?? [],
            distribution: activeSummary?.data?.distribution ?? [],
            trend: activeSummary?.data?.trend ?? [],
            farmer_details: activeSummary?.data?.farmer_details ?? {},
            as_of: cachedAsOf,
          });
          if (useCachedSnap) {
            setSnapshotTasks((activeSnap?.data?.tasks ?? []) as SnapshotTask[]);
          } else {
            setSnapshotTasks([]);
          }
          setSummaryReady(true);
          setSnapshotLoading(false);
        }

        let metaJson: any;
        let summaryJson: any = null;
        let dates: string[] = [];

        if (useCachedMeta) metaJson = activeMeta?.data;
        if (!useCachedMeta) {
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
        }

        const hasRun = Boolean(metaJson?.run?.run_id);
        if (useCachedSummary) {
          summaryJson = activeSummary?.data;
          setSummaryReady(true);
        } else if (!hasRun) {
          summaryJson = {
            kpi: emptyDashboardBundle(snapshotDate).kpi,
            farmers: [],
            task_types: [],
            distribution: [],
            trend: [],
            farmer_details: {},
            as_of: snapshotDate,
          };
          setSummaryReady(true);
        }

        if (useCachedDates) {
          const cached = activeDates?.data;
          dates = Array.isArray(cached?.dates) ? cached.dates.filter((d: any) => typeof d === 'string') : [];
        } else {
          // 日付一覧は表示に必須ではないため、バックグラウンドで更新する。
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
            } catch {
              // ignore
            }
          })();
        }

        if (!active) return;
        setSnapshotRun((metaJson?.run ?? null) as SnapshotRun | null);
        setSnapshotFields([]);
        setSnapshotFieldsLoaded(false);
        setSnapshotTasks([]);
        setDashboardState((prev) => ({
          kpi: summaryJson?.kpi ?? prev.kpi ?? emptyDashboardBundle(snapshotDate).kpi,
          farmers: summaryJson?.farmers ?? prev.farmers ?? [],
          task_types: summaryJson?.task_types ?? prev.task_types ?? [],
          distribution: summaryJson?.distribution ?? prev.distribution ?? [],
          trend: summaryJson?.trend ?? prev.trend ?? [],
          farmer_details: summaryJson?.farmer_details ?? prev.farmer_details ?? {},
          as_of: summaryJson?.as_of ?? prev.as_of ?? snapshotDate,
        }));
        setAvailableDates(dates);

        if (!useCachedSummary && hasRun) {
          void (async () => {
            try {
              const summaryRes = await fetch(
                withApiBase(`/hfr-snapshots/summary?snapshot_date=${encodeURIComponent(snapshotDate)}`),
                { signal: controller.signal },
              );
              const json = await summaryRes.json();
              if (!active || !summaryRes.ok || json?.ok === false) return;
              const expiresAt = Date.now() + SNAPSHOT_CLIENT_CACHE_TTL_MS;
              snapshotPageCache.set(summaryKey, { data: json, expiresAt });
              writeSessionCache(summaryKey, json, expiresAt);
              setDashboardState({
                kpi: json?.kpi ?? emptyDashboardBundle(snapshotDate).kpi,
                farmers: json?.farmers ?? [],
                task_types: json?.task_types ?? [],
                distribution: json?.distribution ?? [],
                trend: json?.trend ?? [],
                farmer_details: json?.farmer_details ?? {},
                as_of: json?.as_of ?? snapshotDate,
              });
              setSummaryReady(true);
            } catch {
              // 背景ロード失敗は画面全体エラーにしない
            }
          })();
        }

        if (useCachedSnap) {
          setSnapshotTasks((activeSnap?.data?.tasks ?? []) as SnapshotTask[]);
        } else {
          setTasksHydrating(true);
          void (async () => {
            try {
              const taskRes = await fetch(
                withApiBase(
                  `/hfr-snapshots?snapshot_date=${encodeURIComponent(snapshotDate)}`
                  + `&include_fields=false&include_tasks=true&task_limit=${SNAPSHOT_TASK_LIMIT}&limit=${SNAPSHOT_TASK_LIMIT}`,
                ),
                { signal: controller.signal },
              );
              const taskJson = await taskRes.json();
              if (!active || !taskRes.ok || taskJson?.ok === false) return;
              snapshotPageCache.set(snapKey, {
                data: taskJson,
                expiresAt: Date.now() + SNAPSHOT_CLIENT_CACHE_TTL_MS,
              });
              writeSessionCache(snapKey, taskJson, Date.now() + SNAPSHOT_CLIENT_CACHE_TTL_MS);
              setSnapshotTasks((taskJson.tasks ?? []) as SnapshotTask[]);
            } catch {
              // 背景ロード失敗は画面全体エラーにしない
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
  }, [snapshotDate, snapshotFields.length, snapshotTasks.length]);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();
    const loadSprayMaster = async () => {
      const sprayTasks = snapshotTasks.filter((t) => t.task_type === 'Spraying');
      if (sprayTasks.length === 0) {
        setSprayCategoryMap({});
        return;
      }
      const cacheKey = `${COUNTRY_UUID_JP}:${RICE_CROP_UUID}:FIELDTREATMENT`;
      const now = Date.now();
      const cached = sprayMasterCache.get(cacheKey);
      if (cached && cached.expiresAt > now) {
        setSprayCategoryMap(cached.data);
        return;
      }
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
        setSprayCategoryMap(next);
      } catch {
        if (!active) return;
        setSprayCategoryMap({});
      }
    };
    void loadSprayMaster();
    return () => {
      active = false;
      controller.abort();
    };
  }, [auth?.login?.login_token, auth?.api_token, snapshotTasks]);

  useEffect(() => {
    if (!snapshotLoading) {
      setSnapshotLoadingElapsedSec(0);
      return;
    }
    const startedAt = Date.now();
    const timer = window.setInterval(() => {
      const elapsed = Math.floor((Date.now() - startedAt) / 1000);
      setSnapshotLoadingElapsedSec(elapsed);
    }, 500);
    return () => window.clearInterval(timer);
  }, [snapshotLoading]);

  const allTasks = useMemo(() => tasksFromSnapshot(snapshotTasks, sprayCategoryMap), [snapshotTasks, sprayCategoryMap]);
  const tasksByFamily = useMemo(() => {
    const map = new Map<string, DashboardTask[]>();
    for (const task of allTasks) {
      if (!map.has(task.typeFamily)) map.set(task.typeFamily, []);
      map.get(task.typeFamily)?.push(task);
    }
    return map;
  }, [allTasks]);
  const familyOptions = useMemo(() => {
    const set = new Set<string>();
    allTasks.forEach((task) => set.add(task.typeFamily));
    const hasProtectionSub = Array.from(set).some((name) => name.startsWith('防除（'));
    if (hasProtectionSub) set.add('防除');
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
  }, [allTasks]);

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

  const linkedTasks = useMemo(() => {
    const today = getJstDayKey(new Date());
    const fromFamilies: DashboardTask[] = allTasks.filter((task) => matchesFamilySelection(task, selectedFamily));
    const filtered = fromFamilies.filter((task) => matchesActionFilter(task, actionFilter, today));
    if (filtered.length > 0) return filtered;
    // 選択値が古い/無効な場合だけ全タスクへフォールバックする。
    // ユーザーが有効な選択肢を選んで0件になった時は0件をそのまま表示する。
    const selectedIsValid =
      selectedFamily === ALL_FAMILY_OPTION ||
      selectedFamily === '防除タスク' ||
      familyOptions.includes(selectedFamily);
    if (actionFilter === 'none' && !selectedIsValid) {
      return allTasks.filter((task) => matchesActionFilter(task, actionFilter, today));
    }
    return filtered;
  }, [tasksByFamily, selectedFamily, actionFilter, allTasks, familyOptions]);

  useEffect(() => {
    if (snapshotTasks.length === 0) {
      setDashboardPending(false);
      return;
    }
    // 初期表示（全タスク・アクションフィルタなし）は API サマリーをそのまま使う。
    // ここで再集計しないことで、表示完了までの待ち時間を短縮する。
    if (summaryReady && selectedFamily === ALL_FAMILY_OPTION && actionFilter === 'none') {
      setDashboardPending(false);
      return;
    }
    let alive = true;
    setDashboardPending(true);
    const today = getJstDayKey(new Date());
    const asOf = snapshotRun?.finished_at || snapshotRun?.started_at || snapshotRun?.snapshot_date || snapshotDate;
    const compute = () => {
      if (!alive) return;
      const next = buildDashboardBundle(linkedTasks, new Set([selectedFamily]), today, asOf);
      if (!alive) return;
      setDashboardState(next);
      setDashboardPending(false);
    };
    const ric = (window as any).requestIdleCallback as ((cb: () => void, opts?: { timeout?: number }) => number) | undefined;
    const cic = (window as any).cancelIdleCallback as ((id: number) => void) | undefined;
    if (ric) {
      const id = ric(compute, { timeout: 250 });
      return () => {
        alive = false;
        if (cic) cic(id);
      };
    }
    const timer = window.setTimeout(compute, 0);
    return () => {
      alive = false;
      window.clearTimeout(timer);
    };
  }, [linkedTasks, selectedFamily, actionFilter, summaryReady, snapshotDate, snapshotRun, snapshotTasks.length]);
  const dashboard = dashboardState;

  const filteredFarmers = useMemo(() => {
    const q = query.trim();
    if (!q) return dashboard.farmers;
    return dashboard.farmers.filter((row) => row.name.includes(q));
  }, [dashboard.farmers, query]);

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

  const selectedFarmer = useMemo(
    () => dashboard.farmers.find((f) => f.id === selectedFarmerId) ?? null,
    [dashboard.farmers, selectedFarmerId]
  );

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

  const canManualUpdate =
    (auth?.email || '').trim().toLowerCase() === 'am@shonai.inc' &&
    submittedFarms.length > 0 &&
    Boolean(auth?.login?.login_token) &&
    Boolean(auth?.api_token);

  const filteredSnapshotTasks = useMemo(() => {
    const out: SnapshotTask[] = [];
    for (const t of linkedTasks) {
      const row = snapshotTaskByUuid.get(t.uuid);
      if (row) out.push(row);
    }
    return out;
  }, [linkedTasks, snapshotTaskByUuid]);
  const filteredSnapshotFields = useMemo(() => {
    if (!snapshotFieldsLoaded) return [] as SnapshotField[];
    const fieldUuidSet = new Set<string>();
    const out: SnapshotField[] = [];
    for (const t of linkedTasks) {
      const fuid = String(t.fieldUuid || '');
      if (!fuid || fieldUuidSet.has(fuid)) continue;
      fieldUuidSet.add(fuid);
      const rows = snapshotFieldsByFieldUuid.get(fuid);
      if (rows && rows.length > 0) out.push(...rows);
    }
    return out;
  }, [linkedTasks, snapshotFieldsByFieldUuid, snapshotFieldsLoaded]);
  const visibleFields = useMemo(
    () => filteredSnapshotFields.slice(0, Math.min(visibleFieldRows, filteredSnapshotFields.length)),
    [filteredSnapshotFields, visibleFieldRows],
  );
  const visibleTasks = useMemo(
    () => filteredSnapshotTasks.slice(0, Math.min(visibleTaskRows, filteredSnapshotTasks.length)),
    [filteredSnapshotTasks, visibleTaskRows],
  );

  const handleRefresh = () => {
    snapshotPageCache.clear();
    snapshotDatesCache.clear();
    clearSnapshotSessionCache();
    setRefreshToken((v) => v + 1);
  };

  const loadSnapshotFields = async () => {
    if (snapshotFieldsLoading) return;
    setSnapshotFieldsLoading(true);
    try {
      const key = `${snapshotDate}:fields:${SNAPSHOT_FIELD_LIMIT}`;
      const now = Date.now();
      const cached = snapshotPageCache.get(key);
      let json: any;
      if (cached && cached.expiresAt > now) {
        json = cached.data;
      } else {
        const res = await fetch(
          withApiBase(
            `/hfr-snapshots?snapshot_date=${encodeURIComponent(snapshotDate)}`
            + `&include_fields=true&include_tasks=false&field_limit=${SNAPSHOT_FIELD_LIMIT}&limit=${SNAPSHOT_FIELD_LIMIT}`,
          ),
        );
        json = await res.json();
        if (!res.ok || json?.ok === false) {
          const reason = json?.detail?.reason || json?.reason || `HTTP ${res.status}`;
          throw new Error(`圃場一覧取得失敗: ${reason}`);
        }
        snapshotPageCache.set(key, { data: json, expiresAt: now + SNAPSHOT_CLIENT_CACHE_TTL_MS });
      }
      setSnapshotFields((json?.fields ?? []) as SnapshotField[]);
      setSnapshotFieldsLoaded(true);
    } catch (err: any) {
      setManualUpdateMsg(err?.message || '圃場一覧の取得に失敗しました');
    } finally {
      setSnapshotFieldsLoading(false);
    }
  };

  const handleManualUpdate = async () => {
    if (!canManualUpdate || !auth?.login?.login_token || !auth?.api_token) return;
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
          farm_uuids: submittedFarms,
          dryRun: false,
          suffix: 'HFR',
          languageCode: 'ja',
        }),
      });
      const json = await res.json();
      if (!res.ok || json?.ok === false) {
        const reason = json?.detail?.reason || json?.reason || `HTTP ${res.status}`;
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

  useEffect(() => {
    setSelectedFarmerId(null);
    setActionFilter('none');
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

  const selectFamily = useCallback((family: string) => {
    startFilterTransition(() => {
      setSelectedFamily(family);
    });
  }, []);

  const clearActionFilter = useCallback(() => {
    startFilterTransition(() => setActionFilter('none'));
  }, []);

  const selectFarmer = useCallback((id: string | null) => {
    setSelectedFarmerId(id);
  }, []);

  if (snapshotLoading) {
    const remainingSec = Math.max(0, SNAPSHOT_LOAD_ESTIMATE_SEC - snapshotLoadingElapsedSec);
    const progress = Math.min(95, (snapshotLoadingElapsedSec / SNAPSHOT_LOAD_ESTIMATE_SEC) * 100);
    return (
      <div className="task-progress-page">
        <LoadingOverlay
          message="スナップショットを読み込んでいます..."
          progress={progress}
          details={[
            `基準日: ${getJstDayKey(new Date())}`,
            `経過: ${snapshotLoadingElapsedSec}秒`,
            `残り目安: ${remainingSec}秒`,
          ]}
        />
      </div>
    );
  }

  if (snapshotErr) {
    return (
      <div className="task-progress-page">
        <section className="task-progress-header card">
          <h2>圃場タスク管理ダッシュボード</h2>
          <p style={{ color: '#ef4444' }}>{snapshotErr}</p>
        </section>
      </div>
    );
  }

  return (
    <div className="task-progress-page">
      <section className="task-progress-header card">
        <div>
          <h2>圃場タスク管理ダッシュボード</h2>
          <p>遅延の早期発見にフォーカスした全体監視ビュー</p>
        </div>
        <div className="task-progress-header-tools">
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="農業者名で検索"
            aria-label="農業者名で検索"
          />
          {availableDates.length > 0 && (
            <label className="snapshot-date-label">
              保存日
              <select
                value={availableDates.includes(snapshotDate) ? snapshotDate : availableDates[0]}
                onChange={(e) => applySnapshotDate(e.target.value)}
              >
                {availableDates.map((d) => (
                  <option key={d} value={d}>{d}</option>
                ))}
              </select>
            </label>
          )}
          <button type="button" onClick={handleRefresh}>再読込</button>
          {tasksHydrating && <span>詳細タスク読込中...</span>}
          {canManualUpdate && (
            <button type="button" onClick={handleManualUpdate} disabled={manualUpdateLoading}>
              {manualUpdateLoading ? 'HFR更新中...' : 'HFR更新（選択農場）'}
            </button>
          )}
          <span>{new Date(dashboard.as_of).toLocaleString('ja-JP')}</span>
          <span className="source-tag">snapshot: {snapshotRun?.run_id ?? '-'}</span>
        </div>
        {manualUpdateMsg && <p className="manual-update-msg">{manualUpdateMsg}</p>}
        <TaskFamilyFilterPanel
          familyOptions={familyOptions}
          selectedFamily={selectedFamily}
          isFilterPending={isFilterPending}
          dashboardPending={dashboardPending}
          actionFilter={actionFilter}
          onSelectFamily={selectFamily}
          onClearActionFilter={clearActionFilter}
        />
      </section>

      <section className="task-progress-kpis">
        <article className={`card kpi-card kpi-green ${actionFilter === 'none' ? 'kpi-active' : ''}`} onClick={() => startFilterTransition(() => setActionFilter('none'))}>
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
      </section>

      <DashboardChartsPanel
        dashboard={dashboard}
        bubbleRows={bubbleRows}
        sortedFarmers={sortedFarmers}
        selectedFarmer={selectedFarmer}
        selectedDetail={selectedDetail}
        onSelectFarmer={selectFarmer}
        onSort={handleSort}
      />

      <section className="card ranking-card">
        <h3>圃場スナップショット一覧（表示 {visibleFields.length} / 全 {filteredSnapshotFields.length}件）</h3>
        <p className="manual-update-msg">画面は軽量表示。詳細確認はCSV出力を推奨します。</p>
        <div className="snapshot-table-controls">
          <button type="button" onClick={loadSnapshotFields} disabled={snapshotFieldsLoaded || snapshotFieldsLoading}>
            {snapshotFieldsLoading ? '圃場一覧を読込中...' : snapshotFieldsLoaded ? '圃場一覧ロード済み' : '圃場一覧を読み込む'}
          </button>
        </div>
        <div className="snapshot-table-controls">
          <button
            type="button"
            onClick={() => setVisibleFieldRows((v) => Math.min(filteredSnapshotFields.length, v + FIELD_ROWS_STEP))}
            disabled={visibleFields.length >= filteredSnapshotFields.length}
          >
            さらに{FIELD_ROWS_STEP}件表示
          </button>
          <button
            type="button"
            onClick={() => setVisibleFieldRows(filteredSnapshotFields.length)}
            disabled={visibleFields.length >= filteredSnapshotFields.length}
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
                <th>snapshot_date</th>
                <th>run_id</th>
                <th>farm_name</th>
                <th>farm_uuid</th>
                <th>field_name</th>
                <th>field_uuid</th>
                <th>season_uuid</th>
                <th>user_name</th>
                <th>crop_name</th>
                <th>variety_name</th>
                <th>area_m2</th>
                <th>bbch_index</th>
                <th>bbch_scale</th>
                <th>fetched_at</th>
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
                  <td>{typeof row.area_m2 === 'number' ? row.area_m2.toLocaleString('ja-JP') : '-'}</td>
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
        <h3>タスクスナップショット一覧（表示 {visibleTasks.length} / 全 {filteredSnapshotTasks.length}件）</h3>
        <p className="manual-update-msg">画面は軽量表示。大量データはCSV出力で確認してください。</p>
        <div className="snapshot-table-controls">
          <button
            type="button"
            onClick={() => setVisibleTaskRows((v) => Math.min(filteredSnapshotTasks.length, v + TASK_ROWS_STEP))}
            disabled={visibleTasks.length >= filteredSnapshotTasks.length}
          >
            さらに{TASK_ROWS_STEP}件表示
          </button>
          <button
            type="button"
            onClick={() => setVisibleTaskRows(filteredSnapshotTasks.length)}
            disabled={visibleTasks.length >= filteredSnapshotTasks.length}
          >
            すべて表示
          </button>
        </div>
        <div className="table-wrap table-wrap-wide">
          <table>
            <thead>
              <tr>
                <th>snapshot_date</th>
                <th>run_id</th>
                <th>farm_name</th>
                <th>farm_uuid</th>
                <th>field_name</th>
                <th>field_uuid</th>
                <th>season_uuid</th>
                <th>crop_uuid</th>
                <th>task_display</th>
                <th>task_name</th>
                <th>task_type</th>
                <th>occurrence</th>
                <th>task_uuid</th>
                <th>task_date</th>
                <th>planned_date</th>
                <th>execution_date</th>
                <th>status</th>
                <th>user_name</th>
                <th>assignee_name</th>
                <th>product</th>
                <th>dosage</th>
                <th>spray_category</th>
                <th>spray_subtype</th>
                <th>creation_flow_hint</th>
                <th>bbch_index</th>
                <th>bbch_scale</th>
                <th>fetched_at</th>
              </tr>
            </thead>
            <tbody>
              {visibleTasks.map((row, idx) => {
                const family = resolveTaskFamilyFromSnapshot(row, sprayCategoryMap);
                const subtypeInfo = detectSpraySubtype(row, sprayCategoryMap);
                const occurrence = Number(row.occurrence || 1);
                const taskDisplay = `${family} ${occurrence}回目`;
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
                    <td>{taskDisplay}</td>
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
                    <td>{subtypeInfo.subtype !== '-' ? `${subtypeInfo.subtype} (${subtypeInfo.source})` : '-'}</td>
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
    </div>
  );
}
