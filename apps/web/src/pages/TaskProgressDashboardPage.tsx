import { useEffect, useMemo, useState } from 'react';
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
import { withApiBase } from '../utils/apiBase';
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
  plannedDate: string | null;
  executionDate: string | null;
  state: string | null;
  occurrence: number;
};

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
  task_uuid: string;
  farm_uuid: string;
  farm_name: string;
  field_uuid: string;
  field_name: string;
  season_uuid: string;
  task_name: string;
  task_type: string;
  planned_date: string | null;
  execution_date: string | null;
  status: string | null;
  occurrence: number | null;
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

const TYPE_FAMILY_ORDER: string[] = ['播種', '防除', '施肥', '生育調査', '収穫', '水管理', '土づくり', '種子処理', '育苗箱処理'];
const DEFAULT_SELECTED_FAMILIES = new Set<string>(['防除', '播種']);

const TYPE_FAMILY_BY_TASK_TYPE: Record<string, string> = {
  Harvest: '収穫',
  Spraying: '防除',
  WaterManagement: '水管理',
  Scouting: '生育調査',
  CropEstablishment: '播種',
  LandPreparation: '土づくり',
  SeedTreatment: '種子処理',
  SeedBoxTreatment: '育苗箱処理',
};

function resolveTaskFamilyFromSnapshot(task: SnapshotTask): string {
  const byName = (task.task_name || '').trim();
  if (byName) return byName;
  return TYPE_FAMILY_BY_TASK_TYPE[task.task_type] || task.task_type || 'その他';
}

function tasksFromSnapshot(snapshotTasks: SnapshotTask[]): DashboardTask[] {
  return snapshotTasks.map((task, idx) => ({
    uuid: task.task_uuid || `task-${idx + 1}`,
    farmUuid: task.farm_uuid || '',
    farmName: task.farm_name || '不明農場',
    fieldUuid: task.field_uuid || '',
    fieldName: task.field_name || '不明圃場',
    seasonKey: task.season_uuid || 'field',
    typeFamily: resolveTaskFamilyFromSnapshot(task),
    plannedDate: task.planned_date,
    executionDate: task.execution_date,
    state: task.status,
    occurrence: Number(task.occurrence || 1),
  }));
}

function getJstDayKey(value: string | Date | null | undefined): string {
  if (!value) return '';
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
  return y && m && d ? `${y}-${m}-${d}` : '';
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
  if (task.executionDate) return true;
  const s = (task.state ?? '').toUpperCase();
  return s.includes('DONE') || s.includes('COMPLETED') || s.includes('EXECUTED');
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
  const tasks = tasksAll.filter((task) => selectedFamilies.has(task.typeFamily));
  const asOf = asOfValue || new Date().toISOString();
  const today = referenceDay || getJstDayKey(new Date());
  const in3days = addDaysToKey(today, 3);

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

    rows.forEach((task) => {
      const planned = getJstDayKey(task.plannedDate);
      if (!planned) return;
      const done = isCompleted(task);

      if (planned <= today) {
        due += 1;
        if (done) completed += 1;
      }
      if (planned < today && !done) overdue += 1;
      if (planned === today && !done) dueToday += 1;
      if (planned > today && planned <= in3days && !done) upcoming3 += 1;
      if (planned > today && !done) future += 1;
    });

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
      const planned = getJstDayKey(task.plannedDate);
      return planned >= recent14From && planned <= today && planned < today && !isCompleted(task);
    }).length;
    const prevOverdue = item.tasks.filter((task) => {
      const planned = getJstDayKey(task.plannedDate);
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
      const planned = getJstDayKey(task.plannedDate);
      if (!planned || planned > day) return;
      due += 1;
      const completedDay = getJstDayKey(task.executionDate);
      if (completedDay && completedDay <= day) {
        completed += 1;
      } else if (planned < day) {
        overdue += 1;
      }
    });

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

export function TaskProgressDashboardPage() {
  const [query, setQuery] = useState('');
  const [selectedFarmerId, setSelectedFarmerId] = useState<string | null>(null);
  const [sortKey, setSortKey] = useState<SortKey>('delay_rate');
  const [sortDir, setSortDir] = useState<SortDir>('desc');
  const [selectedFamilies, setSelectedFamilies] = useState<Set<string>>(new Set(DEFAULT_SELECTED_FAMILIES));
  const [snapshotDate, setSnapshotDate] = useState<string>(getJstDayKey(new Date()));
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [snapshotLoading, setSnapshotLoading] = useState(false);
  const [snapshotErr, setSnapshotErr] = useState<string | null>(null);
  const [snapshotRun, setSnapshotRun] = useState<SnapshotRun | null>(null);
  const [snapshotTasks, setSnapshotTasks] = useState<SnapshotTask[]>([]);
  const [refreshToken, setRefreshToken] = useState(0);

  useEffect(() => {
    let active = true;
    const controller = new AbortController();

    const load = async () => {
      setSnapshotLoading(true);
      setSnapshotErr(null);
      try {
        const [snapRes, datesRes] = await Promise.all([
          fetch(withApiBase(`/hfr-snapshots?snapshot_date=${encodeURIComponent(snapshotDate)}&limit=50000`), {
            signal: controller.signal,
            cache: 'no-store',
          }),
          fetch(withApiBase('/hfr-snapshots/dates?limit=365'), {
            signal: controller.signal,
            cache: 'no-store',
          }),
        ]);

        const snapJson = await snapRes.json();
        if (!snapRes.ok || snapJson?.ok === false) {
          const reason = snapJson?.detail?.reason || snapJson?.reason || `HTTP ${snapRes.status}`;
          throw new Error(`スナップショット取得失敗: ${reason}`);
        }

        let dates: string[] = [];
        if (datesRes.ok) {
          const datesJson = await datesRes.json();
          dates = Array.isArray(datesJson?.dates) ? datesJson.dates.filter((d: any) => typeof d === 'string') : [];
        }

        if (!active) return;
        setSnapshotRun((snapJson.run ?? null) as SnapshotRun | null);
        setSnapshotTasks((snapJson.tasks ?? []) as SnapshotTask[]);
        setAvailableDates(dates);
      } catch (error: any) {
        if (!active || error?.name === 'AbortError') return;
        setSnapshotErr(error?.message || 'スナップショットの取得に失敗しました');
        setSnapshotRun(null);
        setSnapshotTasks([]);
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

  const allTasks = useMemo(() => tasksFromSnapshot(snapshotTasks), [snapshotTasks]);
  const familyOptions = useMemo(() => {
    const set = new Set<string>();
    allTasks.forEach((task) => set.add(task.typeFamily));
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
    setSelectedFamilies((prev) => {
      const normalized = new Set(Array.from(prev).filter((family) => familyOptions.includes(family)));
      if (normalized.size > 0) return normalized;
      const defaults = Array.from(DEFAULT_SELECTED_FAMILIES).filter((family) => familyOptions.includes(family));
      if (defaults.length > 0) return new Set(defaults);
      return new Set([familyOptions[0]]);
    });
  }, [familyOptions]);

  const dashboard = useMemo(
    () =>
      buildDashboardBundle(
        allTasks,
        selectedFamilies,
        snapshotDate,
        snapshotRun?.finished_at || snapshotRun?.started_at || snapshotRun?.snapshot_date,
      ),
    [allTasks, selectedFamilies, snapshotDate, snapshotRun],
  );

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

  useEffect(() => {
    setSelectedFarmerId(null);
  }, [snapshotDate]);

  const handleSort = (nextKey: SortKey) => {
    if (sortKey === nextKey) {
      setSortDir((prev) => (prev === 'asc' ? 'desc' : 'asc'));
      return;
    }
    setSortKey(nextKey);
    setSortDir(nextKey === 'name' ? 'asc' : 'desc');
  };

  const toggleFamily = (family: string) => {
    setSelectedFamilies((prev) => {
      const next = new Set(prev);
      if (next.has(family)) {
        next.delete(family);
      } else {
        next.add(family);
      }
      return next;
    });
  };

  if (snapshotLoading) {
    return (
      <div className="task-progress-page">
        <section className="task-progress-header card">
          <h2>圃場タスク管理ダッシュボード</h2>
          <p>スナップショットを読み込んでいます...</p>
        </section>
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
          <label className="snapshot-date-label">
            基準日
            <input
              type="date"
              value={snapshotDate}
              onChange={(e) => setSnapshotDate(e.target.value)}
              max={getJstDayKey(new Date())}
            />
          </label>
          <button type="button" onClick={() => setRefreshToken((v) => v + 1)}>再読込</button>
          {availableDates.length > 0 && (
            <button type="button" onClick={() => setSnapshotDate(availableDates[0])}>最新日</button>
          )}
          <span>{new Date(dashboard.as_of).toLocaleString('ja-JP')}</span>
          <span className="source-tag">snapshot: {snapshotRun?.run_id ?? '-'}</span>
        </div>
        <div className="task-family-filter">
          <span className="task-family-filter__label">表示タスク（複数選択）</span>
          <div className="task-family-filter__options">
            {familyOptions.map((family) => {
              const checked = selectedFamilies.has(family);
              return (
                <label key={family} className={`task-family-chip ${checked ? 'checked' : ''}`}>
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={() => toggleFamily(family)}
                  />
                  <span>{family}</span>
                </label>
              );
            })}
          </div>
        </div>
      </section>

      <section className="task-progress-kpis">
        <article className="card kpi-card kpi-green">
          <h3>全体完了率</h3>
          <strong>{dashboard.kpi.completion_rate}%</strong>
          <p>{dashboard.kpi.completed_count} / {dashboard.kpi.due_count} タスク完了</p>
        </article>
        <article className="card kpi-card kpi-red">
          <h3>遅延中タスク</h3>
          <strong>{dashboard.kpi.overdue_count}</strong>
          <p>遅延率 {dashboard.kpi.delay_rate}%</p>
        </article>
        <article className="card kpi-card kpi-yellow">
          <h3>本日期限</h3>
          <strong>{dashboard.kpi.due_today_count}</strong>
          <p>要確認タスク</p>
        </article>
        <article className="card kpi-card kpi-blue">
          <h3>今後3日以内</h3>
          <strong>{dashboard.kpi.upcoming_3days_count}</strong>
          <p>直近予定タスク</p>
        </article>
        <article className="card kpi-card kpi-gray">
          <h3>未来タスク合計</h3>
          <strong>{dashboard.kpi.future_count}</strong>
          <p>未着手タスク</p>
        </article>
      </section>

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
                <Scatter data={bubbleRows} onClick={(entry) => setSelectedFarmerId(String((entry as any).id))}>
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
                      <th><button onClick={() => handleSort('name')}>農業者名</button></th>
                      <th><button onClick={() => handleSort('field_count')}>圃場数</button></th>
                      <th><button onClick={() => handleSort('due_task_count')}>タスク</button></th>
                      <th><button onClick={() => handleSort('overdue_count')}>遅延</button></th>
                      <th><button onClick={() => handleSort('delay_rate')}>遅延率</button></th>
                      <th><button onClick={() => handleSort('completion_rate')}>完了率</button></th>
                      <th>傾向</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedFarmers.map((f, idx) => {
                      const trend = trendLabel(f.trend_direction);
                      return (
                        <tr key={f.id} onClick={() => setSelectedFarmerId(f.id)}>
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
                <button onClick={() => setSelectedFarmerId(null)} aria-label="閉じる">✕</button>
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
    </div>
  );
}
