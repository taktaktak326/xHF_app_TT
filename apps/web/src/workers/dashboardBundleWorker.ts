type DashboardTask = {
  uuid: string;
  farmUuid: string;
  farmName: string;
  fieldUuid: string;
  seasonKey: string;
  typeFamily: string;
  completed: boolean;
  scheduledDay: string;
  executionDay: string;
  occurrence: number;
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

type DailyFarmBar = {
  date: string;
  _dateKey: string;
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
  as_of: string;
};

type WorkerRequest = {
  id: number;
  tasks: DashboardTask[];
  snapshotDate: string;
  asOf: string;
};

type WorkerResponse = {
  id: number;
  bundle?: DashboardBundle;
  error?: string;
};

function getScheduledDay(task: DashboardTask): string {
  return task.scheduledDay;
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

function computeDashboardBundle(filteredTasks: DashboardTask[], snapshotDate: string, asOf: string): DashboardBundle {
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

  const totalDue = farmers.reduce((s, f) => s + f.due_task_count, 0);
  const totalCompleted = farmers.reduce((s, f) => s + f.completed_count, 0);
  const totalOverdue = farmers.reduce((s, f) => s + f.overdue_count, 0);
  const totalDueToday = farmers.reduce((s, f) => s + f.due_today_count, 0);
  const totalUpcoming = farmers.reduce((s, f) => s + f.upcoming_3days_count, 0);
  const totalFuture = farmers.reduce((s, f) => s + f.future_task_count, 0);

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
  const taskDates = [...dailyFarmMap.keys()].sort();
  const rangeStart = taskDates.length > 0 && taskDates[0] < trendDays[0] ? taskDates[0] : trendDays[0];
  const rangeEnd = taskDates.length > 0 && taskDates[taskDates.length - 1] > trendDays[trendDays.length - 1]
    ? taskDates[taskDates.length - 1]
    : trendDays[trendDays.length - 1];
  const allBarDays: string[] = [];
  const startMs = new Date(rangeStart + 'T00:00:00+09:00').getTime();
  const endMs = new Date(rangeEnd + 'T00:00:00+09:00').getTime();
  for (let ms = startMs; ms <= endMs; ms += 86400000) {
    allBarDays.push(getJstDayKey(new Date(ms)));
  }
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

self.onmessage = (event: MessageEvent<WorkerRequest>) => {
  const { id, tasks, snapshotDate, asOf } = event.data;
  try {
    const bundle = computeDashboardBundle(tasks || [], snapshotDate, asOf);
    const response: WorkerResponse = { id, bundle };
    (self as unknown as Worker).postMessage(response);
  } catch (err: any) {
    const response: WorkerResponse = { id, error: String(err?.message || err || 'worker_failed') };
    (self as unknown as Worker).postMessage(response);
  }
};
