import { useEffect, useMemo, useState } from 'react';
import type { FC } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { useData } from '../context/DataContext';
import type { LoginAndTokenResp, Field, BaseTask } from '../types/farm';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceArea } from 'recharts';
import './FarmsPage.css'; // Reuse common styles
import './SprayingWeatherPage.css';
import { withApiBase } from '../utils/apiBase';
import LoadingOverlay from '../components/LoadingOverlay';

// =============================================================================
// Type Definitions
// =============================================================================

interface DailyWeather {
  date: string;
  airTempCMin: number;
  airTempCMax: number;
  precipitationBestMm: number;
  windSpeedMSAvg: number;
}

interface ClimatologyWeather {
  date: string;
  airTempCMax: number;
  airTempCAvg: number;
  airTempCMin: number;
}

interface HourlyWeather {
  startDatetime: string;
  airTempCAvg: number;
  windSpeedMSAvg: number;
  relativeHumidityPctAvg: number;
}

interface SprayFactor {
  factor: 'DELTA_T' | 'AIR_TEMPERATURE' | 'PRECIPITATION' | 'WIND' | 'RELATIVE_HUMIDITY';
  result: 'good' | 'moderate' | 'poor';
}

interface SprayWeather {
  fromDate: string;
  result: 'RECOMMENDED' | 'NOT_RECOMMENDED' | 'POSSIBLE' | 'moderate';
  factors?: SprayFactor[];
}

interface WeatherData {
  weatherHistoricForecastDaily?: DailyWeather[];
  sprayWeather?: SprayWeather[];
  weatherClimatologyDaily?: ClimatologyWeather[];
  weatherHistoricForecastHourly?: HourlyWeather[];
}

type PlannedTaskEntry = {
  uuid: string;
  fieldName: string;
  plannedDate: string | null;
  executionDate: string | null;
  seasonUuid?: string | null;
};

const FACTOR_LABELS: Record<SprayFactor['factor'], string> = {
  PRECIPITATION: '降水',
  AIR_TEMPERATURE: '気温',
  WIND: '風',
  RELATIVE_HUMIDITY: '湿度',
  DELTA_T: 'ΔT',
};

const FACTOR_ORDER: SprayFactor['factor'][] = [
  'PRECIPITATION',
  'AIR_TEMPERATURE',
  'WIND',
  'RELATIVE_HUMIDITY',
  'DELTA_T',
];

const formatFactorResult = (result?: SprayFactor['result'] | 'bad'): { label: string; tone: string } => {
  if (!result) return { label: '不明', tone: 'neutral' };
  if (result === 'bad') return { label: '不適', tone: 'bad' };
  switch (result) {
    case 'good':
      return { label: '良好', tone: 'good' };
    case 'moderate':
      return { label: '注意', tone: 'moderate' };
    case 'poor':
      return { label: '不適', tone: 'bad' };
    default:
      return { label: '不明', tone: 'neutral' };
  }
};

const toJstPlannedDateIso = (dateInput: string): string | null => {
  if (!dateInput) return null;
  const [y, m, d] = dateInput.split('-').map(Number);
  if (!y || !m || !d) return null;
  const utcMs = Date.UTC(y, m - 1, d, 0, 0, 0) - 9 * 60 * 60 * 1000;
  if (!Number.isFinite(utcMs)) return null;
  return new Date(utcMs).toISOString().replace('.000Z', 'Z');
};

const buildSprayWindows = (sprayEntries: SprayWeather[]) => {
  const sorted = [...sprayEntries]
    .map(s => ({ hour: getJstHour(s.fromDate), result: s.result }))
    .sort((a, b) => a.hour - b.hour);
  const windows: Array<{ start: number; end: number; type: 'recommended' | 'possible' }> = [];
  const classify = (result: SprayWeather['result']) =>
    result === 'RECOMMENDED' ? 'recommended' : result === 'POSSIBLE' || result === 'moderate' ? 'possible' : null;
  let active: { start: number; type: 'recommended' | 'possible' } | null = null;
  sorted.forEach((entry, index) => {
    const type = classify(entry.result);
    if (!type) {
      if (active) {
        windows.push({ start: active.start, end: sorted[index - 1]?.hour ?? active.start, type: active.type });
        active = null;
      }
      return;
    }
    if (!active) {
      active = { start: entry.hour, type };
      return;
    }
    if (active.type !== type || entry.hour !== sorted[index - 1]?.hour + 1) {
      windows.push({ start: active.start, end: sorted[index - 1]?.hour ?? active.start, type: active.type });
      active = { start: entry.hour, type };
    }
  });
  if (active) {
    const lastHour = sorted[sorted.length - 1]?.hour ?? active.start;
    windows.push({ start: active.start, end: lastHour, type: active.type });
  }
  return windows;
};

const getBbchLabelForDate = (
  predictions: CountryCropGrowthStagePrediction[] | null | undefined,
  dateInput: string
): string => {
  if (!predictions || !dateInput) return '';
  for (const pred of predictions) {
    if (!pred?.startDate) continue;
    const start = getJstDateKey(pred.startDate);
    if (!start) continue;
    let end = '';
    if (pred.endDate) {
      const endDate = new Date(pred.endDate);
      endDate.setDate(endDate.getDate() - 1);
      end = getJstDateKeyFromDate(endDate);
    }
    const inRange = end ? start <= dateInput && dateInput <= end : start <= dateInput;
    if (!inRange) continue;
    const stageName = pred.cropGrowthStageV2?.name ?? '';
    return stageName ? `BBCH ${pred.index} - ${stageName}` : `BBCH ${pred.index}`;
  }
  return '';
};
const formatJstDateLabel = (dateString: string): string => {
  if (!dateString) return '';
  const date = dateString.includes('T')
    ? new Date(dateString)
    : new Date(`${dateString}T00:00:00+09:00`);
  return date.toLocaleDateString('ja-JP', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    timeZone: 'Asia/Tokyo',
  });
};

const getJstDateKey = (dateString: string): string => formatJstDateLabel(dateString).replace(/\//g, '-');

const getJstDateKeyFromDate = (date: Date): string => {
  return date
    .toLocaleDateString('ja-JP', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      timeZone: 'Asia/Tokyo',
    })
    .replace(/\//g, '-');
};

const getJstHour = (dateString: string): number => {
  const hourText = new Date(dateString).toLocaleString('ja-JP', {
    hour: '2-digit',
    hour12: false,
    timeZone: 'Asia/Tokyo',
  });
  const parsed = Number.parseInt(hourText, 10);
  return Number.isFinite(parsed) ? parsed : 0;
};

type PlannedTaskBadge = {
  id: string;
  fieldName: string;
  dateKey: string;
};

// =============================================================================
// API Client
// =============================================================================

async function fetchWeatherByFieldApi(params: { auth: LoginAndTokenResp; fieldUuid: string }): Promise<any> {
  const requestBody = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    field_uuid: params.fieldUuid,
  };

  const res = await fetch(withApiBase('/weather-by-field'), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(requestBody),
  });
  return res.json();
}

export function SprayingWeatherPage() {
  const { fieldUuid } = useParams<{ fieldUuid: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const { auth } = useAuth();
  const { combinedOut } = useData();

  const [weatherData, setWeatherData] = useState<WeatherData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const clusterState = location.state as {
    clusterId?: string;
    representativeUuid?: string;
    fieldUuids?: string[];
    fieldNames?: string[];
    radiusKm?: number;
  } | null;
  const clusterFields = clusterState?.fieldNames ?? [];
  const isCluster = clusterFields.length > 1;

  const fieldName = useMemo(() => {
    if (!combinedOut?.response?.data?.fieldsV2 || !fieldUuid) return '...';
    const field = combinedOut.response?.data?.fieldsV2?.find((f: Field) => f.uuid === fieldUuid);
    return field ? field.name : 'Unknown Field';
  }, [combinedOut, fieldUuid]);
  const plannedTasksByDate = useMemo(() => {
    if (!combinedOut?.response?.data?.fieldsV2) return {} as Record<string, PlannedTaskBadge[]>;
    const fieldUuids = clusterState?.fieldUuids?.length ? clusterState.fieldUuids : fieldUuid ? [fieldUuid] : [];
    if (fieldUuids.length === 0) return {} as Record<string, PlannedTaskBadge[]>;
    const map: Record<string, PlannedTaskBadge[]> = {};
    const tasks: Record<string, PlannedTaskEntry[]> = {};
    combinedOut.response.data.fieldsV2
      .filter((f: Field) => fieldUuids.includes(f.uuid))
      .forEach((field: Field) => {
        field.cropSeasonsV2?.forEach(season => {
          (season.sprayingsV2 ?? []).forEach((task: BaseTask) => {
            if (!task.plannedDate) return;
            const dateKey = getJstDateKey(task.plannedDate);
            if (!dateKey) return;
            const entry = { id: task.uuid, fieldName: field.name, dateKey };
            if (!map[dateKey]) map[dateKey] = [];
            map[dateKey].push(entry);
            if (!tasks[dateKey]) tasks[dateKey] = [];
            tasks[dateKey].push({
              uuid: task.uuid,
              fieldName: field.name,
              plannedDate: task.plannedDate,
              executionDate: task.executionDate,
              seasonUuid: season.uuid,
            });
          });
        });
      });
    return { badges: map, tasks };
  }, [combinedOut, clusterState, fieldUuid]);
  const bbchBySeason = useMemo(() => {
    const fields = combinedOut?.response?.data?.fieldsV2 ?? [];
    const map = new Map<string, CountryCropGrowthStagePrediction[]>();
    fields.forEach((field: Field) => {
      field.cropSeasonsV2?.forEach(season => {
        if (!season.uuid) return;
        map.set(season.uuid, season.countryCropGrowthStagePredictions ?? []);
      });
    });
    return map;
  }, [combinedOut]);

  useEffect(() => {
    if (!fieldUuid) {
      navigate('/weather'); // Redirect if no field is selected
      return;
    }

    const fetchWeather = async () => {
      if (!auth) return;
      setLoading(true);
      setError(null);
      try {
        const result = await fetchWeatherByFieldApi({ auth, fieldUuid });
        if (!result.ok) {
          throw new Error(result.detail || 'Failed to fetch weather data');
        }
        setWeatherData(result.response.data.fieldV2);
      } catch (e: any) {
        setError(e.message);
      } finally {
        setLoading(false);
      }
    };

    fetchWeather();
  }, [auth, fieldUuid, navigate]);

  if (!fieldUuid) {
    return null; // Should be redirected
  }

  return (
    <div className="weather-page-container">
      {loading && <LoadingOverlay message="天気情報を取得しています..." />}
      <h2>
        <button onClick={() => navigate('/weather')} className="back-button">
          &larr;
        </button>
        {isCluster ? `クラスタ (${clusterFields.length}圃場) の天気情報` : `${fieldName} の天気情報`}
      </h2>
      {isCluster && (
        <div className="weather-cluster-info">
          <p>代表圃場: {fieldName}</p>
          <p>クラスタ半径: {clusterState?.radiusKm ?? 2}km</p>
          <div className="weather-cluster-list">
            {clusterFields.map((name, index) => (
              <span key={`${name}-${index}`} className="weather-cluster-chip">{name}</span>
            ))}
          </div>
        </div>
      )}

      {error && <p style={{ color: 'crimson' }}>エラー: {error}</p>}

      {weatherData && (
        <WeatherDisplay
          weatherData={weatherData}
          plannedTasksByDate={plannedTasksByDate.badges}
          plannedTasksDetail={plannedTasksByDate.tasks}
          bbchBySeason={bbchBySeason}
        />
      )}
    </div>
  );
}

// =============================================================================
// Sub Components
// =============================================================================
interface GroupedWeatherData {
  [date: string]: {
    daily?: DailyWeather;
    hourly: HourlyWeather[];
    spray: SprayWeather[];
  };
}

type CandidateDay = {
  dateKey: string;
  windows: Array<{ start: number; end: number; type: 'recommended' | 'possible' }>;
  dayTone: 'recommended' | 'possible' | 'bad';
  factorSummary: Record<SprayFactor['factor'], { good: number; moderate: number; bad: number; label: string; tone: string }>;
};

const WeatherDisplay: FC<{
  weatherData: WeatherData;
  plannedTasksByDate: Record<string, PlannedTaskBadge[]>;
  plannedTasksDetail: Record<string, PlannedTaskEntry[]>;
  bbchBySeason: Map<string, CountryCropGrowthStagePrediction[]>;
}> = ({ weatherData, plannedTasksByDate, plannedTasksDetail, bbchBySeason }) => {
  const { auth } = useAuth();
  const [modalState, setModalState] = useState<{
    dateKey: string;
    dayData: GroupedWeatherData[string];
    tasks: PlannedTaskEntry[];
    candidateDays: CandidateDay[];
  } | null>(null);
  const groupedData = useMemo(() => {
    const data: GroupedWeatherData = {};

    weatherData.weatherHistoricForecastHourly?.forEach(hour => {
      const date = getJstDateKey(hour.startDatetime);
      if (!data[date]) {
        data[date] = { hourly: [], spray: [] };
      }
      data[date].hourly.push(hour);
    });

    weatherData.weatherHistoricForecastDaily?.forEach(day => {
      const date = getJstDateKey(day.date);
      if (data[date]) {
        data[date].daily = day;
      }
    });

    weatherData.sprayWeather?.forEach(period => {
      const date = getJstDateKey(period.fromDate);
      if (data[date]) {
        data[date].spray.push(period);
      }
    });

    return Object.entries(data).sort(([dateA], [dateB]) => dateA.localeCompare(dateB));
  }, [weatherData]);
  const candidateDays = useMemo(() => {
    const summarizeFactors = (sprayEntries: SprayWeather[]) => {
      const summary = {} as Record<SprayFactor['factor'], { good: number; moderate: number; bad: number; label: string; tone: string }>;
      FACTOR_ORDER.forEach((factor) => {
        summary[factor] = { good: 0, moderate: 0, bad: 0, label: '不明', tone: 'neutral' };
      });
      sprayEntries.forEach(entry => {
        (entry.factors ?? []).forEach(factor => {
          const bucket = summary[factor.factor];
          if (!bucket) return;
          if (factor.result === 'good') bucket.good += 1;
          else if (factor.result === 'moderate') bucket.moderate += 1;
          else bucket.bad += 1;
        });
      });
      FACTOR_ORDER.forEach((factor) => {
        const bucket = summary[factor];
        if (!bucket) return;
        if (bucket.good === 0 && bucket.moderate === 0 && bucket.bad === 0) {
          bucket.label = '不明';
          bucket.tone = 'neutral';
          return;
        }
        if (bucket.bad >= bucket.good && bucket.bad >= bucket.moderate) {
          bucket.label = '不適';
          bucket.tone = 'bad';
        } else if (bucket.good >= bucket.moderate) {
          bucket.label = '良好';
          bucket.tone = 'good';
        } else {
          bucket.label = '注意';
          bucket.tone = 'moderate';
        }
      });
      return summary;
    };
    return groupedData.map(([dateKey, dayData]) => {
      const windows = buildSprayWindows(dayData.spray);
      const dayTone = windows.some(w => w.type === 'recommended')
        ? 'recommended'
        : windows.some(w => w.type === 'possible')
          ? 'possible'
          : 'bad';
      return {
        dateKey,
        windows,
        dayTone,
        factorSummary: summarizeFactors(dayData.spray),
      };
    });
  }, [groupedData]);

  return (
    <div className="weather-content">
      {groupedData.map(([date, dayData]) => (
        <DailyWeatherSummaryCard
          key={date}
          date={date}
          dayData={dayData}
          plannedTasks={plannedTasksByDate[date] ?? []}
          onOpenAdjustModal={() => {
            const tasks = plannedTasksDetail[date] ?? [];
            if (tasks.length === 0) return;
            setModalState({ dateKey: date, dayData, tasks, candidateDays });
          }}
        />
      ))}
      {modalState && (
        <AdjustPlannedDateModal
          dateKey={modalState.dateKey}
          dayData={modalState.dayData}
          tasks={modalState.tasks}
          candidateDays={modalState.candidateDays}
          bbchBySeason={bbchBySeason}
          auth={auth}
          onClose={() => setModalState(null)}
        />
      )}
    </div>
  );
};

const DailyWeatherSummaryCard: FC<{
  date: string;
  dayData: GroupedWeatherData[string];
  plannedTasks: PlannedTaskBadge[];
  onOpenAdjustModal?: () => void;
}> = ({ date, dayData, plannedTasks, onOpenAdjustModal }) => {
  const chartData = useMemo(() => dayData.hourly.map(h => ({
    time: getJstHour(h.startDatetime),
    '気温 (°C)': Number(h.airTempCAvg).toFixed(1),
    '湿度 (%)': Number(h.relativeHumidityPctAvg).toFixed(0),
    '風速 (m/s)': Number(h.windSpeedMSAvg).toFixed(1),
  })), [dayData.hourly]);
  const sprayWindows = useMemo(() => buildSprayWindows(dayData.spray), [dayData.spray]);
  const sprayByHour = useMemo(() => {
    const map = new Map<number, SprayWeather>();
    dayData.spray.forEach(entry => {
      const hour = getJstHour(entry.fromDate);
      if (!map.has(hour)) {
        map.set(hour, entry);
      }
    });
    return map;
  }, [dayData.spray]);
  const plannedTaskChips = useMemo(() => {
    const unique = Array.from(new Set(plannedTasks.map(task => task.fieldName)));
    const maxVisible = 3;
    const visible = unique.slice(0, maxVisible);
    const extra = unique.length - visible.length;
    return { visible, extra };
  }, [plannedTasks]);

  const sprayStatusText = {
    RECOMMENDED: '高推奨', // 変更
    NOT_RECOMMENDED: '範囲外',
    POSSIBLE: '中推奨', // 変更
    moderate: '低推奨', // 変更
    bad: '不適',
  };

  return (
    <div className="daily-summary-card">
      <div className="daily-summary-header">
        <div className="daily-summary-title">
          <h3>{formatJstDateLabel(date)}</h3>
          {plannedTasks.length > 0 && (
            <div className="planned-task-badges">
              <span className="planned-task-label">散布予定</span>
              {plannedTaskChips.visible.map(name => (
                <button type="button" key={name} className="planned-task-badge planned-task-badge--action" onClick={onOpenAdjustModal}>
                  {name}
                </button>
              ))}
              {plannedTaskChips.extra > 0 && (
                <button type="button" className="planned-task-badge planned-task-badge--extra" onClick={onOpenAdjustModal}>
                  +{plannedTaskChips.extra}
                </button>
              )}
            </div>
          )}
        </div>
        {dayData.daily && (
          <p>
            気温: {dayData.daily.airTempCMin}°C / {dayData.daily.airTempCMax}°C
            <span style={{ margin: '0 1em' }}>|</span>
            降水: {dayData.daily.precipitationBestMm} mm
          </p>
        )}
      </div>

      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={chartData} margin={{ top: 5, right: 20, left: -10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#4a4a4f" />
          <XAxis dataKey="time" unit="時" stroke="#9e9e9e" />
          <YAxis yAxisId="left" stroke="#8884d8" />
          <YAxis yAxisId="right" orientation="right" stroke="#82ca9d" />
          {sprayWindows.map((window, index) => (
            <ReferenceArea
              key={`${window.type}-${window.start}-${window.end}-${index}`}
              x1={window.start}
              x2={window.end + 1}
              yAxisId="left"
              ifOverflow="extendDomain"
              fill={window.type === 'recommended' ? '#00bfa6' : '#f5c84b'}
              fillOpacity={window.type === 'recommended' ? 0.18 : 0.12}
              strokeOpacity={0}
            />
          ))}
          <Tooltip contentStyle={{ backgroundColor: '#252529', border: '1px solid #4a4a4f' }} />
          <Legend />
          <Line yAxisId="left" type="monotone" dataKey="気温 (°C)" stroke="#8884d8" dot={false} />
          <Line yAxisId="right" type="monotone" dataKey="湿度 (%)" stroke="#82ca9d" dot={false} />
          <Line yAxisId="left" type="monotone" dataKey="風速 (m/s)" stroke="#ffc658" dot={false} />
        </LineChart>
      </ResponsiveContainer>

      <div className="hourly-forecast-list">
        <div className="hourly-forecast-units">単位: 気温 °C / 風速 m/s / 湿度 %</div>
        <div className="hourly-scroll-row">
          {dayData.hourly.map(hour => {
            const sprayInfo = sprayByHour.get(getJstHour(hour.startDatetime));
            const rawResult = sprayInfo?.result || 'NOT_RECOMMENDED';
            const resultText = sprayStatusText[rawResult] || rawResult;
            const resultClass = (rawResult === 'moderate' ? 'possible' : rawResult).toLowerCase();
            const factorMap = new Map((sprayInfo?.factors ?? []).map(f => [f.factor, f.result]));
            return (
              <details key={hour.startDatetime} className={`hourly-chip hourly-chip--${resultClass}`}>
                <summary className="hourly-chip-summary">
                  <div className="hourly-chip-time">
                    {new Date(hour.startDatetime).toLocaleTimeString('ja-JP', { hour: '2-digit', timeZone: 'Asia/Tokyo' })}:00
                  </div>
                  <span className={`spray-badge spray-badge-${resultClass}`}>{resultText}</span>
                  <div className="hourly-chip-metrics">
                    <span>気温 {Number(hour.airTempCAvg).toFixed(1)}</span>
                    <span>風速 {Number(hour.windSpeedMSAvg).toFixed(1)}</span>
                    <span>湿度 {Number(hour.relativeHumidityPctAvg).toFixed(0)}</span>
                  </div>
                </summary>
                <div className="hourly-chip-details">
                  <div className="hourly-detail-grid">
                    <div>
                      <span className="hourly-detail-label">気温</span>
                      <span>{Number(hour.airTempCAvg).toFixed(1)}°C</span>
                    </div>
                    <div>
                      <span className="hourly-detail-label">風速</span>
                      <span>{Number(hour.windSpeedMSAvg).toFixed(1)} m/s</span>
                    </div>
                    <div>
                      <span className="hourly-detail-label">湿度</span>
                      <span>{Number(hour.relativeHumidityPctAvg).toFixed(0)}%</span>
                    </div>
                  </div>
                  <div className="hourly-factor-list">
                    <span className="hourly-detail-label">要因</span>
                    <div className="hourly-factor-grid">
                      {FACTOR_ORDER.map((factor) => {
                        const raw = factorMap.get(factor);
                        const { label, tone } = formatFactorResult(raw as SprayFactor['result'] | undefined);
                        return (
                          <span key={factor} className={`hourly-factor-item hourly-factor-item--${tone}`}>
                            {FACTOR_LABELS[factor]}: {label}
                          </span>
                        );
                      })}
                    </div>
                  </div>
                </div>
              </details>
            );
          })}
        </div>
      </div>
    </div>
  );
};

const AdjustPlannedDateModal: FC<{
  dateKey: string;
  dayData: GroupedWeatherData[string];
  tasks: PlannedTaskEntry[];
  candidateDays: CandidateDay[];
  bbchBySeason: Map<string, CountryCropGrowthStagePrediction[]>;
  auth: LoginAndTokenResp | null;
  onClose: () => void;
}> = ({ dateKey, dayData, tasks, candidateDays, bbchBySeason, auth, onClose }) => {
  const [selectedTaskIds, setSelectedTaskIds] = useState(() => new Set(tasks.map(t => t.uuid)));
  const [selectedDateKey, setSelectedDateKey] = useState(dateKey);
  const [selectedWindow, setSelectedWindow] = useState<number | null>(0);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const selectedCandidate = useMemo(
    () => candidateDays.find(entry => entry.dateKey === selectedDateKey),
    [candidateDays, selectedDateKey]
  );
  const windows = selectedCandidate?.windows ?? buildSprayWindows(dayData.spray);
  const factorSummary = selectedCandidate?.factorSummary ?? ({} as CandidateDay['factorSummary']);
  const hasCandidates = windows.length > 0;
  const selectedDateInput = selectedDateKey;

  const toggleTask = (uuid: string) => {
    setSelectedTaskIds(prev => {
      const next = new Set(prev);
      if (next.has(uuid)) {
        next.delete(uuid);
      } else {
        next.add(uuid);
      }
      return next;
    });
  };

  const handleApply = async () => {
    if (!auth) {
      setError('ログイン情報がありません。再ログインしてください。');
      return;
    }
    if (!selectedDateInput) {
      setError('変更先の日付を選択してください。');
      return;
    }
    const plannedDate = toJstPlannedDateIso(selectedDateInput);
    if (!plannedDate) {
      setError('日付が不正です。');
      return;
    }
    const targets = tasks.filter(task => selectedTaskIds.has(task.uuid));
    if (targets.length === 0) {
      setError('変更するタスクを選択してください。');
      return;
    }
    setSaving(true);
    setError(null);
    try {
      for (const task of targets) {
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
            // ignore
          }
          throw new Error(detail);
        }
      }
      onClose();
    } catch (err) {
      const message = err instanceof Error ? err.message : '更新に失敗しました。';
      setError(message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="weather-modal-backdrop" onClick={onClose}>
      <div className="weather-modal" onClick={(e) => e.stopPropagation()}>
        <div className="weather-modal-header">
          <h3>散布予定の変更</h3>
          <button type="button" onClick={onClose}>閉じる</button>
        </div>
        <div className="weather-modal-body">
          <div className="weather-modal-section">
            <h4>候補日時</h4>
            {candidateDays.length > 0 && (
              <div className="weather-date-selector">
                {candidateDays.map(day => (
                  <button
                    key={day.dateKey}
                    type="button"
                    className={`weather-date-chip weather-date-chip--${day.dayTone}${day.dateKey === selectedDateKey ? ' active' : ''}`}
                    onClick={() => {
                      setSelectedDateKey(day.dateKey);
                      setSelectedWindow(0);
                    }}
                    disabled={saving}
                  >
                    {formatJstDateLabel(day.dateKey)}
                  </button>
                ))}
              </div>
            )}
            <div className="weather-factor-summary">
              {FACTOR_ORDER.map((factor) => {
                const summary = factorSummary?.[factor];
                if (!summary) return null;
                return (
                  <div key={factor} className="weather-factor-row">
                    <span className="weather-factor-name">{FACTOR_LABELS[factor]}:</span>
                    <span className={`weather-factor-status weather-factor-status--${summary.tone}`}>{summary.label}</span>
                    <span className="weather-factor-counts">
                      良好 {summary.good} / 注意 {summary.moderate} / 不適 {summary.bad}
                    </span>
                  </div>
                );
              })}
            </div>
            {!hasCandidates ? (
              <p className="weather-modal-empty">総合判定で注意/良好の時間帯がありません。</p>
            ) : (
              <div className="weather-window-list">
                {windows.map((window, index) => (
                  <label key={`${window.type}-${window.start}-${window.end}-${index}`} className={`weather-window weather-window--${window.type}`}>
                    <input
                      type="radio"
                      name="spray-window"
                      checked={selectedWindow === index}
                      onChange={() => setSelectedWindow(index)}
                      disabled={saving}
                    />
                    <span>
                      {String(window.start).padStart(2, '0')}:00 - {String(window.end + 1).padStart(2, '0')}:00
                    </span>
                    <span className="weather-window-tag">
                      {window.type === 'recommended' ? '良好' : '注意'}
                    </span>
                  </label>
                ))}
              </div>
            )}
          </div>
          <div className="weather-modal-section">
            <h4>対象タスク</h4>
            <div className="weather-task-list">
              {tasks.map(task => {
                const predictions = task.seasonUuid ? bbchBySeason.get(task.seasonUuid) : null;
                const bbch = getBbchLabelForDate(predictions, selectedDateInput);
                return (
                  <label key={task.uuid} className="weather-task-item">
                    <input
                      type="checkbox"
                      checked={selectedTaskIds.has(task.uuid)}
                      onChange={() => toggleTask(task.uuid)}
                      disabled={saving}
                    />
                    <div>
                      <div className="weather-task-title">{task.fieldName}</div>
                      <div className="weather-task-meta">
                        現在: {task.plannedDate ? formatJstDateLabel(task.plannedDate) : '-'} / 変更先BBCH: {bbch || '-'}
                      </div>
                    </div>
                  </label>
                );
              })}
            </div>
          </div>
          {error && <div className="weather-modal-error">{error}</div>}
        </div>
        <div className="weather-modal-footer">
          <button type="button" onClick={onClose} disabled={saving}>キャンセル</button>
          <button type="button" onClick={handleApply} disabled={saving || !hasCandidates}>
            変更を保存
          </button>
        </div>
      </div>
    </div>
  );
};
