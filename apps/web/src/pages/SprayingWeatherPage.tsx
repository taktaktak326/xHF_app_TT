import { useEffect, useMemo, useState } from 'react';
import type { FC } from 'react';
import { useParams, useNavigate, useLocation } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { useData } from '../context/DataContext';
import { useFarms } from '../context/FarmContext';
import type { LoginAndTokenResp, Field, BaseTask, CountryCropGrowthStagePrediction } from '../types/farm';
import { LineChart, Line, Bar, ComposedChart, Cell, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, ReferenceArea } from 'recharts';
import './FarmsPage.css'; // Reuse common styles
import './SprayingWeatherPage.css';
import { withApiBase } from '../utils/apiBase';
import LoadingOverlay from '../components/LoadingOverlay';
import { postJsonCached } from '../utils/cachedJsonFetch';

// =============================================================================
// Type Definitions
// =============================================================================

interface DailyWeather {
  date: string;
  airTempCMin: number;
  airTempCMax: number;
  airTempCAvg?: number;
  sunshineDurationH?: number;
  precipitationBestMm: number;
  windSpeedMSAvg: number;
  windDirectionDeg?: number;
  relativeHumidityPctAvg?: number;
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

type SprayWindow = {
  start: number;
  end: number;
  type: 'recommended' | 'possible';
};

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

const buildSprayWindows = (sprayEntries: SprayWeather[]): SprayWindow[] => {
  const sorted: Array<{ hour: number; result: SprayWeather['result'] }> = [...sprayEntries]
    .map(s => ({ hour: getJstHour(s.fromDate), result: s.result }))
    .sort((a, b) => a.hour - b.hour);
  const windows: SprayWindow[] = [];
  const classify = (result: SprayWeather['result']) =>
    result === 'RECOMMENDED' ? 'recommended' : result === 'POSSIBLE' || result === 'moderate' ? 'possible' : null;
  let activeStart: number | null = null;
  let activeType: SprayWindow['type'] | null = null;
  sorted.forEach((entry, index) => {
    const type = classify(entry.result);
    if (!type) {
      if (activeStart !== null && activeType) {
        const prevHour = index > 0 ? sorted[index - 1].hour : activeStart;
        windows.push({ start: activeStart, end: prevHour, type: activeType });
        activeStart = null;
        activeType = null;
      }
      return;
    }
    if (activeStart === null || !activeType) {
      activeStart = entry.hour;
      activeType = type;
      return;
    }
    if (activeType !== type || entry.hour !== sorted[index - 1]?.hour + 1) {
      const prevHour = index > 0 ? sorted[index - 1].hour : activeStart;
      windows.push({ start: activeStart, end: prevHour, type: activeType });
      activeStart = entry.hour;
      activeType = type;
    }
  });
  if (activeStart !== null && activeType) {
    const lastHour = sorted.length > 0 ? sorted[sorted.length - 1].hour : activeStart;
    windows.push({ start: activeStart, end: lastHour, type: activeType });
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

const getJstDateParts = (date: Date): { year: number; month: number; day: number } => {
  const parts = date
    .toLocaleDateString('ja-JP', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      timeZone: 'Asia/Tokyo',
    })
    .split('/');
  const [yearText, monthText, dayText] = parts;
  return {
    year: Number(yearText),
    month: Number(monthText),
    day: Number(dayText),
  };
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

const toFiniteNumber = (value: unknown): number | null => {
  if (value === null || value === undefined) return null;
  const num = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(num) ? num : null;
};

const estimateSoilStatus = (days: DailyWeather[]): { label: string; tone: 'dry' | 'ok' | 'wet'; reason: string } | null => {
  const validDays = days.filter(Boolean);
  if (validDays.length === 0) return null;
  let precipSum = 0;
  let humiditySum = 0;
  let tempSum = 0;
  let windSum = 0;
  let sunshineSum = 0;
  let count = 0;
  validDays.forEach((day) => {
    const precip = toFiniteNumber(day.precipitationBestMm);
    const humidity = toFiniteNumber(day.relativeHumidityPctAvg);
    const temp = toFiniteNumber(day.airTempCAvg);
    const wind = toFiniteNumber(day.windSpeedMSAvg);
    const sunshine = toFiniteNumber(day.sunshineDurationH);
    if (precip === null || humidity === null || temp === null || wind === null || sunshine === null) {
      return;
    }
    precipSum += precip;
    humiditySum += humidity;
    tempSum += temp;
    windSum += wind;
    sunshineSum += sunshine;
    count += 1;
  });
  if (count === 0) return null;
  const avgPrecip = precipSum / count;
  const avgHumidity = humiditySum / count;
  const avgTemp = tempSum / count;
  const avgWind = windSum / count;
  const avgSunshine = sunshineSum / count;
  const dryingIndex = Math.max(0, avgTemp * 0.3 + avgWind * 1.5 + avgSunshine * 0.8 - avgHumidity * 0.2);
  const soilBalance = avgPrecip - dryingIndex;
  const tone: 'dry' | 'ok' | 'wet' = soilBalance <= -2 ? 'dry' : soilBalance >= 2 ? 'wet' : 'ok';
  const label = tone === 'dry' ? '乾燥ぎみ' : tone === 'wet' ? '湿りすぎ' : '適度';
  const rainNote = avgPrecip <= 1 ? '雨が少ない' : avgPrecip <= 5 ? '雨は少なめ' : '雨が多い';
  const sunNote = avgSunshine >= 5 ? '日照が多い' : avgSunshine >= 3 ? '日照は普通' : '日照が少ない';
  const humidityNote = avgHumidity >= 80 ? '湿度が高い' : avgHumidity <= 60 ? '湿度が低い' : '湿度は普通';
  const reason = `直近${count}日: ${rainNote}・${sunNote}・${humidityNote}ため、${label}と判断。`;
  return { label, tone, reason };
};

const toJstBoundaryIso = (year: number, month: number, day: number, endOfDay: boolean): string => {
  const utcMs = Date.UTC(
    year,
    month - 1,
    day,
    endOfDay ? 23 : 0,
    endOfDay ? 59 : 0,
    endOfDay ? 59 : 0,
    endOfDay ? 999 : 0
  ) - 9 * 60 * 60 * 1000;
  return new Date(utcMs).toISOString().replace('.000Z', 'Z');
};

const buildHistoryRange = (): { fromDate: string; tillDate: string } | null => {
  const now = new Date();
  const { year, month, day } = getJstDateParts(now);
  if (!year || !month || !day) return null;
  const fromDate = toJstBoundaryIso(year - 5, month, day, false);
  const tillDate = toJstBoundaryIso(year, month, day, true);
  return { fromDate, tillDate };
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
  const cacheKey = `weather-by-field:${params.fieldUuid}:default`;
  const requestBody = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    field_uuid: params.fieldUuid,
  };
  const { json: out, status, source } = await postJsonCached<any>(
    withApiBase('/weather-by-field'),
    requestBody,
    undefined,
    { cacheKey, cache: 'session' },
  );
  if (!out) {
    return { ok: true, status, response: { data: { fieldV2: {} } }, source };
  }
  if (typeof out === 'string') {
    return { ok: false, status, detail: out, source };
  }
  return { ...out, source };
}

async function fetchWeatherByFieldApiWithRange(params: {
  auth: LoginAndTokenResp;
  fieldUuid: string;
  fromDate?: string;
  tillDate?: string;
}): Promise<any> {
  const cacheKey = `weather-by-field:${params.fieldUuid}:${params.fromDate ?? 'default'}:${params.tillDate ?? 'default'}`;
  const requestBody: Record<string, string> = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    field_uuid: params.fieldUuid,
  };
  if (params.fromDate) requestBody.from_date = params.fromDate;
  if (params.tillDate) requestBody.till_date = params.tillDate;
  const { json: out, status, source } = await postJsonCached<any>(
    withApiBase('/weather-by-field'),
    requestBody,
    undefined,
    { cacheKey, cache: 'session' },
  );
  if (!out) {
    return { ok: true, status, response: { data: { fieldV2: {} } }, source };
  }
  if (typeof out === 'string') {
    return { ok: false, status, detail: out, source };
  }
  return { ...out, source };
}

export function SprayingWeatherPage() {
  const { fieldUuid } = useParams<{ fieldUuid: string }>();
  const navigate = useNavigate();
  const location = useLocation();
  const { auth } = useAuth();
  const { combinedOut } = useData();

  const [weatherData, setWeatherData] = useState<WeatherData | null>(null);
  const [historyWeatherData, setHistoryWeatherData] = useState<WeatherData | null>(null);
  const [loading, setLoading] = useState(false);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [historyError, setHistoryError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'spray' | 'history'>('spray');

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
    const empty = { badges: {} as Record<string, PlannedTaskBadge[]>, tasks: {} as Record<string, PlannedTaskEntry[]> };
    if (!combinedOut?.response?.data?.fieldsV2) return empty;
    const fieldUuids = clusterState?.fieldUuids?.length ? clusterState.fieldUuids : fieldUuid ? [fieldUuid] : [];
    if (fieldUuids.length === 0) return empty;
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
    setWeatherData(null);
    setHistoryWeatherData(null);
    setError(null);
    setHistoryError(null);
  }, [fieldUuid]);

  const historyRange = useMemo(() => buildHistoryRange(), []);

  useEffect(() => {
    setHistoryWeatherData(null);
    setHistoryError(null);
  }, [historyRange?.fromDate, historyRange?.tillDate]);

  useEffect(() => {
    if (!fieldUuid) {
      navigate('/weather'); // Redirect if no field is selected
      return;
    }

    if (activeTab !== 'spray') {
      return;
    }
    if (weatherData) {
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
  }, [activeTab, auth, fieldUuid, navigate, weatherData]);

  useEffect(() => {
    if (!fieldUuid) return;
    if (activeTab !== 'history') return;
    if (historyWeatherData) return;
    if (!historyRange) return;

    const fetchHistory = async () => {
      if (!auth) return;
      setHistoryLoading(true);
      setHistoryError(null);
      try {
        const result = await fetchWeatherByFieldApiWithRange({
          auth,
          fieldUuid,
          fromDate: historyRange.fromDate,
          tillDate: historyRange.tillDate,
        });
        if (!result.ok) {
          throw new Error(result.detail || 'Failed to fetch weather data');
        }
        setHistoryWeatherData(result.response.data.fieldV2);
      } catch (e: any) {
        setHistoryError(e.message);
      } finally {
        setHistoryLoading(false);
      }
    };

    fetchHistory();
  }, [activeTab, auth, fieldUuid, historyRange, historyWeatherData]);

  if (!fieldUuid) {
    return null; // Should be redirected
  }

  return (
    <div className="weather-page-container">
      {loading && <LoadingOverlay message="天気情報を取得しています..." />}
      {historyLoading && activeTab === 'history' && <LoadingOverlay message="過去の天気情報を取得しています..." />}
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
      {historyError && activeTab === 'history' && <p style={{ color: 'crimson' }}>エラー: {historyError}</p>}

      <div className="weather-tabs" role="tablist" aria-label="天気タブ">
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === 'spray'}
          className={`weather-tab ${activeTab === 'spray' ? 'active' : ''}`}
          onClick={() => setActiveTab('spray')}
        >
          散布天気
        </button>
        <button
          type="button"
          role="tab"
          aria-selected={activeTab === 'history'}
          className={`weather-tab ${activeTab === 'history' ? 'active' : ''}`}
          onClick={() => setActiveTab('history')}
        >
          過去の天気
        </button>
      </div>

      {weatherData && activeTab === 'spray' && (
        <WeatherDisplay
          weatherData={weatherData}
          plannedTasksByDate={plannedTasksByDate.badges}
          plannedTasksDetail={plannedTasksByDate.tasks}
          bbchBySeason={bbchBySeason}
        />
      )}
      {activeTab === 'history' && (
        <PastWeatherPanel weatherData={historyWeatherData} plannedTasksDetail={plannedTasksByDate.tasks} />
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
  windows: SprayWindow[];
  dayTone: 'recommended' | 'possible' | 'bad';
  factorSummary: Record<SprayFactor['factor'], { good: number; moderate: number; bad: number; label: string; tone: string }>;
};

const PastWeatherPanel: FC<{
  weatherData: WeatherData | null;
  plannedTasksDetail: Record<string, PlannedTaskEntry[]>;
}> = ({ weatherData, plannedTasksDetail }) => {
  const plannedMonths = useMemo(() => {
    const monthMap = new Map<string, number>();
    Object.entries(plannedTasksDetail).forEach(([dateKey, tasks]) => {
      if (!dateKey || tasks.length === 0) return;
      const [year, month] = dateKey.split('-');
      if (!year || !month) return;
      const key = `${year}-${month}`;
      monthMap.set(key, (monthMap.get(key) ?? 0) + tasks.length);
    });
    return Array.from(monthMap.entries()).sort(([a], [b]) => a.localeCompare(b));
  }, [plannedTasksDetail]);
  const hasPlannedMonths = plannedMonths.length > 0;

  const historyDailyByMonth = useMemo(() => {
    const data = weatherData?.weatherHistoricForecastDaily ?? [];
    const monthMap = new Map<string, DailyWeather[]>();
    data.forEach((entry) => {
      const dateKey = getJstDateKey(entry.date);
      const [year, month] = dateKey.split('-');
      if (!year || !month) return;
      const key = `${year}-${month}`;
      if (!monthMap.has(key)) monthMap.set(key, []);
      monthMap.get(key)?.push(entry);
    });
    monthMap.forEach((entries, key) => {
      monthMap.set(
        key,
        entries.sort((a, b) => getJstDateKey(a.date).localeCompare(getJstDateKey(b.date)))
      );
    });
    return monthMap;
  }, [weatherData]);

  const historyDailyMonths = useMemo(() => Array.from(historyDailyByMonth.keys()).sort(), [historyDailyByMonth]);
  const historyMonthLabels = useMemo(() => {
    const months = new Set<string>();
    historyDailyMonths.forEach((key) => {
      const [, month] = key.split('-');
      if (month) months.add(month);
    });
    return Array.from(months).sort();
  }, [historyDailyMonths]);
  const [selectedHistoryMonthLabel, setSelectedHistoryMonthLabel] = useState<string | null>(null);
  const [historyChartMode, setHistoryChartMode] = useState<'monthly' | 'daily'>('monthly');

  useEffect(() => {
    if (historyMonthLabels.length === 0) {
      setSelectedHistoryMonthLabel(null);
      return;
    }
    if (selectedHistoryMonthLabel && historyMonthLabels.includes(selectedHistoryMonthLabel)) return;
    setSelectedHistoryMonthLabel(historyMonthLabels[0]);
  }, [historyMonthLabels, selectedHistoryMonthLabel]);

  useEffect(() => {
    if (historyChartMode === 'daily' && !selectedHistoryMonthLabel && historyMonthLabels.length > 0) {
      setSelectedHistoryMonthLabel(historyMonthLabels[0]);
    }
  }, [historyChartMode, historyMonthLabels, selectedHistoryMonthLabel]);

  const historySummaryByMonth = useMemo(() => {
    const data = weatherData?.weatherHistoricForecastDaily ?? [];
    const monthMap = new Map<string, DailyWeather[]>();
    data.forEach((entry) => {
      const dateKey = getJstDateKey(entry.date);
      const [year, month] = dateKey.split('-');
      if (!year || !month) return;
      const key = `${year}-${month}`;
      if (!monthMap.has(key)) monthMap.set(key, []);
      monthMap.get(key)?.push(entry);
    });

    const toFiniteNumber = (value: unknown): number | null => {
      if (value === null || value === undefined) return null;
      const num = typeof value === 'number' ? value : Number(value);
      return Number.isFinite(num) ? num : null;
    };

    const summarize = (days: DailyWeather[]) => {
      if (!days.length) return null;
      const acc = {
        minSum: 0,
        minCount: 0,
        maxSum: 0,
        maxCount: 0,
        avgSum: 0,
        avgCount: 0,
        precipSum: 0,
        sunshineSum: 0,
        humiditySum: 0,
        humidityCount: 0,
        windSum: 0,
        windCount: 0,
      };
      days.forEach((day) => {
        const minTemp = toFiniteNumber(day.airTempCMin);
        if (minTemp !== null) {
          acc.minSum += minTemp;
          acc.minCount += 1;
        }
        const maxTemp = toFiniteNumber(day.airTempCMax);
        if (maxTemp !== null) {
          acc.maxSum += maxTemp;
          acc.maxCount += 1;
        }
        const avgTemp = toFiniteNumber(day.airTempCAvg);
        if (avgTemp !== null) {
          acc.avgSum += avgTemp;
          acc.avgCount += 1;
        }
        const precipitation = toFiniteNumber(day.precipitationBestMm);
        if (precipitation !== null) {
          acc.precipSum += precipitation;
        }
        const sunshine = toFiniteNumber(day.sunshineDurationH);
        if (sunshine !== null) {
          acc.sunshineSum += sunshine;
        }
        const humidity = toFiniteNumber(day.relativeHumidityPctAvg);
        if (humidity !== null) {
          acc.humiditySum += humidity;
          acc.humidityCount += 1;
        }
        const windSpeed = toFiniteNumber(day.windSpeedMSAvg);
        if (windSpeed !== null) {
          acc.windSum += windSpeed;
          acc.windCount += 1;
        }
      });
      const round = (value: number) => Math.round(value * 10) / 10;
      return {
        avgMin: acc.minCount ? round(acc.minSum / acc.minCount) : null,
        avgAvg: acc.avgCount ? round(acc.avgSum / acc.avgCount) : null,
        avgMax: acc.maxCount ? round(acc.maxSum / acc.maxCount) : null,
        totalPrecip: round(acc.precipSum),
        totalSunshine: round(acc.sunshineSum),
        avgHumidity: acc.humidityCount ? round(acc.humiditySum / acc.humidityCount) : null,
        avgWind: acc.windCount ? round(acc.windSum / acc.windCount) : null,
      };
    };

    const summaryMap = new Map<string, ReturnType<typeof summarize>>();
    monthMap.forEach((entries, key) => {
      summaryMap.set(key, summarize(entries));
    });
    return summaryMap;
  }, [weatherData]);

  const toNumeric = (value: unknown): number | null => {
    if (value === null || value === undefined) return null;
    const num = typeof value === 'number' ? value : Number(value);
    return Number.isFinite(num) ? num : null;
  };

  const computeDrySunny = (day: DailyWeather) => {
    const sunshine = toNumeric(day.sunshineDurationH);
    const precip = toNumeric(day.precipitationBestMm);
    const humidity = toNumeric(day.relativeHumidityPctAvg);
    const isSunny = sunshine !== null && sunshine >= 5;
    const isDry = precip !== null && precip <= 1;
    const isMoist = humidity !== null && humidity >= 55 && humidity <= 85;
    return { isSunny, isDry, isMoist };
  };

  const historyYearKeys = useMemo(() => {
    const now = new Date();
    const { year } = getJstDateParts(now);
    if (!year) return [];
    const targetYears = [year - 4, year - 3, year - 2, year - 1, year].map(String);
    const years = new Set<string>();
    historySummaryByMonth.forEach((_summary, key) => {
      const [keyYear] = key.split('-');
      if (keyYear) years.add(keyYear);
    });
    return targetYears.filter((target) => years.has(target));
  }, [historySummaryByMonth]);

  const selectedDailyChartData = useMemo(() => {
    if (!selectedHistoryMonthLabel) return [];
    const dayMap = new Map<string, Record<string, number | string | boolean | null>>();
    historyYearKeys.forEach((year) => {
      const key = `${year}-${selectedHistoryMonthLabel}`;
      const entries = historyDailyByMonth.get(key) ?? [];
      const goodWindowDays = new Set<string>();
      entries.forEach((day, index) => {
        const current = computeDrySunny(day);
        const prev = index > 0 ? computeDrySunny(entries[index - 1]) : null;
        if (prev && current.isSunny && current.isDry && current.isMoist && prev.isSunny && prev.isDry && prev.isMoist) {
          const dateKey = getJstDateKey(day.date);
          const dayLabel = dateKey.split('-')[2] ?? dateKey;
          goodWindowDays.add(dayLabel);
        }
      });

      entries.forEach((day) => {
        const dateKey = getJstDateKey(day.date);
        const dayLabel = dateKey.split('-')[2] ?? dateKey;
        if (!dayMap.has(dayLabel)) dayMap.set(dayLabel, { label: dayLabel });
        const row = dayMap.get(dayLabel)!;
        row[`precip_${year}`] = toNumeric(day.precipitationBestMm);
        row[`temp_${year}`] = toNumeric(day.airTempCAvg);
        row[`good_${year}`] = goodWindowDays.has(dayLabel);
      });
    });

    return Array.from(dayMap.values()).sort((a, b) => Number(a.label) - Number(b.label));
  }, [historyDailyByMonth, historyYearKeys, selectedHistoryMonthLabel]);

  type HistorySummary = NonNullable<ReturnType<typeof historySummaryByMonth.get>>;
  const historyChartData = useMemo(() => {
    const monthData = new Map<string, Record<string, HistorySummary>>();
    historySummaryByMonth.forEach((summary, key) => {
      if (!summary) return;
      const [, month] = key.split('-');
      const year = key.split('-')[0];
      if (!month || !year) return;
      if (!monthData.has(month)) monthData.set(month, {});
      monthData.get(month)![year] = summary;
    });
    const months = Array.from(monthData.keys()).sort();
    return months.map((month) => {
      const row: Record<string, number | null | string> = { month };
      historyYearKeys.forEach((year) => {
        const summary = monthData.get(month)?.[year];
        row[`precip_${year}`] = summary?.totalPrecip ?? null;
        row[`temp_${year}`] = summary?.avgAvg ?? null;
      });
      return row;
    });
  }, [historySummaryByMonth, historyYearKeys]);

  return (
    <div className="weather-content">
      <div className="weather-section">
        <h3>過去の天気（前年・2年前の実績）</h3>
        <p className="weather-history-note">
          今日から過去5年分のデータを取得し、散布タスクの予定月に合わせて表示します。
          {!hasPlannedMonths && ' 散布予定がないため、当月を基準に表示しています。'}
        </p>
      </div>

      <div className="weather-section">
        <h3>散布タスクの予定月</h3>
        {plannedMonths.length === 0 ? (
          <p className="weather-history-empty">散布タスクの予定月が見つかりません。</p>
        ) : (
          <div className="weather-history-months">
            {plannedMonths.map(([monthKey, count]) => (
              <div key={monthKey} className="weather-history-month">
                <span>{monthKey}</span>
                <span>{count} 件</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="weather-section">
        <h3>月別の年比較グラフ（今年を含めて5年分）</h3>
        {historyChartData.length === 0 || historyYearKeys.length === 0 ? (
          <p className="weather-history-empty">グラフを表示できるデータがありません。</p>
        ) : (
          <div className="weather-history-chart">
            <div className="weather-history-chart-header">
              <h4>
                {historyChartMode === 'monthly'
                  ? '降水量（棒）＋平均気温（線）'
                  : `${selectedHistoryMonthLabel ?? ''}月 日別天気（複数年）`}
              </h4>
              {historyChartMode === 'daily' && (
                <button type="button" className="weather-history-back" onClick={() => setHistoryChartMode('monthly')}>
                  月別に戻る
                </button>
              )}
            </div>
            {historyChartMode === 'monthly' ? (
              <ResponsiveContainer width="100%" height={300}>
                <ComposedChart
                  data={historyChartData}
                  margin={{ top: 10, right: 20, left: 0, bottom: 0 }}
                  onClick={(event) => {
                    const label = (event as { activeLabel?: string })?.activeLabel;
                    if (!label) return;
                    setSelectedHistoryMonthLabel(label);
                    setHistoryChartMode('daily');
                  }}
                >
                  <CartesianGrid strokeDasharray="3 3" stroke="#4a4a4f" />
                  <XAxis dataKey="month" unit="月" stroke="#9e9e9e" />
                  <YAxis yAxisId="left" stroke="#82ca9d" />
                  <YAxis yAxisId="right" orientation="right" stroke="#ffb74d" />
                  <Tooltip />
                  <Legend />
                  {historyYearKeys.map((year, index) => (
                    <Bar
                      key={`precip-${year}`}
                      yAxisId="left"
                      dataKey={`precip_${year}`}
                      name={`${year}年 降水`}
                      fill={index % 2 === 0 ? '#5aa9ff' : '#8ad1ff'}
                    />
                  ))}
                  {historyYearKeys.map((year, index) => (
                    <Line
                      key={`temp-${year}`}
                      yAxisId="right"
                      type="monotone"
                      dataKey={`temp_${year}`}
                      name={`${year}年 気温`}
                      stroke={index % 2 === 0 ? '#ffb74d' : '#ffd59a'}
                      dot={false}
                    />
                  ))}
                </ComposedChart>
              </ResponsiveContainer>
            ) : (
              <ResponsiveContainer width="100%" height={320}>
                <ComposedChart data={selectedDailyChartData} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#4a4a4f" />
                  <XAxis dataKey="label" stroke="#9e9e9e" />
                  <YAxis yAxisId="left" stroke="#82ca9d" />
                  <YAxis yAxisId="right" orientation="right" stroke="#ffb74d" />
                  <Tooltip />
                  <Legend />
                  {historyYearKeys.map((year, index) => (
                    <Bar
                      key={`daily-precip-${year}`}
                      yAxisId="left"
                      dataKey={`precip_${year}`}
                      name={`${year}年 降水`}
                      fill={index % 2 === 0 ? '#5aa9ff' : '#8ad1ff'}
                    >
                      {selectedDailyChartData.map((entry, idx) => (
                        <Cell
                          key={`cell-${year}-${entry.label}-${idx}`}
                          fill={
                            entry[`good_${year}`]
                              ? '#6cc17a'
                              : index % 2 === 0
                                ? '#5aa9ff'
                                : '#8ad1ff'
                          }
                        />
                      ))}
                    </Bar>
                  ))}
                  {historyYearKeys.map((year, index) => (
                    <Line
                      key={`daily-temp-${year}`}
                      yAxisId="right"
                      type="monotone"
                      dataKey={`temp_${year}`}
                      name={`${year}年 気温`}
                      stroke={index % 2 === 0 ? '#ffb74d' : '#ffd59a'}
                      dot={false}
                    />
                  ))}
                </ComposedChart>
              </ResponsiveContainer>
            )}
          </div>
        )}
      </div>

      {historyChartMode === 'daily' && (
        <div className="weather-section">
          <h3>月別ドリルダウン（2日連続の晴天目安）</h3>
          {historyDailyMonths.length === 0 ? (
            <p className="weather-history-empty">日別データがありません。</p>
          ) : (
            <>
              <div className="weather-history-controls">
                <label htmlFor="history-month-select">対象月</label>
                <select
                  id="history-month-select"
                  value={selectedHistoryMonthLabel ?? ''}
                  onChange={(event) => setSelectedHistoryMonthLabel(event.target.value)}
                >
                  {historyMonthLabels.map((monthKey) => (
                    <option key={monthKey} value={monthKey}>
                      {monthKey}月
                    </option>
                  ))}
                </select>
              </div>
              <p className="weather-history-note">
                目安: 日照 5h 以上・降水 1mm 以下・湿度 55-85% が2日連続した場合をハイライト。
              </p>
            </>
          )}
        </div>
      )}

    </div>
  );
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
  const candidateDays = useMemo((): CandidateDay[] => {
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
      const dayTone: CandidateDay['dayTone'] = windows.some(w => w.type === 'recommended')
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
      {groupedData.map(([date, dayData], index) => {
        const prev1 = groupedData[index - 1]?.[1]?.daily;
        const prev2 = groupedData[index - 2]?.[1]?.daily;
        const soilStatus = estimateSoilStatus([dayData.daily, prev1, prev2].filter(Boolean) as DailyWeather[]);
        return (
          <DailyWeatherSummaryCard
            key={date}
            date={date}
            dayData={dayData}
            plannedTasks={plannedTasksByDate[date] ?? []}
            soilStatus={soilStatus}
            onOpenAdjustModal={() => {
              const tasks = plannedTasksDetail[date] ?? [];
              if (tasks.length === 0) return;
              setModalState({ dateKey: date, dayData, tasks, candidateDays });
            }}
          />
        );
      })}
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
  soilStatus: { label: string; tone: 'dry' | 'ok' | 'wet'; reason: string } | null;
  onOpenAdjustModal?: () => void;
}> = ({ date, dayData, plannedTasks, soilStatus, onOpenAdjustModal }) => {
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
          <div className="daily-summary-metrics">
            <p>
              気温: {dayData.daily.airTempCMin}°C / {dayData.daily.airTempCMax}°C
              <span style={{ margin: '0 1em' }}>|</span>
              降水: {dayData.daily.precipitationBestMm} mm
            </p>
            {soilStatus && (
              <span
                className={`soil-status-badge soil-status-badge--${soilStatus.tone}`}
                title={`土壌推定（目安）: ${soilStatus.reason}`}
              >
                土壌: {soilStatus.label}
              </span>
            )}
          </div>
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
  const { clearCombinedCache, fetchCombinedDataIfNeeded } = useFarms();
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
      clearCombinedCache();
      fetchCombinedDataIfNeeded({ includeTasks: true, force: true });
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
