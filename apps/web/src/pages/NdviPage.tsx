import { useEffect, useState, useMemo, useCallback, Fragment, memo, type ChangeEvent } from 'react';
import type { FC } from 'react';
import { useAuth } from '../context/AuthContext';
import { useFarms } from '../context/FarmContext';
import Select, { components, type OptionProps, type GroupBase, type MultiValue } from 'react-select';
import { useData } from '../context/DataContext';
import { Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Brush, Area, ComposedChart, Bar, Legend, Scatter } from 'recharts';
import { Bar as TimelineBar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip as ChartJSTooltip,
  Legend as ChartJSLegend,
  CategoryScale,
  LinearScale,
  BarElement,
  TimeScale,
  TimeSeriesScale,
  type ChartData,
  type Chart,
  type TooltipItem,
} from 'chart.js';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import zoomPlugin from 'chartjs-plugin-zoom';
import annotationPlugin from 'chartjs-plugin-annotation';
import 'chartjs-adapter-date-fns';
import { enUS, ja } from 'date-fns/locale';
import { format, startOfDay, subDays, addDays, differenceInCalendarDays } from 'date-fns';
import './FarmsPage.css'; // 共通スタイルをインポート
import './NdviPage.css'; // NDVIページ専用のスタイルをインポート
// FarmsPageから型定義をインポートまたは共有
import type { LoginAndTokenResp, Field, CropSeason, CountryCropGrowthStagePrediction } from '../types/farm';
import { withApiBase } from '../utils/apiBase';
import LoadingSpinner from '../components/LoadingSpinner';
import LoadingOverlay from '../components/LoadingOverlay';
import { formatCombinedLoadingMessage } from '../utils/loadingMessage';
import { useLanguage } from '../context/LanguageContext';
import { getCurrentLanguage, tr } from '../i18n/runtime';
import { postJsonCached } from '../utils/cachedJsonFetch';

ChartJS.register(
  ArcElement,
  ChartJSTooltip,
  ChartJSLegend,
  CategoryScale,
  LinearScale,
  BarElement,
  TimeScale,
  TimeSeriesScale,
  ChartDataLabels,
  zoomPlugin,
  annotationPlugin,
);

type WeatherMetricKey =
  | 'temperatureAvg'
  | 'temperatureMin'
  | 'temperatureMax'
  | 'sunshineHours'
  | 'precipitationMm'
  | 'humidityAvg'
  | 'windSpeedAvg'
  | 'gdd';

const WEATHER_METRICS_CONFIG: Array<{
  key: WeatherMetricKey;
  labelKey: string;
  color: string;
  type: 'line' | 'area' | 'bar';
  yAxisId: 'temp' | 'precip' | 'humidity' | 'wind' | 'sunshine' | 'degreeDays';
  strokeDasharray?: string;
  unit: string;
}> = [
  { key: 'temperatureAvg', labelKey: 'ndvi.weather.temperature_avg', color: '#ff7043', type: 'line', yAxisId: 'temp', unit: '°C' },
  { key: 'temperatureMax', labelKey: 'ndvi.weather.temperature_max', color: '#ff8a65', type: 'line', yAxisId: 'temp', strokeDasharray: '4 4', unit: '°C' },
  { key: 'temperatureMin', labelKey: 'ndvi.weather.temperature_min', color: '#4fc3f7', type: 'line', yAxisId: 'temp', strokeDasharray: '4 4', unit: '°C' },
  { key: 'sunshineHours', labelKey: 'ndvi.weather.sunshine', color: '#ffd54f', type: 'area', yAxisId: 'sunshine', unit: 'h' },
  { key: 'precipitationMm', labelKey: 'ndvi.weather.precipitation', color: '#4fc3f7', type: 'bar', yAxisId: 'precip', unit: 'mm' },
  { key: 'humidityAvg', labelKey: 'ndvi.weather.humidity', color: '#64b5f6', type: 'line', yAxisId: 'humidity', unit: '%' },
  { key: 'windSpeedAvg', labelKey: 'ndvi.weather.wind_speed', color: '#9575cd', type: 'line', yAxisId: 'wind', strokeDasharray: '6 3', unit: 'm/s' },
  { key: 'gdd', labelKey: 'ndvi.weather.gdd', color: '#ffa726', type: 'line', yAxisId: 'degreeDays', unit: '°C·d' },
];

const WIND_DIRECTIONS_JA = [
  '北', '北北東', '北東', '東北東', '東', '東南東', '南東', '南南東',
  '南', '南南西', '南西', '西南西', '西', '西北西', '北西', '北北西',
];

const WIND_DIRECTIONS_EN = [
  'N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
  'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW',
];

const formatWindDirection = (deg: number | null): string => {
  if (deg === null || !Number.isFinite(deg)) return '-';
  const normalized = ((deg % 360) + 360) % 360;
  const index = Math.round(normalized / 22.5) % 16;
  const dirs = getCurrentLanguage() === 'ja' ? WIND_DIRECTIONS_JA : WIND_DIRECTIONS_EN;
  return `${dirs[index]} (${Math.round(normalized)}°)`;
};

async function fetchBiomassNdviApi(params: { auth: LoginAndTokenResp; cropSeasonUuids: string[]; fromDate: string }): Promise<any> {
  const cacheKey = `biomass-ndvi:${[...params.cropSeasonUuids].sort().join(',')}|${params.fromDate}`;
  const requestBody = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    crop_season_uuids: params.cropSeasonUuids,
    from_date: params.fromDate,
  };

  const { status, json, source } = await postJsonCached<any>(
    withApiBase('/biomass-ndvi'),
    requestBody,
    undefined,
    { cacheKey, cache: params.cropSeasonUuids.length > 0 ? 'session' : 'none' },
  );

  if (!json) {
    return { ok: false, status, response_text: 'Empty response from server', source };
  }
  if (typeof json === 'string') {
    return { ok: false, status, detail: json, source };
  }
  return { ...json, source };
}

async function fetchBiomassLaiApi(params: { auth: LoginAndTokenResp; cropSeasonUuids: string[]; fromDate: string; tillDate: string }): Promise<any> {
  const cacheKey = `biomass-lai:${[...params.cropSeasonUuids].sort().join(',')}|${params.fromDate}|${params.tillDate}`;
  const requestBody = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    crop_season_uuids: params.cropSeasonUuids,
    from_date: params.fromDate,
    till_date: params.tillDate,
  };

  const { status, json, source } = await postJsonCached<any>(
    withApiBase('/biomass-lai'),
    requestBody,
    undefined,
    { cacheKey, cache: params.cropSeasonUuids.length > 0 ? 'session' : 'none' },
  );
  if (!json) {
    return { ok: false, status, detail: 'Empty response from server', source };
  }
  if (typeof json === 'string') {
    return { ok: false, status, detail: json, source };
  }
  return { ...json, source };
}

async function fetchWeatherByFieldApi(params: { auth: LoginAndTokenResp; fieldUuid: string; fromDate: string; tillDate: string }): Promise<any> {
  const cacheKey = `weather-by-field:${params.fieldUuid}|${params.fromDate}|${params.tillDate}`;
  const requestBody = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    field_uuid: params.fieldUuid,
    from_date: params.fromDate,
    till_date: params.tillDate,
  };
  const { status, json, source } = await postJsonCached<any>(
    withApiBase('/weather-by-field'),
    requestBody,
    undefined,
    { cacheKey, cache: 'session' },
  );

  if (!json) {
    return { ok: true, status, response: { data: { fieldV2: {} } }, source };
  }
  if (typeof json === 'string') {
    return { ok: false, status, detail: json, source };
  }
  return { ...json, source };
}

type FieldWithSeasons = {
  fieldUuid: string;
  fieldName: string;
  fieldArea: number;
  seasons: CropSeason[];
}

interface SelectOption {
    value: string; // cropSeasonUuid
    label: string;
    fieldName: string;
    fieldUuid: string;
}

interface BiomassRecord {
  uuid?: string;
  average: number | null;
  cropSeasonUuid: string;
  acquisitionDate: string | null;
}

interface WeatherDailyAggregate {
  isoDate: string;
  timestamp: number;
  temperatureMin: number | null;
  temperatureMax: number | null;
  temperatureAvg: number | null;
  sunshineHours: number | null;
  precipitationMm: number | null;
  humidityAvg: number | null;
  windSpeedAvg: number | null;
  windDirectionDeg: number | null;
  gdd?: number | null;
}

interface FieldWeatherDaily extends WeatherDailyAggregate {
  fieldUuid: string;
  fieldName: string;
}

interface NdviDetailRow {
  seasonUuid: string;
  fieldUuid: string;
  fieldName: string;
  area: number;
  cropName: string;
  varietyName: string;
  seasonStartDate: string;
  seasonStartEpoch: number;
  cropEstablishment: string;
  date: string;
  dateEpoch: number;
  ndvi?: number | null;
  lai?: number | null;
  accumulatedTemperature?: number | null;
  accumulatedTemperatureBase?: number | null;
  bbch: string;
  bbchIndex: string;
  bbchIndexNum?: number | null;
  weatherDateIso: string;
  weatherTemperatureAvg?: number | null;
  weatherPrecipitation?: number | null;
  weatherSunshineHours?: number | null;
  weatherHumidity?: number | null;
  weatherWindSpeed?: number | null;
  weatherWindDirectionDeg?: number | null;
  weatherWindDirection: string;
  cumulativePrecip?: number | null;
}

type TaskEvent = {
  date: number;
  label: string;
  type: string;
  fieldName: string;
  seasonUuid: string;
  hint?: string | null;
  products?: { name: string | null; totalApplication: number | null; unit: string | null }[];
};

const SPRAY_MARK_STYLES: Record<string, { label: string; color: string; shape: 'triangle' | 'diamond' | 'square' | 'circle' | 'cross' | 'star' | 'wye' }> = {
  CROP_PROTECTION: { label: '防除', color: '#ff7043', shape: 'triangle' },
  NUTRITION_MANAGEMENT: { label: '施肥', color: '#81c784', shape: 'diamond' },
  WEED_MANAGEMENT: { label: '除草', color: '#4db6ac', shape: 'wye' },
};

interface ChartSeries {
  name: string;
  dataKey: string;
  metric: 'ndvi' | 'lai';
  color: string;
  seasonUuid: string;
}

type BrushChangeEvent = {
  startIndex?: number;
  endIndex?: number;
};

type SelectedPrediction = {
  seasonUuid: string;
  fieldName: string;
  cropName: string;
  varietyName: string;
  seasonStartDate: string;
  predictions: CountryCropGrowthStagePrediction[];
};

type TimelineDataset = {
  label: string;
  data: any[];
  bbchIndex: string;
  backgroundColor: string;
  barPercentage: number;
  categoryPercentage: number;
};

// =============================================================================
// Custom Hooks
// =============================================================================

const hexToRgba = (hex: string, alpha: number): string => {
  if (!hex) return `rgba(100, 108, 255, ${alpha})`;
  const normalized = hex.replace('#', '');
  if (normalized.length !== 6) {
    return `rgba(100, 108, 255, ${alpha})`;
  }
  const r = parseInt(normalized.slice(0, 2), 16);
  const g = parseInt(normalized.slice(2, 4), 16);
  const b = parseInt(normalized.slice(4, 6), 16);
  if (Number.isNaN(r) || Number.isNaN(g) || Number.isNaN(b)) {
    return `rgba(100, 108, 255, ${alpha})`;
  }
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
};

/**
 * 圃場と作期のデータを整形して選択肢として提供するフック
 */
const useFieldSeasonOptions = () => {
  const { combinedOut } = useData();

  const fieldSeasons: FieldWithSeasons[] = useMemo(() => {
    if (!combinedOut) return [];
    const fields: Field[] = combinedOut.response?.data?.fieldsV2 ?? [];
    return fields
      .map(field => ({
        fieldUuid: field.uuid,
        fieldName: field.name,
        fieldArea: field.area ?? 0,
        seasons: field.cropSeasonsV2?.filter((season: CropSeason) => season.uuid && season.startDate) ?? [],
      }))
      .filter(f => f.seasons.length > 0);
  }, [combinedOut]);

  const groupedOptions: GroupBase<SelectOption>[] = useMemo(() => {
    const allSeasonPairs = fieldSeasons.flatMap(field =>
      field.seasons.map(season => ({ field, season }))
    );

    const seasonsByCrop = allSeasonPairs.reduce((acc, { field, season }) => {
      const cropName = season.crop.name;
      if (!acc[cropName]) acc[cropName] = [];
      acc[cropName].push({ field, season });
      return acc;
    }, {} as Record<string, { field: FieldWithSeasons; season: CropSeason }[]>);

    return Object.entries(seasonsByCrop).map(([cropName, pairs]) => ({
      label: cropName,
      options: pairs.map(({ field, season }) => ({
        value: season.uuid,
        label: `${field.fieldName} (${season.variety.name})`,
        fieldName: field.fieldName,
        fieldUuid: field.fieldUuid,
      })),
    }));
  }, [fieldSeasons]);

  return { groupedOptions, fieldSeasons };
};

/**
 * NDVI/LAIデータの取得とキャッシュ管理を行うフック
 */
type BiomassFetchResult = {
  fromDate: string;
  tillDate: string;
  selectedSeasonUuids: string[];
  seasonFieldMeta: Record<string, { fieldUuid: string; fieldName: string; fieldArea: number }>;
};

const useBiomassData = (selectedOptions: MultiValue<SelectOption>, fieldSeasons: FieldWithSeasons[]) => {
  const { auth } = useAuth();
  const [ndviData, setNdviData] = useState<BiomassRecord[] | null>(null);
  const [laiData, setLaiData] = useState<BiomassRecord[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dataSource, setDataSource] = useState<'api' | 'cache' | null>(null);

  const handleFetchBiomass = useCallback(async (): Promise<BiomassFetchResult | null> => {
    if (!auth || selectedOptions.length === 0) return null;

    const hasCache = ndviData && laiData;
    if (hasCache) {
      const currentUuids = selectedOptions.map(opt => opt.value).sort();
      const cachedUuids = [...new Set(ndviData!.map(rec => rec.cropSeasonUuid))].sort();
      if (JSON.stringify(currentUuids) === JSON.stringify(cachedUuids)) {
        setDataSource('cache');
        const selectedSet = new Set(currentUuids);
        const seasonFieldMeta = fieldSeasons.reduce((acc, field) => {
          field.seasons.forEach(season => {
            if (!selectedSet.has(season.uuid)) return;
            acc[season.uuid] = {
              fieldUuid: field.fieldUuid,
              fieldName: field.fieldName,
              fieldArea: field.fieldArea,
            };
          });
          return acc;
        }, {} as Record<string, { fieldUuid: string; fieldName: string; fieldArea: number }>);

        return {
          fromDate: ndviData?.[0]?.acquisitionDate ?? new Date().toISOString(),
          tillDate: ndviData?.[ndviData.length - 1]?.acquisitionDate ?? new Date().toISOString(),
          selectedSeasonUuids: currentUuids,
          seasonFieldMeta,
        };
      }
    }

    setLoading(true);
    setError(null);
    setNdviData(null);
    setLaiData(null);
    try {
      if (!fieldSeasons || fieldSeasons.length === 0) throw new Error("圃場データがまだ読み込まれていません。");

      const selectedSeasonUuids = selectedOptions.map(opt => opt.value);
      const allSeasons = fieldSeasons.flatMap(f => f.seasons);
      const selectedSeasons = allSeasons.filter(s => selectedSeasonUuids.includes(s.uuid));
      if (selectedSeasons.length === 0) throw new Error("選択された作期が圃場データ内に見つかりません。");

      const fromDate = selectedSeasons.map(s => s.startDate).sort()[0];
      const tillDate = new Date().toISOString();

      const selectedSet = new Set(selectedSeasonUuids);
      const seasonFieldMeta = fieldSeasons.reduce((acc, field) => {
        field.seasons.forEach(season => {
          if (!selectedSet.has(season.uuid)) return;
          acc[season.uuid] = {
            fieldUuid: field.fieldUuid,
            fieldName: field.fieldName,
            fieldArea: field.fieldArea,
          };
        });
        return acc;
      }, {} as Record<string, { fieldUuid: string; fieldName: string; fieldArea: number }>);

      const [ndviResult, laiResult] = await Promise.all([
        fetchBiomassNdviApi({ auth, cropSeasonUuids: selectedSeasonUuids, fromDate }),
        fetchBiomassLaiApi({ auth, cropSeasonUuids: selectedSeasonUuids, fromDate, tillDate }),
      ]);

      if (!ndviResult.ok) {
        throw new Error(ndviResult.response_text || `NDVIデータの取得に失敗しました (status: ${ndviResult.status})`);
      }
      if (!laiResult.ok) {
        throw new Error(laiResult.detail || `LAIデータの取得に失敗しました (status: ${laiResult.status})`);
      }

      // If one source is API and the other is cache, we can just say API.
      // If both are cache, it's cache.
      setDataSource(ndviResult.source ?? 'api');
      setNdviData(ndviResult.response?.data?.biomassAnalysisNdvi ?? []);
      setLaiData(laiResult.response?.data?.biomassAnalysis ?? []);
      return {
        fromDate,
        tillDate,
        selectedSeasonUuids,
        seasonFieldMeta,
      };
    } catch (e: any) {
      setError(e.message || "NDVIデータの取得中にエラーが発生しました。");
      return null;
    } finally {
      setLoading(false);
    }
  }, [auth, selectedOptions, ndviData, laiData, fieldSeasons]);

  const reset = useCallback(() => {
    setNdviData(null);
    setLaiData(null);
    setDataSource(null);
    setError(null);
  }, []);

  return { ndviData, laiData, loading, error, dataSource, handleFetchBiomass, reset };
};

export function NdviPage() {
  const { auth } = useAuth();
  const { language, t } = useLanguage();
  const { submittedFarms, fetchCombinedDataIfNeeded } = useFarms();
  const {
    combinedOut,
    combinedLoading,
    combinedErr,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();

  const [selectedOptions, setSelectedOptions] = useState<MultiValue<SelectOption>>([]);
  const [sortConfig, setSortConfig] = useState<{ key: keyof NdviDetailRow; direction: 'ascending' | 'descending' } | null>({ key: 'dateEpoch', direction: 'descending' });
  const [highlightedSeason, setHighlightedSeason] = useState<string | null>(null);
  const [visibleSeries, setVisibleSeries] = useState<Record<string, boolean>>({});
  const [brushRange, setBrushRange] = useState<{ startIndex: number; endIndex: number } | null>(null);
  const [syncedRange, setSyncedRange] = useState<{ min: number; max: number } | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const rowsPerPage = 10;
  const [weatherData, setWeatherData] = useState<WeatherDailyAggregate[] | null>(null);
  const [fieldWeatherLookup, setFieldWeatherLookup] = useState<Record<string, Record<string, FieldWeatherDaily>>>({});
  const [weatherLoading, setWeatherLoading] = useState(false);
  const [weatherError, setWeatherError] = useState<string | null>(null);
  const [weatherCacheKey, setWeatherCacheKey] = useState<string | null>(null);
  const [visibleWeatherMetrics, setVisibleWeatherMetrics] = useState<Record<WeatherMetricKey, boolean>>({
    temperatureAvg: true,
    temperatureMin: false,
    temperatureMax: false,
    sunshineHours: true,
    precipitationMm: true,
    humidityAvg: true,
    windSpeedAvg: true,
    gdd: false,
  });
  const [gddStartDate, setGddStartDate] = useState<string | null>(null);
  const [gddDateBounds, setGddDateBounds] = useState<{ min: string; max: string } | null>(null);
  
  const { groupedOptions, fieldSeasons } = useFieldSeasonOptions();
  const { ndviData, laiData, loading: biomassLoading, error: biomassError, dataSource: biomassDataSource, handleFetchBiomass, reset: resetBiomass } = useBiomassData(selectedOptions, fieldSeasons);
  const showGlobalLoading = biomassLoading || weatherLoading || combinedLoading;
  const combinedLoadingMessage = formatCombinedLoadingMessage(
    t('label.data'),
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  );
  const overlayMessage = combinedLoading
    ? combinedLoadingMessage
    : biomassLoading
    ? t('ndvi.loading.remote_sensing')
    : weatherLoading
    ? t('ndvi.loading.weather')
    : t('ndvi.loading.data');

  const weatherChartData = useMemo(() => {
    if (!weatherData) return [];
    const sorted = [...weatherData].sort((a, b) => a.timestamp - b.timestamp);
    const startTimestamp = gddStartDate
      ? Date.parse(`${gddStartDate}T00:00:00Z`)
      : null;
    let cumulativeGdd = 0;
    return sorted.map(entry => {
      let gddValue: number | null = null;
      if (startTimestamp !== null) {
        if (entry.timestamp >= startTimestamp) {
          if (entry.temperatureAvg !== null && entry.temperatureAvg !== undefined) {
            cumulativeGdd += Math.max(0, entry.temperatureAvg);
          }
          gddValue = cumulativeGdd;
        }
      }
      return {
        timestamp: entry.timestamp,
        isoDate: entry.isoDate,
        dateLabel: new Date(entry.timestamp).toLocaleDateString('ja-JP'),
        temperatureMin: entry.temperatureMin,
        temperatureMax: entry.temperatureMax,
        temperatureAvg: entry.temperatureAvg,
        sunshineHours: entry.sunshineHours,
        precipitationMm: entry.precipitationMm,
        humidityAvg: entry.humidityAvg,
        windSpeedAvg: entry.windSpeedAvg,
        windDirectionDeg: entry.windDirectionDeg,
        gdd: gddValue,
      };
    });
  }, [weatherData, gddStartDate]);

  const gddLookup = useMemo(() => {
    const map = new Map<string, number>();
    weatherChartData.forEach(entry => {
      if (entry.isoDate && entry.gdd !== null && entry.gdd !== undefined) {
        map.set(entry.isoDate, entry.gdd);
      }
    });
    return map;
  }, [weatherChartData]);
  const { chartData, series } = useMemo<{
    chartData: Array<Record<string, any>>;
    series: ChartSeries[];
  }>(() => {
    if ((!ndviData || ndviData.length === 0) && (!laiData || laiData.length === 0)) {
      return { chartData: [], series: [] };
    }

    const dataByDate: Record<string, any> = {};
    const chartSeries: ChartSeries[] = [];
    const ndviPalette = ['#8884d8', '#00bcd4', '#ff9800', '#0088FE', '#a1887f', '#8e24aa'];
    const laiPalette = ['#82ca9d', '#00C49F', '#2ca882', '#66bb6a', '#43a047', '#2e7d32'];
    const weatherLookup = new Map<string, WeatherDailyAggregate>();
    weatherChartData.forEach(w => {
      weatherLookup.set(w.isoDate, w);
    });

    (ndviData || []).forEach((item: BiomassRecord) => {
      if (!item.acquisitionDate) return;
      const date = new Date(item.acquisitionDate).getTime();
      if (!Number.isFinite(date)) return;
      if (!dataByDate[date]) {
        dataByDate[date] = { acquisitionDate: date };
      }
      dataByDate[date][`${item.cropSeasonUuid}_ndvi`] = item.average ?? null;
    });

    (laiData || []).forEach((item: BiomassRecord) => {
      if (!item.acquisitionDate) return;
      const date = new Date(item.acquisitionDate).getTime();
      if (!Number.isFinite(date)) return;
      if (!dataByDate[date]) {
        dataByDate[date] = { acquisitionDate: date };
      }
      dataByDate[date][`${item.cropSeasonUuid}_lai`] = item.average ?? null;
    });

    // 天気を統合（NDVIがない日も表示用に入れる）
    weatherChartData.forEach(w => {
      const date = w.timestamp;
      if (!Number.isFinite(date)) return;
      if (!dataByDate[date]) {
        dataByDate[date] = { acquisitionDate: date };
      }
      const entry = dataByDate[date];
      entry.weatherPrecipitation = w.precipitationMm ?? null;
      entry.weatherTemperatureAvg = w.temperatureAvg ?? null;
    });

    selectedOptions.forEach((opt, index) => {
      chartSeries.push({
        name: `${opt.label} (NDVI)`,
        dataKey: `${opt.value}_ndvi`,
        metric: 'ndvi',
        color: ndviPalette[index % ndviPalette.length],
        seasonUuid: opt.value,
      });
      chartSeries.push({
        name: `${opt.label} (LAI)`,
        dataKey: `${opt.value}_lai`,
        metric: 'lai',
        color: laiPalette[index % laiPalette.length],
        seasonUuid: opt.value,
      });
    });

    const chartData = Object.values(dataByDate).sort((a, b) => a.acquisitionDate - b.acquisitionDate);
    const filledChartData = chartData.map((entry) => {
      const next = { ...entry } as Record<string, any>;
      chartSeries.forEach(seriesInfo => {
        if (!(seriesInfo.dataKey in next)) {
          next[seriesInfo.dataKey] = null;
        }
      });
      const iso = new Date(next.acquisitionDate).toISOString().split('T')[0];
      const weather = weatherLookup.get(iso);
      if (weather) {
        next.weatherPrecipitation = weather.precipitationMm ?? null;
        next.weatherTemperatureAvg = weather.temperatureAvg ?? null;
      } else {
        next.weatherPrecipitation ??= null;
        next.weatherTemperatureAvg ??= null;
      }
      return next;
    });

    return { chartData: filledChartData, series: chartSeries };
  }, [ndviData, laiData, selectedOptions, weatherChartData]);

  // 農場選択が変更されたら、選択をリセット
  useEffect(() => {
    setSelectedOptions([]);
    resetBiomass();
    setSyncedRange(null);
    setWeatherData(null);
    setWeatherError(null);
    setWeatherLoading(false);
    setWeatherCacheKey(null);
    setGddStartDate(null);
    setGddDateBounds(null);
    setFieldWeatherLookup({});
  }, [submittedFarms, resetBiomass]);

  useEffect(() => {
    // NDVIデータがリセットされたら、チャートの表示範囲もリセット
    if (!ndviData) {
      setSyncedRange(null);
    }
  }, [ndviData]);

  useEffect(() => {
    if (selectedOptions.length === 0) {
      setWeatherData(null);
      setWeatherCacheKey(null);
      setGddStartDate(null);
      setGddDateBounds(null);
      setFieldWeatherLookup({});
    }
  }, [selectedOptions]);

  useEffect(() => {
    if (!chartData.length) {
      setBrushRange(null);
      return;
    }

    if (!syncedRange) {
      setBrushRange(prev => {
        const defaultRange = { startIndex: 0, endIndex: chartData.length - 1 };
        if (prev && prev.startIndex === defaultRange.startIndex && prev.endIndex === defaultRange.endIndex) {
          return prev;
        }
        return defaultRange;
      });
      return;
    }

    let startIndex = chartData.findIndex(entry => entry.acquisitionDate >= syncedRange.min);
    if (startIndex < 0) startIndex = 0;

    let endIndex = chartData.findIndex(entry => entry.acquisitionDate > syncedRange.max);
    if (endIndex < 0) {
      endIndex = chartData.length - 1;
    } else {
      endIndex = Math.max(startIndex, endIndex - 1);
    }

    setBrushRange(prev => {
      if (prev && prev.startIndex === startIndex && prev.endIndex === endIndex) {
        return prev;
      }
      return { startIndex, endIndex };
    });
  }, [chartData, syncedRange]);

  useEffect(() => {
    setVisibleSeries(prev => {
      const next: Record<string, boolean> = {};
      series.forEach(s => {
        next[s.dataKey] = prev[s.dataKey] ?? true;
      });
      const prevKeys = Object.keys(prev);
      const nextKeys = Object.keys(next);
      if (
        prevKeys.length === nextKeys.length &&
        nextKeys.every(key => prev[key] === next[key])
      ) {
        return prev;
      }
      return next;
    });
  }, [series]);

  const handleBrushChange = useCallback((range?: BrushChangeEvent) => {
    if (!chartData.length) return;
    const startIndex = range && typeof range.startIndex === 'number' ? range.startIndex : 0;
    const endIndex = range && typeof range.endIndex === 'number' ? range.endIndex : chartData.length - 1;
    const boundedStart = Math.max(0, Math.min(chartData.length - 1, startIndex));
    const boundedEnd = Math.max(boundedStart, Math.min(chartData.length - 1, endIndex));
    const startPoint = chartData[boundedStart];
    const endPoint = chartData[boundedEnd];
    if (!startPoint || !endPoint) return;

    const min = startPoint.acquisitionDate;
    const max = endPoint.acquisitionDate;
    setSyncedRange(prev => {
      if (prev && prev.min === min && prev.max === max) {
        return prev;
      }
      return { min, max };
    });
  }, [chartData, setSyncedRange]);

  const toggleSeriesVisibility = useCallback((dataKey: string) => {
    setVisibleSeries(prev => ({
      ...prev,
      [dataKey]: !(prev[dataKey] ?? true),
    }));
  }, []);

  const setAllVisibility = useCallback((visible: boolean) => {
    setVisibleSeries(() => {
      const next: Record<string, boolean> = {};
      series.forEach(s => {
        next[s.dataKey] = visible;
      });
      return next;
    });
  }, [series]);

  const showOnlyMetric = useCallback((metric: 'ndvi' | 'lai') => {
    setVisibleSeries(() => {
      const next: Record<string, boolean> = {};
      series.forEach(s => {
        next[s.dataKey] = s.metric === metric;
      });
      return next;
    });
  }, [series]);

  const toggleWeatherMetric = useCallback((metric: WeatherMetricKey) => {
    setVisibleWeatherMetrics(prev => ({
      ...prev,
      [metric]: !prev[metric],
    }));
  }, []);

  const setAllWeatherMetrics = useCallback((value: boolean) => {
    setVisibleWeatherMetrics(prev => {
      const next = { ...prev } as Record<WeatherMetricKey, boolean>;
      (Object.keys(next) as WeatherMetricKey[]).forEach(key => {
        next[key] = value;
      });
      return next;
    });
  }, []);

  const clampDateWithinBounds = useCallback(
    (value: string | null): string | null => {
      if (!gddDateBounds) return value;
      if (!value) return gddDateBounds.min;
      if (value < gddDateBounds.min) return gddDateBounds.min;
      if (value > gddDateBounds.max) return gddDateBounds.max;
      return value;
    },
    [gddDateBounds],
  );

  const handleGddStartDateInput = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      const raw = event.target.value || null;
      const clamped = clampDateWithinBounds(raw);
      setGddStartDate(clamped);
    },
    [clampDateWithinBounds],
  );

  const fetchWeatherForSelection = useCallback(async (meta: BiomassFetchResult | null) => {
    if (!auth) return;
    if (!meta) {
      setWeatherData(null);
      setFieldWeatherLookup({});
      setWeatherCacheKey(null);
      setGddDateBounds(null);
      setGddStartDate(null);
      return;
    }

    const toDateOnly = (value: string | null | undefined) => {
      if (!value) return null;
      const parts = value.split('T');
      return parts[0] || null;
    };
    const minDateIso = toDateOnly(meta.fromDate);
    const maxDateIso = toDateOnly(meta.tillDate);
    if (minDateIso && maxDateIso) {
      setGddDateBounds({ min: minDateIso, max: maxDateIso });
      setGddStartDate(prev => {
        if (!prev) return minDateIso;
        if (prev < minDateIso) return minDateIso;
        if (prev > maxDateIso) return maxDateIso;
        return prev;
      });
    } else {
      setGddDateBounds(null);
      setGddStartDate(null);
    }

    const fieldMetaMap = new Map<string, { fieldName: string; fieldArea: number }>();
    meta.selectedSeasonUuids.forEach(seasonUuid => {
      const info = meta.seasonFieldMeta[seasonUuid];
      if (info) {
        fieldMetaMap.set(info.fieldUuid, { fieldName: info.fieldName, fieldArea: info.fieldArea });
      }
    });

    if (fieldMetaMap.size === 0) {
      setWeatherData(null);
      setFieldWeatherLookup({});
      setWeatherCacheKey(null);
      return;
    }

    const cacheKey = `${meta.fromDate}|${meta.tillDate}|${[...fieldMetaMap.keys()].sort().join(',')}`;
    if (
      weatherCacheKey === cacheKey &&
      weatherData && weatherData.length > 0 &&
      fieldWeatherLookup && Object.keys(fieldWeatherLookup).length > 0
    ) {
      return;
    }

    setWeatherLoading(true);
    setWeatherError(null);

    try {
      const responses = await Promise.all(
        [...fieldMetaMap.entries()].map(async ([fieldUuid, info]) => {
          const res = await fetchWeatherByFieldApi({ auth, fieldUuid, fromDate: meta.fromDate, tillDate: meta.tillDate });
          if (!res.ok) {
            throw new Error(res.detail || `天気データの取得に失敗しました (${info.fieldName})`);
          }
          const fieldV2 = res.response?.data?.fieldV2;
          const daily = fieldV2?.weatherHistoricForecastDaily ?? [];
          return { fieldUuid, fieldName: info.fieldName, daily };
        })
      );

      const perFieldLookup: Record<string, Record<string, FieldWeatherDaily>> = {};

      type AggregateEntry = {
        tempMinSum: number; tempMinCount: number;
        tempMaxSum: number; tempMaxCount: number;
        tempAvgSum: number; tempAvgCount: number;
        sunshineSum: number; sunshineCount: number;
        precipSum: number; precipCount: number;
        humiditySum: number; humidityCount: number;
        windSpeedSum: number; windSpeedCount: number;
        windDirX: number; windDirY: number; windDirCount: number;
      };

      const aggregateMap = new Map<string, AggregateEntry>();

      const ensureEntry = (date: string): AggregateEntry => {
        if (!aggregateMap.has(date)) {
          aggregateMap.set(date, {
            tempMinSum: 0, tempMinCount: 0,
            tempMaxSum: 0, tempMaxCount: 0,
            tempAvgSum: 0, tempAvgCount: 0,
            sunshineSum: 0, sunshineCount: 0,
            precipSum: 0, precipCount: 0,
            humiditySum: 0, humidityCount: 0,
            windSpeedSum: 0, windSpeedCount: 0,
            windDirX: 0, windDirY: 0, windDirCount: 0,
          });
        }
        return aggregateMap.get(date)!;
      };

      responses.forEach(({ fieldUuid, fieldName, daily }) => {
        if (!perFieldLookup[fieldUuid]) {
          perFieldLookup[fieldUuid] = {};
        }
        daily.forEach((day: any) => {
          const isoDate = day?.date;
          if (!isoDate) return;
          const entry = ensureEntry(isoDate);

          const addValue = (raw: any, target: keyof AggregateEntry) => {
            if (raw === null || raw === undefined || raw === '') return;
            const num = Number(raw);
            if (!Number.isFinite(num)) return;
            switch (target) {
              case 'tempMinSum': entry.tempMinSum += num; entry.tempMinCount += 1; break;
              case 'tempMaxSum': entry.tempMaxSum += num; entry.tempMaxCount += 1; break;
              case 'tempAvgSum': entry.tempAvgSum += num; entry.tempAvgCount += 1; break;
              case 'sunshineSum': entry.sunshineSum += num; entry.sunshineCount += 1; break;
              case 'precipSum': entry.precipSum += num; entry.precipCount += 1; break;
              case 'humiditySum': entry.humiditySum += num; entry.humidityCount += 1; break;
              case 'windSpeedSum': entry.windSpeedSum += num; entry.windSpeedCount += 1; break;
              default: break;
            }
          };

          addValue(day.airTempCMin, 'tempMinSum');
          addValue(day.airTempCMax, 'tempMaxSum');
          addValue(day.airTempCAvg, 'tempAvgSum');
          addValue(day.sunshineDurationH, 'sunshineSum');
          addValue(day.precipitationBestMm, 'precipSum');
          addValue(day.relativeHumidityPctAvg, 'humiditySum');
          addValue(day.windSpeedMSAvg, 'windSpeedSum');

          const windDir = Number(day.windDirectionDeg);
          if (Number.isFinite(windDir)) {
            const rad = (windDir * Math.PI) / 180;
            entry.windDirX += Math.cos(rad);
            entry.windDirY += Math.sin(rad);
            entry.windDirCount += 1;
          }

          const timestamp = Date.parse(`${isoDate}T00:00:00Z`);
          perFieldLookup[fieldUuid][isoDate] = {
            fieldUuid,
            fieldName,
            isoDate,
            timestamp: Number.isFinite(timestamp) ? timestamp : Date.now(),
            temperatureMin: day.airTempCMin != null ? Number(day.airTempCMin) : null,
            temperatureMax: day.airTempCMax != null ? Number(day.airTempCMax) : null,
            temperatureAvg: day.airTempCAvg != null ? Number(day.airTempCAvg) : null,
            sunshineHours: day.sunshineDurationH != null ? Number(day.sunshineDurationH) : null,
            precipitationMm: day.precipitationBestMm != null ? Number(day.precipitationBestMm) : null,
            humidityAvg: day.relativeHumidityPctAvg != null ? Number(day.relativeHumidityPctAvg) : null,
            windSpeedAvg: day.windSpeedMSAvg != null ? Number(day.windSpeedMSAvg) : null,
            windDirectionDeg: Number.isFinite(windDir) ? windDir : null,
          };
        });
      });

      const aggregated = Array.from(aggregateMap.entries())
        .map(([isoDate, entry]) => {
          const timestamp = Date.parse(`${isoDate}T00:00:00Z`);
          if (!Number.isFinite(timestamp)) return null;
          const windDirectionDeg = entry.windDirCount > 0
            ? ((Math.atan2(entry.windDirY, entry.windDirX) * 180) / Math.PI + 360) % 360
            : null;
          return {
            isoDate,
            timestamp,
            temperatureMin: entry.tempMinCount > 0 ? entry.tempMinSum / entry.tempMinCount : null,
            temperatureMax: entry.tempMaxCount > 0 ? entry.tempMaxSum / entry.tempMaxCount : null,
            temperatureAvg: entry.tempAvgCount > 0 ? entry.tempAvgSum / entry.tempAvgCount : null,
            sunshineHours: entry.sunshineCount > 0 ? entry.sunshineSum / entry.sunshineCount : null,
            precipitationMm: entry.precipCount > 0 ? entry.precipSum / entry.precipCount : null,
            humidityAvg: entry.humidityCount > 0 ? entry.humiditySum / entry.humidityCount : null,
            windSpeedAvg: entry.windSpeedCount > 0 ? entry.windSpeedSum / entry.windSpeedCount : null,
            windDirectionDeg,
          } as WeatherDailyAggregate;
        })
        .filter((item): item is WeatherDailyAggregate => item !== null)
        .sort((a, b) => a.timestamp - b.timestamp);

      setWeatherData(aggregated);
      setFieldWeatherLookup(perFieldLookup);
      setWeatherCacheKey(cacheKey);
    } catch (error: any) {
      console.error('⚠️ [Weather Fetch] failed', error);
      setWeatherError(error.message || '天気データの取得中にエラーが発生しました。');
      setWeatherData(null);
      setWeatherCacheKey(null);
      setGddStartDate(null);
      setGddDateBounds(null);
      setFieldWeatherLookup({});
    } finally {
      setWeatherLoading(false);
    }
  }, [auth, weatherCacheKey, weatherData, fieldWeatherLookup]);

  useEffect(() => {
    if (!highlightedSeason) return;
    const stillExists = series.some(s => s.seasonUuid === highlightedSeason);
    if (!stillExists) {
      setHighlightedSeason(null);
    }
  }, [series, highlightedSeason, setHighlightedSeason]);

  const hasVisibleSeries = useMemo(
    () => series.some(s => visibleSeries[s.dataKey] !== false),
    [series, visibleSeries],
  );
  const allVisible = useMemo(
    () => series.length > 0 && series.every(s => visibleSeries[s.dataKey] !== false),
    [series, visibleSeries],
  );
  const allHidden = useMemo(
    () => series.length > 0 && series.every(s => visibleSeries[s.dataKey] === false),
    [series, visibleSeries],
  );
  const hasNdviSeries = useMemo(() => series.some(s => s.metric === 'ndvi'), [series]);
  const hasLaiSeries = useMemo(() => series.some(s => s.metric === 'lai'), [series]);

  const sortedTableData = useMemo(() => {
    const hasNdvi = Array.isArray(ndviData) && ndviData.length > 0;
    const hasLai = Array.isArray(laiData) && laiData.length > 0;
    if (!hasNdvi && !hasLai) {
      return [];
    }

    // BBCHデータをMapに整形
    const bbchMap = new Map<string, { index: string; name: string; start: number; end: number }[]>();
    const seasonInfoMap = new Map<string, { field: Field; season: CropSeason }>();

    if (combinedOut?.response?.data?.fieldsV2) {
      const fields: Field[] = combinedOut.response.data.fieldsV2;
      for (const field of fields) {
        for (const season of field.cropSeasonsV2 ?? []) {
          seasonInfoMap.set(season.uuid, { field, season });
          if (season.countryCropGrowthStagePredictions) {
            const stages = season.countryCropGrowthStagePredictions.map(p => ({
              index: p.index,
              name: p.cropGrowthStageV2?.name ?? `BBCH ${p.index}`,
              start: new Date(p.startDate).getTime(),
              end: new Date(p.endDate).getTime(),
            }));
            bbchMap.set(season.uuid, stages);
          }
        }
      }
    }

    const ndviMap = new Map<string, Map<string, number | null>>();
    (ndviData || []).forEach(record => {
      if (!record.acquisitionDate) return;
      if (!ndviMap.has(record.cropSeasonUuid)) ndviMap.set(record.cropSeasonUuid, new Map());
      ndviMap.get(record.cropSeasonUuid)!.set(record.acquisitionDate, record.average ?? null);
    });

    const laiMap = new Map<string, Map<string, number | null>>();
    (laiData || []).forEach(record => {
      if (!record.acquisitionDate) return;
      if (!laiMap.has(record.cropSeasonUuid)) laiMap.set(record.cropSeasonUuid, new Map());
      laiMap.get(record.cropSeasonUuid)!.set(record.acquisitionDate, record.average ?? null);
    });

    const weatherMap = new Map<string, WeatherDailyAggregate>();
    (weatherData || []).forEach(entry => {
      weatherMap.set(entry.isoDate, entry);
    });

    const combinedKeys = new Set<string>();
    (ndviData || []).forEach(record => {
      if (!record.acquisitionDate) return;
      combinedKeys.add(`${record.cropSeasonUuid}|${record.acquisitionDate}`);
    });
    (laiData || []).forEach(record => {
      if (!record.acquisitionDate) return;
      combinedKeys.add(`${record.cropSeasonUuid}|${record.acquisitionDate}`);
    });

    const detailedData: NdviDetailRow[] = Array.from(combinedKeys).map((key) => {
      const [seasonUuid, acquisitionDate] = key.split('|');
      const dateObj = new Date(acquisitionDate);
      const dateValue = dateObj.getTime();
      const dateEpoch = Number.isNaN(dateValue) ? Number.NEGATIVE_INFINITY : dateValue;
      const formattedDate = Number.isFinite(dateEpoch) ? dateObj.toLocaleDateString('ja-JP') : 'N/A';
      const seasonStages = bbchMap.get(seasonUuid) ?? [];
      const stage = Number.isFinite(dateEpoch) ? seasonStages.find(s => dateEpoch >= s.start && dateEpoch <= s.end) : undefined;
      const seasonInfo = seasonInfoMap.get(seasonUuid);
      const isoDate = acquisitionDate?.split('T')[0] ?? '';
      const fieldWeather = seasonInfo?.field.uuid && isoDate
        ? fieldWeatherLookup[seasonInfo.field.uuid]?.[isoDate]
        : undefined;
      const weather = fieldWeather ?? (isoDate ? weatherMap.get(isoDate) : undefined);
      const accumulatedTemperature = isoDate ? gddLookup.get(isoDate) ?? null : null;
      const accumulatedTemperatureBase = gddStartDate ? 0 : null;
      const bbchIndexNum = stage && Number.isFinite(Number(stage.index)) ? Number(stage.index) : null;

      return {
        seasonUuid,
        fieldUuid: seasonInfo?.field.uuid ?? 'N/A',
        fieldName: seasonInfo?.field.name ?? 'N/A',
        area: seasonInfo?.field.area ?? 0,
        cropName: seasonInfo?.season.crop.name ?? 'N/A',
        varietyName: seasonInfo?.season.variety.name ?? 'N/A',
        seasonStartDate: seasonInfo?.season.startDate ? new Date(seasonInfo.season.startDate).toLocaleDateString('ja-JP') : 'N/A',
        seasonStartEpoch: seasonInfo?.season.startDate ? new Date(seasonInfo.season.startDate).getTime() : Number.NEGATIVE_INFINITY,
        cropEstablishment: (() => {
          const method = seasonInfo?.season?.cropEstablishmentMethodCode;
          if (method === 'TRANSPLANTING') return '移植';
          if (method === 'DIRECT_SEEDING') return '直播';
          if (method === 'MYKOS_DRY_DIRECT_SEEDING') return '節水型乾田直播';
          return 'N/A';
        })(),
        date: formattedDate,
        dateEpoch,
        ndvi: ndviMap.get(seasonUuid)?.get(acquisitionDate),
        lai: laiMap.get(seasonUuid)?.get(acquisitionDate),
        accumulatedTemperature,
        accumulatedTemperatureBase,
        bbch: stage ? `${stage.name}` : '-',
        bbchIndex: stage ? stage.index : '-',
        bbchIndexNum,
        weatherDateIso: isoDate,
        weatherTemperatureAvg: weather?.temperatureAvg ?? null,
        weatherPrecipitation: weather?.precipitationMm ?? null,
        weatherSunshineHours: weather?.sunshineHours ?? null,
        weatherHumidity: weather?.humidityAvg ?? null,
        weatherWindSpeed: weather?.windSpeedAvg ?? null,
        weatherWindDirectionDeg: weather?.windDirectionDeg ?? null,
        weatherWindDirection: weather ? formatWindDirection(weather.windDirectionDeg ?? null) : 'N/A',
      };
    });

    if (sortConfig) {
      detailedData.sort((a, b) => {
        const aValue = a[sortConfig.key] ?? -1;
        const bValue = b[sortConfig.key] ?? -1;
        if (aValue < bValue) return sortConfig.direction === 'ascending' ? -1 : 1;
        if (aValue > bValue) return sortConfig.direction === 'ascending' ? 1 : -1;
        return 0;
      });
    }
    return detailedData;
  }, [ndviData, laiData, sortConfig, combinedOut, weatherData, gddLookup, gddStartDate]);

  const paginatedData = useMemo(() => {
    const startIndex = (currentPage - 1) * rowsPerPage;
    return sortedTableData.slice(startIndex, startIndex + rowsPerPage);
  }, [sortedTableData, currentPage, rowsPerPage]);

  // テーブルに行が無くても選択中シーズンのタスクは表示できるよう、選択オプションからセットを作成
  const selectedSeasonSet = useMemo(
    () => new Set(selectedOptions.map(opt => opt.value)),
    [selectedOptions],
  );

  // BBCHの値を作付開始から終端まで埋めるための日付→BBCHインデックスのマップ
  const bbchIndexMap = useMemo(() => {
    const map = new Map<string, number>();
    const fields: Field[] | undefined = combinedOut?.response?.data?.fieldsV2;
    if (!fields) return map;

    const addRange = (fromIso: string, toIso: string, indexNum: number) => {
      const start = Date.parse(`${fromIso}T00:00:00Z`);
      const end = Date.parse(`${toIso}T00:00:00Z`);
      if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return;
      const oneDay = 24 * 60 * 60 * 1000;
      for (let t = start; t <= end; t += oneDay) {
        const iso = new Date(t).toISOString().split('T')[0];
        map.set(iso, indexNum);
      }
    };

    fields.forEach(field => {
      (field.cropSeasonsV2 || []).forEach(season => {
        if (!selectedSeasonSet.has(season.uuid)) return;
        (season.countryCropGrowthStagePredictions || []).forEach(pred => {
          const idxNum = Number(pred.index);
          if (!Number.isFinite(idxNum)) return;
          const fromIso = pred.startDate?.split('T')[0];
          const toIso = (pred.endDate || pred.startDate)?.split('T')[0];
          if (!fromIso || !toIso) return;
          addRange(fromIso, toIso, idxNum);
        });
      });
    });

    return map;
  }, [combinedOut, selectedSeasonSet]);

  const taskEvents = useMemo<TaskEvent[]>(() => {
    const fields: Field[] | undefined = combinedOut?.response?.data?.fieldsV2;
    if (!fields || fields.length === 0) return [];
    const events: TaskEvent[] = [];
    const addEvent = (payload: { dateRaw?: string | null; label: string; type: string; fieldName: string; seasonUuid: string; creationFlowHint?: string | null; products?: { name: string | null; totalApplication: number | null; unit: string | null }[] }) => {
      const raw = payload.dateRaw;
      if (!raw) return;
      const ts = new Date(raw).getTime();
      if (!Number.isFinite(ts)) return;
      events.push({
        date: ts,
        label: payload.label,
        type: payload.type,
        fieldName: payload.fieldName,
        seasonUuid: payload.seasonUuid,
        hint: payload.creationFlowHint,
        products: payload.products,
      });
    };
    const mapSprayLabel = (hint?: string | null) => {
      const upper = (hint || '').toUpperCase();
      if (upper === 'CROP_PROTECTION') return '防除';
      if (upper === 'NUTRITION_MANAGEMENT') return '施肥';
      if (upper === 'WEED_MANAGEMENT') return '除草';
      return '散布';
    };

    fields.forEach(field => {
      (field.cropSeasonsV2 || []).forEach(season => {
        if (!selectedSeasonSet.has(season.uuid)) return;
        const common = { fieldName: field.name ?? 'N/A', seasonUuid: season.uuid };
        season.harvests?.forEach(task => addEvent({ dateRaw: task.executionDate || task.plannedDate, label: '収穫', type: 'Harvest', ...common }));
        season.waterManagementTasks?.forEach(task => addEvent({ dateRaw: task.executionDate || task.plannedDate, label: '水管理', type: 'WaterManagement', ...common }));
        season.scoutingTasks?.forEach(task => addEvent({ dateRaw: task.executionDate || task.plannedDate, label: '巡回', type: 'Scouting', ...common }));
        season.sprayingsV2?.forEach(task => {
          const hint = task.creationFlowHint ?? task.dosedMap?.creationFlowHint ?? null;
          const products = (task.dosedMap?.recipeV2 ?? []).map((p: any) => ({
            name: p?.name ?? null,
            totalApplication: p?.totalApplication != null ? Number(p.totalApplication) : null,
            unit: p?.unit ?? null,
          }));
          addEvent({
            dateRaw: task.executionDate || task.plannedDate,
            label: mapSprayLabel(hint),
            type: 'Spraying',
            ...common,
            creationFlowHint: hint,
            products,
          });
        });
      });
    });
    return events.sort((a, b) => a.date - b.date);
  }, [combinedOut, selectedSeasonSet]);

  const totalPages = Math.ceil(sortedTableData.length / rowsPerPage);

  const handleFetchAll = useCallback(async () => {
    const meta = await handleFetchBiomass();
    if (meta) {
      await fetchWeatherForSelection(meta);
    }
  }, [handleFetchBiomass, fetchWeatherForSelection]);

  const downloadAsCsv = () => {
    const headers = [
      '圃場UUID', '圃場名', '面積(a)', '作物', '品種', '作付日', '作付方法',
      '観測日', 'NDVI', 'LAI平均', '平均気温(°C)', '降水量(mm)', '日照時間(h)', '湿度(%)', '風速(m/s)', '風向', 'BBCHステージ', 'BBCH'
    ];

    const rows = sortedTableData.map(item => {
      return [
        item.fieldUuid,
        item.fieldName,
        (item.area / 100).toFixed(2),
        item.cropName,
        item.varietyName,
        item.seasonStartDate,
        item.cropEstablishment,
        item.date,
        item.ndvi !== undefined && item.ndvi !== null ? item.ndvi.toFixed(3) : 'N/A',
        item.lai !== undefined && item.lai !== null ? item.lai.toFixed(3) : 'N/A',
        item.weatherTemperatureAvg !== undefined && item.weatherTemperatureAvg !== null ? item.weatherTemperatureAvg.toFixed(1) : 'N/A',
        item.weatherPrecipitation !== undefined && item.weatherPrecipitation !== null ? item.weatherPrecipitation.toFixed(1) : 'N/A',
        item.weatherSunshineHours !== undefined && item.weatherSunshineHours !== null ? item.weatherSunshineHours.toFixed(1) : 'N/A',
        item.weatherHumidity !== undefined && item.weatherHumidity !== null ? item.weatherHumidity.toFixed(0) : 'N/A',
        item.weatherWindSpeed !== undefined && item.weatherWindSpeed !== null ? item.weatherWindSpeed.toFixed(1) : 'N/A',
        item.weatherWindDirection ?? 'N/A',
        item.bbch,
        item.bbchIndex,
      ].map(cell => `"${String(cell ?? '').replace(/"/g, '""')}"`).join(',');
    });

    const bom = new Uint8Array([0xEF, 0xBB, 0xBF]);
    const csvContent = [headers.join(','), ...rows].join('\n');
    const blob = new Blob([bom, csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.setAttribute("href", url);
    link.setAttribute("download", `ndvi_data_${new Date().toISOString().split('T')[0]}.csv`);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const selectedPredictions = useMemo<SelectedPrediction[]>(() => {
    if (!combinedOut?.response?.data?.fieldsV2 || selectedOptions.length === 0) {
      return [];
    }

    const selectedUuidSet = new Set(selectedOptions.map(opt => opt.value));
    const fields: Field[] = combinedOut.response.data.fieldsV2;

    const predictions = fields.flatMap(field =>
      field.cropSeasonsV2
        ?.filter(
          (season: CropSeason) =>
            selectedUuidSet.has(season.uuid) &&
            season.countryCropGrowthStagePredictions &&
            season.countryCropGrowthStagePredictions.length > 0,
        )
        .map((season: CropSeason) => ({
          seasonUuid: season.uuid,
          fieldName: field.name,
          cropName: season.crop.name,
          varietyName: season.variety.name,
          seasonStartDate: season.startDate,
          predictions: season.countryCropGrowthStagePredictions!,
        })) ?? [],
    );

    return predictions.sort(
      (a, b) => new Date(a.seasonStartDate).getTime() - new Date(b.seasonStartDate).getTime(),
    );
  }, [combinedOut, selectedOptions]);

  const ndviRange = useMemo(() => {
    if (!chartData || chartData.length === 0) return null;
    const first = Number(chartData[0].acquisitionDate);
    const last = Number(chartData[chartData.length - 1].acquisitionDate);
    if (!Number.isFinite(first) || !Number.isFinite(last)) return null;
    return { min: first, max: last };
  }, [chartData]);

useEffect(() => {
  if (ndviRange && !syncedRange) {
    setSyncedRange(ndviRange);
  }
}, [ndviRange, syncedRange]);

// タスク付きのcombinedOutを確保（イベント描画用）
useEffect(() => {
  fetchCombinedDataIfNeeded({ includeTasks: true });
}, [fetchCombinedDataIfNeeded]);

	  return (
	    <div className="ndvi-page-container">
	      {showGlobalLoading && (
	        <LoadingOverlay message={overlayMessage} />
	      )}
	      <h2>{t('nav.ndvi')}</h2>
	      {!submittedFarms || submittedFarms.length === 0 ? (
	        <p>{t('tasks.select_farms_hint')}</p>
	      ) : combinedErr ? (
	        <p style={{ color: 'crimson' }}>{t('error.prefix', { message: combinedErr })}</p>
	      ) : (
	        <div style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
	          <div>
	            <h3>{t('ndvi.select_seasons_title')}</h3>
	            <Select
	              isMulti
	              options={groupedOptions} // グループ化されたオプションを使用
	              isLoading={combinedLoading} // combinedLoadingを使用
	              placeholder={t('ndvi.select_placeholder')}
	              value={selectedOptions}
	              onChange={(options) => setSelectedOptions(options)}
              closeMenuOnSelect={false} // 項目選択後もメニューを開いたままにする
              hideSelectedOptions={false} // 選択済みの項目をリストから隠さない
              components={{ Option: CheckboxOption }}
              styles={{
                control: (base) => ({ ...base, background: '#252529', borderColor: '#4a4a4f' }),
                menu: (base) => ({ ...base, background: '#252529' }),
                option: (base, state) => ({
                  ...base,
                  backgroundColor: state.isFocused ? '#313136' : '#252529',
                  // チェックボックスのクリックを妨げないように
                  ':active': { ...base[':active'], backgroundColor: state.isSelected ? base.backgroundColor : '#313136' },
                }),
                multiValue: (base) => ({ ...base, backgroundColor: '#313136' }),
                multiValueLabel: (base) => ({ ...base, color: 'white' }),
              }}
            />
          </div>
	          <div>
	            <button onClick={handleFetchAll} disabled={selectedOptions.length === 0 || showGlobalLoading}>
	              {showGlobalLoading ? (
	                <span className="ndvi-button-loading">
	                  <LoadingSpinner size={18} />
	                  <span>{t('loading.fetching_short')}</span>
	                </span>
	              ) : (
	                t('ndvi.fetch_selected')
	              )}
	            </button>
	          </div>
	          {biomassError && <p style={{ color: 'crimson' }}>{t('error.prefix', { message: biomassError })}</p>}
	          {biomassDataSource && (
	            <p style={{ color: biomassDataSource === 'cache' ? '#4caf50' : '#2196f3', fontWeight: 'bold' }}>
	              (
	                {biomassDataSource === 'cache'
	                  ? t('ndvi.source.biomass_cache')
	                  : t('ndvi.source.biomass_api')}
	              )
	            </p>
	          )}
	          {weatherLoading && <p style={{ color: '#2196f3' }}>{t('ndvi.loading.weather')}</p>}
	          {weatherError && <p style={{ color: 'crimson' }}>{t('ndvi.weather_failed', { error: weatherError })}</p>}
	          {series.length > 0 && (
	            <div className="series-toggle-container">
	              <div className="series-toggle-actions">
	                <button onClick={() => setAllVisibility(true)} disabled={series.length === 0 || allVisible}>
	                  {t('action.show_all')}
	                </button>
	                <button onClick={() => setAllVisibility(false)} disabled={series.length === 0 || allHidden}>
	                  {t('action.hide_all')}
	                </button>
	                <button onClick={() => showOnlyMetric('ndvi')} disabled={!hasNdviSeries}>
	                  {t('ndvi.show_only_ndvi')}
	                </button>
	                <button onClick={() => showOnlyMetric('lai')} disabled={!hasLaiSeries}>
	                  {t('ndvi.show_only_lai')}
	                </button>
	              </div>
              <div className="series-toggle-list">
                {series.map(s => {
                  const isVisible = visibleSeries[s.dataKey] !== false;
                  const isHighlighted = highlightedSeason === s.seasonUuid;
                  return (
                    <label
                      key={s.dataKey}
                      className={`series-toggle-item${isVisible ? '' : ' inactive'}${isHighlighted ? ' highlighted' : ''}`}
                    >
                      <input
                        type="checkbox"
                        checked={isVisible}
                        onChange={() => toggleSeriesVisibility(s.dataKey)}
                      />
                      <span className="series-toggle-swatch" style={{ backgroundColor: s.color, borderColor: s.color }} />
                      <span className="series-toggle-label">{s.name}</span>
                    </label>
                  );
                })}
              </div>
            </div>
          )}
          {chartData && chartData.length > 0 && (
            <div>
              <div className="chart-container" style={{ marginTop: '2rem', height: '400px' }}>
                {hasVisibleSeries ? (
                  <ResponsiveContainer width="100%" height={400}>
                    <ComposedChart data={chartData} syncId="ndvi-bbch" margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                      <defs>
                        {series.map(s => (
                          <linearGradient key={`gradient-${s.dataKey}`} id={`gradient-${s.dataKey}`} x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor={hexToRgba(s.color, 0.45)} stopOpacity={0.9} />
                            <stop offset="100%" stopColor={hexToRgba(s.color, 0.02)} stopOpacity={0.2} />
                          </linearGradient>
                        ))}
                      </defs>
                      <CartesianGrid strokeDasharray="3 3" stroke="#4a4a4f" />
                      <XAxis
                        type="number"
                        dataKey="acquisitionDate"
                        domain={syncedRange ? [syncedRange.min, syncedRange.max] : ['dataMin', 'dataMax']}
                        tickFormatter={(unixTime) => new Date(unixTime).toLocaleDateString('ja-JP', { month: 'numeric', day: 'numeric' })}
                        stroke="#9e9e9e"
                        allowDataOverflow
                      />
                      <YAxis yAxisId="left" domain={[0, 1]} stroke="#9e9e9e" />
                      <YAxis yAxisId="right" orientation="right" hide />
                      <Tooltip
                        contentStyle={{ backgroundColor: '#252529', border: '1px solid #4a4a4f' }}
                        labelFormatter={(unixTime) => new Date(unixTime).toLocaleDateString('ja-JP')}
                        formatter={(value: number, name: string) => {
                          const formattedValue = typeof value === 'number' ? value.toFixed(3) : value;
                          return [formattedValue, name];
                        }}
                      />
                      {series
                        .filter(s => visibleSeries[s.dataKey] !== false)
                        .map(s => {
                          const isHighlighted = highlightedSeason === s.seasonUuid;
                          const gradientId = `gradient-${s.dataKey}`;
                          return (
                            <Fragment key={s.dataKey}>
                              <Area
                                type="monotone"
                                dataKey={s.dataKey}
                                yAxisId="left"
                                stroke="none"
                                fill={`url(#${gradientId})`}
                                fillOpacity={isHighlighted ? 0.5 : 0.18}
                                isAnimationActive={false}
                                connectNulls
                                tooltipType="none"
                              />
                              <Line
                                type="monotone"
                                dataKey={s.dataKey}
                                name={s.name}
                                stroke={s.color}
                                strokeWidth={isHighlighted ? 3.2 : 1.8}
                                strokeDasharray={s.metric === 'lai' ? '6 3' : undefined}
                                dot={false}
                                activeDot={{ r: isHighlighted ? 5 : 3, fill: s.color, strokeWidth: 0 }}
                                yAxisId="left"
                                isAnimationActive={false}
                                onMouseEnter={() => setHighlightedSeason(s.seasonUuid)}
                                onMouseLeave={() => setHighlightedSeason(null)}
                                connectNulls
                              />
                            </Fragment>
                          );
                        })}
                      {chartData.length > 1 && (
                        <Brush
                          dataKey="acquisitionDate"
                          height={30}
                          stroke="#646cff"
                          fill="rgba(83, 90, 255, 0.12)"
                          travellerWidth={12}
                          startIndex={brushRange?.startIndex ?? 0}
                          endIndex={brushRange?.endIndex ?? Math.max(0, chartData.length - 1)}
                          tickFormatter={(unixTime: number) => new Date(unixTime).toLocaleDateString('ja-JP', { month: 'numeric', day: 'numeric' })}
                          onChange={handleBrushChange}
                        />
                      )}
                    </ComposedChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="chart-placeholder">
                    表示するデータ系列が選択されていません。チェックボックスで表示したい系列を選択してください。
                  </div>
                )}
              </div>
	              {weatherChartData.length > 0 && (
	                  <div className="weather-chart-section">
	                    <div className="weather-chart-header">
	                      <h3>{t('ndvi.weather.title_daily')}</h3>
	                      <div className="weather-toggle-actions">
	                        <button onClick={() => setAllWeatherMetrics(true)} disabled={Object.values(visibleWeatherMetrics).every(Boolean)}>
	                          {t('action.show_all')}
	                        </button>
	                      </div>
	                    </div>
	                  {gddDateBounds && (
	                    <div className="weather-gdd-control" style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', marginBottom: '0.75rem', flexWrap: 'wrap' }}>
	                      <label style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
	                        <span>{t('ndvi.gdd.start_date')}</span>
	                        <input
                          type="date"
                          min={gddDateBounds.min}
                          max={gddDateBounds.max}
                          value={gddStartDate ?? gddDateBounds.min}
                          onChange={handleGddStartDateInput}
                          style={{ padding: '0.25rem 0.5rem' }}
                        />
	                      </label>
	                      <span style={{ fontSize: '0.85em', color: '#9e9e9e' }}>
	                        {t('ndvi.gdd.note', { min: gddDateBounds.min })}
	                      </span>
	                    </div>
	                  )}
	                  <div className="weather-toggle-grid">
	                    {WEATHER_METRICS_CONFIG.map(metric => {
                      const active = visibleWeatherMetrics[metric.key];
                      return (
                        <label
                          key={metric.key}
                          className={`weather-toggle-item${active ? '' : ' inactive'}`}
                        >
                          <input
                            type="checkbox"
                            checked={active}
                            onChange={() => toggleWeatherMetric(metric.key)}
	                          />
	                          <span className="weather-toggle-swatch" style={{ backgroundColor: metric.color }} />
	                          <span className="weather-toggle-label">{t(metric.labelKey)}</span>
	                        </label>
	                      );
	                    })}
                  </div>
                  <div className="weather-chart-container">
                    <ResponsiveContainer width="100%" height={320}>
                      <ComposedChart
                        data={weatherChartData}
                        syncId="ndvi-bbch"
                        margin={{ top: 20, right: 30, left: 20, bottom: 5 }}
                      >
                        <defs>
                          <linearGradient id="sunshineGradient" x1="0" y1="0" x2="0" y2="1">
                            <stop offset="0%" stopColor="rgba(255, 213, 79, 0.55)" />
                            <stop offset="100%" stopColor="rgba(255, 213, 79, 0.05)" />
                          </linearGradient>
                        </defs>
                        <CartesianGrid strokeDasharray="3 3" stroke="#4a4a4f" />
                        <XAxis
                          type="number"
	                          dataKey="timestamp"
	                          domain={syncedRange ? [syncedRange.min, syncedRange.max] : ['dataMin', 'dataMax']}
	                          tickFormatter={(unixTime) =>
	                            new Date(unixTime).toLocaleDateString(
	                              language === 'ja' ? 'ja-JP' : 'en-US',
	                              { month: 'numeric', day: 'numeric' },
	                            )
	                          }
	                          stroke="#9e9e9e"
	                          allowDataOverflow
	                        />
                        <YAxis yAxisId="temp" stroke="#ff7043" width={42} domain={["auto", "auto"]} allowDecimals />
                        <YAxis yAxisId="precip" orientation="right" stroke="#4fc3f7" width={42} domain={[0, 'dataMax']} allowDecimals />
                        <YAxis yAxisId="humidity" hide domain={[0, 100]} allowDecimals />
                        <YAxis yAxisId="wind" hide domain={[0, 'dataMax']} allowDecimals />
                        <YAxis yAxisId="sunshine" hide domain={[0, 'dataMax']} allowDecimals />
                        <YAxis yAxisId="degreeDays" hide domain={[0, 'dataMax']} allowDecimals />
                        <Tooltip content={(props) => renderWeatherTooltip(props as WeatherTooltipArgs, visibleWeatherMetrics)} />
	                        {visibleWeatherMetrics.precipitationMm && (
	                          <Bar
	                            dataKey="precipitationMm"
	                            yAxisId="precip"
	                            fill="#4fc3f7"
	                            fillOpacity={0.7}
	                            name={t('ndvi.weather.precipitation')}
	                            maxBarSize={24}
	                          />
	                        )}
	                        {visibleWeatherMetrics.sunshineHours && (
	                          <Area
	                            type="monotone"
	                            dataKey="sunshineHours"
	                            yAxisId="sunshine"
	                            stroke="#ffd54f"
	                            fill="url(#sunshineGradient)"
	                            fillOpacity={0.35}
	                            name={t('ndvi.weather.sunshine')}
	                          />
	                        )}
	                        {visibleWeatherMetrics.temperatureAvg && (
	                          <Line
	                            type="monotone"
	                            dataKey="temperatureAvg"
	                            yAxisId="temp"
	                            stroke="#ff7043"
	                            strokeWidth={2.2}
	                            dot={false}
	                            name={t('ndvi.weather.temperature_avg')}
	                          />
	                        )}
	                        {visibleWeatherMetrics.temperatureMax && (
	                          <Line
                            type="monotone"
                            dataKey="temperatureMax"
                            yAxisId="temp"
                            stroke="#ff8a65"
	                            strokeDasharray="4 4"
	                            strokeWidth={1.6}
	                            dot={false}
	                            name={t('ndvi.weather.temperature_max')}
	                          />
	                        )}
	                        {visibleWeatherMetrics.temperatureMin && (
	                          <Line
                            type="monotone"
                            dataKey="temperatureMin"
                            yAxisId="temp"
                            stroke="#4fc3f7"
	                            strokeDasharray="4 4"
	                            strokeWidth={1.6}
	                            dot={false}
	                            name={t('ndvi.weather.temperature_min')}
	                          />
	                        )}
	                        {visibleWeatherMetrics.gdd && (
	                          <Line
                            type="monotone"
                            dataKey="gdd"
                            yAxisId="degreeDays"
	                            stroke="#ffa726"
	                            strokeWidth={2.4}
	                            dot={false}
	                            name={t('ndvi.weather.gdd')}
	                          />
	                        )}
	                        {visibleWeatherMetrics.humidityAvg && (
	                          <Line
                            type="monotone"
                            dataKey="humidityAvg"
                            yAxisId="humidity"
	                            stroke="#64b5f6"
	                            strokeWidth={1.8}
	                            dot={false}
	                            name={t('ndvi.weather.humidity')}
	                          />
	                        )}
	                        {visibleWeatherMetrics.windSpeedAvg && (
	                          <Line
                            type="monotone"
                            dataKey="windSpeedAvg"
                            yAxisId="wind"
                            stroke="#9575cd"
	                            strokeDasharray="6 3"
	                            strokeWidth={1.8}
	                            dot={false}
	                            name={t('ndvi.weather.wind_speed')}
	                          />
	                        )}
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </div>
              )}
	              <SelectedGrowthStageTimeline
	                items={selectedPredictions}
	                language={language}
	                ndviRange={ndviRange}
	                syncedRange={syncedRange}
	                onRangeChange={(range) =>
	                  setSyncedRange(prev => {
	                    if (prev && Math.abs(prev.min - range.min) < 1 && Math.abs(prev.max - range.max) < 1) {
	                      return prev;
	                    }
	                    return range;
	                  })
	                }
	              />
            </div>
          )}
          {chartData && chartData.length > 0 && (
            <div className="table-with-pagination">
              <NdviDetailTable
                tableData={paginatedData}
                sortConfig={sortConfig}
                requestSort={(key) => {
                  let direction: 'ascending' | 'descending' = 'ascending';
                  if (sortConfig && sortConfig.key === key && sortConfig.direction === 'ascending') direction = 'descending';
                  setSortConfig({ key: key as keyof NdviDetailRow, direction });
                }}
                onHover={setHighlightedSeason}
                highlightedSeason={highlightedSeason}
                gddStartDate={gddStartDate}
              />
              <div className="pagination-controls">
                <button onClick={() => setCurrentPage(p => Math.max(1, p - 1))} disabled={currentPage === 1}>
                  前へ
                </button>
                <span>
                  {sortedTableData.length > 0 ? `${(currentPage - 1) * rowsPerPage + 1}-${Math.min(currentPage * rowsPerPage, sortedTableData.length)}` : 0} / {sortedTableData.length} 件
                </span>
                <button onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))} disabled={currentPage === totalPages}>
                  次へ
                </button>
              </div>
              {sortedTableData.length > 0 && (
                <div className="download-button-container">
                  <button onClick={downloadAsCsv}>CSVダウンロード</button>
                </div>
              )}
              <NdviComboChart data={sortedTableData} tasks={taskEvents} weather={weatherChartData} bbchByDate={bbchIndexMap} />
            </div>
          )}
        </div>
      )}
    </div>
  );
}

type SelectedGrowthStageTimelineProps = {
  items: SelectedPrediction[];
  language: 'ja' | 'en';
  ndviRange: { min: number; max: number } | null;
  syncedRange: { min: number; max: number } | null;
  onRangeChange: (range: { min: number; max: number }) => void;
};

const SelectedGrowthStageTimeline: FC<SelectedGrowthStageTimelineProps> = memo(({ items, language, ndviRange, syncedRange, onRangeChange }) => {
  const timeline = useMemo(() => {
    if (!items || items.length === 0) {
      return {
        chartData: null as ChartData<'bar'> | null,
        chartHeight: 0,
        maxDate: null as Date | null,
        minDate: null as Date | null,
      };
    }

    const labels = items.map(item => `${item.fieldName} (${item.varietyName})`);
    const datasets: TimelineDataset[] = [];
    let maxDate: Date | null = null;
    let minDate: Date | null = null;

    const stageColors = [
      '#4E79A7', '#F28E2B', '#E15759', '#76B7B2', '#59A14F',
      '#EDC948', '#B07AA1', '#FF9DA7', '#9C755F', '#BAB0AC',
    ];

    const bbchIndices = [...new Set(items.flatMap(group => group.predictions.map(pred => pred.index)))].sort();

    bbchIndices.forEach((bbchIndex, i) => {
      const dataset: TimelineDataset = {
        label: `BBCH ${bbchIndex}`,
        bbchIndex,
        data: [],
        backgroundColor: stageColors[i % stageColors.length],
        barPercentage: 0.6,
        categoryPercentage: 0.8,
      };

      items.forEach(group => {
        const prediction = group.predictions.find(p => p.index === bbchIndex);
        if (!prediction) return;

        const start = new Date(prediction.startDate);
        if (Number.isNaN(start.getTime())) return;
        const rawEnd = prediction.endDate ? new Date(prediction.endDate) : null;
        let endTime = rawEnd && !Number.isNaN(rawEnd.getTime()) ? rawEnd.getTime() : addDays(start, 1).getTime();
        const startTime = start.getTime();
        if (endTime <= startTime) {
          endTime = addDays(start, 1).getTime();
        }

        const end = new Date(endTime);
        if (!minDate || start < minDate) {
          minDate = start;
        }
        if (!maxDate || end > maxDate) {
          maxDate = end;
        }

        dataset.data.push({
          x: [startTime, endTime],
          y: `${group.fieldName} (${group.varietyName})`,
          stageName: prediction.cropGrowthStageV2?.name ?? tr('gsp.stage.unknown'),
        });
      });

      if (dataset.data.length > 0) {
        datasets.push(dataset);
      }
    });

    if (datasets.length === 0) {
      return {
        chartData: null as ChartData<'bar'> | null,
        chartHeight: 0,
        maxDate: null as Date | null,
        minDate: null as Date | null,
      };
    }

    const chartData: ChartData<'bar'> = {
      labels,
      datasets: datasets as unknown as any[],
    };
    const chartHeight = Math.max(240, labels.length * 50 + 120);

    return { chartData, chartHeight, maxDate, minDate };
  }, [items, language]);

  const emitRange = useCallback(
    (chart: Chart<'bar'>) => {
      if (!chart?.scales?.x) return;
      const scale = chart.scales.x;
      const min = typeof scale.min === 'number' ? scale.min : Number(scale.min);
      const max = typeof scale.max === 'number' ? scale.max : Number(scale.max);
      if (Number.isFinite(min) && Number.isFinite(max)) {
        onRangeChange({ min, max });
      }
    },
    [onRangeChange],
  );

  const today = new Date();
  const todayStart = startOfDay(today);
  const ndviMin = ndviRange ? startOfDay(new Date(ndviRange.min)).getTime() : null;
  const ndviMax = ndviRange ? addDays(startOfDay(new Date(ndviRange.max)), 1).getTime() : null;

  const defaultMin = ndviMin ?? subDays(todayStart, 14).getTime();
  const defaultMax = ndviMax ?? addDays(todayStart, 14).getTime();
  const viewMin = syncedRange?.min ?? defaultMin;
  const viewMax = syncedRange?.max ?? defaultMax;
  const chartKey = `${Number.isFinite(viewMin) ? Math.round(viewMin) : 'auto'}-${Number.isFinite(viewMax) ? Math.round(viewMax) : 'auto'}`;

  const limitMin = timeline.minDate ? subDays(timeline.minDate, 14).getTime() : subDays(today, 100).getTime();
  const limitMax = timeline.maxDate ? addDays(timeline.maxDate, 30).getTime() : addDays(today, 180).getTime();

  const options = useMemo(
    () => ({
      indexAxis: 'y' as const,
      responsive: true,
      maintainAspectRatio: false,
      layout: { padding: { top: 30, bottom: 30 } },
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            title: (tooltipItems: TooltipItem<'bar'>[]): string => (
              tooltipItems.length > 0 ? tooltipItems[0].label ?? '' : ''
            ),
            label: (context: TooltipItem<'bar'>): string => {
              const raw = context.raw as { x?: [number, number]; stageName?: string };
              const bbchIndex = (context.dataset as any).bbchIndex || '';
              const stageName = raw?.stageName ?? '';
              if (raw?.x) {
                const [start, end] = raw.x;
                const startLabel = format(new Date(start), 'MM/dd');
                const endLabel = format(new Date(end), 'MM/dd');
                return `BBCH ${bbchIndex}: ${stageName} [${startLabel} - ${endLabel}]`;
              }
              return `BBCH ${bbchIndex}: ${stageName}`;
            },
	            footer: (tooltipItems: TooltipItem<'bar'>[]): string => {
	              if (tooltipItems.length === 0) return '';
	              const raw = tooltipItems[0].raw as { x?: [number, number] };
	              if (!raw?.x) return '';
	              const [start, end] = raw.x;
	              const duration = differenceInCalendarDays(new Date(end), new Date(start));
	              return tr('gsp.duration_days', { days: duration });
	            },
	          },
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
          color: (context: any) => {
            const bg = context.dataset.backgroundColor as string;
            const color = bg.charAt(0) === '#' ? bg.substring(1, 7) : bg;
            const r = parseInt(color.substring(0, 2), 16);
            const g = parseInt(color.substring(2, 4), 16);
            const b = parseInt(color.substring(4, 6), 16);
            return (r * 0.299 + g * 0.587 + b * 0.114) > 186 ? '#000000' : '#FFFFFF';
          },
          font: { weight: 'bold', size: 12 },
          anchor: 'center' as const,
          align: 'center' as const,
        },
        zoom: {
          // ズーム/パンを禁止する
          pan: { enabled: false },
          zoom: { wheel: { enabled: false }, pinch: { enabled: false }, drag: { enabled: false } },
        },
        annotation: {
          annotations: {
            todayLine: {
              type: 'line',
              xMin: todayStart.getTime(),
              xMax: todayStart.getTime(),
              borderColor: 'rgba(255, 99, 132, 0.8)',
              borderWidth: 2,
              borderDash: [6, 6],
              label: {
                content: tr('chart.today'),
                display: true,
                position: 'start',
              },
            },
          },
        },
      },
      scales: {
        x: {
          type: 'time' as const,
          position: 'top' as const,
          min: viewMin,
          max: viewMax,
          adapters: { date: { locale: language === 'ja' ? ja : enUS } },
          time: {
            tooltipFormat: 'yyyy/MM/dd',
            minUnit: 'day',
            displayFormats:
              language === 'ja'
                ? { day: 'M/d', week: 'M/d', month: 'yyyy年 M月', year: 'yyyy年' }
                : { day: 'M/d', week: 'M/d', month: 'MMM yyyy', year: 'yyyy' },
          },
        },
        y: {
          stacked: true,
          ticks: { autoSkip: false },
        },
      },
    }),
    [emitRange, limitMin, limitMax, ndviMin, ndviMax, todayStart, viewMin, viewMax, language],
  );

  if (!timeline.chartData || timeline.chartData.datasets.length === 0) {
    return (
      <div className="chart-container" style={{ marginTop: '1rem', padding: '1.5rem' }}>
        <p style={{ color: '#b0b0b5', margin: 0 }}>{tr('ndvi.no_stage_predictions')}</p>
      </div>
    );
  }

  return (
    <div className="chart-container" style={{ marginTop: '1rem', height: `${timeline.chartHeight}px` }}>
      <TimelineBar key={chartKey} options={options as any} data={timeline.chartData} />
    </div>
  );
});

type WeatherTooltipArgs = {
  active?: boolean;
  payload?: Array<{ payload?: Record<string, unknown> }>;
};

const renderWeatherTooltip = (
  props: WeatherTooltipArgs,
  visibleMetrics: Record<WeatherMetricKey, boolean>,
) => {
  const { active, payload } = props;
  if (!active || !payload || payload.length === 0) return null;
  const datum = payload[0]?.payload as (Record<string, any> & { isoDate?: string; dateLabel?: string; windDirectionDeg?: number | null; timestamp?: number });
  if (!datum) return null;

  const dateLabel = datum.isoDate
    ? new Date(`${datum.isoDate}T00:00:00Z`).toLocaleDateString(getCurrentLanguage() === 'ja' ? 'ja-JP' : 'en-US')
    : datum.dateLabel || '';

  const rows = WEATHER_METRICS_CONFIG
    .filter(metric => visibleMetrics[metric.key])
    .map(metric => {
      const raw = datum[metric.key];
      if (raw === null || raw === undefined || raw === '') return null;
      const num = Number(raw);
      if (!Number.isFinite(num)) return null;
      let digits = 1;
      if (metric.key === 'humidityAvg') digits = 0;
      if (metric.key === 'precipitationMm') digits = 1;
      if (metric.key === 'sunshineHours') digits = 1;
      if (metric.key === 'windSpeedAvg') digits = 1;
      if (metric.key === 'gdd') digits = 1;
      const formatted = `${num.toFixed(digits)} ${metric.unit}`.trim();
      return {
        label: tr(metric.labelKey),
        color: metric.color,
        value: formatted,
      };
    })
    .filter((row): row is { label: string; color: string; value: string } => row !== null);

  const windDir = datum.windDirectionDeg;
  if (windDir !== null && windDir !== undefined && Number.isFinite(Number(windDir))) {
    rows.push({
      label: tr('ndvi.weather.wind_direction'),
      color: '#9575cd',
      value: formatWindDirection(Number(windDir)),
    });
  }

  if (rows.length === 0) return null;

  return (
    <div className="ndvi-tooltip">
      <div className="ndvi-tooltip__header">{dateLabel}</div>
      <div className="ndvi-tooltip__body">
        {rows.map(row => (
          <div key={row.label} className="ndvi-tooltip__row">
            <span className="ndvi-tooltip__marker" style={{ backgroundColor: row.color }} />
            <span className="ndvi-tooltip__label">{row.label}</span>
            <span className="ndvi-tooltip__value">{row.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
};

// =============================================================================
// react-select custom components
// =============================================================================

const CheckboxOption = (props: OptionProps<SelectOption, true, GroupBase<SelectOption>>) => {
  // `props.innerProps` にはクリックイベントなどが含まれているため、
  // これを最も外側の要素に適用します。
  return (
    <components.Option {...props}>
      <input type="checkbox" checked={props.isSelected} onChange={() => null} />
      <label style={{ marginLeft: '8px' }}>{props.label}</label>
    </components.Option>
  );
};

// =============================================================================
// Summary Table Component
// =============================================================================

type NdviDetailTableProps = {
  tableData: NdviDetailRow[];
  sortConfig: { key: keyof NdviDetailRow; direction: 'ascending' | 'descending' } | null;
  requestSort: (key: keyof NdviDetailRow) => void;
  onHover: (seasonUuid: string | null) => void;
  highlightedSeason: string | null;
  gddStartDate: string | null;
};

const NdviDetailTable: FC<NdviDetailTableProps> = ({ tableData, sortConfig, requestSort, onHover, highlightedSeason, gddStartDate }) => {

  const getSortIndicator = (key: keyof NdviDetailRow) => {
    if (!sortConfig || sortConfig.key !== key) return ' ↕';
    return sortConfig.direction === 'ascending' ? ' ▲' : ' ▼';
  };

  const gddLabel = `積算温度 (°C日)${gddStartDate ? `（${gddStartDate}以降）` : '（未設定）'}`;

  return (
    <div className="table-container" style={{ marginTop: '2rem' }}>
      <table className="fields-table ndvi-table">
        <thead>
          <tr>
            <th onClick={() => requestSort('fieldUuid')} className="sortable">
              圃場UUID {getSortIndicator('fieldUuid')}
            </th>
            <th onClick={() => requestSort('fieldName')} className="sortable">
              圃場名 {getSortIndicator('fieldName')}
            </th>
            <th onClick={() => requestSort('area')} className="sortable">
              面積(a) {getSortIndicator('area')}
            </th>
            <th onClick={() => requestSort('cropName')} className="sortable">
              作物 {getSortIndicator('cropName')}
            </th>
            <th onClick={() => requestSort('varietyName')} className="sortable">
              品種 {getSortIndicator('varietyName')}
            </th>
            <th onClick={() => requestSort('seasonStartDate')} className="sortable">
              作付日 {getSortIndicator('seasonStartDate')}
            </th>
            <th onClick={() => requestSort('cropEstablishment')} className="sortable">
              作付方法 {getSortIndicator('cropEstablishment')}
            </th>
            <th onClick={() => requestSort('dateEpoch')} className="sortable">
              観測日 {getSortIndicator('dateEpoch')}
            </th>
            <th onClick={() => requestSort('ndvi')} className="sortable">
              NDVI {getSortIndicator('ndvi')}
            </th>
            <th onClick={() => requestSort('lai')} className="sortable">
              LAI平均 {getSortIndicator('lai')}
            </th>
            <th onClick={() => requestSort('accumulatedTemperature')} className="sortable">
              {gddLabel} {getSortIndicator('accumulatedTemperature')}
            </th>
            <th onClick={() => requestSort('weatherTemperatureAvg')} className="sortable">
              平均気温(°C) {getSortIndicator('weatherTemperatureAvg')}
            </th>
            <th onClick={() => requestSort('weatherPrecipitation')} className="sortable">
              降水量(mm) {getSortIndicator('weatherPrecipitation')}
            </th>
            <th onClick={() => requestSort('weatherSunshineHours')} className="sortable">
              日照時間(h) {getSortIndicator('weatherSunshineHours')}
            </th>
            <th onClick={() => requestSort('weatherHumidity')} className="sortable">
              湿度(%) {getSortIndicator('weatherHumidity')}
            </th>
            <th onClick={() => requestSort('weatherWindSpeed')} className="sortable">
              風速(m/s) {getSortIndicator('weatherWindSpeed')}
            </th>
            <th className="sortable" onClick={() => requestSort('weatherWindDirection')}>
              風向 {getSortIndicator('weatherWindDirection')}
            </th>
            <th onClick={() => requestSort('bbch')} className="sortable">
              BBCHステージ {getSortIndicator('bbch')}
            </th>
            <th onClick={() => requestSort('bbchIndex')} className="sortable">
              BBCH {getSortIndicator('bbchIndex')}
            </th>
          </tr>
        </thead>
        <tbody>
          {tableData.map((item) => (
            <tr
              key={`${item.seasonUuid}-${item.date}`}
              onMouseEnter={() => onHover(item.seasonUuid)}
              onMouseLeave={() => onHover(null)}
              className={highlightedSeason === item.seasonUuid ? 'is-highlighted' : undefined}
            >
              <td>{item.fieldUuid}</td>
              <td>{item.fieldName}</td>
              <td className="numeric">{(item.area / 100).toFixed(2)}</td>
              <td>{item.cropName}</td>
              <td>{item.varietyName}</td>
              <td>{item.seasonStartDate}</td>
              <td>{item.cropEstablishment}</td>
              <td>{item.date}</td>
              <td className="numeric">{item.ndvi !== undefined && item.ndvi !== null ? item.ndvi.toFixed(3) : 'N/A'}</td>
              <td className="numeric">{item.lai !== undefined && item.lai !== null ? item.lai.toFixed(3) : 'N/A'}</td>
              <td className="numeric">
                {item.accumulatedTemperature !== undefined && item.accumulatedTemperature !== null
                  ? `${item.accumulatedTemperature.toFixed(1)} °C日`
                  : 'N/A'}
              </td>
              <td className="numeric">{item.weatherTemperatureAvg !== undefined && item.weatherTemperatureAvg !== null ? item.weatherTemperatureAvg.toFixed(1) : 'N/A'}</td>
              <td className="numeric">{item.weatherPrecipitation !== undefined && item.weatherPrecipitation !== null ? item.weatherPrecipitation.toFixed(1) : 'N/A'}</td>
              <td className="numeric">{item.weatherSunshineHours !== undefined && item.weatherSunshineHours !== null ? item.weatherSunshineHours.toFixed(1) : 'N/A'}</td>
              <td className="numeric">{item.weatherHumidity !== undefined && item.weatherHumidity !== null ? item.weatherHumidity.toFixed(0) : 'N/A'}</td>
              <td className="numeric">{item.weatherWindSpeed !== undefined && item.weatherWindSpeed !== null ? item.weatherWindSpeed.toFixed(1) : 'N/A'}</td>
              <td>{item.weatherWindDirection ?? 'N/A'}</td>
              <td>{item.bbch}</td>
              <td>{item.bbchIndex}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

const NdviComboChart: FC<{ data: NdviDetailRow[]; tasks: TaskEvent[]; weather: WeatherDailyAggregate[]; bbchByDate: Map<string, number> }> = ({ data, tasks, weather, bbchByDate }) => {
  const [selectedSpray, setSelectedSpray] = useState<null | { date: number; label: string; type: string; fieldName: string; products?: { name: string | null; totalApplication: number | null; unit: string | null }[] }>(null);
  const { comboData, spraySeries } = useMemo(() => {
    const normalizeHint = (hint?: string | null) => {
      const upper = (hint || '').toUpperCase();
      if (upper === 'CROP_PROTECTION') return 'CROP_PROTECTION';
      if (upper === 'NUTRITION_MANAGEMENT') return 'NUTRITION_MANAGEMENT';
      if (upper === 'WEED_MANAGEMENT') return 'WEED_MANAGEMENT';
      return null;
    };
    const ndviByIso = new Map<string, { ndvi: number | null; lai: number | null; bbchIndex: number | null; seasonStartEpoch: number }>();
    data.forEach(row => {
      const iso = row.weatherDateIso || (row.date ? row.date.replace(/\//g, '-') : '');
      if (!iso) return;
      ndviByIso.set(iso, {
        ndvi: row.ndvi ?? null,
        lai: row.lai ?? null,
        bbchIndex: row.bbchIndexNum ?? null,
        seasonStartEpoch: row.seasonStartEpoch,
      });
    });

    const weatherSorted = [...weather].sort((a, b) => a.timestamp - b.timestamp);
    const fallbackDates = Array.from(ndviByIso.keys())
      .map(iso => Date.parse(`${iso}T00:00:00Z`))
      .filter((v): v is number => Number.isFinite(v))
      .sort((a, b) => a - b);
    const baseDates = weatherSorted.length > 0 ? weatherSorted.map(w => w.timestamp) : fallbackDates;
    const rows = baseDates.map(ts => {
      const iso = new Date(ts).toISOString().split('T')[0];
      const ndviEntry = ndviByIso.get(iso);
      const weatherEntry = weatherSorted.find(w => w.timestamp === ts);
      const bbchFromPrediction = bbchByDate.get(iso) ?? null;
      return {
        chartDate: ts,
        dateLabel: new Date(ts).toLocaleDateString('ja-JP'),
        seasonStartEpoch: ndviEntry?.seasonStartEpoch ?? ts,
        ndvi: ndviEntry?.ndvi ?? null,
        lai: ndviEntry?.lai ?? null,
        gdd: weatherEntry?.gdd ?? null,
        bbchIndex: ndviEntry?.bbchIndex ?? bbchFromPrediction,
      };
    });

    const sprayGroups = new Map<string, { key: string; label: string; color: string; shape: 'triangle' | 'diamond' | 'square' | 'circle' | 'cross' | 'star' | 'wye'; data: { chartDate: number; y: number; label: string; type: string; fieldName: string; products?: { name: string | null; totalApplication: number | null; unit: string | null }[] }[] }>();
    tasks
      .filter(t => t.type === 'Spraying')
      .forEach(t => {
        const key = normalizeHint(t.hint);
        if (!key) return; // 想定外のcreationFlowHintは表示しない
        const style = SPRAY_MARK_STYLES[key];
        if (!sprayGroups.has(key)) {
          sprayGroups.set(key, { key, label: style.label, color: style.color, shape: style.shape, data: [] });
        }
        const group = sprayGroups.get(key)!;
        const tsRaw = Number.isFinite(t.date) ? t.date : NaN;
        const ts = Number.isFinite(tsRaw) ? startOfDay(new Date(tsRaw)).getTime() : NaN;
        if (!Number.isFinite(ts) || ts <= 0) return;
        group.data.push({
          chartDate: ts,
          y: 1,
          label: t.label,
          type: t.type,
          fieldName: t.fieldName,
          products: t.products,
        });
      });
    return { comboData: rows, spraySeries: Array.from(sprayGroups.values()) };
  }, [data, tasks, weather, bbchByDate]);

  const domain = useMemo(() => {
    const starts = comboData
      .map(r => r.seasonStartEpoch)
      .filter(v => Number.isFinite(v)) as number[];
    const minStart = starts.length ? Math.min(...starts) : undefined;
    const today = startOfDay(new Date()).getTime();
    const minData = comboData.length ? comboData[0].chartDate : undefined;
    const domainMin = minStart ?? minData ?? today;
    const domainMax = Math.max(
      today,
      comboData.length ? comboData[comboData.length - 1].chartDate : today,
    );
    return [domainMin, domainMax] as [number, number];
  }, [comboData]);

  if (comboData.length === 0) {
    return <p style={{ padding: '0.5rem 0' }}>可視化に十分なデータがありません。</p>;
  }

  return (
    <div className="combo-chart-card" style={{ marginTop: '1.5rem' }}>
      <h3 style={{ marginBottom: '0.25rem' }}>NDVI/LAI × 天気 × 積算温度</h3>
      <p style={{ color: '#9e9e9e', marginTop: 0, marginBottom: '0.5rem' }}>現在のテーブル全行のデータを重ねています。</p>
      <ResponsiveContainer width="100%" height={320}>
        <ComposedChart data={comboData} margin={{ top: 10, right: 40, left: 0, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#4a4a4f" />
          <XAxis
            xAxisId="date"
            type="number"
            dataKey="chartDate"
            domain={domain}
            tickFormatter={(value) => new Date(value).toLocaleDateString('ja-JP', { month: 'numeric', day: 'numeric' })}
            stroke="#9e9e9e"
          />
          <YAxis yAxisId="veg" domain={[0, 'auto']} stroke="#9e9e9e" />
          <YAxis yAxisId="weather" orientation="right" stroke="#9e9e9e" />
          <YAxis yAxisId="bbch" orientation="right" stroke="#fdd835" width={36} />
          <YAxis yAxisId="marker" hide domain={[0, 1.2]} />
          <Tooltip
            contentStyle={{ backgroundColor: '#252529', border: '1px solid #4a4a4f' }}
            labelFormatter={(_value, payload: any) => {
              const ts = payload?.[0]?.payload?.chartDate ?? payload?.[0]?.payload?.x;
              return ts ? new Date(ts).toLocaleDateString('ja-JP') : '';
            }}
            formatter={(value: number, name: string) => {
              if (value === null || value === undefined || Number.isNaN(value)) {
                if (name === 'NDVI' || name === 'LAI') return null;
                return ['N/A', name];
              }
              const isSmall = Math.abs(value) < 1 && name === 'NDVI';
              const digits = isSmall ? 3 : 2;
              return [value.toFixed(digits), name];
            }}
          />
          <Legend />
          <Area
            type="monotone"
            dataKey="ndvi"
            name="NDVI"
            yAxisId="veg"
            stroke="#66bb6a"
            fill="#66bb6a"
            fillOpacity={0.2}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="lai"
            name="LAI"
            yAxisId="veg"
            stroke="#aed581"
            strokeWidth={2}
            dot={false}
            connectNulls
          />
          <Line
            type="monotone"
            dataKey="gdd"
            name="積算温度 (°C日)"
            yAxisId="weather"
            stroke="#ffa726"
            strokeWidth={1.6}
            dot={false}
            strokeDasharray="4 4"
            connectNulls
          />
          <Line
            type="stepAfter"
            dataKey="bbchIndex"
            name="BBCH"
            yAxisId="bbch"
            stroke="#fdd835"
            strokeWidth={2}
            dot={false}
            connectNulls
          />
          {spraySeries.map(series => (
            series.data.length > 0 && (
              <Scatter
                key={`spray-${series.key}`}
                name={`散布(${series.label})`}
                data={series.data}
                xAxisId="date"
                yAxisId="marker"
                fill={series.color}
                shape={series.shape}
                dataKey="y"
                isAnimationActive={false}
                onClick={(entry) => {
                  const payload = (entry && (entry as any).payload) || null;
                  if (!payload) return;
                  setSelectedSpray({
                    date: payload.chartDate,
                    label: payload.label,
                    type: payload.type,
                    fieldName: payload.fieldName,
                    products: payload.products,
                  });
                }}
              />
            )
          ))}
        </ComposedChart>
      </ResponsiveContainer>
      {selectedSpray && (
        <div style={{ marginTop: '0.75rem', padding: '0.75rem 1rem', border: '1px solid #3c3c40', borderRadius: 8, background: '#1f1f22' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <div>
              <strong>散布詳細</strong>
              <div style={{ color: '#b0b0b5', fontSize: '0.9em' }}>
                {new Date(selectedSpray.date).toLocaleDateString('ja-JP')} / {selectedSpray.fieldName} / {selectedSpray.label}
              </div>
            </div>
            <button onClick={() => setSelectedSpray(null)} style={{ background: 'transparent', color: '#b0b0b5', border: 'none', cursor: 'pointer' }}>✕</button>
          </div>
          {selectedSpray.products && selectedSpray.products.length > 0 ? (
            <ul style={{ margin: '0.5rem 0 0', paddingLeft: '1rem', color: '#e0e0e5' }}>
              {selectedSpray.products.map((p, idx) => (
                <li key={idx}>
                  {p.name ?? '不明'}: {p.totalApplication != null ? p.totalApplication.toFixed(3) : '-'} {p.unit ?? ''}
                </li>
              ))}
            </ul>
          ) : (
            <div style={{ marginTop: '0.5rem', color: '#b0b0b5' }}>製品情報なし</div>
          )}
        </div>
      )}
    </div>
  );
};
