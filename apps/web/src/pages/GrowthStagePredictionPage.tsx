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
import { ja } from 'date-fns/locale';
import { format, startOfDay, subDays, addDays, differenceInCalendarDays } from 'date-fns';
import { useData } from '../context/DataContext';
import { useFarms } from '../context/FarmContext';
import type { Field, CropSeason, CountryCropGrowthStagePrediction } from '../types/farm';
import './FarmsPage.css'; // 共通スタイルをインポート
import './GrowthStagePredictionPage.css';
import LoadingOverlay from '../components/LoadingOverlay';
import { formatCombinedLoadingMessage } from '../utils/loadingMessage';
import { formatInclusiveEndDate, getLocalDateString } from '../utils/formatters';

ChartJS.register(
  ArcElement, Tooltip, Legend, CategoryScale, LinearScale,
  BarElement, TimeScale, TimeSeriesScale, ChartDataLabels, zoomPlugin, annotationPlugin
);

// =============================================================================
// Type Definitions
// =============================================================================

type GroupedPrediction = {
  seasonUuid: string;
  fieldName: string;
  cropName: string;
  varietyName: string;
  seasonStartDate: string;
  predictions: CountryCropGrowthStagePrediction[];
  prefecture: string | null;
  municipalityLabel: string | null;
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

  return useMemo(() => {
    const fields = selectFieldsFromCombinedOut(combinedOut);
    if (!fields || fields.length === 0) return [];

    const allPredictions = fields.flatMap(field =>
      field.cropSeasonsV2
        ?.filter((season: CropSeason) => season.countryCropGrowthStagePredictions && season.countryCropGrowthStagePredictions.length > 0)
        .map((season: CropSeason) => ({
          seasonUuid: season.uuid,
          fieldName: field.name,
          cropName: season.crop.name,
          varietyName: season.variety.name,
          seasonStartDate: season.startDate,
          predictions: season.countryCropGrowthStagePredictions!,
          prefecture: field.location?.prefecture ?? null,
          municipalityLabel: [
            field.location?.prefectureOffice,
            field.location?.municipality,
            field.location?.subMunicipality,
          ].filter(Boolean).join(' ') || null,
        })) ?? []
    );

    // 作付開始日でソート
    return allPredictions.sort((a, b) => new Date(a.seasonStartDate).getTime() - new Date(b.seasonStartDate).getTime());
  }, [combinedOut]);
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
              stageName: prediction.cropGrowthStageV2?.name ?? '不明なステージ',
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
    combinedErr,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();
  const { submittedFarms, fetchCombinedDataIfNeeded } = useFarms();
  const groupedPredictions = useGroupedPredictions();
  const [fieldQuery, setFieldQuery] = useState('');
  const [selectedCrop, setSelectedCrop] = useState<string>('ALL');
  const [selectedVariety, setSelectedVariety] = useState<string>('ALL');
  const [selectedPrefecture, setSelectedPrefecture] = useState<string>('ALL');
  const [selectedMunicipality, setSelectedMunicipality] = useState<string>('ALL');
  const [enabledStages, setEnabledStages] = useState<string[]>([]);
  const [rowsPerPage, setRowsPerPage] = useState<number>(10);
  const [currentPage, setCurrentPage] = useState<number>(1);
  const [tableRowsPerPage, setTableRowsPerPage] = useState<number>(10);
  const [tableCurrentPage, setTableCurrentPage] = useState<number>(1);
  const [isUpcomingExpanded, setIsUpcomingExpanded] = useState<boolean>(false);

  const cropOptions = useMemo(() => {
    const set = new Set<string>();
    groupedPredictions.forEach(group => set.add(group.cropName));
    return Array.from(set).sort();
  }, [groupedPredictions]);

  const varietyOptions = useMemo(() => {
    const targetGroups = selectedCrop === 'ALL'
      ? groupedPredictions
      : groupedPredictions.filter(group => group.cropName === selectedCrop);
    const set = new Set<string>();
    targetGroups.forEach(group => set.add(group.varietyName));
    return Array.from(set).sort();
  }, [groupedPredictions, selectedCrop]);

  const prefectureOptions = useMemo(() => {
    const set = new Set<string>();
    groupedPredictions.forEach(group => {
      if (group.prefecture) {
        set.add(group.prefecture);
      }
    });
    return Array.from(set).sort();
  }, [groupedPredictions]);

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
    return Array.from(set).sort();
  }, [groupedPredictions, selectedPrefecture]);

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
      const queryMatch =
        lowerQuery.length === 0 ||
        group.fieldName.toLowerCase().includes(lowerQuery) ||
        group.varietyName.toLowerCase().includes(lowerQuery) ||
        group.cropName.toLowerCase().includes(lowerQuery);
      return cropMatch && varietyMatch && prefectureMatch && municipalityMatch && queryMatch;
    });
  }, [groupedPredictions, selectedCrop, selectedVariety, selectedPrefecture, selectedMunicipality, fieldQuery]);

  useEffect(() => {
    setCurrentPage(1);
    setTableCurrentPage(1);
  }, [selectedCrop, selectedVariety, selectedPrefecture, selectedMunicipality, fieldQuery]);

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

  const today = useMemo(() => startOfDay(new Date()), []);
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

    return {
      fieldName: group.fieldName,
      cropName: group.cropName,
      varietyName: group.varietyName,
      seasonStartDate: group.seasonStartDate,
      nextStageIndex: nextStage?.index ?? null,
      nextStageName: nextStage?.cropGrowthStageV2?.name ?? null,
      nextStageStart: nextStage ? new Date(nextStage.start!) : null,
      lastStageIndex: lastStage?.index ?? null,
      lastStageName: lastStage?.cropGrowthStageV2?.name ?? null,
      stageDates,
    };
  }, [today]);

  const tableRows = useMemo(() => tablePagePredictions.map(deriveTableRow), [tablePagePredictions, deriveTableRow]);
  const csvTableRows = useMemo(() => filteredPredictions.map(deriveTableRow), [filteredPredictions, deriveTableRow]);

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
    if (csvTableRows.length === 0) return;

    const headers = [
      '圃場',
      '作物',
      '品種',
      '作付日',
      '次のステージ',
      '予測開始',
      ...stageColumns.map(col => col.label),
    ];

    const rows = csvTableRows.map(row => {
      const nextStageLabel = row.nextStageIndex
        ? `BBCH ${row.nextStageIndex}${row.nextStageName ? ` - ${row.nextStageName}` : ''}`
        : '予定なし';
      const cells = [
        row.fieldName,
        row.cropName,
        row.varietyName,
        row.seasonStartDate ? format(new Date(row.seasonStartDate), 'yyyy/MM/dd') : '不明',
        nextStageLabel,
        row.nextStageStart ? format(row.nextStageStart, 'MM/dd') : '-',
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
    fetchCombinedDataIfNeeded();
  }, [fetchCombinedDataIfNeeded]);

  if (submittedFarms.length === 0) {
    return (
      <div className="farms-page-container">
        <h2>生育ステージ予測</h2>
        <p>ヘッダーのドロップダウンから農場を選択してください。</p>
      </div>
    );
  }

  if (combinedLoading) {
    return (
      <div className="farms-page-container">
        <LoadingOverlay
          message={formatCombinedLoadingMessage(
            '生育ステージ予測データ',
            combinedFetchAttempt,
            combinedFetchMaxAttempts,
            combinedRetryCountdown,
          )}
        />
        <h2>生育ステージ予測</h2>
      </div>
    );
  }

  if (combinedErr) {
    return (
      <div className="farms-page-container">
        <h2>生育ステージ予測</h2>
        <h3 style={{ color: '#ff6b6b' }}>予測データの取得に失敗しました</h3>
        <pre style={{ color: '#ff6b6b', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {combinedErr}
        </pre>
      </div>
    );
  }

  return (
    <div className="farms-page-container">
      <h2>生育ステージ予測</h2>
      <p>
        選択された {submittedFarms.length} 件の農場の生育ステージ予測タイムラインを表示しています。
        {combinedOut?.source && (
          <span style={{ marginLeft: '1em', color: combinedOut.source === 'cache' ? '#4caf50' : '#2196f3', fontWeight: 'bold' }}>
            ({combinedOut.source === 'cache' ? 'キャッシュから取得' : 'APIから取得'})
          </span>
        )}
      </p>

      <div className="gsp-controls">
        <div className="gsp-filter-grid">
          <label>
            圃場 / 作物検索
            <input
              type="text"
              value={fieldQuery}
              onChange={(e) => setFieldQuery(e.currentTarget.value)}
              placeholder="圃場名・作物・品種でフィルタ"
            />
          </label>
          <label>
            作物
            <select value={selectedCrop} onChange={(e) => setSelectedCrop(e.currentTarget.value)}>
              <option value="ALL">すべて</option>
              {cropOptions.map(crop => (
                <option key={crop} value={crop}>{crop}</option>
              ))}
            </select>
          </label>
          <label>
            品種
            <select value={selectedVariety} onChange={(e) => setSelectedVariety(e.currentTarget.value)}>
              <option value="ALL">すべて</option>
              {varietyOptions.map(variety => (
                <option key={variety} value={variety}>{variety}</option>
              ))}
            </select>
          </label>
          <label>
            都道府県
            <select
              value={selectedPrefecture}
              onChange={(e) => {
                const next = e.currentTarget.value;
                setSelectedPrefecture(next);
                setSelectedMunicipality('ALL');
              }}
            >
              <option value="ALL">すべて</option>
              {prefectureOptions.map(pref => (
                <option key={pref} value={pref}>{pref}</option>
              ))}
            </select>
          </label>
          <label>
            市区町村
            <select
              value={selectedMunicipality}
              onChange={(e) => setSelectedMunicipality(e.currentTarget.value)}
            >
              <option value="ALL">すべて</option>
              {municipalityOptions.map(muni => (
                <option key={muni} value={muni}>{muni}</option>
              ))}
            </select>
          </label>
        </div>

        <div className="gsp-stage-filters">
          <div className="gsp-stage-header">
            <strong>表示する BBCH ステージ</strong>
            <div className="gsp-stage-actions">
              <button
                type="button"
                disabled={availableStages.length === 0}
                onClick={() => setEnabledStages(availableStages)}
              >
                すべて選択
              </button>
              <button
                type="button"
                disabled={enabledStages.length === 0}
                onClick={() => setEnabledStages([])}
              >
                全解除
              </button>
            </div>
          </div>
          <div className="gsp-stage-checkboxes">
            {availableStages.length === 0 ? (
              <span>表示可能なステージがありません。</span>
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
        <span>ヒント: グラフはホイール操作またはドラッグでズーム / パンできます。右上の「表示範囲をリセット」ボタンで初期表示に戻せます。</span>
      </div>

      {upcomingStages.length > 0 && (
        <div className="gsp-upcoming">
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '1rem' }}>
            <h3 style={{ margin: 0 }}>直近 {UPCOMING_WINDOW_DAYS} 日以内に開始するステージ</h3>
            <button
              type="button"
              onClick={() => setIsUpcomingExpanded(prev => !prev)}
              aria-expanded={isUpcomingExpanded}
              aria-controls="gsp-upcoming-list"
            >
              {isUpcomingExpanded ? '折りたたむ' : '開く'}
            </button>
          </div>
          {isUpcomingExpanded && (
            <ul id="gsp-upcoming-list">
              {upcomingStages.map((stage, idx) => (
                <li key={`${stage.fieldName}-${stage.stageIndex}-${idx}`}>
                  <strong>{stage.fieldName}</strong> / {stage.cropName}（{stage.varietyName}） : BBCH {stage.stageIndex} {stage.stageName ? `- ${stage.stageName}` : ''}
                  <span style={{ marginLeft: '0.5rem' }}>
                    {format(stage.start, 'MM/dd')} 開始
                  </span>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}

      <div className="gsp-summary">
        フィルタ後の圃場作期: {filteredCount} 件（タイムライン {filteredCount === 0 ? 0 : paginatedPredictions.length} 件 / テーブル {filteredCount === 0 ? 0 : tablePagePredictions.length} 件表示） / 表示中のステージ数: {enabledStages.length}（全 {availableStages.length}）
      </div>

      {filteredCount > 0 && (
        <div className="gsp-pagination-controls">
          <div className="gsp-pagination-left">
            <label htmlFor="gsp-rows-per-page">
              表示件数:{' '}
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
              前へ
            </button>
            <span style={{ margin: '0 0.75rem' }}>
              {currentPage} / {totalPages}
            </span>
            <button
              type="button"
              onClick={() => handlePageChange(1)}
              disabled={currentPage >= totalPages}
            >
              次へ
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
            <h3>作期一覧</h3>
            <button
              type="button"
              onClick={downloadTableCsv}
              disabled={csvTableRows.length === 0}
            >
              CSVダウンロード
            </button>
          </div>
          {filteredCount > 0 && (
            <div className="gsp-pagination-controls gsp-table-pagination">
              <div className="gsp-pagination-left">
                <label htmlFor="gsp-table-rows-per-page">
                  表示件数:{' '}
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
                  前へ
                </button>
                <span style={{ margin: '0 0.75rem' }}>
                  {tableCurrentPage} / {tableTotalPages}
                </span>
                <button
                  type="button"
                  onClick={() => handleTablePageChange(1)}
                  disabled={tableCurrentPage >= tableTotalPages}
                >
                  次へ
                </button>
              </div>
            </div>
          )}
          <table className="gsp-table">
            <thead>
              <tr>
                <th>圃場</th>
                <th>作物</th>
                <th>品種</th>
                <th>作付日</th>
                <th>次のステージ</th>
                <th>予測開始</th>
                {stageColumns.map(col => (
                  <th key={`stage-col-${col.index}`}>{col.label}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {tableRows.map(row => (
                <tr key={`${row.fieldName}-${row.varietyName}-${row.seasonStartDate}`}>
                  <td>{row.fieldName}</td>
                  <td>{row.cropName}</td>
                  <td>{row.varietyName}</td>
                  <td>{row.seasonStartDate ? format(new Date(row.seasonStartDate), 'yyyy/MM/dd') : '不明'}</td>
                  <td>
                    {row.nextStageIndex
                      ? `BBCH ${row.nextStageIndex}${row.nextStageName ? ` - ${row.nextStageName}` : ''}`
                      : '予定なし'}
                  </td>
                  <td>
                    {row.nextStageStart ? format(row.nextStageStart, 'MM/dd') : '-'}
                  </td>
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

  if (!chartData || chartData.datasets.length === 0) {
    return <div className="chart-no-data">表示できる生育予測データがありません。</div>;
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
              return `期間: ${duration}日`;
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
              content: 'Today',
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
        adapters: { date: { locale: ja } },
        time: {
          tooltipFormat: 'yyyy/MM/dd', minUnit: 'day',
          displayFormats: { day: 'M/d', week: 'M/d', month: 'yyyy年 M月', year: 'yyyy年' }
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
          表示範囲をリセット
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
