import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { Ref, UIEvent } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Tooltip,
  Legend,
} from 'chart.js';
import zoomPlugin from 'chartjs-plugin-zoom';
import ChartDataLabels from 'chartjs-plugin-datalabels';
import { Bar } from 'react-chartjs-2';
import { useAuth } from '../context/AuthContext';
import { useFarms } from '../context/FarmContext';
import { useData } from '../context/DataContext';
import type { Field, FieldNote, CropSeason, FieldSeasonPair, CountryCropGrowthStagePrediction } from '../types/farm';
import { formatInclusiveEndDate, getLocalDateString, groupConsecutiveItems } from '../utils/formatters';
import { startOfDay, addDays } from 'date-fns';
import {
  formatCropEstablishmentStage,
  formatCropEstablishmentMethod,
  formatActiveGrowthStage,
  formatNextStageInfo,
  formatRecommendations,
  formatCurrentWaterRecommendations,
} from '../utils/cellFormatters';
import './FarmsPage.css';
import LoadingOverlay from '../components/LoadingOverlay';
import { formatCombinedLoadingMessage } from '../utils/loadingMessage';
import { withApiBase } from '../utils/apiBase';
import { getSessionCache, setSessionCache } from '../utils/sessionCache';

ChartJS.register(CategoryScale, LinearScale, BarElement, Tooltip, Legend, zoomPlugin, ChartDataLabels);

type NoteImageInfo = {
  url: string;
  noteDate: string;
  noteUuid: string;
  fileName?: string | null;
};

type NoteImageItem = {
  url: string;
  noteDate: string;
  noteUuid: string;
  fileName?: string | null;
  noteText?: string | null;
  categories?: string[] | null;
  creatorName?: string | null;
};

type NoteImageState = 'idle' | 'loading' | 'error' | 'empty';

type FieldCenter = {
  latitude: number;
  longitude: number;
};

const getFieldCenter = (field: Field): FieldCenter | null => {
  const candidates = [field.location?.center, (field as any).center, (field as any).centroid];
  for (const candidate of candidates) {
    if (candidate && typeof candidate.latitude === 'number' && typeof candidate.longitude === 'number') {
      return { latitude: candidate.latitude, longitude: candidate.longitude };
    }
  }
  return null;
};

const getFarmName = (field: Field): string => {
  return field.farmV2?.name ?? field.farm?.name ?? '';
};

const getFarmOwnerName = (field: Field): string => {
  const owner = field.farmV2?.owner ?? field.farm?.owner ?? null;
  const name = owner ? `${owner.lastName ?? ''} ${owner.firstName ?? ''}`.trim() : '';
  return name || owner?.email || '';
};

const getBbch89Date = (season: CropSeason | null): string => {
  const preds = season?.countryCropGrowthStagePredictions ?? null;
  if (!Array.isArray(preds) || preds.length === 0) return '';
  const hit = preds.find((p) => String(p?.gsOrder ?? '') === '89' || String(p?.index ?? '') === '89');
  if (!hit?.startDate) return '';
  return getLocalDateString(hit.startDate);
};

const getTargetYieldLabel = (season: CropSeason | null): string => {
  const raw = season?.yieldExpectation ?? null;
  const num = typeof raw === 'number' ? raw : Number(raw);
  if (!Number.isFinite(num)) return '';
  // Xarvioの yieldExpectation が t/10a 相当(例: 0.55 => 550kg/10a)で返るケースがあるため補正
  const kgPer10a = num > 0 && num < 10 ? num * 1000 : num;
  return `${kgPer10a.toLocaleString('ja-JP', { maximumFractionDigits: 1 })} kg/10a`;
};

const isImageAttachment = (att: { mimeType?: string | null; contentType?: string | null; fileName?: string | null; url?: string }) => {
  const mime = (att.mimeType || att.contentType || '').toLowerCase();
  if (mime.startsWith('image/')) return true;
  const name = (att.fileName || att.url || '').toLowerCase();
  return /\.(png|jpe?g|jpeg|gif|webp|svg)$/.test(name);
};

const getLatestNoteImage = (notes: FieldNote[] | null | undefined): NoteImageInfo | null => {
  if (!notes || notes.length === 0) return null;
  const sorted = [...notes].sort(
    (a, b) => new Date(b.creationDate).getTime() - new Date(a.creationDate).getTime()
  );
  for (const note of sorted) {
    const attachment = note.attachments?.find((att) => att.url && isImageAttachment(att));
    if (attachment?.url) {
      return {
        url: attachment.url,
        fileName: attachment.fileName ?? null,
        noteDate: note.creationDate,
        noteUuid: note.uuid,
      };
    }
  }
  return null;
};

const buildNoteImageItems = (notes: FieldNote[] | null | undefined): NoteImageItem[] => {
  if (!notes || notes.length === 0) return [];
  const items: NoteImageItem[] = [];
  notes.forEach((note) => {
    const creatorName = note.creator
      ? `${note.creator.lastName} ${note.creator.firstName}`.trim()
      : null;
    (note.attachments || [])
      .filter((att) => att.url && isImageAttachment(att))
      .forEach((att) => {
        items.push({
          url: att.url,
          fileName: att.fileName ?? null,
          noteDate: note.creationDate,
          noteUuid: note.uuid,
          noteText: note.note ?? null,
          categories: note.categories ?? null,
          creatorName,
        });
      });
  });
  return items.sort((a, b) => new Date(b.noteDate).getTime() - new Date(a.noteDate).getTime());
};

const getFieldNotesCacheKey = (farmUuids: string[]) =>
  `field-notes:${[...farmUuids].sort().join(',')}`;

async function fetchFieldNotesApi(params: { auth: { login: { login_token: string }; api_token: string }; farmUuids: string[] }) {
  const cacheKey = getFieldNotesCacheKey(params.farmUuids);
  if (params.farmUuids.length > 0) {
    const cached = getSessionCache<any>(cacheKey);
    if (cached) return { ...cached, source: 'cache' };
  }
  const requestBody = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    farm_uuids: params.farmUuids,
  };
  const res = await fetch(withApiBase('/field-notes'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(requestBody),
  });
  const out = await res.json();
  if (res.ok && params.farmUuids.length > 0) {
    setSessionCache(cacheKey, { ...out, source: 'api' });
  }
  return { ...out, source: 'api' };
}

// =============================================================================
// Main Component
// =============================================================================

/**
 * テーブルヘッダーコンポーネント
 */
const FieldsTableHeader = ({
  requestSort,
  sortConfig,
  isAllSelectedOnPage,
  isSomeSelectedOnPage,
  onToggleSelectAll,
}: {
  requestSort: (key: string) => void;
  sortConfig: SortConfig;
  isAllSelectedOnPage: boolean;
  isSomeSelectedOnPage: boolean;
  onToggleSelectAll: (nextValue: boolean) => void;
}) => {
  const selectAllRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!selectAllRef.current) return;
    selectAllRef.current.indeterminate = isSomeSelectedOnPage && !isAllSelectedOnPage;
  }, [isAllSelectedOnPage, isSomeSelectedOnPage]);

  const getSortIndicator = (key: string) => {
    if (!sortConfig || sortConfig.key !== key) {
      return ' ↕';
    }
    return sortConfig.direction === 'ascending' ? ' ▲' : ' ▼';
  };

  const headerCell = (key: string, label: string, sortable = true) => (
    <th
      onClick={() => sortable && requestSort(key)}
      className={sortable ? 'sortable' : 'non-sortable'}
    >
      {label}
      {sortable && getSortIndicator(key)}
    </th>
  );

  return (
    <thead>
      <tr>
        <th className="selection-cell non-sortable">
          <input
            ref={selectAllRef}
            type="checkbox"
            checked={isAllSelectedOnPage}
            onChange={(event) => onToggleSelectAll(event.target.checked)}
            aria-label="表示中の圃場をすべて選択"
          />
        </th>
        {headerCell('field.name', '圃場')}
        {headerCell('field.farmV2.name', '農場', false)}
        {headerCell('field.farmV2.owner', 'ユーザー', false)}
        {headerCell('field.location.prefecture', '都道府県', false)}
        {headerCell('field.location.municipality', '市区町村', false)}
        {headerCell('field.location.center.latitude', '緯度', false)}
        {headerCell('field.location.center.longitude', '経度', false)}
        {headerCell('field.area', '面積(a)')}
        {headerCell('season.crop.name', '作物')}
        {headerCell('season.variety.name', '品種')}
        {headerCell('season.startDate', '作付日')}
        {headerCell('season.countryCropGrowthStagePredictions', 'BBCH89到達日', false)}
        {headerCell('season.yieldExpectation', '目標収量', false)}
        {headerCell('season.cropEstablishmentGrowthStageIndex', '作付時の生育ステージ')}
        {headerCell('season.cropEstablishmentMethodCode', '作付方法', false)}
        {headerCell('season.activeGrowthStage.gsOrder', '現在の生育ステージ')}
        {headerCell('nextStage.gsOrder', '次の生育ステージ')}
        {headerCell('nutritionRecommendations', '施肥推奨', false)}
        {headerCell('waterRecommendations', '水管理', false)}
        {headerCell('weedManagementRecommendations', '雑草管理', false)}
        {headerCell('riskAlert', 'リスクアラート', false)}
      </tr>
    </thead>
  );
};

/**
 * 圃場データをテーブル形式で表示するコンポーネント
 */
function FieldsTable({
  fieldSeasonPairs,
  requestSort,
  sortConfig,
  selectedFieldIds,
  onToggleFieldSelection,
  isAllSelectedOnPage,
  isSomeSelectedOnPage,
  onToggleSelectAll,
  noteImageByField,
  noteImageStateByField,
  noteImageErrorByField,
  onOpenNoteList,
  tableRef,
  locationByFieldUuid,
}: {
  fieldSeasonPairs: FieldSeasonPair[];
  requestSort: (key: string) => void;
  sortConfig: SortConfig;
  selectedFieldIds: Set<string>;
  onToggleFieldSelection: (fieldId: string) => void;
  isAllSelectedOnPage: boolean;
  isSomeSelectedOnPage: boolean;
  onToggleSelectAll: (nextValue: boolean) => void;
  noteImageByField: Record<string, NoteImageInfo | null>;
  noteImageStateByField: Record<string, NoteImageState>;
  noteImageErrorByField: Record<string, string>;
  onOpenNoteList: (field: Field) => void;
  tableRef?: Ref<HTMLTableElement>;
  locationByFieldUuid: Record<string, Partial<NonNullable<Field['location']>>>;
}) {
  return (
    <table className="fields-table" ref={tableRef}>
      <FieldsTableHeader
        requestSort={requestSort}
        sortConfig={sortConfig}
        isAllSelectedOnPage={isAllSelectedOnPage}
        isSomeSelectedOnPage={isSomeSelectedOnPage}
        onToggleSelectAll={onToggleSelectAll}
      />
      <tbody>
        {fieldSeasonPairs.map((pair, index) => {
          const { field, season } = pair;
          const noteImage = noteImageByField[field.uuid] ?? null;
          const noteState = noteImageStateByField[field.uuid] ?? 'idle';
          const noteError = noteImageErrorByField[field.uuid] ?? '';
          const locationOverride = locationByFieldUuid[field.uuid];
          const effectiveLocation = locationOverride
            ? ({ ...(field.location ?? {}), ...locationOverride } as Field['location'])
            : field.location;
          const center = getFieldCenter(field);
          const farmName = getFarmName(field);
          const ownerName = getFarmOwnerName(field);
          const bbch89Date = getBbch89Date(season);
          const targetYield = getTargetYieldLabel(season);
          return (
            <tr key={`${field.uuid}-${season?.uuid ?? index}`}>
              <td className="selection-cell">
                <input
                  type="checkbox"
                  checked={selectedFieldIds.has(field.uuid)}
                  onChange={() => onToggleFieldSelection(field.uuid)}
                  aria-label={`${field.name}を選択`}
                />
              </td>
              <td>
                <div className="field-name-cell">
                  <div>
                    <strong>{field.name}</strong>
                    <p style={{ color: '#888', fontSize: '0.8em', margin: '4px 0 0' }}>{field.uuid}</p>
                  </div>
                  <div className="field-note-actions">
                    <button
                      type="button"
                      className="field-note-button"
                      onClick={() => onOpenNoteList(field)}
                    >
                      一覧
                    </button>
                    {noteState === 'loading' && (
                      <span className="field-note-loading">取得中...</span>
                    )}
                    {noteState === 'error' && (
                      <span className="field-note-error">{noteError || '取得失敗'}</span>
                    )}
                  </div>
                  {noteImage && (
                    <div className="field-note-preview">
                      <a href={noteImage.url} target="_blank" rel="noreferrer">
                        <img
                          src={noteImage.url}
                          alt={`${field.name} 最新ノート画像`}
                          loading="lazy"
                        />
                      </a>
                      <div className="field-note-meta">{getLocalDateString(noteImage.noteDate)}</div>
                    </div>
                  )}
                  {noteState === 'empty' && (
                    <div className="field-note-empty">画像付きノートなし</div>
                  )}
                </div>
              </td>
              <td>{farmName || 'N/A'}</td>
              <td>{ownerName || 'N/A'}</td>
              <td><LocationPrefectureCell location={effectiveLocation} /></td>
              <td><LocationMunicipalityCell location={effectiveLocation} /></td>
              <td><CoordinateCell value={center?.latitude ?? null} /></td>
              <td><CoordinateCell value={center?.longitude ?? null} /></td>
              <td>{(field.area / 100).toFixed(2)}</td>
              <td>{season?.crop.name ?? 'N/A'}</td>
              <td>{season?.variety.name ?? 'N/A'}</td>
              <td>
                {season?.startDate ? getLocalDateString(season.startDate) : 'N/A'}
              </td>
              <td>{bbch89Date || 'N/A'}</td>
              <td>{targetYield || 'N/A'}</td>
              <td>{formatCropEstablishmentStage(season)}</td>
              <td>{formatCropEstablishmentMethod(season)}</td>
              <td>{formatActiveGrowthStage(season)}</td>
              <td>
                <NextStageCell pair={pair} />
              </td>
              <td>
                <NutritionRecCell season={season} />
              </td>
              <td>
                <WaterRecCell season={season} />
              </td>
              <td>
                <WeedRecCell season={season} />
              </td>
              <td>
                <RiskAlertCell season={season} />
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

export function FarmsPage() {
  const { auth } = useAuth();
  const { submittedFarms, fetchCombinedDataIfNeeded } = useFarms();
  const {
    combinedOut,
    combinedLoading,
    combinedErr,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();
  const [seasonView, setSeasonView] = useState<'active' | 'closed'>('active');
  const [closedCombinedOut, setClosedCombinedOut] = useState<any | null>(null);
  const [closedLoading, setClosedLoading] = useState(false);
  const [closedError, setClosedError] = useState<string | null>(null);
  // ソート用の状態
  type SortConfig = { key: string; direction: 'ascending' | 'descending' } | null;
  const [sortConfig, setSortConfig] = useState<SortConfig>({
    key: 'field.name',
    direction: 'ascending',
  });
  const [selectedFieldIds, setSelectedFieldIds] = useState<Set<string>>(new Set());
  const [noteImageByField, setNoteImageByField] = useState<Record<string, NoteImageInfo | null>>({});
  const [noteImageStateByField, setNoteImageStateByField] = useState<Record<string, NoteImageState>>({});
  const [noteImageErrorByField, setNoteImageErrorByField] = useState<Record<string, string>>({});
  const notesCacheRef = useRef<Map<string, Record<string, FieldNote[]>>>(new Map());
  const notesInFlightRef = useRef<Map<string, Promise<Record<string, FieldNote[]>>>>(new Map());
  const autoNoteFetchRef = useRef<Set<string>>(new Set());
  const [noteModalField, setNoteModalField] = useState<Field | null>(null);
  const [noteModalState, setNoteModalState] = useState<NoteImageState>('idle');
  const [noteModalError, setNoteModalError] = useState('');
  const [noteModalItems, setNoteModalItems] = useState<NoteImageItem[]>([]);
  const [noteModalLimit, setNoteModalLimit] = useState<number>(10);
  const [noteModalFrom, setNoteModalFrom] = useState('');
  const [noteModalTo, setNoteModalTo] = useState('');
  const [prefCityByFieldUuid, setPrefCityByFieldUuid] = useState<Record<string, Partial<NonNullable<Field['location']>>>>({});
  const prefCityWorkerRef = useRef<Worker | null>(null);
  const prefCityPendingRef = useRef<Set<string>>(new Set());
  const [prefCityDatasetReady, setPrefCityDatasetReady] = useState(false);
  const tableContainerRef = useRef<HTMLDivElement | null>(null);
  const tableRef = useRef<HTMLTableElement | null>(null);
  const tableScrollbarRef = useRef<HTMLDivElement | null>(null);
  const [tableScrollbarWidth, setTableScrollbarWidth] = useState(0);
  const [showTableScrollbar, setShowTableScrollbar] = useState(false);
  const isSyncingScrollRef = useRef(false);

  useEffect(() => {
    // when farms selection changes, reset closed view cache
    setClosedCombinedOut(null);
    setClosedError(null);
    setSelectedFieldIds(new Set());
  }, [submittedFarms]);

  const fetchClosedCombined = useCallback(async () => {
    if (!auth) return;
    if (submittedFarms.length === 0) return;
    setClosedLoading(true);
    setClosedError(null);
    try {
      const res = await fetch(withApiBase('/combined-fields'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          login_token: auth.login.login_token,
          api_token: auth.api_token,
          farm_uuids: submittedFarms,
          languageCode: 'ja',
          countryCode: 'JP',
          cropSeasonLifeCycleStates: ['CLOSED'],
          withBoundarySvg: true,
          stream: false,
          includeTasks: false,
          includeTokens: false,
        }),
      });
      const out = await res.json();
      if (!res.ok || out?.ok === false) {
        const detail = out?.detail ?? out?.message ?? out?.error ?? `HTTP ${res.status}`;
        throw new Error(detail);
      }
      setClosedCombinedOut(out);
    } catch (e) {
      const message = e instanceof Error ? e.message : '過去作期の取得に失敗しました。';
      setClosedError(message);
    } finally {
      setClosedLoading(false);
    }
  }, [auth, submittedFarms]);

  useEffect(() => {
    if (seasonView !== 'closed') return;
    if (closedCombinedOut) return;
    if (closedLoading) return;
    fetchClosedCombined();
  }, [seasonView, closedCombinedOut, closedLoading, fetchClosedCombined]);

  const combinedOutForView = seasonView === 'closed' ? closedCombinedOut : combinedOut;

  const {
    paginatedFieldSeasonPairs,
    sortedFieldSeasonPairs,
    allFieldSeasonPairsWithNextStage,
    totalPages,
    currentPage,
    rowsPerPage,
    setCurrentPage,
    setRowsPerPage,
  } = usePaginatedFields(combinedOutForView, sortConfig, { hideEmptySeasons: seasonView === 'closed' });

  const requestSort = (key: string) => {
    let direction: 'ascending' | 'descending' = 'ascending';
    if (sortConfig && sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
    setCurrentPage(1); // ソート後は1ページ目に戻る
  };

  const fetchFieldNotesMap = async (farmUuid: string, authInfo: { login: { login_token: string }; api_token: string }) => {
    const cached = notesCacheRef.current.get(farmUuid);
    if (cached) return cached;
    const pending = notesInFlightRef.current.get(farmUuid);
    if (pending) return pending;

    const request = (async () => {
      const out = await fetchFieldNotesApi({ auth: authInfo, farmUuids: [farmUuid] });
      const fields = out?.response?.data?.fieldsV2;
      if (!Array.isArray(fields)) {
        throw new Error('fieldNotes response missing');
      }
      const map: Record<string, FieldNote[]> = {};
      fields.forEach((f: { uuid: string; fieldNotes?: FieldNote[] | null }) => {
        map[f.uuid] = f.fieldNotes ?? [];
      });
      notesCacheRef.current.set(farmUuid, map);
      return map;
    })();

    notesInFlightRef.current.set(farmUuid, request);
    try {
      return await request;
    } finally {
      notesInFlightRef.current.delete(farmUuid);
    }
  };

  const handleFetchLatestNoteImage = async (field: Field) => {
    const fieldId = field.uuid;
    setNoteImageStateByField((prev) => ({ ...prev, [fieldId]: 'loading' }));
    setNoteImageErrorByField((prev) => ({ ...prev, [fieldId]: '' }));

    if (!auth) {
      setNoteImageStateByField((prev) => ({ ...prev, [fieldId]: 'error' }));
      setNoteImageErrorByField((prev) => ({ ...prev, [fieldId]: '未ログイン' }));
      return;
    }

    const farmUuid = field.farmV2?.uuid ?? field.farm?.uuid ?? null;
    if (!farmUuid) {
      setNoteImageStateByField((prev) => ({ ...prev, [fieldId]: 'error' }));
      setNoteImageErrorByField((prev) => ({ ...prev, [fieldId]: 'Farm UUIDなし' }));
      return;
    }

    let fieldNotesMap = notesCacheRef.current.get(farmUuid);
    if (!fieldNotesMap) {
      try {
        fieldNotesMap = await fetchFieldNotesMap(farmUuid, auth);
      } catch (error) {
        setNoteImageStateByField((prev) => ({ ...prev, [fieldId]: 'error' }));
        const message = error instanceof Error ? error.message : '取得失敗';
        setNoteImageErrorByField((prev) => ({ ...prev, [fieldId]: message }));
        return;
      }
    }

    const latest = getLatestNoteImage(fieldNotesMap[fieldId] ?? []);
    if (!latest) {
      setNoteImageByField((prev) => ({ ...prev, [fieldId]: null }));
      setNoteImageStateByField((prev) => ({ ...prev, [fieldId]: 'empty' }));
      return;
    }

    setNoteImageByField((prev) => ({ ...prev, [fieldId]: latest }));
    setNoteImageStateByField((prev) => ({ ...prev, [fieldId]: 'idle' }));
  };

  const handleOpenNoteList = async (field: Field) => {
    setNoteModalField(field);
    setNoteModalState('loading');
    setNoteModalError('');
    setNoteModalItems([]);
    setNoteModalLimit(10);
    setNoteModalFrom('');
    setNoteModalTo('');

    if (!auth) {
      setNoteModalState('error');
      setNoteModalError('未ログイン');
      return;
    }

    const farmUuid = field.farmV2?.uuid ?? field.farm?.uuid ?? null;
    if (!farmUuid) {
      setNoteModalState('error');
      setNoteModalError('Farm UUIDなし');
      return;
    }

    try {
      const fieldNotesMap = await fetchFieldNotesMap(farmUuid, auth);
      const items = buildNoteImageItems(fieldNotesMap[field.uuid] ?? []);
      if (items.length === 0) {
        setNoteModalState('empty');
      } else {
        setNoteModalState('idle');
      }
      setNoteModalItems(items);
    } catch (error) {
      setNoteModalState('error');
      const message = error instanceof Error ? error.message : '取得失敗';
      setNoteModalError(message);
    }
  };

  useEffect(() => {
    if (!auth) return;
    paginatedFieldSeasonPairs.forEach(({ field }) => {
      if (autoNoteFetchRef.current.has(field.uuid)) return;
      const state = noteImageStateByField[field.uuid];
      if (state === 'loading') return;
      if (noteImageByField[field.uuid] !== undefined) return;
      autoNoteFetchRef.current.add(field.uuid);
      handleFetchLatestNoteImage(field);
    });
  }, [auth, paginatedFieldSeasonPairs, noteImageByField, noteImageStateByField]);

  useEffect(() => {
    const updateScrollbar = () => {
      const tableEl = tableRef.current;
      const containerEl = tableContainerRef.current;
      if (!tableEl || !containerEl) return;
      const scrollWidth = tableEl.scrollWidth;
      setTableScrollbarWidth(scrollWidth);
      setShowTableScrollbar(scrollWidth > containerEl.clientWidth + 1);
    };

    updateScrollbar();
    window.addEventListener('resize', updateScrollbar);
    return () => window.removeEventListener('resize', updateScrollbar);
  }, [paginatedFieldSeasonPairs, rowsPerPage]);

  const handleTableScroll = (event: UIEvent<HTMLDivElement>) => {
    if (isSyncingScrollRef.current) return;
    isSyncingScrollRef.current = true;
    if (tableScrollbarRef.current) {
      tableScrollbarRef.current.scrollLeft = event.currentTarget.scrollLeft;
    }
    requestAnimationFrame(() => {
      isSyncingScrollRef.current = false;
    });
  };

  const handleScrollbarScroll = (event: UIEvent<HTMLDivElement>) => {
    if (isSyncingScrollRef.current) return;
    isSyncingScrollRef.current = true;
    if (tableContainerRef.current) {
      tableContainerRef.current.scrollLeft = event.currentTarget.scrollLeft;
    }
    requestAnimationFrame(() => {
      isSyncingScrollRef.current = false;
    });
  };

  const filteredNoteModalItems = useMemo(() => {
    let items = noteModalItems;
    if (noteModalFrom) {
      const fromDate = new Date(noteModalFrom);
      items = items.filter((item) => new Date(item.noteDate) >= fromDate);
    }
    if (noteModalTo) {
      const toDate = new Date(noteModalTo);
      toDate.setHours(23, 59, 59, 999);
      items = items.filter((item) => new Date(item.noteDate) <= toDate);
    }
    if (Number.isFinite(noteModalLimit)) {
      items = items.slice(0, noteModalLimit);
    }
    return items;
  }, [noteModalItems, noteModalFrom, noteModalTo, noteModalLimit]);

  const closeNoteModal = () => {
    setNoteModalField(null);
    setNoteModalItems([]);
    setNoteModalState('idle');
    setNoteModalError('');
  };

  useEffect(() => {
    fetchCombinedDataIfNeeded();
  }, [fetchCombinedDataIfNeeded]);

  useEffect(() => {
    const existingIds = new Set(sortedFieldSeasonPairs.map(pair => pair.field.uuid));
    setSelectedFieldIds(prev => {
      const next = new Set<string>();
      prev.forEach((id) => {
        if (existingIds.has(id)) next.add(id);
      });
      return next;
    });
  }, [sortedFieldSeasonPairs]);

  type ExportGeometry = { type: 'Polygon' | 'MultiPolygon' | 'Point'; coordinates: any };

  const parseBoundaryGeometry = (boundary: any): ExportGeometry | null => {
    if (!boundary) return null;
    const pickGeometry = (raw: any) => {
      if (!raw || typeof raw !== 'object') return null;
      const candidate = raw.geojson || raw.geoJson || raw.geometry || raw;
      if (
        candidate &&
        (candidate.type === 'Polygon' || candidate.type === 'MultiPolygon') &&
        Array.isArray(candidate.coordinates)
      ) {
        return { type: candidate.type, coordinates: candidate.coordinates } as ExportGeometry;
      }
      return null;
    };

    if (typeof boundary === 'string') {
      const text = boundary.trim();
      if (text.startsWith('{') && text.endsWith('}')) {
        try {
          const parsed = JSON.parse(text);
          return pickGeometry(parsed);
        } catch {
          return null;
        }
      }
      return null;
    }
    return pickGeometry(boundary);
  };

  const getFieldCenter = (field: Field) => {
    const candidates = [field.location?.center, (field as any).center, (field as any).centroid];
    for (const candidate of candidates) {
      if (
        candidate &&
        typeof candidate.latitude === 'number' &&
        typeof candidate.longitude === 'number'
      ) {
        return { latitude: candidate.latitude, longitude: candidate.longitude };
      }
    }
    return null;
  };

  const buildStyleKey = (pair: FieldSeasonPair) => {
    const crop = pair.season?.crop.name ?? '不明作物';
    const variety = pair.season?.variety.name ?? '';
    return variety ? `${crop} / ${variety}` : crop;
  };

  const buildMyMapsFeature = (pair: FieldSeasonPair, index: number) => {
    const fieldAny = pair.field as Field & { boundary?: any; center?: any; centroid?: any };
    const geometry = parseBoundaryGeometry(fieldAny.boundary);
    const center = geometry ? null : getFieldCenter(pair.field);
    if (!geometry && !center) return null;
    const finalGeometry: ExportGeometry = geometry ?? {
      type: 'Point',
      coordinates: [center!.longitude, center!.latitude],
    };

    return {
      type: 'Feature',
      properties: {
        '圃場名': pair.field.name,
        '作物名': pair.season?.crop.name ?? '',
        '品種名': pair.season?.variety.name ?? '',
        '作付日': pair.season?.startDate ? getLocalDateString(pair.season.startDate) : '',
        '圃場ID': pair.field.uuid,
        __styleKey: buildStyleKey(pair),
        __row: index + 1,
      },
      geometry: finalGeometry,
    };
  };

  const escapeXml = (value: string) =>
    value
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&apos;');

  const hashString = (value: string) => {
    let hash = 0;
    for (let i = 0; i < value.length; i += 1) {
      hash = value.charCodeAt(i) + ((hash << 5) - hash);
    }
    return Math.abs(hash);
  };

  const hslToRgb = (h: number, s: number, l: number) => {
    const sNorm = s / 100;
    const lNorm = l / 100;
    const c = (1 - Math.abs(2 * lNorm - 1)) * sNorm;
    const x = c * (1 - Math.abs(((h / 60) % 2) - 1));
    const m = lNorm - c / 2;
    let r = 0;
    let g = 0;
    let b = 0;
    if (h >= 0 && h < 60) {
      r = c;
      g = x;
      b = 0;
    } else if (h < 120) {
      r = x;
      g = c;
      b = 0;
    } else if (h < 180) {
      r = 0;
      g = c;
      b = x;
    } else if (h < 240) {
      r = 0;
      g = x;
      b = c;
    } else if (h < 300) {
      r = x;
      g = 0;
      b = c;
    } else {
      r = c;
      g = 0;
      b = x;
    }
    return {
      r: Math.round((r + m) * 255),
      g: Math.round((g + m) * 255),
      b: Math.round((b + m) * 255),
    };
  };

  const toKmlColor = (rgb: { r: number; g: number; b: number }, alphaHex: string) => {
    const toHex = (value: number) => value.toString(16).padStart(2, '0');
    return `${alphaHex}${toHex(rgb.b)}${toHex(rgb.g)}${toHex(rgb.r)}`;
  };

  const styleKeyToId = (styleKey: string) => `style-${hashString(styleKey)}`;

  const buildStyle = (styleKey: string) => {
    const hue = hashString(styleKey) % 360;
    const baseRgb = hslToRgb(hue, 62, 52);
    const fillColor = toKmlColor(baseRgb, '88');
    const lineColor = toKmlColor(baseRgb, 'ff');
    const pointColor = toKmlColor(baseRgb, 'ff');
    return `
  <Style id="${styleKeyToId(styleKey)}">
    <LineStyle>
      <color>${lineColor}</color>
      <width>1.6</width>
    </LineStyle>
    <PolyStyle>
      <color>${fillColor}</color>
      <fill>1</fill>
      <outline>1</outline>
    </PolyStyle>
    <IconStyle>
      <color>${pointColor}</color>
      <scale>1.1</scale>
    </IconStyle>
    <LabelStyle>
      <scale>1.0</scale>
    </LabelStyle>
  </Style>`;
  };

  const closeRing = (ring: number[][]) => {
    if (ring.length === 0) return ring;
    const [firstLon, firstLat] = ring[0];
    const [lastLon, lastLat] = ring[ring.length - 1];
    if (firstLon === lastLon && firstLat === lastLat) return ring;
    return [...ring, ring[0]];
  };

  const ringToKml = (ring: number[][]) =>
    closeRing(ring)
      .map(([lon, lat]) => `${lon},${lat},0`)
      .join(' ');

  const polygonToKml = (rings: number[][][]) => {
    const outer = rings[0] ?? [];
    const inner = rings.slice(1);
    const innerKml = inner
      .map(
        (ring) => `
      <innerBoundaryIs>
        <LinearRing>
          <coordinates>${ringToKml(ring)}</coordinates>
        </LinearRing>
      </innerBoundaryIs>`,
      )
      .join('');
    return `
    <Polygon>
      <outerBoundaryIs>
        <LinearRing>
          <coordinates>${ringToKml(outer)}</coordinates>
        </LinearRing>
      </outerBoundaryIs>${innerKml}
    </Polygon>`;
  };

  const geometryToKml = (geometry: ExportGeometry) => {
    if (geometry.type === 'Point') {
      const [lon, lat] = geometry.coordinates as [number, number];
      return `
    <Point>
      <coordinates>${lon},${lat},0</coordinates>
    </Point>`;
    }
    if (geometry.type === 'Polygon') {
      return polygonToKml(geometry.coordinates as number[][][]);
    }
    const polygons = geometry.coordinates as number[][][][];
    return `
    <MultiGeometry>
      ${polygons.map(polygonToKml).join('')}
    </MultiGeometry>`;
  };

  const buildPlacemark = (feature: any) => {
    const props = feature.properties ?? {};
    const name = escapeXml(String(props['圃場名'] ?? '圃場'));
    const styleKey = String(props.__styleKey ?? '');
    const dataEntries = Object.entries(props)
      .filter(([key]) => !String(key).startsWith('__'))
      .map(
        ([key, value]) => `
      <Data name="${escapeXml(String(key))}">
        <value>${escapeXml(String(value ?? ''))}</value>
      </Data>`,
      )
      .join('');
    return `
  <Placemark>
    <name>${name}</name>
    ${styleKey ? `<styleUrl>#${styleKeyToId(styleKey)}</styleUrl>` : ''}
    <ExtendedData>${dataEntries}
    </ExtendedData>${geometryToKml(feature.geometry)}
  </Placemark>`;
  };

  const downloadForMyMaps = (pairs: FieldSeasonPair[], scopeLabel: string) => {
    const features = pairs
      .map((pair, index) => buildMyMapsFeature(pair, index))
      .filter(Boolean);
    if (features.length === 0) return;

    const styleKeys = Array.from(
      new Set((features as any[]).map((feature) => String(feature.properties?.__styleKey ?? ''))),
    ).filter((key) => key.length > 0);
    const styles = styleKeys.map(buildStyle).join('');
    const placemarks = (features as any[]).map(buildPlacemark).join('');
    const payload = `<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>${styles}${placemarks}
</Document>
</kml>`;

    const date = new Date().toISOString().split('T')[0];
    const fileName = `fields_mymaps_${scopeLabel}_${date}.kml`;
    const blob = new Blob([payload], { type: 'application/vnd.google-earth.kml+xml' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = fileName;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  const selectedFieldSeasonPairs = useMemo(
    () => sortedFieldSeasonPairs.filter(pair => selectedFieldIds.has(pair.field.uuid)),
    [sortedFieldSeasonPairs, selectedFieldIds],
  );

  const uniqueFieldIds = useMemo(
    () => Array.from(new Set(sortedFieldSeasonPairs.map(pair => pair.field.uuid))),
    [sortedFieldSeasonPairs],
  );

  const pageFieldIds = useMemo(
    () => Array.from(new Set(paginatedFieldSeasonPairs.map(pair => pair.field.uuid))),
    [paginatedFieldSeasonPairs],
  );

  const isAllSelectedOnPage =
    pageFieldIds.length > 0 && pageFieldIds.every(id => selectedFieldIds.has(id));
  const isSomeSelectedOnPage =
    pageFieldIds.some(id => selectedFieldIds.has(id)) && !isAllSelectedOnPage;

  const handleToggleFieldSelection = (fieldId: string) => {
    setSelectedFieldIds(prev => {
      const next = new Set(prev);
      if (next.has(fieldId)) {
        next.delete(fieldId);
      } else {
        next.add(fieldId);
      }
      return next;
    });
  };

  const handleToggleSelectAllOnPage = (nextValue: boolean) => {
    setSelectedFieldIds(prev => {
      const next = new Set(prev);
      pageFieldIds.forEach((id) => {
        if (nextValue) {
          next.add(id);
        } else {
          next.delete(id);
        }
      });
      return next;
    });
  };

  const showLoading = combinedLoading || closedLoading;
  const loadingMessage = closedLoading
    ? '過去作期の圃場データを取得しています...'
    : formatCombinedLoadingMessage(
        '圃場データ',
        combinedFetchAttempt,
        combinedFetchMaxAttempts,
        combinedRetryCountdown,
      );

  const handlePageChange = (newPage: number) => {
    if (newPage >= 1 && newPage <= totalPages) setCurrentPage(newPage);
  };

  const [riskFilter, setRiskFilter] = useState<'all' | 'high' | 'medium' | 'low' | 'other'>('all');

  const upcomingRiskSummary = useMemo(() => {
    const counts = { high: 0, medium: 0, low: 0, other: 0 };
    const fieldsAtRisk = new Set<string>();

    sortedFieldSeasonPairs.forEach(({ field, season }) => {
      const alerts = getAlertsForSeason(season, 14);
      alerts.forEach((alert) => {
        const status = (alert?.status ?? '').toUpperCase();
        const statusClass = getStatusBadgeClass(status);
        if (statusClass === 'high' || statusClass === 'medium-high') counts.high += 1;
        else if (statusClass === 'medium') counts.medium += 1;
        else if (statusClass === 'medium-low' || statusClass === 'low') counts.low += 1;
        else counts.other += 1;
        fieldsAtRisk.add(field.uuid);
      });
    });

    const total = counts.high + counts.medium + counts.low + counts.other;
    return { total, counts, fieldCount: fieldsAtRisk.size };
  }, [sortedFieldSeasonPairs]);

  const riskDetailData = useMemo(() => {
    const farmMap = new Map<string, { farmName: string; items: Array<{
      alertName: string;
      status: string;
      statusClass: string;
      tier: 'high' | 'medium' | 'low' | 'other';
      range: string;
      fields: Array<{ fieldName: string; cropName: string }>;
    }> }>();

    sortedFieldSeasonPairs.forEach(({ field, season }) => {
      const farmName = field.farmV2?.name ?? field.farm?.name ?? '農場情報なし';
      const items = farmMap.get(farmName) ?? { farmName, items: [] };
      const alerts = getAlertsForSeason(season, 14);
      alerts.forEach((alert) => {
        const statusRaw = (alert?.status ?? '').toUpperCase();
        const statusClass = getStatusBadgeClass(statusRaw);
        const statusLabel = actionStatusLabel(alert.status ?? '') || statusRaw;
        let tier: 'high' | 'medium' | 'low' | 'other' = 'other';
        if (statusClass === 'high' || statusClass === 'medium-high') tier = 'high';
        else if (statusClass === 'medium') tier = 'medium';
        else if (statusClass === 'medium-low' || statusClass === 'low') tier = 'low';
        const alertName = alert.alertName || 'アラート';
        const range = alert.range || 'N/A';
        const existing = items.items.find(item =>
          item.alertName === alertName && item.status === statusLabel && item.range === range
        );
        if (existing) {
          existing.fields.push({ fieldName: field.name, cropName: season?.crop?.name ?? '' });
        } else {
          items.items.push({
            alertName,
            status: statusLabel,
            statusClass,
            tier,
            range,
            fields: [{ fieldName: field.name, cropName: season?.crop?.name ?? '' }],
          });
        }
      });
      if (items.items.length > 0) farmMap.set(farmName, items);
    });

    const list = Array.from(farmMap.values());
    list.sort((a, b) => a.farmName.localeCompare(b.farmName, 'ja'));
    return list;
  }, [sortedFieldSeasonPairs]);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (prefCityWorkerRef.current) return;
    const worker = new Worker(new URL('../workers/prefCityReverseGeocode.ts', import.meta.url), { type: 'module' });
    prefCityWorkerRef.current = worker;
    const datasetUrl = `${window.location.origin.replace(/\/$/, '')}/pref_city_p5.topo.json.gz`;
    worker.postMessage({ type: 'init', baseUrl: window.location.origin });
    // In dev, some browsers restrict fetch from workers depending on origin/blob URLs.
    // Preload the dataset on the main thread and transfer it to the worker as a fallback.
    let cancelled = false;
    const preloadDataset = async (attempt: number) => {
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
        // Vite dev server can briefly restart; retry a few times.
        if (attempt < 3) {
          window.setTimeout(() => preloadDataset(attempt + 1), 800 * attempt);
        }
      }
    };
    preloadDataset(1);
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
          return;
        }
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
      setPrefCityByFieldUuid((prev) => ({
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
    return () => {
      worker.terminate();
      prefCityWorkerRef.current = null;
      prefCityPendingRef.current.clear();
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    const worker = prefCityWorkerRef.current;
    if (!worker) return;
    if (!prefCityDatasetReady) return;
    sortedFieldSeasonPairs.forEach(({ field }) => {
      if (!field?.uuid) return;
      const hasPrefCity = Boolean(field.location?.prefecture) && Boolean(field.location?.municipality);
      if (hasPrefCity) return;
      if (prefCityByFieldUuid[field.uuid]) return;
      if (prefCityPendingRef.current.has(field.uuid)) return;
      const center = getFieldCenter(field);
      const lat = center?.latitude;
      const lon = center?.longitude;
      if (typeof lat !== 'number' || typeof lon !== 'number') return;
      prefCityPendingRef.current.add(field.uuid);
      worker.postMessage({ type: 'lookup', id: field.uuid, lat, lon });
    });
  }, [prefCityByFieldUuid, prefCityDatasetReady, sortedFieldSeasonPairs]);

  const downloadAsCsv = () => {
    const csvEscape = (value: unknown) => {
      const text = String(value ?? '')
        .replace(/\r\n/g, '\n')
        .replace(/\r/g, '\n')
        .replace(/\n/g, ' ');
      return `"${text.replace(/"/g, '""')}"`;
    };

    const buildCsvRow = (cells: unknown[]) => cells.map(csvEscape).join(',');

    const headers = [
      '圃場', '農場', 'ユーザー', '都道府県', '市区町村', '緯度', '経度', '面積(a)', '作物', '品種', '作付日', 'BBCH89到達日', '目標収量(kg/10a)', '作付時の生育ステージ', '作付方法',
      '現在の生育ステージ', '次の生育ステージ', '施肥推奨', '水管理',
      '雑草管理', 'リスクアラート',
    ];

    const rows = sortedFieldSeasonPairs.map(({ field, season, nextStage }) => {
      const locationOverride = prefCityByFieldUuid[field.uuid];
      const effectiveLocation = locationOverride
        ? ({ ...(field.location ?? {}), ...locationOverride } as Field['location'])
        : field.location;
      const center = getFieldCenter(field);
      const latitude = center?.latitude ?? null;
      const longitude = center?.longitude ?? null;
      const farmName = getFarmName(field);
      const ownerName = getFarmOwnerName(field);
      const bbch89Date = getBbch89Date(season);
      const targetYield = getTargetYieldLabel(season);
      return buildCsvRow([
        field.name,
        farmName,
        ownerName,
        formatPrefectureDisplay(effectiveLocation),
        formatMunicipalityDisplay(effectiveLocation),
        latitude !== null ? latitude.toFixed(6) : '',
        longitude !== null ? longitude.toFixed(6) : '',
        (field.area / 100).toFixed(2),
        season?.crop.name ?? '',
        season?.variety.name ?? '',
        season?.startDate ? getLocalDateString(season.startDate) : '',
        bbch89Date,
        targetYield,
        formatCropEstablishmentStage(season),
        formatCropEstablishmentMethod(season),
        formatActiveGrowthStage(season),
        formatNextStageInfo(nextStage ?? null),
        formatRecommendations(season?.nutritionRecommendations ?? null),
        formatRecommendations(season?.waterRecommendations ?? null),
        formatRecommendations(season?.weedManagementRecommendations ?? null),
        formatActionWindowSummary(season, 14),
      ]);
    });

    const bom = new Uint8Array([0xef, 0xbb, 0xbf]);
    const csvContent = [buildCsvRow(headers), ...rows].join('\r\n');
    const blob = new Blob([bom, csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `farm_data_${new Date().toISOString().split('T')[0]}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  if (!auth) return <p>認証情報がありません。ログインしてください。</p>;

  return (
    <div className="farms-page-container">
      {showLoading && <LoadingOverlay message={loadingMessage} />}
      <h2>圃場情報</h2>
      <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', marginBottom: '0.75rem' }}>
        <span style={{ color: '#b9b9c6' }}>作期表示:</span>
        <button
          type="button"
          className={`fields-action-btn${seasonView === 'active' ? ' fields-action-btn--accent' : ''}`}
          onClick={() => {
            setSeasonView('active');
            fetchCombinedDataIfNeeded({ includeTasks: false, force: true });
            setSelectedFieldIds(new Set());
          }}
        >
          現在/予定
        </button>
        <button
          type="button"
          className={`fields-action-btn${seasonView === 'closed' ? ' fields-action-btn--accent' : ''}`}
          onClick={() => {
            setSeasonView('closed');
            setSelectedFieldIds(new Set());
          }}
        >
          過去(完了)
        </button>
        {seasonView === 'closed' && (
          <button type="button" className="fields-action-btn" onClick={fetchClosedCombined} disabled={closedLoading}>
            再取得
          </button>
        )}
        {seasonView === 'closed' && closedError && (
          <span style={{ color: '#ff9e9e' }}>過去作期の取得に失敗: {closedError}</span>
        )}
      </div>
      <div className="risk-summary-card">
        <div className="risk-summary-header">
          <div>
            <div className="risk-summary-title">2週間以内のリスクアラート</div>
            <div className="risk-summary-sub">
              対象圃場: {upcomingRiskSummary.fieldCount} / リスク件数: {upcomingRiskSummary.total}
            </div>
          </div>
          <div className="risk-summary-range">今日から14日</div>
        </div>
        <div className="risk-summary-metrics">
          <button
            type="button"
            className={`risk-pill risk-pill--all${riskFilter === 'all' ? ' active' : ''}`}
            onClick={() => setRiskFilter('all')}
          >
            すべて {upcomingRiskSummary.total}
          </button>
          <button
            type="button"
            className={`risk-pill risk-pill--high${riskFilter === 'high' ? ' active' : ''}`}
            onClick={() => setRiskFilter('high')}
          >
            高 {upcomingRiskSummary.counts.high}
          </button>
          <button
            type="button"
            className={`risk-pill risk-pill--medium${riskFilter === 'medium' ? ' active' : ''}`}
            onClick={() => setRiskFilter('medium')}
          >
            中 {upcomingRiskSummary.counts.medium}
          </button>
          <button
            type="button"
            className={`risk-pill risk-pill--low${riskFilter === 'low' ? ' active' : ''}`}
            onClick={() => setRiskFilter('low')}
          >
            低 {upcomingRiskSummary.counts.low}
          </button>
          {upcomingRiskSummary.counts.other > 0 && (
            <button
              type="button"
              className={`risk-pill risk-pill--other${riskFilter === 'other' ? ' active' : ''}`}
              onClick={() => setRiskFilter('other')}
            >
              他 {upcomingRiskSummary.counts.other}
            </button>
          )}
        </div>
        <div className="risk-detail-card">
          <div className="risk-detail-header">
            <div className="risk-detail-title">農場別の内訳</div>
            <div className="risk-detail-filter">表示: {riskFilter === 'all' ? 'すべて' : riskFilter.toUpperCase()}</div>
          </div>
          {riskDetailData.length === 0 ? (
            <div className="risk-detail-empty">2週間以内のリスクはありません。</div>
          ) : (
            <div className="risk-detail-list">
              {riskDetailData.map(group => {
                const filteredItems = riskFilter === 'all'
                  ? group.items
                  : group.items.filter(item => item.tier === riskFilter);
                if (filteredItems.length === 0) return null;
                return (
                  <details key={group.farmName} className="risk-farm-group">
                    <summary>
                      {group.farmName} <span className="risk-farm-count">{filteredItems.length} 件</span>
                    </summary>
                    <ul className="risk-item-list">
                      {filteredItems.map((item, index) => {
                        const uniqueFields = Array.from(
                          new Map(item.fields.map(fieldItem => [fieldItem.fieldName, fieldItem])).values()
                        );
                        const preview = uniqueFields.slice(0, 3);
                        const extraCount = uniqueFields.length - preview.length;
                        return (
                        <li key={`${group.farmName}-${index}`} className="risk-item">
                          <span className={`status-badge ${item.statusClass}`}>{item.status}</span>
                          <span className="risk-item-name">{item.alertName}</span>
                          <span className="risk-item-range">{item.range}</span>
                          <span className="risk-item-fields">
                            {preview.map(fieldItem => (
                              <span key={fieldItem.fieldName} className="risk-field-chip">
                                {fieldItem.fieldName}
                                {fieldItem.cropName ? ` (${fieldItem.cropName})` : ''}
                              </span>
                            ))}
                            {extraCount > 0 && (
                              <span className="risk-field-chip risk-field-chip--extra">+{extraCount}</span>
                            )}
                          </span>
                        </li>
                      )})}
                    </ul>
                  </details>
                );
              })}
            </div>
          )}
        </div>
      </div>
      {submittedFarms.length > 0 ? (
        <p>
          ヘッダーで選択された {submittedFarms.length} 件の農場の圃場データを表示しています。 
          {combinedOut?.source && (
            <span style={{ marginLeft: '1em', color: combinedOut.source === 'cache' ? '#4caf50' : '#2196f3', fontWeight: 'bold' }}>({combinedOut.source === 'cache' ? 'キャッシュから取得' : 'APIから取得'})</span>
          )}
        </p>
      ) : <p>ヘッダーのドロップダウンから農場を選択してください。</p>}

      {allFieldSeasonPairsWithNextStage.length > 0 && (
        <PlantingStackedChart fieldSeasonPairs={allFieldSeasonPairsWithNextStage} />
      )}

      {/* 圃場データ取得結果表示エリア */}
      <div style={{ marginTop: '2rem' }}>
        {combinedErr && (
          <div>
            <h3 style={{ color: '#ff6b6b' }}>圃場データの取得に失敗しました</h3>
            <pre style={{ color: '#ff6b6b', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
              {combinedErr}
            </pre>
          </div>
        )}
        {sortedFieldSeasonPairs.length > 0 && (
          <div className="fields-actions">
            <div className="fields-actions__meta">
              <div>
                選択: <strong>{selectedFieldIds.size}</strong> 圃場 / 全体: <strong>{uniqueFieldIds.length}</strong> 圃場
              </div>
              <div className="fields-actions__hint">
                Google My Maps 取り込み用のGeoJSONを出力します。
              </div>
            </div>
            <div className="fields-actions__buttons">
              <button
                type="button"
                className="fields-action-btn fields-action-btn--primary"
                onClick={() => downloadForMyMaps(sortedFieldSeasonPairs, 'all')}
              >
                My Mapsダウンロード（全圃場）
              </button>
              <button
                type="button"
                className="fields-action-btn fields-action-btn--accent"
                onClick={() => downloadForMyMaps(selectedFieldSeasonPairs, 'selected')}
                disabled={selectedFieldIds.size === 0}
              >
                My Mapsダウンロード（選択圃場）
              </button>
              <button
                type="button"
                className="fields-action-btn"
                onClick={downloadAsCsv}
              >
                CSVダウンロード
              </button>
            </div>
          </div>
        )}
        {paginatedFieldSeasonPairs.length > 0 && (
          <div className="table-container" ref={tableContainerRef} onScroll={handleTableScroll}>
            <FieldsTable
              fieldSeasonPairs={paginatedFieldSeasonPairs}
              requestSort={requestSort}
              sortConfig={sortConfig}
              selectedFieldIds={selectedFieldIds}
              onToggleFieldSelection={handleToggleFieldSelection}
              isAllSelectedOnPage={isAllSelectedOnPage}
              isSomeSelectedOnPage={isSomeSelectedOnPage}
              onToggleSelectAll={handleToggleSelectAllOnPage}
              noteImageByField={noteImageByField}
              noteImageStateByField={noteImageStateByField}
              noteImageErrorByField={noteImageErrorByField}
              onOpenNoteList={handleOpenNoteList}
              tableRef={tableRef}
              locationByFieldUuid={prefCityByFieldUuid}
            />
          </div>
        )}
        {paginatedFieldSeasonPairs.length > 0 && showTableScrollbar && (
          <div className="table-scrollbar" ref={tableScrollbarRef} onScroll={handleScrollbarScroll}>
            <div className="table-scrollbar__content" style={{ width: tableScrollbarWidth }} />
          </div>
        )}
        {sortedFieldSeasonPairs.length > 0 && (
          <div className="pagination-controls">
            <div>
              <label htmlFor="rows-per-page">表示件数: </label>
              <select
                id="rows-per-page"
                value={rowsPerPage}
                onChange={(e) => {
                  setRowsPerPage(Number(e.target.value));
                  setCurrentPage(1); // 件数を変更したら1ページ目に戻る
                }}
              >
                <option value={10}>10</option>
                <option value={50}>50</option>
                <option value={100}>100</option>
                <option value={200}>200</option>
              </select>
            </div>
            <div>
              <button onClick={() => handlePageChange(currentPage - 1)} disabled={currentPage === 1}>
                前へ
              </button>
              <span> {currentPage} / {totalPages} </span>
              <button onClick={() => handlePageChange(currentPage + 1)} disabled={currentPage === totalPages}>
                次へ
              </button>
            </div>
          </div>
        )}
      </div>
      {noteModalField && (
        <div className="field-notes-modal-backdrop" onClick={closeNoteModal}>
          <div className="field-notes-modal" onClick={(event) => event.stopPropagation()}>
            <div className="field-notes-modal-header">
              <div>
                <h3>ノート画像一覧</h3>
                <div className="field-notes-modal-sub">
                  {noteModalField.name}
                  {noteModalField.farmV2?.name || noteModalField.farm?.name
                    ? ` / ${noteModalField.farmV2?.name ?? noteModalField.farm?.name}`
                    : ''}
                </div>
              </div>
              <button type="button" className="field-notes-modal-close" onClick={closeNoteModal}>
                閉じる
              </button>
            </div>
            <div className="field-notes-modal-filters">
              <div className="field-notes-modal-filter">
                <label htmlFor="note-date-from">開始</label>
                <input
                  id="note-date-from"
                  type="date"
                  value={noteModalFrom}
                  onChange={(event) => setNoteModalFrom(event.target.value)}
                />
              </div>
              <div className="field-notes-modal-filter">
                <label htmlFor="note-date-to">終了</label>
                <input
                  id="note-date-to"
                  type="date"
                  value={noteModalTo}
                  onChange={(event) => setNoteModalTo(event.target.value)}
                />
              </div>
              <div className="field-notes-modal-filter">
                <label htmlFor="note-limit">表示件数</label>
                <select
                  id="note-limit"
                  value={Number.isFinite(noteModalLimit) ? String(noteModalLimit) : 'all'}
                  onChange={(event) => {
                    const value = event.target.value;
                    setNoteModalLimit(value === 'all' ? Infinity : Number(value));
                  }}
                >
                  <option value={1}>1</option>
                  <option value={3}>3</option>
                  <option value={10}>10</option>
                  <option value={30}>30</option>
                  <option value={50}>50</option>
                  <option value="all">全て</option>
                </select>
              </div>
              <div className="field-notes-modal-count">
                表示 {filteredNoteModalItems.length} / 全 {noteModalItems.length}
              </div>
            </div>
            <div className="field-notes-modal-body">
              {noteModalState === 'loading' && <div className="field-notes-modal-empty">読み込み中...</div>}
              {noteModalState === 'error' && (
                <div className="field-notes-modal-error">{noteModalError || '取得に失敗しました。'}</div>
              )}
              {noteModalState !== 'loading' && noteModalState !== 'error' && filteredNoteModalItems.length === 0 && (
                <div className="field-notes-modal-empty">画像付きノートがありません。</div>
              )}
              {noteModalState !== 'loading' && noteModalState !== 'error' && filteredNoteModalItems.length > 0 && (
                <div className="field-notes-modal-grid">
                  {filteredNoteModalItems.map((item) => (
                    <div key={`${item.noteUuid}-${item.url}`} className="field-notes-modal-item">
                      <a href={item.url} target="_blank" rel="noreferrer">
                        <img src={item.url} alt={item.fileName || 'note image'} loading="lazy" />
                      </a>
                      <div className="field-notes-modal-meta">
                        <div className="field-notes-modal-date">{getLocalDateString(item.noteDate)}</div>
                        {item.creatorName && <div className="field-notes-modal-creator">{item.creatorName}</div>}
                        {item.categories && item.categories.length > 0 && (
                          <div className="field-notes-modal-categories">{item.categories.join(', ')}</div>
                        )}
                        {item.noteText && <div className="field-notes-modal-note">{item.noteText}</div>}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// =============================================================================
// Custom Hook for Data Processing
// =============================================================================

type SortConfig = { key: string; direction: 'ascending' | 'descending' } | null;

function usePaginatedFields(combinedOut: any, sortConfig: SortConfig, opts?: { hideEmptySeasons?: boolean }) {
  const [currentPage, setCurrentPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useState(50);

  const allFields = useMemo((): Field[] => {
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
    if (primary && Array.isArray(primary) && primary.length > 0) {
      if (lists.length === 0) return primary as Field[];
      return mergeFieldLists([primary as any[], ...lists]) as Field[];
    }
    if (lists.length > 0) return mergeFieldLists(lists) as Field[];
    return [];
  }, [combinedOut]);

  // 圃場と作期の全ペアを作成 (フック内に移動)
  const allFieldSeasonPairs = useMemo((): Omit<FieldSeasonPair, 'nextStage'>[] => {
    return allFields.reduce<Omit<FieldSeasonPair, 'nextStage'>[]>((acc, field) => {
      const seasons = field.cropSeasonsV2 ?? [];
      if (seasons.length > 0) {
        seasons.forEach((season: CropSeason) => acc.push({ field, season }));
      } else if (!opts?.hideEmptySeasons) {
        acc.push({ field, season: null });
      }
      return acc;
    }, []);
  }, [allFields, opts?.hideEmptySeasons]);

  // ソート用に「次の生育ステージ」を事前に計算
  const allFieldSeasonPairsWithNextStage = useMemo(() => {
    return allFieldSeasonPairs.map(pair => {
      const predictions = pair.season?.countryCropGrowthStagePredictions;
      let nextStage: CountryCropGrowthStagePrediction | null = null;
      if (predictions && predictions.length > 0) {
        const sorted = [...predictions].sort(
          (a, b) => new Date(a.startDate).getTime() - new Date(b.startDate).getTime(),
        );
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const upcoming = sorted.find(pred => new Date(pred.startDate) >= today);
        nextStage = upcoming ?? sorted[sorted.length - 1] ?? null;
      }
      return { ...pair, nextStage };
    });
  }, [allFieldSeasonPairs]);

  // ソートされたデータを計算
  const sortedFieldSeasonPairs = useMemo(() => {
    const sortableItems: FieldSeasonPair[] = [...allFieldSeasonPairsWithNextStage];
    if (sortConfig !== null) {
      sortableItems.sort((a: FieldSeasonPair, b: FieldSeasonPair) => {
        const getNestedValue = (obj: any, path: string) =>
          path.split('.').reduce((o, k) => (o || {})[k], obj);

        const aValue = getNestedValue(a, sortConfig.key);
        const bValue = getNestedValue(b, sortConfig.key);

        if (aValue === null || aValue === undefined) return 1;
        if (bValue === null || bValue === undefined) return -1;

        if (aValue < bValue) {
          return sortConfig.direction === 'ascending' ? -1 : 1;
        }
        if (aValue > bValue) {
          return sortConfig.direction === 'ascending' ? 1 : -1;
        }
        return 0;
      });
    }
    return sortableItems;
  }, [allFieldSeasonPairsWithNextStage, sortConfig]);

  // 現在のページに表示するデータを計算
  const paginatedFieldSeasonPairs = useMemo(() => {
    const startIndex = (currentPage - 1) * rowsPerPage;
    return sortedFieldSeasonPairs.slice(startIndex, startIndex + rowsPerPage);
  }, [sortedFieldSeasonPairs, currentPage, rowsPerPage]);

  const totalPages = Math.ceil(sortedFieldSeasonPairs.length / rowsPerPage);

  // ページネーションの状態が変化したときにページ番号を調整
  useEffect(() => {
    if (currentPage > totalPages && totalPages > 0) {
      setCurrentPage(totalPages);
    }
  }, [currentPage, totalPages]);

  return {
    paginatedFieldSeasonPairs,
    sortedFieldSeasonPairs,
    allFieldSeasonPairsWithNextStage,
    totalPages,
    currentPage,
    rowsPerPage,
    setCurrentPage,
    setRowsPerPage,
  };
}

// =============================================================================
// Sub Components for Table Cells
// =============================================================================

type CellProps = {
  season: CropSeason | null;
};

const LocationPrefectureCell = ({ location }: { location: Field['location'] }) => {
  const value = formatPrefectureDisplay(location);
  return <>{value || 'N/A'}</>;
};

const LocationMunicipalityCell = ({ location }: { location: Field['location'] }) => {
  const value = formatMunicipalityDisplay(location);
  return <>{value || 'N/A'}</>;
};

const CoordinateCell = ({ value }: { value?: number | null }) =>
  value !== null && value !== undefined ? value.toFixed(5) : 'N/A';

const NextStageCell = ({ pair }: { pair: FieldSeasonPair | undefined }) => {
  const nextStage =
    pair?.nextStage ??
    (pair?.season?.countryCropGrowthStagePredictions && pair.season.countryCropGrowthStagePredictions[0]) ??
    null;

  return <>{formatNextStageInfo(nextStage ?? null)}</>;
};

const getStatusBadgeClass = (status: string | null | undefined) => {
  const normalized = (status ?? '').toUpperCase();
  if (normalized === 'CURRENT' || normalized === '現在') return 'high';
  if (normalized === 'HIGH') return 'high';
  if (normalized === 'MEDIUM_HIGH') return 'medium-high';
  if (normalized === 'MEDIUM') return 'medium';
  if (normalized === 'MEDIUM_LOW') return 'medium-low';
  if (normalized === 'LOW') return 'low';
  if (normalized === 'PROTECTED') return 'protected';
  return 'medium';
};

const actionStatusLabel = (value?: string | null) => {
  if (!value) return '';
  const key = value.toUpperCase();
  if (key === 'HIGH') return '高';
  if (key === 'MEDIUM') return '中';
  if (key === 'CURRENT') return '現在';
  if (key === 'MISSED') return '未対応';
  if (key === 'NOT_PRESENT') return '対象外';
  if (key === 'SCHEDULED') return '予定';
  return value;
};

const cropSeasonStatusLabel = (value?: string | null) => {
  if (!value) return '作期ステータス';
  const key = value.toUpperCase();
  if (key === 'DISEASE') return '病害';
  if (key === 'INSECT') return '害虫';
  return value;
};

const RISK_STATUS_PRIORITY = ['HIGH', 'MEDIUM_HIGH', 'MEDIUM', 'MEDIUM_LOW', 'LOW', 'PROTECTED'];
const normalizeRiskStatus = (status?: string | null) => (status ?? '').toUpperCase();

const buildRiskAlerts = (season: CropSeason | null) => {
  if (!season?.risks || season.risks.length === 0) return [];
  const flattened = season.risks
    .filter(risk => risk?.status)
    .map(risk => {
      const stressInfo = season.timingStressesInfo?.find(info => info.stressV2.uuid === risk.stressV2.uuid);
      const status = normalizeRiskStatus(risk.status);
      return {
        ...risk,
        status,
        name: stressInfo?.stressV2.name || 'Unknown Risk',
        groupKey: `${risk.stressV2.uuid}-${status}`,
      };
    });

  const grouped = Array.from(
    flattened.reduce((map, item) => {
      const list = map.get(item.groupKey) ?? [];
      list.push(item);
      map.set(item.groupKey, list);
      return map;
    }, new Map<string, typeof flattened>() as Map<string, typeof flattened>).values()
  ).flatMap(list => groupConsecutiveItems(list, 'groupKey'));
  const sorted = grouped.sort((a, b) => {
    const sev = RISK_STATUS_PRIORITY.indexOf(a.status) - RISK_STATUS_PRIORITY.indexOf(b.status);
    if (sev !== 0) return sev;
    return new Date(a.startDate).getTime() - new Date(b.startDate).getTime();
  });

  const windowGroups = new Map<string, { status: string; startDate: string; endDate: string; names: string[]; order: number }>();
  sorted.forEach((item, index) => {
    if (!item.startDate || !item.endDate) return;
    const key = `${item.status}__${item.startDate}__${item.endDate}`;
    const existing = windowGroups.get(key);
    if (existing) {
      if (!existing.names.includes(item.name)) existing.names.push(item.name);
      return;
    }
    windowGroups.set(key, {
      status: item.status,
      startDate: item.startDate,
      endDate: item.endDate,
      names: [item.name],
      order: index,
    });
  });

  const windowItems = Array.from(windowGroups.values())
    .sort((a, b) => a.order - b.order)
    .map(item => {
      const names = item.names.filter(Boolean).sort();
      return {
        startDate: item.startDate,
        endDate: item.endDate,
        status: item.status,
        names,
        namesKey: names.join('、'),
      };
    });

  const merged = groupConsecutiveItems(windowItems, 'namesKey');

  return merged.map(item => {
    const range = item.startDate && item.endDate
      ? `${getLocalDateString(item.startDate)} - ${formatInclusiveEndDate(item.endDate)}`
      : (item.startDate ? getLocalDateString(item.startDate) : '');
    return {
      names: item.names,
      status: item.status,
      startDate: item.startDate,
      endDate: item.endDate,
      range,
    };
  });
};

function getAlertsForSeason(season: CropSeason | null, days: number) {
  const combined = getCombinedAlerts(season, days);
  if (combined.length > 0) {
    return combined.map(alert => ({
      alertName: alert.alertName || 'アラート',
      status: alert.status ?? '',
      range: alert.startDate && alert.endDate
        ? `${getLocalDateString(alert.startDate)} - ${formatInclusiveEndDate(alert.endDate)}`
        : (alert.startDate ? getLocalDateString(alert.startDate) : ''),
    }));
  }

  return buildRiskAlerts(season).map(item => ({
    alertName: item.names.filter(Boolean).join('、') || 'リスク',
    status: item.status,
    range: item.range,
  }));
}

const getUpcomingActionWindows = (season: CropSeason | null, days: number) => {
  if (!season?.actionWindows || season.actionWindows.length === 0) return [];
  const today = startOfDay(new Date());
  const end = addDays(today, days);
  const items = season.actionWindows
    .filter(win => win?.status && win?.startDate)
    .map(win => ({
      ...win,
      actionType: win.actionType ?? 'ACTION',
      endDate: win.endDate ?? win.startDate,
    }))
    .filter(win => {
      const status = (win.status ?? '').toUpperCase();
      return status === 'HIGH' || status === 'MEDIUM';
    })
    .filter(win => {
      const startDate = startOfDay(new Date(win.startDate));
      const endDate = startOfDay(new Date(win.endDate));
      if (endDate < today) return false;
      if (startDate > end) return false;
      return true;
    });
  return groupConsecutiveItems(items, 'actionType').sort(
    (a, b) => new Date(a.startDate).getTime() - new Date(b.startDate).getTime()
  );
};

const getCombinedAlerts = (season: CropSeason | null, days: number) => {
  const windows = getUpcomingActionWindows(season, days);
  if (windows.length === 0) return [];
  const statusList = (season?.cropSeasonStatus ?? []).filter(status => {
    const value = (status?.status ?? '').toUpperCase();
    return value !== 'MISSED' && value !== 'NOT_PRESENT';
  });
  const items = windows.map(win => {
    const winStart = new Date(win.startDate);
    const winEnd = new Date(win.endDate ?? win.startDate);
    const match = statusList.find(status => {
      if (!status?.startDate) return false;
      const statusStart = new Date(status.startDate);
      const statusEnd = status.endDate ? new Date(status.endDate) : statusStart;
      return statusStart <= winEnd && statusEnd >= winStart;
    });
    return {
      startDate: win.startDate,
      endDate: win.endDate ?? win.startDate,
      status: win.status ?? '',
      alertName: cropSeasonStatusLabel(match?.type ?? ''),
    };
  });
  return groupConsecutiveItems(items, 'alertName').sort(
    (a, b) => new Date(a.startDate).getTime() - new Date(b.startDate).getTime()
  );
};

const formatActionWindowSummary = (season: CropSeason | null, days: number) => {
  const alerts = getCombinedAlerts(season, days);
  if (alerts.length === 0) return 'N/A';
  return alerts
    .map((win) => {
      const label = win.alertName ?? 'アラート';
      const status = actionStatusLabel(win.status ?? '');
      const range = win.startDate && win.endDate
        ? `${getLocalDateString(win.startDate)} - ${formatInclusiveEndDate(win.endDate)}`
        : (win.startDate ? getLocalDateString(win.startDate) : '');
      return `${label} ${status}${range ? ` (${range})` : ''}`.trim();
    })
    .join(' | ');
};

const NutritionRecCell = ({ season }: CellProps) => {
  if (!season?.nutritionRecommendations) return <>N/A</>;
  return <>{formatRecommendations(season.nutritionRecommendations)}</>;
};

const RiskAlertCell = ({ season }: CellProps) => {
  const alerts = getCombinedAlerts(season, 14);
  if (alerts.length > 0) {
    const target = alerts[0];
    const statusRaw = (target.status ?? '').toUpperCase();
    const status = actionStatusLabel(target.status ?? '') || statusRaw;
    const statusClass = getStatusBadgeClass(statusRaw);
    const range = target.startDate && target.endDate
      ? `${getLocalDateString(target.startDate)} - ${formatInclusiveEndDate(target.endDate)}`
      : '';
    return (
      <>
        {target.alertName ? `${target.alertName} ` : ''}
        <span className={`status-badge ${statusClass}`}>{status}</span>
        {range ? ` ${range}` : ''}
      </>
    );
  }

  const riskAlerts = buildRiskAlerts(season);
  if (riskAlerts.length === 0) return <>N/A</>;
  return (
    <>
      {riskAlerts.map((item, index) => {
        const statusClass = getStatusBadgeClass(item.status);
        const nameText = item.names.filter(Boolean).join('、');
        return (
          <div key={`${item.status}-${item.range || 'range'}-${index}`}>
            {nameText ? `${nameText} ` : ''}
            <span className={`status-badge ${statusClass}`}>{item.status}</span>
            {item.range ? ` ${item.range}` : ''}
          </div>
        );
      })}
    </>
  );
};

const WaterRecCell = ({ season }: CellProps) => {
  if (!season?.waterRecommendations || season.waterRecommendations.length === 0) {
    return <>N/A</>;
  }
  return <>{formatCurrentWaterRecommendations(season.waterRecommendations)}</>;
};

const WeedRecCell = ({ season }: CellProps) => {
  if (!season?.weedManagementRecommendations) return <>N/A</>;
  return <>{formatRecommendations(season.weedManagementRecommendations)}</>;
};

type PlantingStackedChartProps = {
  fieldSeasonPairs: FieldSeasonPair[];
};

type PlantingAggregate = {
  day: string;
  total: number;
  buckets: { label: string; count: number }[];
};

const colorFromString = (value: string) => {
  let hash = 0;
  for (let i = 0; i < value.length; i += 1) {
    hash = value.charCodeAt(i) + ((hash << 5) - hash);
  }
  const hue = Math.abs(hash) % 360;
  return `hsl(${hue}, 65%, 55%)`;
};

const PlantingStackedChart = ({ fieldSeasonPairs }: PlantingStackedChartProps) => {
  const [mode, setMode] = useState<'crop' | 'variety'>('crop');
  const getBucketKey = useMemo(
    () => (pair: FieldSeasonPair) =>
      mode === 'crop' ? pair.season?.crop.name ?? 'N/A' : pair.season?.variety.name ?? 'N/A',
    [mode],
  );

  const aggregates: PlantingAggregate[] = useMemo(() => {
    const map = new Map<string, Map<string, Set<string>>>();
    fieldSeasonPairs.forEach(pair => {
      const start = pair.season?.startDate;
      if (!start) return;
      const day = getLocalDateString(start);
      const bucketKey = getBucketKey(pair);
      if (!day || !bucketKey) return;
      if (!map.has(day)) map.set(day, new Map());
      const bucket = map.get(day)!;
      if (!bucket.has(bucketKey)) bucket.set(bucketKey, new Set());
      bucket.get(bucketKey)!.add(pair.field.uuid);
    });

    return Array.from(map.entries())
      .map(([day, bucketMap]) => {
        const buckets = Array.from(bucketMap.entries()).map(([label, set]) => ({
          label,
          count: set.size,
        }));
        const totalSet = new Set<string>();
        bucketMap.forEach(set => set.forEach(id => totalSet.add(id)));
        const total = totalSet.size;
        buckets.sort((a, b) => b.count - a.count);
        return { day, total, buckets };
      })
      .sort((a, b) => new Date(a.day).getTime() - new Date(b.day).getTime());
  }, [fieldSeasonPairs, getBucketKey]);

  const maxTotal = Math.max(...aggregates.map(a => a.total), 0);
  if (aggregates.length === 0) return null;

  const labels = aggregates.map(a => a.day);
  const bucketLabels = Array.from(new Set(aggregates.flatMap(a => a.buckets.map(b => b.label))));
  const totalsByDate = Object.fromEntries(aggregates.map(a => [a.day, a.total]));

  const datasets = bucketLabels.map(label => {
    const data = aggregates.map(a => {
      const bucket = a.buckets.find(b => b.label === label);
      return bucket ? bucket.count : 0;
    });
    return {
      label,
      data,
      backgroundColor: colorFromString(label),
      borderColor: colorFromString(label),
      borderWidth: 1,
      stack: 'counts',
    };
  });

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    layout: {
      padding: { top: 12 },
    },
    interaction: { intersect: false, mode: 'nearest' as const },
    plugins: {
      legend: {
        position: 'bottom' as const,
        labels: { color: '#e0e0e0' },
      },
      tooltip: {
        filter: (item: any) => {
          const value = typeof item.parsed?.y === 'number' ? item.parsed.y : item.raw;
          return typeof value === 'number' && value !== 0;
        },
        callbacks: {
          label: (context: any) => {
            const label = context.dataset.label || '';
            const value = context.parsed.y ?? 0;
            return `${label}: ${value}圃場`;
          },
        },
      },
      datalabels: {
        display: (context: any) => {
          const dataIndex = context.dataIndex;
          const datasetsAll = context.chart.data.datasets || [];
          let topIndex = -1;
          datasetsAll.forEach((ds: any, idx: number) => {
            const value = Number(ds.data?.[dataIndex] ?? 0);
            if (Number.isFinite(value) && value > 0) topIndex = idx;
          });
          if (topIndex < 0) return false;
          return context.datasetIndex === topIndex;
        },
        formatter: (_value: any, context: any) => {
          const date = labels[context.dataIndex];
          if (!date) return '';
          const total = totalsByDate[date];
          if (!total) return '';
          return total;
        },
        color: '#e0e0e0',
        font: { weight: 'bold' as const },
        anchor: 'end' as const,
        align: 'end' as const,
        offset: -6,
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
        ticks: { color: '#e0e0e0' },
        grid: { color: 'rgba(255,255,255,0.1)' },
      },
      y: {
        stacked: true,
        ticks: { color: '#e0e0e0', stepSize: 1 },
        grid: { color: 'rgba(255,255,255,0.1)' },
        beginAtZero: true,
      },
    },
  };

  return (
    <div className="tasks-chart-card planting-stacked">
      <div className="tasks-chart-header">
        <h3>作付日ごとの圃場数</h3>
        <div className="tasks-chart-toggle planting-stacked__actions">
          <button
            type="button"
            className={mode === 'crop' ? 'active' : ''}
            onClick={() => setMode('crop')}
          >
            作物別
          </button>
          <button
            type="button"
            className={mode === 'variety' ? 'active' : ''}
            onClick={() => setMode('variety')}
          >
            品種別
          </button>
        </div>
      </div>

      <div className="planting-stacked__scroll">
        <div className="tasks-chart-wrapper planting-stacked__chart">
          {labels.length === 0 ? (
            <div className="tasks-chart-empty">表示できる作付データがありません。</div>
          ) : (
            <Bar data={{ labels, datasets }} options={chartOptions as any} />
          )}
        </div>
      </div>
      {maxTotal > 0 && (
        <div className="tasks-chart-footnote planting-stacked__hint">
          縦棒の高さが当日の総圃場数、色が{mode === 'crop' ? '作物' : '品種'}別内訳です。
        </div>
      )}
    </div>
  );
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
