import { useEffect, useMemo, useState } from 'react';
import type { FC } from 'react';
import { useAuth } from '../context/AuthContext';
import { useFarms } from '../context/FarmContext';
import { getLocalDateString } from '../utils/formatters';
import type { LoginAndTokenResp, FieldNote } from '../types/farm';
import './FarmsPage.css'; // FarmsPageのスタイルを再利用
import './FieldMemoPage.css';
import { withApiBase } from '../utils/apiBase';
import { createDownloadUrl } from '../utils/apiUtils';
import { postJsonCached } from '../utils/cachedJsonFetch';
import LoadingOverlay from '../components/LoadingOverlay';
import LoadingSpinner from '../components/LoadingSpinner';
import { useLanguage } from '../context/LanguageContext';
import { getCurrentLanguage, tr } from '../i18n/runtime';

// =============================================================================
// Type Definitions
// =============================================================================

type AggregatedNote = FieldNote & {
  fieldName: string;
  farmName?: string;
  farmUuid?: string;
  lon?: number;
  lat?: number;
};

// =============================================================================
// API Client
// =============================================================================

const getFieldNotesCacheKey = (farmUuids: string[]) =>
  `field-notes:${[...farmUuids].sort().join(',')}`;

async function fetchFieldNotesApi(params: { auth: LoginAndTokenResp; farmUuids: string[] }): Promise<any> {
  const cacheKey = getFieldNotesCacheKey(params.farmUuids);
  const requestBody = {
    login_token: params.auth.login.login_token,
    api_token: params.auth.api_token,
    farm_uuids: params.farmUuids,
  };
  const { ok, status, json: out, source } = await postJsonCached<any>(
    withApiBase('/field-notes'),
    requestBody,
    undefined,
    { cacheKey, cache: params.farmUuids.length > 0 ? 'session' : 'none' },
  );
  if (!ok) {
    const detail = typeof out === 'string' ? out : out?.detail ?? out?.message ?? out?.error ?? `HTTP ${status}`;
    return { ok: false, status, detail, source };
  }
  if (!out) {
    return { ok: false, status, detail: 'Empty response from server', source };
  }
  if (typeof out === 'string') {
    return { ok: false, status, detail: out, source };
  }
  return { ...out, source };
}

// =============================================================================
// Custom Hook for Data Processing
// =============================================================================

const useAggregatedNotes = (notesOut: any): AggregatedNote[] => {
  return useMemo(() => {
    if (!notesOut?.response?.data?.fieldsV2) {
      return [];
    }

    const fields = notesOut.response.data.fieldsV2;

    const allNotes = fields.flatMap((field: {
      uuid: string;
      name: string;
      farmV2?: { uuid?: string | null; name?: string | null } | null;
      fieldNotes: FieldNote[];
    }) => {
      const farmUuid = field.farmV2?.uuid ?? undefined;
      const farmName = field.farmV2?.name ?? undefined;
      return field.fieldNotes?.map((note: FieldNote) => {
        let lon: number | undefined;
        let lat: number | undefined;
        if (note.location?.type === 'Point' && note.location.coordinates) {
          [lon, lat] = note.location.coordinates;
        }
        return {
          ...note,
          fieldName: field.name,
          farmName: farmName ?? undefined,
          farmUuid: farmUuid ?? undefined,
          lon,
          lat,
        };
      }) ?? [];
    });

    return allNotes.sort((a: AggregatedNote, b: AggregatedNote) => new Date(b.creationDate).getTime() - new Date(a.creationDate).getTime());
  }, [notesOut]);
};

const isImageAttachment = (att: { mimeType?: string | null; fileName?: string | null; url: string }) => {
  const mime = (att.mimeType || "").toLowerCase();
  if (mime.startsWith("image/")) return true;
  const name = (att.fileName || att.url || "").toLowerCase();
  return /\.(png|jpe?g|jpeg|gif|webp|svg)$/.test(name);
};

const isAudioAttachment = (att: { contentType?: string | null; fileName?: string | null; url: string }) => {
  const mime = (att.contentType || "").toLowerCase();
  if (mime.startsWith("audio/")) return true;
  const name = (att.fileName || att.url || "").toLowerCase();
  return /\.(mp3|m4a|aac|wav|ogg|flac)$/.test(name);
};

const getDisplayName = (att: { fileName?: string | null; url: string }) => {
  if (att.fileName) return att.fileName;
  try {
    const parsed = new URL(att.url);
    const last = parsed.pathname.split("/").filter(Boolean).pop();
    return last ? decodeURIComponent(last) : tr('field_memo.attachment.default');
  } catch {
    return tr('field_memo.attachment.default');
  }
};

const toJstDateParts = (dateString: string) => {
  const d = new Date(dateString);
  const lang = getCurrentLanguage();
  const locale = lang === 'en' ? 'en-US' : 'ja-JP';
  const fmt = new Intl.DateTimeFormat(locale, {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  });
  const parts = fmt.formatToParts(d).reduce<Record<string, string>>((acc, p) => {
    if (p.type !== 'literal') acc[p.type] = p.value;
    return acc;
  }, {});
  return {
    year: parts.year || '0000',
    month: parts.month || '00',
    day: parts.day || '00',
    hour: parts.hour || '00',
    minute: parts.minute || '00',
  };
};

const buildDownloadFileName = (
  att: { fileName?: string | null; url: string },
  note: { creationDate: string; fieldName: string }
) => {
  const extractNameFromUrl = (url: string) => {
    try {
      const parsed = new URL(url);
      const last = parsed.pathname.split("/").filter(Boolean).pop();
      return last ? decodeURIComponent(last) : "";
    } catch {
      return "";
    }
  };

  const sanitizeFieldName = (name: string) =>
    name.replace(/[\\/:*?"<>|]/g, '').replace(/\s+/g, '') || 'field';

  const baseName = (att.fileName || extractNameFromUrl(att.url) || '').trim();
  const baseLower = baseName.toLowerCase();

  const getExt = () => {
    const match = baseLower.match(/\.([a-z0-9]+)(?:$|\?)/);
    if (match) return match[0];
    return '.jpg';
  };

  const isDefaultUpload =
    baseLower === 'upload.jpg' || baseLower === 'upload.jpeg' || baseLower === 'upload';
  const isCameraLike = /^img_\d+(?:\.[a-z0-9]+)?$/.test(baseLower);

  if (isDefaultUpload || isCameraLike) {
    const { year, month, day, hour, minute } = toJstDateParts(note.creationDate);
    const sanitizedField = sanitizeFieldName(note.fieldName);
    return `${year}_${month}_${day}_${hour}${minute}_${sanitizedField}${getExt()}`;
  }

  return baseName || getDisplayName(att);
};

const formatDateInput = (date: Date) => {
  const y = date.getFullYear();
  const m = String(date.getMonth() + 1).padStart(2, '0');
  const d = String(date.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
};

const sanitizeZipName = (names: string[]) => {
  const base = (names.length > 0 ? names.join('_') : 'attachments').trim() || 'attachments';
  const safe = base.replace(/[\\/:*?"<>|]/g, '_');
  return safe.toLowerCase().endsWith('.zip') ? safe : `${safe}.zip`;
};

const filterNotes = (notes: AggregatedNote[], filters: FiltersState) => {
  const from = filters.dateFrom ? new Date(filters.dateFrom) : null;
  const to = filters.dateTo ? new Date(filters.dateTo) : null;
  if (to) to.setHours(23, 59, 59, 999);
  const textKeyword = filters.textKeyword.toLowerCase().trim();

  return notes.filter(note => {
    const created = new Date(note.creationDate);
    if (from && created < from) return false;
    if (to && created > to) return false;

    if (filters.creator) {
      const name = note.creator ? `${note.creator.lastName} ${note.creator.firstName}`.trim() : '';
      if (name !== filters.creator) return false;
    }

    if (filters.onlyImages) {
      if (!note.attachments || !note.attachments.some(att => isImageAttachment(att))) return false;
    }
    if (filters.onlyAudio) {
      if (!note.audioAttachments || !note.audioAttachments.some(att => isAudioAttachment(att))) return false;
    }

    if (textKeyword) {
      const haystack = [
        note.note || '',
        note.fieldName || '',
        note.categories?.join(' ') || '',
        ...(note.attachments || []).map(att => att.fileName || ''),
      ].join(' ').toLowerCase();
      if (!haystack.includes(textKeyword)) return false;
    }

    return true;
  });
};

// =============================================================================
// Main Component
// =============================================================================

export function FieldMemoPage() {
  const { auth } = useAuth();
  const { submittedFarms } = useFarms();
  const { t } = useLanguage();
  const [notesOut, setNotesOut] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<{ url: string; fileName?: string | null } | null>(null);
  const [zipLoading, setZipLoading] = useState(false);
  const [zipError, setZipError] = useState<string | null>(null);
  const [zipProgress, setZipProgress] = useState<number | null>(null);
  const [zipStep, setZipStep] = useState<'preparing' | 'downloading' | null>(null);
  const [farmNameMap, setFarmNameMap] = useState<Record<string, string>>({});
  const [filters, setFilters] = useState(() => {
    return {
      dateFrom: '',
      dateTo: '',
      textKeyword: '',
      creator: '',
      onlyImages: false,
      onlyAudio: false,
    };
  });
  const [datePreset, setDatePreset] = useState<'7d' | '30d' | 'thisYear' | 'all' | 'custom'>('all');
  const [page, setPage] = useState(1);
  const pageSize = 50;

  const aggregatedNotes = useAggregatedNotes(notesOut);
  const filteredNotes = useMemo(() => filterNotes(aggregatedNotes, filters), [aggregatedNotes, filters]);
  const farmNames = useMemo(() => {
    const names = aggregatedNotes.map(n => n.farmName || '').filter(Boolean);
    const unique = Array.from(new Set(names));
    return unique.length > 0 ? unique : ['field_notes'];
  }, [aggregatedNotes]);

  useEffect(() => {
    const fetchFarms = async () => {
      if (!auth || submittedFarms.length === 0) return;
      try {
        const { json } = await postJsonCached<any>(
          withApiBase('/farms'),
          {
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            includeTokens: false,
          },
          undefined,
          { cacheKey: `farms:list:${auth?.login?.gigya_uuid ?? 'unknown'}`, cache: 'session' },
        );
        const farms = json?.response?.data?.farms;
        if (Array.isArray(farms)) {
          const map: Record<string, string> = {};
          farms.forEach((f: any) => {
            if (f?.uuid && f?.name) map[f.uuid] = f.name;
          });
          setFarmNameMap(map);
        }
      } catch (e) {
        console.warn('[FieldMemo] fetch farms for zip name failed', e);
      }
    };
    fetchFarms();
  }, [auth, submittedFarms]);

  const zipFarmNames = useMemo(() => {
    if (submittedFarms.length === 0) return farmNames;
    const names = submittedFarms.map(uuid => farmNameMap[uuid]).filter(Boolean);
    return names.length > 0 ? names : farmNames;
  }, [submittedFarms, farmNameMap, farmNames]);

  const zipFileName = useMemo(() => sanitizeZipName(zipFarmNames), [zipFarmNames]);

  // 農場選択が変更されたら、既存のデータをクリアする
  useEffect(() => {
    setNotesOut(null);
    setError(null);
  }, [submittedFarms]);

  useEffect(() => {
    setPage(1);
  }, [filters, aggregatedNotes.length]);

  const downloadableAttachments = useMemo(() => {
    return filteredNotes.flatMap(note =>
      (note.attachments || [])
        .filter(att => Boolean(att.url))
        .map(att => {
          const farmUuid = note.farmUuid ?? (submittedFarms.length === 1 ? submittedFarms[0] : undefined);
          const farmName = note.farmName
            ?? (farmUuid ? farmNameMap[farmUuid] : undefined);
          return {
            url: att.url as string,
            fileName: buildDownloadFileName(att, { creationDate: note.creationDate, fieldName: note.fieldName }),
            farmUuid,
            farmName,
          };
        }),
    );
  }, [filteredNotes, farmNameMap, submittedFarms]);

  const handleDownloadAll = async () => {
    if (downloadableAttachments.length === 0) return;
    setZipLoading(true);
    setZipError(null);
    setZipProgress(0);
    setZipStep('preparing');
    try {
      const res = await fetch(withApiBase('/attachments/zip'), {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ attachments: downloadableAttachments, zipName: zipFileName }),
      });
      if (!res.ok) {
        const text = await res.text();
        throw new Error(text || `HTTP ${res.status}`);
      }
      const contentLength = Number(res.headers.get('content-length') || 0);
      const reader = res.body?.getReader();
      if (!reader) {
        throw new Error(t('field_memo.error.read_response'));
      }
      setZipStep('downloading');
      const chunks: Uint8Array[] = [];
      let received = 0;
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) {
          chunks.push(value);
          received += value.length;
          if (contentLength > 0) {
            setZipProgress(Math.round((received / contentLength) * 100));
          }
        }
      }
      const blob = new Blob(chunks, { type: 'application/zip' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = zipFileName;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(url);
    } catch (e: any) {
      setZipError(e?.message || t('field_memo.error.download_failed'));
    } finally {
      setZipLoading(false);
      setZipProgress(null);
      setZipStep(null);
    }
  };

  const handleFetchNotes = async () => {
    if (!auth || submittedFarms.length === 0) return;

    setLoading(true);
    setError(null);
    try {
      const result = await fetchFieldNotesApi({ auth, farmUuids: submittedFarms });
      if (!result.ok) throw new Error(result.detail || 'Failed to fetch notes');
      setNotesOut(result);
    } catch (e: any) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="farms-page-container">
      {loading && <LoadingOverlay message={t('field_memo.loading')} />}
      <h2>{t('field_memo.title')}</h2>
      {submittedFarms.length === 0 && <p>{t('risk.select_farm_hint')}</p>}
      {submittedFarms.length > 0 && (
        <>
          <div className="controls-panel" style={{ justifyContent: 'flex-start' }}>
            <button onClick={handleFetchNotes} disabled={loading}>
              {loading ? (
                <span className="button-loading-inline">
                  <LoadingSpinner size={18} />
                  <span>{t('field_memo.fetching')}</span>
                </span>
              ) : (
                t('field_memo.fetch')
              )}
            </button>
          </div>

          {error && <h3 style={{ color: '#ff6b6b' }}>{t('field_memo.fetch_failed', { error })}</h3>}

          {notesOut && (
            <>
              <p>
                {t('field_memo.summary', { count: submittedFarms.length })}
                {notesOut.source && (
                  <span style={{ marginLeft: '1em', color: notesOut.source === 'cache' ? '#4caf50' : '#2196f3', fontWeight: 'bold' }}>
                    ({notesOut.source === 'cache' ? t('source.cache') : t('source.api')})
                  </span>
                )}
              </p>
              <div style={{ marginBottom: '0.75rem', display: 'flex', gap: '0.75rem', alignItems: 'center' }}>
                <button
                  onClick={handleDownloadAll}
                  disabled={zipLoading || downloadableAttachments.length === 0}
                >
                  {zipLoading ? t('field_memo.zip.preparing') : t('field_memo.zip.download')}
                </button>
                <span style={{ color: '#9e9e9e', fontSize: '0.9em' }}>
                  {t('field_memo.zip.meta', { count: downloadableAttachments.length, name: zipFileName })}
                </span>
              </div>
              {zipError && <p style={{ color: '#ff6b6b' }}>{zipError}</p>}
              <FilterBar
                filters={filters}
                onChange={setFilters}
                notes={aggregatedNotes}
                datePreset={datePreset}
                setDatePreset={setDatePreset}
              />
              <PaginatedNotes
                notes={filteredNotes}
                page={page}
                setPage={setPage}
                pageSize={pageSize}
                onPreview={setPreview}
              />
            </>
          )}
        </>
      )}
      {preview && (
        <div className="attachment-preview-backdrop" onClick={() => setPreview(null)}>
          <div className="attachment-preview" onClick={(e) => e.stopPropagation()}>
            <div className="attachment-preview-header">
              <span className="attachment-preview-title">{preview.fileName || t('field_memo.attachment.default')}</span>
              <button className="close-btn" onClick={() => setPreview(null)} aria-label={t('action.close')}>×</button>
            </div>
            <img src={preview.url} alt={preview.fileName || 'attachment preview'} />
          </div>
        </div>
      )}
      {zipLoading && (
        <div className="zip-modal-backdrop">
          <div className="zip-modal">
            <LoadingSpinner size={28} />
            <div className="zip-modal-text">
              <div>
                {zipStep === 'downloading'
                  ? t('field_memo.zip.downloading')
                  : t('field_memo.zip.creating', { count: downloadableAttachments.length })}
              </div>
              <div style={{ fontSize: '0.9em', color: '#b0b0b0' }}>
                {t('field_memo.zip.target_count', { count: downloadableAttachments.length })}
              </div>
              {zipStep === 'preparing' ? (
                <>
                  <div className="zip-progress">
                    <div
                      className="zip-progress-bar"
                      style={{ width: '35%' }}
                    />
                  </div>
                  <div style={{ fontSize: '0.85em', color: '#c0c0c0' }}>
                    {t('field_memo.zip.compressing')}
                  </div>
                </>
              ) : (
                <>
                  <div className="zip-progress">
                    <div
                      className="zip-progress-bar"
                      style={{ width: `${Math.max(5, zipProgress ?? 5)}%` }}
                    />
                  </div>
                  <div style={{ fontSize: '0.85em', color: '#c0c0c0' }}>
                    {(() => {
                      const pct = zipProgress ?? 0;
                      const clampedPct = Math.min(100, Math.max(0, pct));
                      return t('field_memo.zip.downloaded_pct', { pct: clampedPct });
                    })()}
                  </div>
                </>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// =============================================================================
// Sub Components
// =============================================================================

type FiltersState = {
  dateFrom: string;
  dateTo: string;
  textKeyword: string;
  creator: string;
  onlyImages: boolean;
  onlyAudio: boolean;
};

const FilterBar: FC<{
  filters: FiltersState;
  onChange: (f: FiltersState) => void;
  notes: AggregatedNote[];
  datePreset: '7d' | '30d' | 'thisYear' | 'all' | 'custom';
  setDatePreset: (p: '7d' | '30d' | 'thisYear' | 'all' | 'custom') => void;
}> = ({ filters, onChange, notes, datePreset, setDatePreset }) => {
  const { language, t } = useLanguage();
  const creatorOptions = useMemo(() => {
    const names = notes
      .map(n => (n.creator ? `${n.creator.lastName} ${n.creator.firstName}`.trim() : ''))
      .filter(Boolean);
    return Array.from(new Set(names)).sort();
  }, [notes]);

  const update = (partial: Partial<FiltersState>) => onChange({ ...filters, ...partial });

  const applyPreset = (kind: '7d' | '30d' | 'thisYear' | 'all') => {
    const today = new Date();
    const setRange = (from: Date | null, to: Date | null) =>
      update({
        dateFrom: from ? formatDateInput(from) : '',
        dateTo: to ? formatDateInput(to) : '',
      });
    if (kind === '7d') {
      const from = new Date();
      from.setDate(today.getDate() - 7);
      setRange(from, today);
    } else if (kind === '30d') {
      const from = new Date();
      from.setDate(today.getDate() - 30);
      setRange(from, today);
    } else if (kind === 'thisYear') {
      const from = new Date(today.getFullYear(), 0, 1);
      setRange(from, today);
    } else if (kind === 'all') {
      setRange(null, today);
    }
    setDatePreset(kind);
  };

  return (
    <div className="memo-filter-bar">
      <div className="memo-filter-row single-line">
        <div className="date-range-field">
          <div className="date-range-label">{t('field_memo.filter.date_range')}</div>
          <div className="date-range-inputs">
            <input
              type="date"
              value={filters.dateFrom}
              onChange={(e) => {
                setDatePreset('custom');
                update({ dateFrom: e.target.value });
              }}
              placeholder={t('field_memo.filter.start_date')}
            />
            <span className="date-range-sep">{language === 'ja' ? '〜' : '–'}</span>
            <input
              type="date"
              value={filters.dateTo}
              onChange={(e) => {
                setDatePreset('custom');
                update({ dateTo: e.target.value });
              }}
              placeholder={t('field_memo.filter.end_date')}
            />
          </div>
          <div className="date-preset-buttons">
            <button type="button" className={datePreset === '7d' ? 'active' : ''} onClick={() => applyPreset('7d')}>{t('field_memo.filter.preset.7d')}</button>
            <button type="button" className={datePreset === '30d' ? 'active' : ''} onClick={() => applyPreset('30d')}>{t('field_memo.filter.preset.30d')}</button>
            <button type="button" className={datePreset === 'thisYear' ? 'active' : ''} onClick={() => applyPreset('thisYear')}>{t('field_memo.filter.preset.this_year')}</button>
            <button type="button" className={datePreset === 'all' ? 'active' : ''} onClick={() => applyPreset('all')}>{t('field_memo.filter.preset.all')}</button>
          </div>
        </div>
        <label>
          {t('field_memo.filter.creator')}
          <select value={filters.creator} onChange={(e) => update({ creator: e.target.value })}>
            <option value="">{t('field_memo.filter.creator_all')}</option>
            {creatorOptions.map(name => (
              <option key={name} value={name}>{name}</option>
            ))}
          </select>
        </label>
        <label className="memo-filter-wide">
          {t('field_memo.filter.free_text')}
          <input
            type="text"
            placeholder={t('field_memo.filter.free_text_ph')}
            value={filters.textKeyword}
            onChange={(e) => update({ textKeyword: e.target.value })}
          />
        </label>
        <div className="memo-filter-toggles inline">
          <label>
            <input
              type="checkbox"
              checked={filters.onlyImages}
              onChange={(e) => update({ onlyImages: e.target.checked })}
            />
            {t('field_memo.filter.has_image')}
          </label>
          <label>
            <input
              type="checkbox"
              checked={filters.onlyAudio}
              onChange={(e) => update({ onlyAudio: e.target.checked })}
            />
            {t('field_memo.filter.has_audio')}
          </label>
        </div>
      </div>
    </div>
  );
};

const PaginatedNotes: FC<{
  notes: AggregatedNote[];
  page: number;
  setPage: (p: number) => void;
  pageSize: number;
  onPreview: (att: { url: string; fileName?: string | null } | null) => void;
}> = ({ notes, page, setPage, pageSize, onPreview }) => {
  const { t } = useLanguage();
  const filteredNotes = notes;
  const totalPages = Math.max(1, Math.ceil(filteredNotes.length / pageSize));
  const currentPage = Math.min(page, totalPages);
  const start = (currentPage - 1) * pageSize;
  const current = filteredNotes.slice(start, start + pageSize);
  const startDisplay = filteredNotes.length === 0 ? 0 : start + 1;
  const endDisplay = start + current.length;

  const handlePrev = () => setPage(Math.max(1, currentPage - 1));
  const handleNext = () => setPage(Math.min(totalPages, currentPage + 1));

  return (
    <>
      <div className="memo-pagination">
        <span>{t('field_memo.pagination.summary', { total: filteredNotes.length, start: startDisplay, end: endDisplay })}</span>
        <div className="memo-pagination-controls">
          <button onClick={handlePrev} disabled={currentPage === 1}>{t('pagination.prev')}</button>
          <span>{currentPage} / {totalPages}</span>
          <button onClick={handleNext} disabled={currentPage === totalPages}>{t('pagination.next')}</button>
        </div>
      </div>
      <div className="table-container">
        <NotesTable notes={current} onPreview={onPreview} />
      </div>
    </>
  );
};

const NotesTable: FC<{ notes: AggregatedNote[]; onPreview: (att: { url: string; fileName?: string | null } | null) => void }> = ({ notes, onPreview }) => {
  const { t } = useLanguage();
  return (
    <table className="fields-table">
      <thead>
        <tr>
          <th>{t('table.created_at')}</th>
          <th>{t('table.field')}</th>
          <th>{t('table.category')}</th>
          <th>{t('table.memo')}</th>
          <th>{t('table.creator')}</th>
          <th>{t('table.attachment_images')}</th>
          <th>{t('table.attachment_audio')}</th>
          <th>{t('table.location')}</th>
        </tr>
      </thead>
      <tbody>
        {notes.map(note => <NoteRow key={note.uuid} note={note} onPreview={onPreview} />)}
        {notes.length === 0 && (
          <tr>
            <td colSpan={8} style={{ textAlign: 'center', padding: '2rem' }}>{t('field_memo.empty')}</td>
          </tr>
        )}
      </tbody>
    </table>
  );
};

const NoteRow: FC<{ note: AggregatedNote; onPreview: (att: { url: string; fileName?: string | null } | null) => void }> = ({ note, onPreview }) => (
    <tr>
        <td>{getLocalDateString(note.creationDate)}</td>
        <td>{note.fieldName}</td>
        <td>{note.categories?.join(', ') || '-'}</td>
        <td className="note-cell"><div className="note-content">{note.note}</div></td>
        <td>{note.creator ? `${note.creator.lastName} ${note.creator.firstName}` : '-'}</td>
        <td>
            {note.attachments && note.attachments.filter(att => Boolean(att.url)).length > 0 ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
                    {note.attachments
                        .filter(att => Boolean(att.url))
                        .map(att => {
                            const downloadName = buildDownloadFileName(att, { creationDate: note.creationDate, fieldName: note.fieldName });
                            const downloadUrl = createDownloadUrl(att.url, downloadName);
                            return (
                                <div key={att.uuid} className="attachment-item">
                                    {isImageAttachment(att) && (
                                        <img
                                            src={att.url}
                                            alt={downloadName || 'attachment thumbnail'}
                                            className="attachment-thumb"
                                            loading="lazy"
                                            onClick={() => onPreview(att)}
                                        />
                                    )}
                                    <a href={downloadUrl} className="attachment-cell" title={downloadName || undefined}>
                                        <span>{downloadName}</span>
                                        <span style={{ marginLeft: '0.5em' }}>💾</span>
                                    </a>
                                </div>
                            );
                        })}
                </div>
            ) : (
                '-'
            )}
        </td>
        <td>
            {note.audioAttachments && note.audioAttachments.length > 0 ? (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                    {note.audioAttachments
                        .filter(att => isAudioAttachment(att))
                        .map(att => {
                            const name = getDisplayName(att);
                            const downloadUrl = createDownloadUrl(att.url, name);
                            return (
                                <div key={att.url} className="attachment-audio-item">
                                    <audio controls src={att.url} preload="none" className="attachment-audio-player" />
                                    <a href={downloadUrl} className="attachment-cell" title={name}>
                                        <span>{name}</span>
                                        <span style={{ marginLeft: '0.5em' }}>💾</span>
                                    </a>
                                </div>
                            );
                        })}
                </div>
            ) : (
                '-'
            )}
        </td>
        <td>{note.lat && note.lon ? `${note.lat.toFixed(4)}, ${note.lon.toFixed(4)}` : '-'}</td>
    </tr>
);
