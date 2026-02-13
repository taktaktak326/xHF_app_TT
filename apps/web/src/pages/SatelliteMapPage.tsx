import { useEffect, useMemo, useState, useCallback } from 'react';
import './FarmsPage.css';
import './SatelliteMapPage.css';
import { useData } from '../context/DataContext';
import { useAuth } from '../context/AuthContext';
import { useLanguage } from '../context/LanguageContext';
import LoadingOverlay from '../components/LoadingOverlay';
import { formatCombinedLoadingMessage } from '../utils/loadingMessage';
import { withApiBase } from '../utils/apiBase';
import { postJsonCached } from '../utils/cachedJsonFetch';
import { getCurrentLanguage, tr } from '../i18n/runtime';

const TYPE_OPTIONS = [
  'BIOMASS_SINGLE_IMAGE_LAI',
  'BIOMASS_NDVI',
  'TRUE_COLOR_ANALYSIS',
  'BIOMASS_PROXY_MONITORING_VECTOR_ANALYSIS',
  'WEED_CLASSIFICATION_NDVI',
  'BIOMASS_MULTI_IMAGE_LAI',
];

const labelForType = (type?: string) => {
  if (!type) return tr('satellite.type.unnamed');
  if (type === 'BIOMASS_SINGLE_IMAGE_LAI') return tr('satellite.type.biomass_single_image_lai');
  if (type === 'BIOMASS_NDVI') return tr('satellite.type.biomass_ndvi');
  if (type === 'TRUE_COLOR_ANALYSIS') return tr('satellite.type.true_color');
  if (type === 'BIOMASS_PROXY_MONITORING_VECTOR_ANALYSIS') return tr('satellite.type.cloud_free');
  if (type === 'WEED_CLASSIFICATION_NDVI') return tr('satellite.type.weed_ndvi');
  if (type === 'BIOMASS_MULTI_IMAGE_LAI') return tr('satellite.type.biomass_multi_image_lai');
  return type;
};
const labelForMagnitudeType = (type?: string) => {
  if (!type) return '';
  if (type === 'LAI') return tr('satellite.mag.lai_abs');
  if (type === 'LAI_CONTRAST') return tr('satellite.mag.lai_rel');
  if (type === 'NDVI') return tr('satellite.mag.ndvi_abs');
  if (type === 'NDVI_CONTRAST') return tr('satellite.mag.ndvi_rel');
  if (type === 'AVERAGE_NDVI') return tr('satellite.mag.ndvi_avg');
  return type;
};
const shouldShowMagnitudeLabel = (layerType?: string) =>
  layerType === 'BIOMASS_SINGLE_IMAGE_LAI' || layerType === 'BIOMASS_NDVI';

const calcDaysAgo = (value?: string) => {
  if (!value || value === '-') return null;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  const diff = Date.now() - d.getTime();
  return Math.max(0, Math.floor(diff / 86400000));
};


const formatDateJst = (value?: string) => {
  if (!value) return '-';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return '-';
  const lang = getCurrentLanguage();
  const locale = lang === 'en' ? 'en-US' : 'ja-JP';
  return d.toLocaleDateString(locale, {
    timeZone: 'Asia/Tokyo',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  });
};

export function SatelliteMapPage() {
  const {
    combinedOut,
    combinedLoading,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();
  const { auth } = useAuth();
  const { language, t } = useLanguage();
  const fields = useMemo(() => combinedOut?.response?.data?.fieldsV2 ?? [], [combinedOut]);
  const collator = useMemo(() => new Intl.Collator(language === 'en' ? 'en' : 'ja'), [language]);
  const [selectedField, setSelectedField] = useState<any | null>(null);
  const [layers, setLayers] = useState<any[] | null>(null);
  const [layerSource, setLayerSource] = useState<'api' | 'cache' | null>(null);
  const [layerLoading, setLayerLoading] = useState(false);
  const [layerError, setLayerError] = useState<string | null>(null);
  const [imagePreviews, setImagePreviews] = useState<Record<string, string>>({});
  const [selectedType, setSelectedType] = useState<string>('all');
  const [previewLimit, setPreviewLimit] = useState<number | 'all'>(12);
  const [selectedFieldUuid, setSelectedFieldUuid] = useState<string | null>(null);
  const [laiVariant, setLaiVariant] = useState<'LAI' | 'LAI_CONTRAST'>('LAI');
  const [ndviVariant, setNdviVariant] = useState<'AVERAGE_NDVI' | 'NDVI' | 'NDVI_CONTRAST'>('NDVI');
  const [latestOnly, setLatestOnly] = useState(false);
  const [latestSummaries, setLatestSummaries] = useState<
    Array<{ fieldName: string; fieldUuid: string; type: string; count: number; latestDate: string; source?: string }>
  >([]);
  const [latestLoading, setLatestLoading] = useState(false);
  const [latestError, setLatestError] = useState<string | null>(null);
  const [latestSort, setLatestSort] = useState<{ key: 'field' | 'groundCount' | 'groundDate' | 'growthCount' | 'growthDate'; direction: 'asc' | 'desc' }>({
    key: 'groundCount',
    direction: 'asc',
  });
  const [latestCollapsed, setLatestCollapsed] = useState(false);

  const filtered = useMemo(() => {
    const sorted = [...fields].sort((a, b) => collator.compare(a?.name ?? '', b?.name ?? ''));
    return sorted;
  }, [fields, collator]);

  const filteredLayersForDisplay = useMemo(() => {
    if (!layers) return [];
    if (selectedType === 'all') return layers;
    return layers.filter((l) => l?.type === selectedType);
  }, [layers, selectedType]);

  const layerTypeOptions = useMemo(() => TYPE_OPTIONS, []);

  const limitedLayers = useMemo(() => {
    if (!filteredLayersForDisplay) return [];
    const sorted = [...filteredLayersForDisplay].sort((a: any, b: any) => {
      const da = a?.date ? new Date(a.date).getTime() : 0;
      const db = b?.date ? new Date(b.date).getTime() : 0;
      return db - da;
    });
    if (!latestOnly || sorted.length === 0) return sorted;
    const latestDate = sorted[sorted.length - 1]?.date ?? null;
    return latestDate ? sorted.filter((l: any) => l?.date === latestDate) : sorted;
  }, [filteredLayersForDisplay, latestOnly]);

  const magnitudeVisible = useCallback(
    (mag: any) => {
      const mType = mag?.type;
      if (!mType) return false;
      if (selectedType === 'BIOMASS_SINGLE_IMAGE_LAI') {
        return mType === laiVariant;
      }
      if (selectedType === 'BIOMASS_NDVI') {
        return mType === ndviVariant;
      }
      return true;
    },
    [selectedType, laiVariant, ndviVariant],
  );

  const allowedImageUrls = useMemo(() => {
    const urls: string[] = [];
    limitedLayers.forEach((layer: any) => {
      (layer?.magnitudes ?? []).forEach((mag: any) => {
        if (!magnitudeVisible(mag)) return;
        if (mag?.imageUrl && !urls.includes(mag.imageUrl)) {
          urls.push(mag.imageUrl);
        }
      });
    });
    if (previewLimit === 'all') return urls;
    return urls.slice(0, previewLimit);
  }, [limitedLayers, previewLimit, magnitudeVisible]);
  const allowedImageSet = useMemo(() => new Set(allowedImageUrls), [allowedImageUrls]);

  const latestByField = useMemo(() => {
    const map = new Map<string, {
      fieldName: string;
      groundCount: number;
      groundDate: string;
      growthCount: number;
      growthDate: string;
    }>();
    latestSummaries.forEach(item => {
      const entry = map.get(item.fieldUuid) || {
        fieldName: item.fieldName,
        groundCount: 0,
        groundDate: '-',
        growthCount: 0,
        growthDate: '-',
      };
      if (item.type === 'BIOMASS_MULTI_IMAGE_LAI') {
        entry.groundCount = item.count;
        entry.groundDate = item.latestDate ?? '-';
      } else if (item.type === 'BIOMASS_SINGLE_IMAGE_LAI') {
        entry.growthCount = item.count;
        entry.growthDate = item.latestDate ?? '-';
      }
      map.set(item.fieldUuid, entry);
    });
    return Array.from(map.values());
  }, [latestSummaries]);

  const displayMagnitudes = useMemo(() => {
    const items: Array<{ layer: any; mag: any; dateValue: number }> = [];
    limitedLayers.forEach((layer: any) => {
      const dateValue = layer?.date ? new Date(layer.date).getTime() : 0;
      (layer?.magnitudes ?? []).forEach((mag: any) => {
        if (mag?.imageUrl && allowedImageSet.has(mag.imageUrl)) {
          items.push({ layer, mag, dateValue });
        }
      });
    });
    // 古い順（左から古い→新しい）
    return items.sort((a, b) => a.dateValue - b.dateValue);
  }, [limitedLayers, allowedImageSet]);

  const sortedLatestByField = useMemo(() => {
    const arr = [...latestByField];
    const compare = (a: any, b: any) => {
      switch (latestSort.key) {
        case 'field':
          return a.fieldName.localeCompare(b.fieldName, language === 'en' ? 'en' : 'ja');
        case 'groundCount':
          return (a.groundCount ?? 0) - (b.groundCount ?? 0);
        case 'growthCount':
          return (a.growthCount ?? 0) - (b.growthCount ?? 0);
        case 'groundDate': {
          const da = a.groundDate ? new Date(a.groundDate).getTime() : 0;
          const db = b.groundDate ? new Date(b.groundDate).getTime() : 0;
          return da - db;
        }
        case 'growthDate': {
          const da = a.growthDate ? new Date(a.growthDate).getTime() : 0;
          const db = b.growthDate ? new Date(b.growthDate).getTime() : 0;
          return da - db;
        }
        default:
          return 0;
      }
    };
    arr.sort(compare);
    if (latestSort.direction === 'desc') arr.reverse();
    return arr;
  }, [latestByField, latestSort, language]);

  const loadingMessage = formatCombinedLoadingMessage(
    t('satellite.loading_label'),
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  );

  const noData = !combinedLoading && (!fields || fields.length === 0);

  const fetchLayers = useCallback(async (field: any, type: string = selectedType) => {
    if (!auth) {
      setLayerError(t('satellite.error.no_auth'));
      return;
    }
    if (!field?.uuid) {
      setLayerError(t('satellite.error.no_field'));
      return;
    }
    setLayerLoading(true);
    setLayerError(null);
    setLayers(null);
    setLayerSource(null);
    try {
      const types = type === 'all' ? undefined : [type];
      const { ok, status, json: data } = await postJsonCached<any>(
        withApiBase('/field-data-layers'),
        {
          login_token: auth.login.login_token,
          api_token: auth.api_token,
          field_uuid: field?.uuid,
          types,
        },
        undefined,
        { cacheKey: `field-data-layers:${field?.uuid ?? 'none'}:${type}`, cache: 'session' },
      );
      if (!ok || data?.ok === false) {
        const detail = data?.detail || data?.response_text || `status ${data?.status ?? status}`;
        throw new Error(t('satellite.error.fetch_layers_failed_detail', { detail }));
      }
      setLayers(data?.response?.data?.fieldV2?.fieldDataLayers ?? []);
      setLayerSource(data?.source ?? null);
    } catch (e: any) {
      setLayerError(e?.message || t('satellite.error.fetch_layers_failed'));
    } finally {
      setLayerLoading(false);
    }
  }, [auth, selectedType, t]);

  const onFieldChange = useCallback((uuid: string) => {
    setSelectedFieldUuid(uuid);
    setSelectedType('all');
    const target = filtered.find((f) => f?.uuid === uuid) || null;
    setSelectedField(target);
  }, [filtered, fetchLayers]);

  useEffect(() => {
    if (!auth) return;
    const urls = allowedImageUrls;
    if (urls.length === 0) {
      setImagePreviews((prev) => {
        Object.values(prev).forEach((u) => URL.revokeObjectURL(u));
        return {};
      });
      return;
    }

    let cancelled = false;
    const inFlight: string[] = [];

    // 既存のプレビューで不要になったものを削除
    setImagePreviews((prev) => {
      const next: Record<string, string> = {};
      Object.entries(prev).forEach(([key, value]) => {
        if (urls.includes(key)) {
          next[key] = value;
        } else {
          URL.revokeObjectURL(value);
        }
      });
      return next;
    });

    const fetchOne = async (url: string) => {
      try {
        const res = await fetch(withApiBase('/field-data-layer/image'), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            image_url: url,
          }),
        });
        if (!res.ok || cancelled) return;
        const blob = await res.blob();
        if (cancelled) {
          const tmp = URL.createObjectURL(blob);
          URL.revokeObjectURL(tmp);
          return;
        }
        const objectUrl = URL.createObjectURL(blob);
        inFlight.push(objectUrl);
        setImagePreviews((prev) => ({ ...prev, [url]: objectUrl }));
      } catch {
        // ignore
      }
    };

    urls.forEach((url) => { void fetchOne(url); });
    return () => {
      cancelled = true;
      setImagePreviews((prev) => {
        Object.values(prev).forEach((u) => URL.revokeObjectURL(u));
        return {};
      });
      inFlight.forEach((u) => URL.revokeObjectURL(u));
    };
  }, [auth, allowedImageUrls]);

  const openImage = (rawUrl?: string) => {
    if (!rawUrl) return;
    const url = imagePreviews[rawUrl] ?? rawUrl;
    window.open(url, '_blank', 'noreferrer');
  };

  const fetchLatestAllFields = useCallback(async () => {
    if (!auth) {
      setLatestError(t('satellite.error.no_auth'));
      return;
    }
    if (!fields.length) {
      setLatestError(t('satellite.error.no_fields_yet'));
      return;
    }
    setLatestLoading(true);
    setLatestError(null);
      setLatestSummaries([]);
      setLatestOnly(true);
    try {
      const entries: Array<{ fieldName: string; fieldUuid: string; type: string; count: number; latestDate: string; source?: string }> = [];
      for (const field of fields) {
        const { ok, json: data } = await postJsonCached<any>(
          withApiBase('/field-data-layers'),
          {
            login_token: auth.login.login_token,
            api_token: auth.api_token,
            field_uuid: field?.uuid,
            types: TYPE_OPTIONS,
          },
          undefined,
          { cacheKey: `field-data-layers:${field?.uuid ?? 'none'}:TYPE_OPTIONS`, cache: 'session' },
        );
        if (!ok || data?.ok === false) {
          continue;
        }
        const layersRes = data?.response?.data?.fieldV2?.fieldDataLayers ?? [];
        const grouped = new Map<string, { count: number; latest: string | null }>();
        layersRes.forEach((layer: any) => {
          const t = layer?.type;
          if (!t) return;
          const date = layer?.date ?? null;
          const bucket = grouped.get(t) || { count: 0, latest: null };
          bucket.count += 1;
          if (date) {
            if (!bucket.latest || new Date(date).getTime() > new Date(bucket.latest).getTime()) {
              bucket.latest = date;
            }
          }
          grouped.set(t, bucket);
        });
        // ensure target types exist even ifゼロ
        ['BIOMASS_MULTI_IMAGE_LAI', 'BIOMASS_SINGLE_IMAGE_LAI'].forEach((t) => {
          if (!grouped.has(t)) grouped.set(t, { count: 0, latest: null });
        });
        grouped.forEach((val, type) => {
          if (type !== 'BIOMASS_MULTI_IMAGE_LAI' && type !== 'BIOMASS_SINGLE_IMAGE_LAI') return;
          entries.push({
            fieldName: field?.name ?? '(no name)',
            fieldUuid: field?.uuid ?? '',
            type,
            count: val.count,
            latestDate: val.latest ?? '-',
            source: data?.source ?? undefined,
          });
        });
      }
      setLatestSummaries(entries);
    } catch (e: any) {
      setLatestError(e?.message || t('satellite.error.fetch_latest_failed'));
    } finally {
      setLatestLoading(false);
    }
  }, [auth, fields, t]);

  return (
    <div className="farms-page-container satellite-page">
      {combinedLoading && <LoadingOverlay message={loadingMessage} />}
      <div className="satellite-map-panel">
        <div className="satellite-map-panel__header">
          <div>
            <h3 style={{ margin: 0 }}>{t('satellite.panel.select_title')}</h3>
            <p style={{ margin: '4px 0 0', color: '#aab2c8' }}>
              {t('satellite.panel.select_desc')}
            </p>
          </div>
          <div className="satellite-selector-row">
            <button
              className="map-button"
              onClick={fetchLatestAllFields}
              disabled={latestLoading}
            >
              {latestLoading ? t('satellite.button.loading') : t('satellite.button.check_all')}
            </button>
            {latestLoading && (
              <div className="satellite-progress" aria-live="polite">
                <div className="satellite-progress__bar" />
                <span className="satellite-progress__text">{t('satellite.progress.checking_all')}</span>
              </div>
            )}
          </div>
        </div>
        <div className="satellite-selector-row">
          <label className="satellite-selector">
            {t('satellite.label.field')}
            <select
              value={selectedFieldUuid ?? ''}
              onChange={(e) => onFieldChange(e.target.value)}
            >
              <option value="">{t('satellite.option.select')}</option>
              {filtered.map((f: any) => (
                <option key={f?.uuid} value={f?.uuid}>{f?.name ?? '(no name)'}</option>
              ))}
            </select>
          </label>
          <label className="satellite-selector">
            {t('satellite.label.type')}
            <select
              value={selectedType}
              onChange={(e) => {
                const next = e.target.value;
                setSelectedType(next);
                if (next !== 'BIOMASS_SINGLE_IMAGE_LAI') setLaiVariant('LAI');
                if (next !== 'BIOMASS_NDVI') setNdviVariant('NDVI');
              }}
            >
              <option value="all">{t('satellite.option.all')}</option>
              {layerTypeOptions.map((t) => (
                <option key={t} value={t}>{labelForType(t)}</option>
              ))}
            </select>
          </label>
          <label className="satellite-selector">
            {t('satellite.label.preview_limit')}
            <select
              value={previewLimit}
              onChange={(e) => setPreviewLimit(e.target.value === 'all' ? 'all' : Number(e.target.value))}
            >
              <option value={8}>{t('satellite.option.images_n', { count: 8 })}</option>
              <option value={12}>{t('satellite.option.images_n', { count: 12 })}</option>
              <option value={24}>{t('satellite.option.images_n', { count: 24 })}</option>
              <option value="all">{t('satellite.option.all_images')}</option>
            </select>
          </label>
          {selectedType === 'BIOMASS_SINGLE_IMAGE_LAI' && (
            <div className="satellite-toggle-row">
              <label className="satellite-selector-inline">
                <input
                  type="radio"
                  name="laiVariant"
                  value="LAI"
                  checked={laiVariant === 'LAI'}
                  onChange={() => setLaiVariant('LAI')}
                />
                {t('satellite.type.biomass_single_image_lai')} ({t('satellite.mag.lai_abs')})
              </label>
              <label className="satellite-selector-inline">
                <input
                  type="radio"
                  name="laiVariant"
                  value="LAI_CONTRAST"
                  checked={laiVariant === 'LAI_CONTRAST'}
                  onChange={() => setLaiVariant('LAI_CONTRAST')}
                />
                {t('satellite.type.biomass_single_image_lai')} ({t('satellite.mag.lai_rel')})
              </label>
            </div>
          )}
          {selectedType === 'BIOMASS_NDVI' && (
            <div className="satellite-toggle-row">
              <label className="satellite-selector-inline">
                <input
                  type="radio"
                  name="ndviVariant"
                  value="NDVI"
                  checked={ndviVariant === 'NDVI'}
                  onChange={() => setNdviVariant('NDVI')}
                />
                {t('satellite.type.biomass_ndvi')} ({t('satellite.mag.ndvi_abs')})
              </label>
              <label className="satellite-selector-inline">
                <input
                  type="radio"
                  name="ndviVariant"
                  value="NDVI_CONTRAST"
                  checked={ndviVariant === 'NDVI_CONTRAST'}
                  onChange={() => setNdviVariant('NDVI_CONTRAST')}
                />
                {t('satellite.type.biomass_ndvi')} ({t('satellite.mag.ndvi_rel')})
              </label>
              <label className="satellite-selector-inline">
                <input
                  type="radio"
                  name="ndviVariant"
                  value="AVERAGE_NDVI"
                  checked={ndviVariant === 'AVERAGE_NDVI'}
                  onChange={() => setNdviVariant('AVERAGE_NDVI')}
                />
                {t('satellite.type.biomass_ndvi')} ({t('satellite.mag.ndvi_avg')})
              </label>
            </div>
          )}
          <button
            className="map-button"
            onClick={() => {
              const field = selectedField ?? fields.find((f: any) => f?.uuid === selectedFieldUuid);
              if (!field) {
                setLayerError(t('satellite.error.no_field'));
                return;
              }
              setLayerError(null);
              setLatestOnly(false);
              fetchLayers(field, selectedType);
            }}
            disabled={layerLoading}
            style={{ marginLeft: 'auto' }}
          >
            {layerLoading ? t('satellite.button.loading') : t('satellite.button.fetch_layers')}
          </button>
        </div>
        {noData && <p className="satellite-empty map-empty">{t('satellite.empty.no_data')}</p>}
      </div>

      <div className="satellite-summary-panel">
        {latestLoading && <p className="satellite-meta">{t('satellite.summary.loading_latest')}</p>}
        {latestError && <p className="satellite-error">{latestError}</p>}
        {latestSummaries.length > 0 && (
          <div className="satellite-layers">
            <div className="satellite-layer-filter">
              <span>{t('satellite.summary.fields_layers', { fields: new Set(latestSummaries.map(s => s.fieldUuid)).size, layers: latestSummaries.length })}</span>
              <button
                className="map-button map-button--tiny"
                onClick={() => setLatestCollapsed((c) => !c)}
              >
                {latestCollapsed ? t('satellite.button.expand') : t('satellite.button.collapse')}
              </button>
            </div>
            {!latestCollapsed && (
              <table className="latest-table">
                <thead>
                  <tr>
                    <th onClick={() => setLatestSort(prev => ({ key: 'field', direction: prev.key === 'field' && prev.direction === 'asc' ? 'desc' : 'asc' }))}>
                      {t('table.field')}
                    </th>
                    <th onClick={() => setLatestSort(prev => ({ key: 'groundCount', direction: prev.key === 'groundCount' && prev.direction === 'asc' ? 'desc' : 'asc' }))}>
                      {t('satellite.table.ground_map')}
                    </th>
                    <th onClick={() => setLatestSort(prev => ({ key: 'groundDate', direction: prev.key === 'groundDate' && prev.direction === 'asc' ? 'desc' : 'asc' }))}>
                      {t('satellite.table.latest_date')}
                    </th>
                    <th onClick={() => setLatestSort(prev => ({ key: 'growthCount', direction: prev.key === 'growthCount' && prev.direction === 'asc' ? 'desc' : 'asc' }))}>
                      {t('satellite.table.growth_map')}
                    </th>
                    <th onClick={() => setLatestSort(prev => ({ key: 'growthDate', direction: prev.key === 'growthDate' && prev.direction === 'asc' ? 'desc' : 'asc' }))}>
                      {t('satellite.table.latest_date')}
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {sortedLatestByField.map((item, idx) => {
                    const groundDays = calcDaysAgo(item.groundDate);
                    const growthDays = calcDaysAgo(item.growthDate);
                    return (
                      <tr key={`${item.fieldName}-${idx}`}>
                        <td>{item.fieldName}</td>
                        <td className={item.groundCount === 0 ? 'latest-zero' : ''}>{t('satellite.table.count_suffix', { count: item.groundCount })}</td>
                        <td className={item.groundCount === 0 ? 'latest-zero' : ''}>
                          {item.groundDate === '-' ? t('satellite.table.not_fetched') : formatDateJst(item.groundDate)}
                          {groundDays !== null ? t('satellite.table.days_ago', { days: groundDays }) : ''}
                        </td>
                        <td className={item.growthCount === 0 ? 'latest-zero' : ''}>{t('satellite.table.count_suffix', { count: item.growthCount })}</td>
                        <td className={item.growthCount === 0 ? 'latest-zero' : ''}>
                          {item.growthDate === '-' ? t('satellite.table.not_fetched') : formatDateJst(item.growthDate)}
                          {growthDays !== null ? t('satellite.table.days_ago', { days: growthDays }) : ''}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
          </div>
        )}
      </div>

      <div className="satellite-detail-panel">
        <div className="satellite-detail-header">
          <div>
            <h3 style={{ margin: 0 }}>{t('satellite.detail.title')}</h3>
            <p style={{ margin: '4px 0 0', color: '#aab2c8' }}>
              {t('satellite.detail.hint')}
            </p>
          </div>
          {selectedField && (
            <div className="satellite-detail-meta">
              <span className="satellite-chip">{selectedField?.name ?? t('field.default_name')}</span>
              {layerSource && (
                <span className="satellite-chip satellite-chip--source">
                  {layerSource === 'cache' ? 'Cache' : 'API'}
                </span>
              )}
              <button
                className="map-button map-button--ghost"
                onClick={() => fetchLayers(selectedField)}
                disabled={layerLoading}
              >
                {layerLoading ? t('satellite.button.refreshing') : t('satellite.button.refresh')}
              </button>
            </div>
          )}
        </div>
        {layerLoading && <p className="satellite-meta">{t('satellite.loading.layers')}</p>}
        {layerError && <p className="satellite-error">{layerError}</p>}
        {!layerLoading && !layerError && !selectedField && (
          <p className="satellite-empty">{t('satellite.empty.select_field')}</p>
        )}
        {!latestOnly && !layerLoading && layers && layers.length === 0 && selectedField && (
          <p className="satellite-empty">{t('satellite.empty.no_layers')}</p>
        )}
        {!latestOnly && !layerLoading && layers && layers.length > 0 && (
          <div className="satellite-layers">
            <div className="satellite-layer-filter">
              <span>{t('satellite.summary.images_layers', { images: displayMagnitudes.length, layers: limitedLayers.length })}</span>
            </div>
            {displayMagnitudes.length === 0 && (
              <p className="satellite-empty">{t('satellite.empty.no_images')}</p>
            )}
            {displayMagnitudes.length > 0 && (
              <div className="satellite-magnitudes-grid">
                {displayMagnitudes.map(({ layer, mag }, idx) => (
                  <div key={mag.imageUrl ?? mag.vectorTilesUrl ?? `${mag.type}-${idx}`} className="satellite-magnitude">
                    {mag.imageUrl && (
                      <div className="satellite-image-block">
                        <div className="satellite-image-wrapper">
                          {imagePreviews[mag.imageUrl] ? (
                            <img
                              className="satellite-image clickable-image"
                              src={imagePreviews[mag.imageUrl]}
                              alt={mag.type ?? 'image'}
                              loading="lazy"
                              onClick={() => openImage(mag.imageUrl)}
                            />
                          ) : (
                            <div className="satellite-image-placeholder">{t('satellite.image.loading')}</div>
                          )}
                        </div>
                      </div>
                    )}
                    <div className="satellite-thumb-date">{formatDateJst(layer?.date)}</div>
                    <div className="satellite-thumb-label">
                      <span className="satellite-chip">{labelForType(layer?.type)}</span>
                      {shouldShowMagnitudeLabel(layer?.type) && mag?.type && (
                        <span className="satellite-chip satellite-chip--sub">
                          {labelForMagnitudeType(mag.type)}
                        </span>
                      )}
                    </div>
                    {mag.vectorTilesUrl && (
                      <a className="map-button map-button--tiny" href={mag.vectorTilesUrl} target="_blank" rel="noreferrer">
                        {t('satellite.link.tiles')}
                      </a>
                    )}
                    {mag.vectorTilesStyleUrl && (
                      <a className="map-button map-button--tiny" href={mag.vectorTilesStyleUrl} target="_blank" rel="noreferrer">
                        {t('satellite.link.style')}
                      </a>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}
