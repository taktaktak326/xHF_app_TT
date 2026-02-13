import { useEffect, useMemo, useState } from 'react';
import type { FC } from 'react';
import { useData } from '../context/DataContext';
import { useFarms } from '../context/FarmContext';
import type { Field, Risk, TimingStressInfo } from '../types/farm';
import { getLocalDateString, formatInclusiveEndDate, groupConsecutiveItems } from '../utils/formatters';
import { formatCombinedLoadingMessage } from '../utils/loadingMessage';
import './FarmsPage.css'; // FarmsPageのスタイルを再利用
import './RiskPage.css';
import LoadingOverlay from '../components/LoadingOverlay';
import { useLanguage } from '../context/LanguageContext';
import { tr } from '../i18n/runtime';

// =============================================================================
// Type Definitions
// =============================================================================

type AggregatedRisk = Risk & {
  fieldName: string;
  cropName: string;
  riskName: string;
  seasonUuid: string;
  groupKey: string;
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

// =============================================================================
// Custom Hook for Data Processing
// =============================================================================

/**
 * combinedOut データから全てのリスク情報を集約・加工して返すカスタムフック
 */
const useAggregatedRisks = (): AggregatedRisk[] => {
  const { combinedOut } = useData();

  return useMemo(() => {
    const fields = selectFieldsFromCombinedOut(combinedOut);
    if (!fields || fields.length === 0) return [];

    const allRisks = fields.flatMap(field => {
      return field.cropSeasonsV2?.flatMap(season => {
        if (!season.risks || season.risks.length === 0) {
          return [];
        }

        // リスク名を取得するためのマップを作成
        const stressInfoMap = new Map<string, TimingStressInfo>();
        season.timingStressesInfo?.forEach(info => {
          stressInfoMap.set(info.stressV2.uuid, info);
        });

        const risksWithInfo = season.risks
          .filter(risk => ['HIGH', 'MEDIUM'].includes(risk.status))
	          .map(risk => {
	            const stressInfo = stressInfoMap.get(risk.stressV2.uuid);
	            return {
	              ...risk,
	              fieldName: field.name,
	              cropName: season.crop.name,
	              riskName: stressInfo?.stressV2.name || tr('risk.unknown'),
	              seasonUuid: season.uuid,
	              // groupConsecutiveItemsで使うためのキー
	              groupKey: `${stressInfo?.stressV2.uuid}-${risk.status}`,
	            };
	          });
        
        return groupConsecutiveItems(risksWithInfo, 'groupKey');
      }) ?? [];
    });

    // 開始日でソート（新しいものが上）
    return allRisks.sort((a, b) => new Date(b.startDate).getTime() - new Date(a.startDate).getTime());
  }, [combinedOut]);
};

// =============================================================================
// Main Component
// =============================================================================

export function RiskPage() {
  const {
    combinedOut,
    combinedLoading,
    combinedErr,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();
  const { submittedFarms, fetchCombinedDataIfNeeded } = useFarms();
  const aggregatedRisks = useAggregatedRisks();
  const { t } = useLanguage();

  useEffect(() => {
    fetchCombinedDataIfNeeded();
  }, [fetchCombinedDataIfNeeded]);

  if (submittedFarms.length === 0) {
    return (
      <div className="farms-page-container">
        <h2>{t('risk.title')}</h2>
        <p>{t('risk.select_farm_hint')}</p>
      </div>
    );
  }

  if (combinedLoading) {
    return (
      <div className="farms-page-container">
        <h2>{t('risk.title')}</h2>
        <LoadingOverlay
          message={formatCombinedLoadingMessage(
            t('risk.loading_label'),
            combinedFetchAttempt,
            combinedFetchMaxAttempts,
            combinedRetryCountdown,
          )}
        />
      </div>
    );
  }

  if (combinedErr) {
    return (
      <div className="farms-page-container">
        <h2>{t('risk.title')}</h2>
        <h3 style={{ color: '#ff6b6b' }}>{t('risk.load_failed')}</h3>
        <pre style={{ color: '#ff6b6b', whiteSpace: 'pre-wrap', wordBreak: 'break-all' }}>
          {combinedErr}
        </pre>
      </div>
    );
  }

  return (
    <div className="farms-page-container">
      <h2>{t('risk.title')}</h2>
      <p>
        {t('risk.summary', { count: submittedFarms.length })}
        {combinedOut?.source && (
          <span style={{ marginLeft: '1em', color: combinedOut.source === 'cache' ? '#4caf50' : '#2196f3', fontWeight: 'bold' }}>
            ({combinedOut.source === 'cache' ? t('risk.source.cache') : t('risk.source.api')})
          </span>
        )}
      </p>
      <div className="table-container">
        <RisksTable risks={aggregatedRisks} />
      </div>
    </div>
  );
}

// =============================================================================
// Sub Components
// =============================================================================

const RisksTable: FC<{ risks: AggregatedRisk[] }> = ({ risks }) => {
  const [rowsPerPage, setRowsPerPage] = useState<number>(20);
  const [currentPage, setCurrentPage] = useState<number>(1);
  const { t } = useLanguage();

  useEffect(() => {
    setCurrentPage(1);
  }, [risks, rowsPerPage]);

  const totalPages = Math.max(1, Math.ceil(risks.length / rowsPerPage));

  useEffect(() => {
    setCurrentPage(prev => {
      const next = Math.min(Math.max(prev, 1), totalPages);
      return next;
    });
  }, [totalPages]);

  const paginatedRisks = useMemo(() => {
    if (risks.length === 0) {
      return [];
    }
    const start = (currentPage - 1) * rowsPerPage;
    return risks.slice(start, start + rowsPerPage);
  }, [risks, currentPage, rowsPerPage]);

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

  return (
    <>
      {risks.length > 0 && (
        <div className="risk-pagination">
          <div className="risk-pagination__left">
            <label htmlFor="risk-rows-per-page">
              {t('risk.rows_per_page')}{' '}
              <select
                id="risk-rows-per-page"
                value={rowsPerPage}
                onChange={(e) => handleRowsPerPageChange(Number(e.currentTarget.value))}
              >
                {[10, 20, 50, 100].map(size => (
                  <option key={size} value={size}>{size}</option>
                ))}
              </select>
            </label>
          </div>
          <div className="risk-pagination__right">
            <button type="button" onClick={() => handlePageChange(-1)} disabled={currentPage <= 1}>
              {t('pagination.prev')}
            </button>
            <span className="risk-pagination__page">
              {currentPage} / {totalPages}
            </span>
            <button type="button" onClick={() => handlePageChange(1)} disabled={currentPage >= totalPages}>
              {t('pagination.next')}
            </button>
          </div>
        </div>
      )}

      <table className="fields-table">
        <thead>
          <tr>
            <th>{t('table.field')}</th>
            <th>{t('table.crop')}</th>
            <th>{t('table.risk')}</th>
            <th>{t('table.status')}</th>
            <th>{t('table.period')}</th>
          </tr>
        </thead>
        <tbody>
          {paginatedRisks.map(risk => (
            <RiskRow key={`${risk.seasonUuid}-${risk.groupKey}-${risk.startDate}`} risk={risk} />
          ))}
          {risks.length === 0 && (
            <tr>
              <td colSpan={5} style={{ textAlign: 'center', padding: '2rem' }}>{t('risk.empty')}</td>
            </tr>
          )}
        </tbody>
      </table>
    </>
  );
};

function formatRiskPeriod(risk: AggregatedRisk): string {
  const formattedStart = getLocalDateString(risk.startDate);
  const formattedEnd = formatInclusiveEndDate(risk.endDate);
  if (formattedStart === formattedEnd) {
    return formattedStart;
  }
  return `${formattedStart} - ${formattedEnd}`;
}

const RiskRow: FC<{ risk: AggregatedRisk }> = ({ risk }) => (
  <tr>
    <td>{risk.fieldName}</td>
    <td>{risk.cropName}</td>
    <td>{risk.riskName}</td>
    <td>
      <span className={`status-badge ${risk.status.toLowerCase()}`}>{risk.status}</span>
    </td>
    <td>
      {formatRiskPeriod(risk)}
    </td>
  </tr>
);
