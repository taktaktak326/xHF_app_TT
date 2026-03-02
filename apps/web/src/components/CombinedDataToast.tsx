import { useEffect } from 'react';
import { useData } from '../context/DataContext';
import { useLanguage } from '../context/LanguageContext';
import { useFarms } from '../context/FarmContext';
import './CombinedDataToast.css';

export const CombinedDataToast = () => {
  const {
    combinedInProgress,
    combinedLoading,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
    combinedFetchProgress,
  } = useData();
  const { cancelCombinedFetch } = useFarms();
  const { t } = useLanguage();

  const visible = combinedInProgress || combinedLoading;
  const retryInfo =
    combinedRetryCountdown !== null
      ? t('toast.retry_in', { seconds: combinedRetryCountdown })
      : combinedFetchAttempt > 0
        ? t('toast.fetch_attempt', { attempt: combinedFetchAttempt, max: combinedFetchMaxAttempts })
        : null;

  useEffect(() => {
    // no-op, placeholder if we later want auto-dismiss
  }, [visible]);

  if (!visible) return null;

  const progressInfo = (() => {
    if (!combinedFetchProgress) return null;
    const farmsCount = combinedFetchProgress.farmUuids?.length ?? null;
    const farmsLine = typeof farmsCount === 'number' ? `対象農場: ${farmsCount}` : null;
    if (combinedFetchProgress.mode === 'chunked') {
      const done = combinedFetchProgress.requestsDone;
      const total = combinedFetchProgress.requestsTotal;
      const pct = total ? Math.round((done / Math.max(1, total)) * 100) : null;
      const details = [{ icon: '', label: `分割取得: ${done}${total ? ` / ${total}` : ''}`, cls: 'toast-step--info' }];
      if (farmsLine) details.unshift({ icon: '', label: farmsLine, cls: 'toast-step--info' });
      const active = combinedFetchProgress.activeFarmLabels ?? [];
      if (active.length > 0) {
        details.push({ icon: '', label: `取得中: ${active.join(', ')}`, cls: 'toast-step--pending' });
      }
      return { pct, details };
    }
    const parts = combinedFetchProgress.parts || {};
    const keys = Object.keys(parts);
    if (keys.length === 0) return null;
    const doneCount = keys.filter((k) => parts[k]?.status && parts[k].status !== 'pending').length;
    const pct = Math.round((doneCount / Math.max(1, keys.length)) * 100);
    const labelFor = (k: string) => {
      // Prefer user-facing labels (keep English for now; can be i18n later).
      if (k === 'base') return '圃場・作期（基本情報）';
      if (k === 'insights') return 'リスク集計（insights）';
      if (k === 'predictions') return '生育ステージ予測';
      if (k === 'tasks') return 'タスク（作業計画）';
      if (k === 'tasks_sprayings') return '散布タスク';
      if (k === 'risk1') return '推奨（防除/雑草/施肥）';
      if (k === 'risk2') return 'ステータス（栄養/水/リスク）';
      return k;
    };
    const statusClass = (status: string | undefined) => {
      if (status === 'ok') return 'toast-step--ok';
      if (status === 'error') return 'toast-step--error';
      return 'toast-step--pending';
    };
    const iconFor = (status: string | undefined) => {
      if (status === 'ok') return '✓';
      if (status === 'error') return '✗';
      return '…';
    };
    const detailItems = keys
      .sort()
      .map((k) => ({ icon: iconFor(parts[k]?.status), label: labelFor(k), cls: statusClass(parts[k]?.status) }));
    if (farmsLine) detailItems.unshift({ icon: '', label: farmsLine, cls: 'toast-step--info' });
    return { pct, details: detailItems };
  })();

  return (
    <div className="combined-toast">
      <div className="combined-toast__header">
        <span className="combined-toast__title">{t('toast.combined_loading')}</span>
        <button
          type="button"
          className="combined-toast__cancel"
          onClick={cancelCombinedFetch}
        >
          {t('farm_selector.cancel_loading')}
        </button>
      </div>
      {retryInfo && (
        <div className="combined-toast__body">
          <p>{retryInfo}</p>
        </div>
      )}
      {progressInfo && (
        <div className="combined-toast__body">
          {typeof progressInfo.pct === 'number' && (
            <div className="combined-toast__progress">
              <div className="combined-toast__progress-bar" style={{ width: `${progressInfo.pct}%` }} />
              <span className="combined-toast__progress-label">{progressInfo.pct}%</span>
            </div>
          )}
          {progressInfo.details.length > 0 && (
            <ul className="combined-toast__details">
              {progressInfo.details.slice(0, 8).map((item, idx) => (
                <li key={idx} className={item.cls}>
                  {item.icon && <span className="toast-step__icon">{item.icon}</span>}
                  <span>{item.label}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      )}
    </div>
  );
};
