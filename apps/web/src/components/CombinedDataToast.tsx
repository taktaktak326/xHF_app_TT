import { useEffect } from 'react';
import { useData } from '../context/DataContext';
import './CombinedDataToast.css';

export const CombinedDataToast = () => {
  const {
    combinedInProgress,
    combinedLoading,
    combinedFetchAttempt,
    combinedFetchMaxAttempts,
    combinedRetryCountdown,
  } = useData();

  const visible = combinedInProgress || combinedLoading;
  const retryInfo =
    combinedRetryCountdown !== null
      ? `再試行まで ${combinedRetryCountdown}s`
      : combinedFetchAttempt > 0
        ? `取得試行 ${combinedFetchAttempt}/${combinedFetchMaxAttempts}`
        : null;

  useEffect(() => {
    // no-op, placeholder if we later want auto-dismiss
  }, [visible]);

  if (!visible) return null;

  return (
    <div className="combined-toast">
      <div className="combined-toast__header">
        <span className="combined-toast__title">圃場データを取得しています…</span>
      </div>
      {retryInfo && (
        <div className="combined-toast__body">
          <p>{retryInfo}</p>
        </div>
      )}
    </div>
  );
};
