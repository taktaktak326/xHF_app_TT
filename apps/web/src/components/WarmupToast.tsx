import { useEffect } from 'react';
import { useWarmup } from '../context/WarmupContext';
import { useLanguage } from '../context/LanguageContext';
import './WarmupToast.css';

export const WarmupToast = () => {
  const { status, progress, details, error, retryWarmup, dismiss } = useWarmup();
  const { t } = useLanguage();

  useEffect(() => {
    if (status === 'success') {
      const timer = setTimeout(() => {
        dismiss();
      }, 3500);
      return () => clearTimeout(timer);
    }
    return undefined;
  }, [status, dismiss]);

  if (status === 'idle') {
    return null;
  }

  const messageMap = {
    running: t('warmup.running'),
    success: t('warmup.success'),
    failed: t('warmup.failed'),
  } as const;

  const showProgress = status === 'running';

  return (
    <div className={`warmup-toast warmup-toast--${status}`}>
      <div className="warmup-toast__header">
        <span className="warmup-toast__title">{messageMap[status]}</span>
        <button className="warmup-toast__close" onClick={dismiss} aria-label={t('action.close')}>
          ×
        </button>
      </div>
      {showProgress && (
        <div className="warmup-toast__progress">
          <div className="warmup-toast__progress-bar" style={{ width: `${Math.round(progress)}%` }} />
        </div>
      )}
      <div className="warmup-toast__body">
        {details.map((line, idx) => (
          <p key={idx}>{line}</p>
        ))}
        {error && status === 'failed' && <p className="warmup-toast__error">{error}</p>}
      </div>
      {status === 'failed' && (
        <div className="warmup-toast__actions">
          <button onClick={retryWarmup}>{t('action.retry')}</button>
        </div>
      )}
    </div>
  );
};
