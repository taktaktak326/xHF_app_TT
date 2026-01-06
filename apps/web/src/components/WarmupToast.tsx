import { useEffect } from 'react';
import { useWarmup } from '../context/WarmupContext';
import './WarmupToast.css';

export const WarmupToast = () => {
  const { status, progress, details, error, retryWarmup, dismiss } = useWarmup();

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
    running: 'アプリを起動しています...',
    success: 'アプリの準備が完了しました',
    failed: 'アプリの準備に失敗しました',
  };

  const showProgress = status === 'running';

  return (
    <div className={`warmup-toast warmup-toast--${status}`}>
      <div className="warmup-toast__header">
        <span className="warmup-toast__title">{messageMap[status]}</span>
        <button className="warmup-toast__close" onClick={dismiss} aria-label="閉じる">
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
          <button onClick={retryWarmup}>再試行</button>
        </div>
      )}
    </div>
  );
};
