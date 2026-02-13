import type { FC, ReactNode } from 'react';
import LoadingSpinner from './LoadingSpinner';
import './LoadingOverlay.css';
import { tr } from '../i18n/runtime';

type LoadingOverlayProps = {
  message?: string;
  spinnerSize?: number;
  children?: ReactNode;
  progress?: number;
  details?: string[];
};

const LoadingOverlay: FC<LoadingOverlayProps> = ({
  message = tr('loading.default'),
  spinnerSize = 40,
  children,
  progress,
  details,
}) => {
  const clampedProgress = typeof progress === 'number' ? Math.min(100, Math.max(0, progress)) : null;
  return (
    <div className="loading-overlay">
      <div className="loading-overlay__content">
        <LoadingSpinner size={spinnerSize} />
        <span>{message}</span>
        {clampedProgress !== null && (
          <div className="loading-overlay__progress">
            <div className="loading-overlay__progress-bar" style={{ width: `${clampedProgress}%` }} />
            <span className="loading-overlay__progress-label">{Math.round(clampedProgress)}%</span>
          </div>
        )}
        {details && details.length > 0 && (
          <ul className="loading-overlay__details">
            {details.map((line, idx) => (
              <li key={idx}>{line}</li>
            ))}
          </ul>
        )}
        {children && <div className="loading-overlay__extra">{children}</div>}
      </div>
    </div>
  );
};

export default LoadingOverlay;
