import type { FC } from 'react';
import './LoadingSpinner.css';

type LoadingSpinnerProps = {
  size?: number;
  className?: string;
  label?: string;
};

export const LoadingSpinner: FC<LoadingSpinnerProps> = ({ size = 20, className, label }) => {
  const spinnerClass = ['loading-spinner', className].filter(Boolean).join(' ');
  const dimension = `${size}px`;

  return (
    <span className="loading-spinner-wrapper" role="status" aria-live="polite">
      <span
        className={spinnerClass}
        style={{ width: dimension, height: dimension }}
        aria-hidden="true"
      />
      {label && <span className="loading-spinner-label">{label}</span>}
    </span>
  );
};

export default LoadingSpinner;
