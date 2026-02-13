import { tr } from '../i18n/runtime';

export const formatCombinedLoadingMessage = (
  label: string,
  attempt: number,
  maxAttempts: number,
  countdown: number | null
): string => {
  const base = tr('combined.loading.fetching', { label });
  if (countdown && countdown > 0) {
    if (attempt <= 1) {
      return tr('combined.loading.preparing', { label, seconds: countdown });
    }
    return tr('combined.loading.retrying_next', { label, attempt, max: maxAttempts, seconds: countdown });
  }
  if (attempt > 1) {
    return tr('combined.loading.retrying', { label, attempt, max: maxAttempts });
  }
  return base;
};
