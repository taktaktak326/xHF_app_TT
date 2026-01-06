export const formatCombinedLoadingMessage = (
  label: string,
  attempt: number,
  maxAttempts: number,
  countdown: number | null
): string => {
  const base = `${label}を取得しています...`;
  if (countdown && countdown > 0) {
    if (attempt <= 1) {
      return `${label}を準備しています...（自動再取得まで ${countdown} 秒）`;
    }
    return `${label}を取得しています...（再試行 ${attempt}/${maxAttempts}・次の再取得まで ${countdown} 秒）`;
  }
  if (attempt > 1) {
    return `${label}を取得しています...（再試行 ${attempt}/${maxAttempts}）`;
  }
  return base;
};
