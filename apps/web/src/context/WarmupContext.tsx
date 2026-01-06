import { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react';
import type { ReactNode } from 'react';
import { withApiBase } from '../utils/apiBase';

type WarmupStatus = 'idle' | 'running' | 'success' | 'failed';

interface WarmupContextValue {
  status: WarmupStatus;
  progress: number;
  details: string[];
  error: string | null;
  startWarmup: (force?: boolean) => Promise<void>;
  retryWarmup: () => Promise<void>;
  dismiss: () => void;
}

const WarmupContext = createContext<WarmupContextValue | undefined>(undefined);

const STATUS_POLL_INTERVAL_MS = 5000;

export const WarmupProvider = ({ children }: { children: ReactNode }) => {
  const [status, setStatus] = useState<WarmupStatus>('idle');
  const [progress, setProgress] = useState(0);
  const [details, setDetails] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);

  const statusRef = useRef<WarmupStatus>('idle');
  const lastEntryCountRef = useRef<number | null>(null);
  const autoDismissTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pollTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const clearAutoDismiss = useCallback(() => {
    if (autoDismissTimerRef.current) {
      clearTimeout(autoDismissTimerRef.current);
      autoDismissTimerRef.current = null;
    }
  }, []);

  const scheduleAutoDismiss = useCallback(() => {
    clearAutoDismiss();
    autoDismissTimerRef.current = setTimeout(() => {
      setStatus('idle');
      setProgress(0);
      setDetails([]);
      setError(null);
    }, 4000);
  }, [clearAutoDismiss]);

  const stopPolling = useCallback(() => {
    if (pollTimerRef.current) {
      clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    }
  }, []);

  const applyPayload = useCallback((payload: any) => {
    const nextState = (payload?.state as WarmupStatus | undefined) ?? 'idle';
    const entryCount =
      typeof payload?.entryCount === 'number' ? payload.entryCount : lastEntryCountRef.current;
    lastEntryCountRef.current = entryCount ?? null;

    if (nextState === 'success') {
      setStatus('success');
      setProgress(100);
      setDetails([
        'アプリの準備が完了しました',
        `位置情報データ: ${entryCount ?? '不明'} 件をロードしました`,
      ]);
      setError(null);
      if (statusRef.current !== 'success') {
        scheduleAutoDismiss();
      }
      return;
    }

    if (nextState === 'running') {
      setStatus('running');
      setProgress((prev) => {
        if (prev <= 0) return 25;
        if (prev >= 90) return prev;
        return Math.min(90, prev + 10);
      });
      setDetails([
        'バックエンドを初期化中',
        `位置情報データを準備しています（現在 ${entryCount ?? 0} 件）`,
      ]);
      setError(null);
      return;
    }

    if (nextState === 'failed') {
      setStatus('failed');
      setProgress(0);
      setDetails([
        'バックエンドの初期化に失敗しました',
        '再試行ボタンをタップしてもう一度お試しください',
        `位置情報データの読み込み状況: ${entryCount ?? 0} 件`,
      ]);
      setError(payload?.error ?? 'ウォームアップに失敗しました');
      return;
    }

    setStatus('idle');
    setProgress(0);
    setDetails([]);
    setError(null);
  }, [scheduleAutoDismiss]);

  const refreshStatus = useCallback(async () => {
    try {
      const res = await fetch(withApiBase('/warmup/status'));
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      const payload = await res.json();
      applyPayload(payload);
    } catch (err) {
      console.warn('[Warmup] status check failed', err);
    }
  }, [applyPayload]);

  const startPolling = useCallback(() => {
    if (pollTimerRef.current) {
      return;
    }
    pollTimerRef.current = setInterval(() => {
      refreshStatus().catch((err) => console.warn('[Warmup] polling error', err));
    }, STATUS_POLL_INTERVAL_MS);
  }, [refreshStatus]);

  const startWarmup = useCallback(
    async (force = false) => {
      clearAutoDismiss();
      const url = withApiBase(force ? '/warmup?force=1' : '/warmup');
      try {
        const res = await fetch(url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
        });
        if (!res.ok) {
          const body = await res.text();
          throw new Error(body || `HTTP ${res.status}`);
        }
        const payload = await res.json();
        applyPayload(payload);
        if ((payload?.state ?? 'idle') === 'running') {
          startPolling();
        } else if ((payload?.state ?? 'idle') === 'success') {
          stopPolling();
        }
      } catch (err) {
        console.error('[Warmup] start failed', err);
        setStatus('failed');
        setProgress(0);
        setDetails([
          'バックエンドの初期化に失敗しました',
          '再試行ボタンをタップしてもう一度お試しください',
        ]);
        setError(err instanceof Error ? err.message : 'ウォームアップに失敗しました');
      }
    },
    [applyPayload, clearAutoDismiss, startPolling, stopPolling],
  );

  const retryWarmup = useCallback(() => startWarmup(true), [startWarmup]);

  const dismiss = useCallback(() => {
    clearAutoDismiss();
    stopPolling();
    setStatus('idle');
    setProgress(0);
    setDetails([]);
    setError(null);
  }, [clearAutoDismiss, stopPolling]);

  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  useEffect(() => {
    refreshStatus();
  }, [refreshStatus]);

  useEffect(() => {
    if (status === 'running') {
      startPolling();
    } else {
      stopPolling();
    }
  }, [startPolling, status, stopPolling]);

  useEffect(
    () => () => {
      clearAutoDismiss();
      stopPolling();
    },
    [clearAutoDismiss, stopPolling],
  );

  return (
    <WarmupContext.Provider
      value={{ status, progress, details, error, startWarmup, retryWarmup, dismiss }}
    >
      {children}
    </WarmupContext.Provider>
  );
};

export const useWarmup = () => {
  const ctx = useContext(WarmupContext);
  if (!ctx) {
    throw new Error('useWarmup must be used within a WarmupProvider');
  }
  return ctx;
};
