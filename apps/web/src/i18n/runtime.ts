import { t, type Language } from './messages';

export function getCurrentLanguage(): Language {
  if (typeof window === 'undefined') return 'ja';
  const raw = window.localStorage.getItem('xhf-lang');
  return raw === 'en' || raw === 'ja' ? raw : 'ja';
}

export function tr(key: string, vars?: Record<string, string | number>): string {
  return t(getCurrentLanguage(), key, vars);
}

