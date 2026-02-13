import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { t as translate, type Language } from '../i18n/messages';

export const LANGUAGE_STORAGE_KEY = 'xhf-lang';

type LanguageContextValue = {
  language: Language;
  setLanguage: (lang: Language) => void;
  toggleLanguage: () => void;
  t: (key: string, vars?: Record<string, string | number>) => string;
};

const LanguageContext = createContext<LanguageContextValue | undefined>(undefined);

function readStoredLanguage(): Language | null {
  if (typeof window === 'undefined') return null;
  const raw = localStorage.getItem(LANGUAGE_STORAGE_KEY);
  return raw === 'en' || raw === 'ja' ? raw : null;
}

export function LanguageProvider({ children }: { children: ReactNode }) {
  const [language, setLanguageState] = useState<Language>(() => readStoredLanguage() ?? 'ja');

  const setLanguage = useCallback((next: Language) => {
    setLanguageState(next);
    if (typeof window !== 'undefined') {
      localStorage.setItem(LANGUAGE_STORAGE_KEY, next);
    }
  }, []);

  const toggleLanguage = useCallback(() => {
    setLanguage(language === 'ja' ? 'en' : 'ja');
  }, [language, setLanguage]);

  useEffect(() => {
    const stored = readStoredLanguage();
    if (stored && stored !== language) setLanguageState(stored);
  }, [language]);

  const value = useMemo<LanguageContextValue>(() => {
    return {
      language,
      setLanguage,
      toggleLanguage,
      t: (key, vars) => translate(language, key, vars),
    };
  }, [language, setLanguage, toggleLanguage]);

  return <LanguageContext.Provider value={value}>{children}</LanguageContext.Provider>;
}

export function useLanguage(): LanguageContextValue {
  const ctx = useContext(LanguageContext);
  if (!ctx) throw new Error('useLanguage must be used within LanguageProvider');
  return ctx;
}

