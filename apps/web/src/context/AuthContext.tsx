import { createContext, useContext, useEffect, useState } from 'react';
import type { ReactNode } from 'react';
import type { LoginAndTokenResp } from '../types/farm';

// App.tsxから型定義を移動またはインポート

interface AuthContextType {
  auth: LoginAndTokenResp | null;
  setAuth: (auth: LoginAndTokenResp | null) => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider = ({ children }: { children: ReactNode }) => {
  const STORAGE_KEY = 'xhf-auth';

  const readStoredAuth = () => {
    if (typeof window === 'undefined') return null;
    const stored = sessionStorage.getItem(STORAGE_KEY);
    if (!stored) return null;
    try {
      const parsed = JSON.parse(stored) as LoginAndTokenResp | null;
      return parsed && typeof parsed === 'object' ? parsed : null;
    } catch {
      sessionStorage.removeItem(STORAGE_KEY);
      return null;
    }
  };

  const [auth, setAuthState] = useState<LoginAndTokenResp | null>(() => readStoredAuth());

  useEffect(() => {
    if (auth !== null) return;
    const stored = readStoredAuth();
    if (stored) {
      setAuthState(stored);
    }
  }, []);

  const setAuth = (nextAuth: LoginAndTokenResp | null) => {
    setAuthState(nextAuth);
    if (typeof window === 'undefined') return;
    if (nextAuth) {
      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(nextAuth));
    } else {
      sessionStorage.removeItem(STORAGE_KEY);
    }
  };

  return <AuthContext.Provider value={{ auth, setAuth }}>{children}</AuthContext.Provider>;
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
};
