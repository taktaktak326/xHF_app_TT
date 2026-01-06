import { createContext, useContext, useState } from 'react';
import type { ReactNode } from 'react';
import type { CombinedOut } from '../types/farm';

interface DataContextType {
  combinedOut: CombinedOut | null;
  setCombinedOut: (data: CombinedOut | null) => void;
  combinedLoading: boolean;
  setCombinedLoading: (loading: boolean) => void;
  combinedInProgress: boolean;
  setCombinedInProgress: (value: boolean) => void;
  combinedErr: string | null;
  setCombinedErr: (error: string | null) => void;
  combinedFetchAttempt: number;
  setCombinedFetchAttempt: (attempt: number) => void;
  combinedFetchMaxAttempts: number;
  setCombinedFetchMaxAttempts: (max: number) => void;
  combinedRetryCountdown: number | null;
  setCombinedRetryCountdown: (value: number | null) => void;
}

const DataContext = createContext<DataContextType | undefined>(undefined);

export const DataProvider = ({ children }: { children: ReactNode }) => {
  const [combinedOut, setCombinedOut] = useState<CombinedOut | null>(null);
  const [combinedLoading, setCombinedLoading] = useState(false);
  const [combinedInProgress, setCombinedInProgress] = useState(false);
  const [combinedErr, setCombinedErr] = useState<string | null>(null);
  const [combinedFetchAttempt, setCombinedFetchAttempt] = useState(0);
  const [combinedFetchMaxAttempts, setCombinedFetchMaxAttempts] = useState(1);
  const [combinedRetryCountdown, setCombinedRetryCountdown] = useState<number | null>(null);

  return (
    <DataContext.Provider
      value={{
        combinedOut,
        setCombinedOut,
        combinedLoading,
        setCombinedLoading,
        combinedInProgress,
        setCombinedInProgress,
        combinedErr,
        setCombinedErr,
        combinedFetchAttempt,
        setCombinedFetchAttempt,
        combinedFetchMaxAttempts,
        setCombinedFetchMaxAttempts,
        combinedRetryCountdown,
        setCombinedRetryCountdown,
      }}
    >
      {children}
    </DataContext.Provider>
  );
};

export const useData = () => {
  const context = useContext(DataContext);
  if (!context) {
    throw new Error('useData must be used within a DataProvider');
  }
  return context;
};
