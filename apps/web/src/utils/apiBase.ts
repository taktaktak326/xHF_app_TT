const resolvedBase =
  (import.meta.env.VITE_API_BASE && import.meta.env.VITE_API_BASE.trim()) ||
  (import.meta.env.DEV ? 'http://localhost:8080/api' : '/api');

export const API_BASE = resolvedBase.replace(/\/+$/, '') || '/api';

export const withApiBase = (path: string) => {
  const normalizedPath = path.startsWith('/') ? path : `/${path}`;
  return `${API_BASE}${normalizedPath}`;
};
