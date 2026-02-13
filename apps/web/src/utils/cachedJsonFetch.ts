import { getSessionCache, setSessionCache } from './sessionCache';

export type CachedJsonSource = 'api' | 'cache';

export type CachedJsonResult<T> = {
  ok: boolean;
  status: number;
  json: T;
  source: CachedJsonSource;
  savedAt?: number;
};

type CacheMode = 'session' | 'none';

type CachedJsonFetchOptions = {
  cache?: CacheMode;
  cacheKey?: string;
  shouldCache?: (result: { ok: boolean; status: number; json: unknown }) => boolean;
};

const inFlight = new Map<string, Promise<CachedJsonResult<unknown>>>();

const fnv1aHex = (input: string): string => {
  let hash = 0x811c9dc5;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, '0');
};

const bodyToCacheKeyPart = (body: RequestInit['body']): string | null => {
  if (body === undefined || body === null) return '';
  if (typeof body === 'string') return body;
  // Avoid caching non-string bodies (FormData/Blob/etc.) to prevent surprises.
  return null;
};

const isOkFalsePayload = (value: unknown): boolean => {
  if (!value || typeof value !== 'object') return false;
  return (value as { ok?: unknown }).ok === false;
};

const readJsonOrText = async (res: Response): Promise<unknown> => {
  const text = await res.text();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
};

export async function fetchJsonCached<T>(
  input: RequestInfo | URL,
  init?: RequestInit,
  options?: CachedJsonFetchOptions,
): Promise<CachedJsonResult<T>> {
  const cacheMode: CacheMode = options?.cache ?? 'session';
  const method = (init?.method ?? 'GET').toUpperCase();
  const url = typeof input === 'string' ? input : input instanceof URL ? input.toString() : input.url;
  const bodyKey = bodyToCacheKeyPart(init?.body);

  // If body isn't cacheable (e.g., FormData), fall back to plain fetch.
  if (cacheMode === 'none' || bodyKey === null) {
    const res = await fetch(input, init);
    const json = (await readJsonOrText(res)) as T;
    return { ok: res.ok, status: res.status, json, source: 'api' };
  }

  const derivedKey = `jsonfetch:${method}:${url}:${fnv1aHex(bodyKey)}`;
  const cacheKeyRaw = options?.cacheKey ?? derivedKey;
  // Avoid colliding with any legacy sessionStorage keys used elsewhere in the app.
  const cacheKey = `cjson:v1:${cacheKeyRaw}`;

  const cached = cacheMode === 'session' ? getSessionCache<CachedJsonResult<T>>(cacheKey) : null;
  if (cached) {
    return { ...cached, source: 'cache' };
  }

  const pending = inFlight.get(cacheKey) as Promise<CachedJsonResult<T>> | undefined;
  if (pending) {
    return pending;
  }

  const request = (async () => {
    const res = await fetch(input, init);
    const json = (await readJsonOrText(res)) as T;
    const base = { ok: res.ok, status: res.status, json };

    const shouldCache =
      options?.shouldCache ??
      ((r) => r.ok && r.json !== null && typeof r.json === 'object' && !isOkFalsePayload(r.json));

    const out: CachedJsonResult<T> = { ...base, source: 'api' };
    if (cacheMode === 'session' && shouldCache(base)) {
      const savedAt = Date.now();
      const toStore: CachedJsonResult<T> = { ...out, savedAt, source: 'api' };
      setSessionCache(cacheKey, toStore);
      return toStore;
    }
    return out;
  })();

  inFlight.set(cacheKey, request as Promise<CachedJsonResult<unknown>>);
  try {
    return await request;
  } finally {
    inFlight.delete(cacheKey);
  }
}

export async function postJsonCached<T>(
  url: string,
  payload: unknown,
  init?: Omit<RequestInit, 'method' | 'body'>,
  options?: CachedJsonFetchOptions,
): Promise<CachedJsonResult<T>> {
  return fetchJsonCached<T>(
    url,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...(init?.headers ?? {}) },
      ...init,
      body: JSON.stringify(payload ?? {}),
    },
    options,
  );
}
