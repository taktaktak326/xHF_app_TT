import { useEffect, useRef, useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { LANGUAGE_STORAGE_KEY, useLanguage } from '../context/LanguageContext';
import type { LoginAndTokenResp } from '../types/farm';
import { withApiBase } from '../utils/apiBase';
import { useNavigate } from 'react-router-dom';
import LoadingOverlay from './LoadingOverlay';
import LoadingSpinner from './LoadingSpinner';
import './LoginForm.css';

async function loginAndToken(
  email: string,
  password: string,
  opts?: { signal?: AbortSignal },
): Promise<LoginAndTokenResp> {
  const res = await fetch(withApiBase('/login-and-token'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
    signal: opts?.signal,
  });
  const text = await res.text();
  let j: any = null;
  try {
    j = text ? JSON.parse(text) : null;
  } catch {
    throw new Error(`Response is not JSON (status ${res.status}): ${text?.slice(0, 200)}`);
  }
  if (!res.ok || !j?.ok) {
    const msg = j?.detail?.gigya_errorMessage || j?.detail || j?.message || `HTTP ${res.status}`;
    const normalizedMessage = typeof msg === 'string' ? msg : JSON.stringify(msg, null, 2);
    if (
      j?.detail?.gigya_errorCode === 403042 ||
      normalizedMessage === 'Invalid LoginID' ||
      normalizedMessage === 'Invalid loginId'
    ) {
      throw new Error('Incorrect email or password');
    }
    throw new Error(normalizedMessage);
  }
  return j;
}

export function LoginForm() {
  const [email, setEmail] = useState('');
  const [pw, setPw] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [slow, setSlow] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const { setAuth } = useAuth();
  const { language, toggleLanguage, t } = useLanguage();
  const navigate = useNavigate();
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (!submitting) {
      setSlow(false);
      return undefined;
    }
    setSlow(false);
    const timer = setTimeout(() => setSlow(true), 8000);
    return () => clearTimeout(timer);
  }, [submitting]);

  function cancelSubmit() {
    abortRef.current?.abort();
    abortRef.current = null;
    setSubmitting(false);
    setErr(t('login.canceled'));
  }

  async function handleSubmit() {
    setErr(null);
    setSubmitting(true);
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      // 新規ログイン前にブラウザ側のストレージをクリアして古いトークン/キャッシュを除去
      if (typeof window !== 'undefined') {
        sessionStorage.clear();
        const preservedLang = localStorage.getItem(LANGUAGE_STORAGE_KEY);
        localStorage.clear();
        if (preservedLang) localStorage.setItem(LANGUAGE_STORAGE_KEY, preservedLang);
      }
      const authData = await loginAndToken(email, pw, { signal: controller.signal });
      setAuth(authData);
      navigate('/farms'); // ログイン後に圃場ページへ遷移
    } catch (e: any) {
      const fallback = t('login.failed');
      const msg = e?.name === 'AbortError' ? t('login.canceled') : e?.message || fallback;
      if (msg.startsWith('Response is not JSON')) {
        setErr(`${t('login.invalid_json')} (${msg.replace('Response is not JSON', '').trim()})`);
      } else if (msg === 'Incorrect email or password') {
        setErr(t('login.invalid_credentials'));
      } else {
        setErr(msg || fallback);
      }
    } finally {
      setSubmitting(false);
      abortRef.current = null;
    }
  }

  return (
    <div className="login-form-container">
      {submitting && (
        <LoadingOverlay
          message={t('login.submitting')}
          details={slow ? [t('login.slow'), t('login.slow_hint')] : undefined}
        >
          <button onClick={cancelSubmit}>{t('action.cancel')}</button>
        </LoadingOverlay>
      )}
      <div className="login-form">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '1rem' }}>
          <h2 style={{ margin: 0 }}>{t('login.title')}</h2>
          <button
            onClick={toggleLanguage}
            title={language === 'ja' ? t('action.switch_to_en') : t('action.switch_to_ja')}
            style={{ flexShrink: 0 }}
            disabled={submitting}
          >
            {language === 'ja' ? 'JA' : 'EN'}
          </button>
        </div>
        <div className="login-form-grid">
          <input
            placeholder={t('login.email_placeholder')}
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            disabled={submitting}
          />
          <input
            placeholder={t('login.password_placeholder')}
            type="password"
            value={pw}
            onChange={(e) => setPw(e.target.value)}
            disabled={submitting}
          />
          <button onClick={handleSubmit} disabled={submitting}>
            {submitting ? <LoadingSpinner size={18} label={t('login.submitting')} /> : t('login.submit')}
          </button>
        </div>
        {err && <p className="login-error">{err}</p>}
      </div>
    </div>
  );
}
