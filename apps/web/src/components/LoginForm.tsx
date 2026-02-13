import { useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { LANGUAGE_STORAGE_KEY, useLanguage } from '../context/LanguageContext';
import { withApiBase } from '../utils/apiBase';
import { useNavigate } from 'react-router-dom';
import './LoginForm.css';

// App.tsxからAPIクライアントと型定義を移動またはインポート
type LoginAndTokenResp = any; // 仮の型
async function loginAndToken(email: string, password: string): Promise<LoginAndTokenResp> {
  const res = await fetch(withApiBase('/login-and-token'), {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password }),
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
  const [err, setErr] = useState<string | null>(null);
  const { setAuth } = useAuth();
  const { language, toggleLanguage, t } = useLanguage();
  const navigate = useNavigate();

  async function handleSubmit() {
    setErr(null);
    setSubmitting(true);
    try {
      // 新規ログイン前にブラウザ側のストレージをクリアして古いトークン/キャッシュを除去
      if (typeof window !== 'undefined') {
        sessionStorage.clear();
        const preservedLang = localStorage.getItem(LANGUAGE_STORAGE_KEY);
        localStorage.clear();
        if (preservedLang) localStorage.setItem(LANGUAGE_STORAGE_KEY, preservedLang);
      }
      const authData = await loginAndToken(email, pw);
      setAuth(authData);
      navigate('/farms'); // ログイン後に圃場ページへ遷移
    } catch (e: any) {
      const fallback = t('login.failed');
      const msg = e?.message || fallback;
      if (msg.startsWith('Response is not JSON')) {
        setErr(`${t('login.invalid_json')} (${msg.replace('Response is not JSON', '').trim()})`);
      } else if (msg === 'Incorrect email or password') {
        setErr(t('login.invalid_credentials'));
      } else {
        setErr(msg || fallback);
      }
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="login-form-container">
      <div className="login-form">
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '1rem' }}>
          <h2 style={{ margin: 0 }}>{t('login.title')}</h2>
          <button
            onClick={toggleLanguage}
            title={language === 'ja' ? t('action.switch_to_en') : t('action.switch_to_ja')}
            style={{ flexShrink: 0 }}
          >
            {language === 'ja' ? 'JA' : 'EN'}
          </button>
        </div>
        <div className="login-form-grid">
          <input placeholder={t('login.email_placeholder')} value={email} onChange={(e) => setEmail(e.target.value)} />
          <input placeholder={t('login.password_placeholder')} type="password" value={pw} onChange={(e) => setPw(e.target.value)} />
          <button onClick={handleSubmit} disabled={submitting}>
            {submitting ? t('login.submitting') : t('login.submit')}
          </button>
        </div>
        {err && <p className="login-error">{err}</p>}
      </div>
    </div>
  );
}
