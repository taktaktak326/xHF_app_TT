import { useState } from 'react';
import { useAuth } from '../context/AuthContext';
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
    throw new Error(`応答がJSONではありません (status ${res.status}): ${text?.slice(0, 200)}`);
  }
  if (!res.ok || !j?.ok) {
    const msg = j?.detail?.gigya_errorMessage || j?.detail || j?.message || `HTTP ${res.status}`;
    const normalizedMessage = typeof msg === 'string' ? msg : JSON.stringify(msg, null, 2);
    if (
      j?.detail?.gigya_errorCode === 403042 ||
      normalizedMessage === 'Invalid LoginID' ||
      normalizedMessage === 'Invalid loginId'
    ) {
      throw new Error('メールアドレスまたはパスワードが正しくありません');
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
  const navigate = useNavigate();

  async function handleSubmit() {
    setErr(null);
    setSubmitting(true);
    try {
      // 新規ログイン前にブラウザ側のストレージをクリアして古いトークン/キャッシュを除去
      if (typeof window !== 'undefined') {
        sessionStorage.clear();
        localStorage.clear();
      }
      const authData = await loginAndToken(email, pw);
      setAuth(authData);
      navigate('/farms'); // ログイン後に圃場ページへ遷移
    } catch (e: any) {
      setErr(e?.message || 'ログインに失敗しました');
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="login-form-container">
      <div className="login-form">
        <h2>ログイン</h2>
        <div className="login-form-grid">
          <input placeholder="email" value={email} onChange={(e) => setEmail(e.target.value)} />
          <input placeholder="password" type="password" value={pw} onChange={(e) => setPw(e.target.value)} />
          <button onClick={handleSubmit} disabled={submitting}>
            {submitting ? 'ログイン中...' : 'ログイン'}
          </button>
        </div>
        {err && <p className="login-error">{err}</p>}
      </div>
    </div>
  );
}
