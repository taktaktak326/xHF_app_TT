import { useState } from 'react';
import { NavLink, Outlet, useNavigate } from 'react-router-dom';
import { useAuth } from '../context/AuthContext';
import { FarmSelector } from './FarmSelector';
import { useFarms } from '../context/FarmContext';
import { useData } from '../context/DataContext';
import { withApiBase } from '../utils/apiBase';
import './Layout.css';

const navClass = ({ isActive }: { isActive: boolean }) => (isActive ? 'nav-link active' : 'nav-link');

export function Layout() {
  const { setAuth } = useAuth();
  const navigate = useNavigate();
  const { clearSelectedFarms } = useFarms();
  const { combinedLoading, combinedInProgress } = useData();
  const [clearCacheStatus, setClearCacheStatus] = useState<'idle' | 'loading' | 'success' | 'error'>('idle');

  const handleLogout = () => {
    setAuth(null);
    navigate('/login');
  };

  const handleClearCache = async () => {
    setClearCacheStatus('loading');
    try {
      const res = await fetch(withApiBase('/cache/graphql/clear'), { method: 'POST' });
      if (res.ok) {
        setClearCacheStatus('success');
        clearSelectedFarms(); // ★ 選択中の農場をリセット
      }
      else setClearCacheStatus('error');
    } catch {
      setClearCacheStatus('error');
    }
    setTimeout(() => setClearCacheStatus('idle'), 2000); // 2秒後に元に戻す
  };

  return (
    <div>
      {(combinedLoading || combinedInProgress) && (
        <div className="global-loading-bar" aria-hidden="true">
          <div className="global-loading-bar__inner" />
        </div>
      )}
      <header style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0.5rem 1rem', borderBottom: '1px solid #444', gap: '1rem', position: 'relative', zIndex: 20 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '2rem' }}>
          <FarmSelector />
          <nav style={{ display: 'flex', gap: '1rem' }}>
            <NavLink to="/farms" className={navClass}>圃場情報</NavLink>
            <NavLink to="/tasks" className={navClass}>タスク</NavLink>
            <NavLink to="/ndvi" className={navClass}>リモートセンシング</NavLink>
            <NavLink to="/satellite-map" className={navClass}>衛星マップ</NavLink>
            <NavLink to="/crop-registration" className={navClass}>作付登録</NavLink>
            <NavLink to="/field-memo" className={navClass}>圃場メモ</NavLink>
            <NavLink to="/risks" className={navClass}>リスク</NavLink>
            <NavLink to="/growth-stage-predictions" className={navClass}>生育ステージ予測</NavLink>
            <NavLink to="/weather" className={navClass}>天気</NavLink>
          </nav>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
          <button
            onClick={handleClearCache}
            disabled={clearCacheStatus === 'loading'}
            title="APIキャッシュをクリア"
            style={{ background: 'none', border: 'none', cursor: 'pointer', padding: '8px', color: clearCacheStatus === 'success' ? '#4caf50' : clearCacheStatus === 'error' ? '#ff6b6b' : 'white' }}
          >
            <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="23 4 23 10 17 10"></polyline>
              <polyline points="1 20 1 14 7 14"></polyline>
              <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
            </svg>
          </button>
          <button onClick={handleLogout} style={{ flexShrink: 0 }}>ログアウト</button>
        </div>
      </header>
      <main style={{ padding: '1rem' }}>
        <Outlet /> {/* ここに各ページコンポーネントが描画される */}
      </main>
    </div>
  );
}
