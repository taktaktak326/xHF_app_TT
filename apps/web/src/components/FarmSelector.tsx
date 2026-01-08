import { useCallback, useEffect, useMemo, useState } from 'react';
import { useAuth } from '../context/AuthContext';
import { useFarms } from '../context/FarmContext';
import { useData } from '../context/DataContext';
import { withApiBase } from '../utils/apiBase';
import { useWarmup } from '../context/WarmupContext';
import '../pages/FarmsPage.css'; // スタイルを再利用
import LoadingSpinner from '../components/LoadingSpinner';

// FarmsPage.tsxから型定義とAPIクライアントを移動またはインポート
type LoginAndTokenResp = any;
type Farm = any;
type FarmsOut = any;

async function fetchFarmsApi(auth: LoginAndTokenResp): Promise<FarmsOut> {
  const res = await fetch(withApiBase('/farms'), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      login_token: auth.login.login_token,
      api_token: auth.api_token,
      includeTokens: false,
    }),
  });
  const j = await res.json();
  if (!res.ok || j?.ok === false) {
    const detail = j?.detail ?? (typeof j?.response_text === "string" ? j.response_text.slice(0, 300) : "");
    throw new Error(`GraphQL error (status ${j?.status ?? res.status})${detail ? " - " + detail : ""}`);
  }
  return j as FarmsOut;
}

function farmLabel(f: Farm) {
  const owner = [f.owner?.firstName, f.owner?.lastName].filter(Boolean).join(" ") || f.owner?.email || "";
  return `${f.name ?? "(no name)"} — ${owner}`.trim();
}

export function FarmSelector() {
  const { auth } = useAuth();
  const { selectedFarms, setSelectedFarms, submitSelectedFarms } = useFarms();
  const { combinedOut, combinedLoading, combinedInProgress } = useData();
  const { status: warmupStatus, startWarmup } = useWarmup();

  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [out, setOut] = useState<FarmsOut | null>(null);

  const [dropdownOpen, setDropdownOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortKey, setSortKey] = useState<'name_asc' | 'name_desc'>('name_asc');

  const farms: Farm[] = useMemo(
    () => out?.response?.data?.farms?.filter(Boolean) ?? [],
    [out]
  );

  const collator = useMemo(() => new Intl.Collator('ja'), []);

  const filteredFarms = useMemo(() => {
    const sorted = [...farms].sort((a, b) => {
      const nameA = (a.name || '').toString();
      const nameB = (b.name || '').toString();
      const dir = sortKey === 'name_desc' ? -1 : 1;
      return collator.compare(nameA, nameB) * dir;
    });

    if (!searchTerm) return sorted;
    const lowerCaseSearchTerm = searchTerm.toLowerCase();
    return sorted.filter(farm =>
      farmLabel(farm).toLowerCase().includes(lowerCaseSearchTerm)
    );
  }, [farms, searchTerm, sortKey, collator]);

  const selectedFarmNames = useMemo(() => {
    const selectedSet = new Set(selectedFarms);
    return farms
      .filter(f => selectedSet.has(f.uuid))
      .map(f => f.name ?? "(no name)");
  }, [farms, selectedFarms]);

  const tooltipText = useMemo(() => {
    const formatList = (items: string[]) => {
      const limit = 20;
      if (items.length <= limit) return items.join('、');
      const head = items.slice(0, limit).join('、');
      return `${head}、ほか${items.length - limit}件`;
    };
    if (selectedFarmNames.length > 0) {
      return `選択中の農場: ${formatList(selectedFarmNames)}`;
    }
    return '選択中の農場はありません';
  }, [selectedFarmNames]);

  const loadFarms = useCallback(async () => {
    if (!auth) return;
    setErr(null);
    setOut(null);
    setLoading(true);
    try {
      const resp = await fetchFarmsApi(auth);
      setOut(resp);
      startWarmup().catch(() => {
        /* ウォームアップステータスの更新はトースト側で扱う */
      });
    } catch (e: any) {
      setErr(e?.message || "農場取得に失敗しました");
    } finally {
      setLoading(false);
    }
  }, [auth, startWarmup]);

  useEffect(() => {
    loadFarms();
  }, [loadFarms]);

  useEffect(() => {
    if (!auth) return;
    if (warmupStatus !== 'success') return;
    if (loading) return;
    if (out) return;
    // warmup完了後にまだデータがなければ再取得
    loadFarms();
  }, [auth, warmupStatus, loading, out, loadFarms]);

  function onCardClick(uuid: string) {
    const newSelected = selectedFarms.includes(uuid)
      ? selectedFarms.filter(id => id !== uuid)
      : [...selectedFarms, uuid];
    setSelectedFarms(newSelected);
  }

  return (
    <div className="farm-selection-container">
      <div
        className="farm-selection-header"
        onClick={() => setDropdownOpen(!dropdownOpen)}
        title={tooltipText}
        aria-label={tooltipText}
      >
        <span>{selectedFarms.length} 件の農場を選択中</span>
        {(combinedLoading || combinedInProgress) && (
          <span className="farm-selection-loading" aria-live="polite">
            <LoadingSpinner size={14} />
            <span>読み込み中…</span>
          </span>
        )}
        <span style={{ color: '#9e9e9e', fontSize: '0.9em' }}>全 {farms.length} 件</span>
        <span className="farm-selection-header-toggle">
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
            style={{ transform: dropdownOpen ? 'rotate(180deg)' : 'rotate(0deg)', transition: 'transform 0.2s' }}
          >
            <path d="M7 10L12 15L17 10" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </span>
      </div>

      {dropdownOpen && (
        <div className="farm-dropdown">
          <div className="farm-dropdown-toolbar farm-dropdown-toolbar--sticky">
            <button
              type="button"
              onClick={(e) => {
                e.stopPropagation();
                setSortKey((prev) => (prev === 'name_asc' ? 'name_desc' : 'name_asc'));
              }}
              title={`農場名 ${sortKey === 'name_asc' ? '昇順' : '降順'}`}
              style={{ width: 36, height: 36, display: 'inline-flex', alignItems: 'center', justifyContent: 'center' }}
            >
              {sortKey === 'name_asc' ? '△' : '▽'}
            </button>
            <input
              type="text"
              placeholder="農場を検索..."
              className="farm-search-input"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              onClick={(e) => e.stopPropagation()} // ヘッダーのクリックイベントが発火しないように
            />
            <button
              onClick={(e) => {
                e.stopPropagation();
                submitSelectedFarms(); // ★ 選択を「確定」する
                setDropdownOpen(false);
              }}
              disabled={selectedFarms.length === 0}
              style={{ marginLeft: 'auto', backgroundColor: '#646cff', color: 'white' }}
            >データを取得</button>
          </div>

          {loading && <p>農場読み込み中...</p>}
          {err && <p style={{ color: "crimson" }}>エラー: {err}</p>}

          {filteredFarms.length > 0 && (
            <div className="farm-list">
              {filteredFarms.map((f) => (
                <div
                  key={f.uuid}
                  className={`farm-card ${selectedFarms.includes(f.uuid) ? 'selected' : ''}`}
                  onClick={(e) => { e.stopPropagation(); onCardClick(f.uuid); }}
                >
                  <input
                    type="checkbox"
                    className="farm-card-checkbox"
                    checked={selectedFarms.includes(f.uuid)}
                    readOnly
                  />
                  <div className="farm-card-info">
                    <h4>{f.name ?? "(no name)"}</h4>
                    <p>{[f.owner?.firstName, f.owner?.lastName].filter(Boolean).join(" ") || f.owner?.email || ""}</p>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
