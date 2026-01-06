# アプリ仕様概要

## 1. プロダクト概要
- **名称**: (暫定) Xarvio Helper Frontend (xhf-app)
- **目的**: Xarvio プラットフォームから圃場データ／NDVI・LAI情報／タスク・リスク情報などをまとめて取得し、日本語 UI で閲覧・分析する。
- **利用者想定**: Xarvio アカウントを持つ社内ユーザー（農業支援担当者など）。

## 2. システム構成
| コンポーネント | 技術スタック | 役割 |
|---|---|---|
| フロントエンド | React + TypeScript + Vite | SPA。Cloudflare Pages で配信。|
| バックエンド | FastAPI (Python) | Xarvio/Gigya API のプロキシとデータマージ処理。Render で稼働。|
| 認証 | Gigya / Xarvio API | フロントから FastAPI を経由して Gigya 認証 → Xarvio API トークンを取得。|
| データ元 | Xarvio GraphQL / REST API | 圃場データ、NDVI、タスク、リスク、天気など。|

### 2.1 デプロイ先
- Render Web Service (API)
- Cloudflare Pages (フロント)

## 3. 認証フロー
1. ログイン画面でメール・パスワードを入力。
2. FastAPI `/api/login-and-token` が Gigya で認証し、Xarvio API トークンを取得。
3. 取得した `login_token` と `api_token` は `AuthContext` で保持し、各 API 呼び出しで使用。

## 4. フロントエンド主要画面
| ページ | パス | 主な機能 |
|---|---|---|
| ログイン | `/login` | Gigya/Xarvio アカウントでログイン。|
| 圃場情報 | `/farms` | 圃場・作期情報一覧、ソート／ページング／CSV ダウンロード。|
| タスク一覧 | `/tasks` | 圃場ごとのタスク集約 (収穫・散布・水管理など)。|
| NDVI | `/ndvi` | 作期選択、NDVI/LAI タイムライン、天気データ、テーブル表示・CSV。|
| 圃場メモ | `/field-memo` | 圃場メモ取得、一覧表示。|
| リスク | `/risks` | Xarvio リスク情報を一覧化。|
| 生育ステージ予測 | `/growth-stage-predictions` | BBCH ステージのガントチャート表示。|
| 散布天気 | `/weather` → `/weather/:fieldUuid` | 圃場別の散布適性・天気データ。|

### 4.1 UI 共通要素
- ヘッダー: 農場選択ドロップダウン (`FarmSelector`)、ナビゲーション、キャッシュクリア、ログアウト。
- ProtectedRoute: 認証済みでなければ `/login` にリダイレクト。

## 5. バックエンド API エンドポイント ( `/api` 配下 )
| メソッド | パス | 説明 |
|---|---|---|
| POST | `/login` / `/login-and-token` | Gigya 認証 & Xarvio トークン取得。|
| POST | `/farms` `/fields` | 圃場／圃場詳細取得 (GraphQL)。|
| POST | `/combined-fields` | 複数 GraphQL クエリを並列呼び出しし、圃場データをマージ。|
| POST | `/combined-field-data-tasks` | タスク関連の圃場データ取得。|
| POST | `/biomass-ndvi` | NDVI 値取得 (GraphQL)。|
| POST | `/biomass-lai` | LAI 取得 (REST)。|
| POST | `/field-notes` | 圃場メモ取得。|
| POST | `/weather-by-field` | 天気・散布適性データ取得。|
| GET/DELETE/POST | `/cache/graphql/*` | GraphQL レスポンスの簡易キャッシュ閲覧／削除。|
| GET | `/healthz` / `/api/healthz` | ヘルスチェック。|

## 6. 主なコンテキスト & フロントロジック
- `AuthContext`: `login_token` / `api_token` の保持。
- `FarmContext`: 農場選択、Combined Data の取得・キャッシュ制御。
- `DataContext`: 圃場データ共通ストア (`combinedOut`, ローディング／エラー状態)。
- `useBiomassData`: NDVI/LAI データのキャッシュと取得ロジック。
- `useFieldSeasonOptions`: 圃場・作期選択用データ整形。

## 7. 開発・デプロイフロー
1. **バックエンド修正**
   - Render の Web Service で自動デプロイ (Git 連携)
   - Root Directory: `apps/api`
   - Environment: `Docker`
   - Environment Variables:
     - `GIGYA_BASE=https://accounts.eu1.gigya.com`
     - `GIGYA_API_KEY=...`
     - `XARVIO_TOKEN_API_URL=https://fm-api.xarvio.com/api/users/tokens`
     - `XARVIO_GRAPHQL_ENDPOINT=https://fm-api.xarvio.com/api/graphql/data`

2. **フロントエンド修正**
   - Cloudflare Pages で自動デプロイ (Git 連携)
   - Build command: `npm run build --workspace=web`
   - Build output directory: `apps/web/dist`
   - Environment Variables:
     - `VITE_API_BASE=https://<render-api-host>/api`

3. **確認**
   ```bash
   curl https://<render-api-host>/api/healthz
   ```
   - Cloudflare Pages の URL でログイン → 各ページの動作確認。

## 8. 環境変数 (Render)
| 変数名 | 役割 |
|---|---|
| `GIGYA_BASE` | Gigya 認証エンドポイントのベース URL (`https://accounts.eu1.gigya.com`) |
| `GIGYA_API_KEY` | Gigya API キー |
| `XARVIO_TOKEN_API_URL` | Xarvio トークン取得エンドポイント |
| `XARVIO_GRAPHQL_ENDPOINT` | Xarvio GraphQL エンドポイント |
| `PREF_CITY_ENABLED` | pref/city データを使う場合は `true`、使わない場合は `false` |
| その他 `.env` 相当 | Render の環境変数に必要なものを追加 |

## 9. 今後の改善メモ
- GraphQL レスポンスのキャッシュ戦略の高度化 (Redis 等)。
- CI/CD パイプラインの自動化 (GitHub Actions → Render / Cloudflare Pages)。
- テスト整備 (フロントのユニットテスト、バックエンドの統合テスト)。
