# HFRスナップショット & xHF26ダッシュボード 仕様書

最終更新: 2026-03-01
対象実装:
- API: `apps/api/main.py`
- ストレージ: `apps/api/services/hfr_snapshot_store.py`
- 農薬マスタキャッシュ: `apps/api/services/crop_product_cache_store.py`
- UI: `apps/web/src/pages/TaskProgressDashboardPage.tsx`
- 定期実行: `.github/workflows/hfr-snapshot.yml`

## 1. 目的
- HFR末尾の圃場（例: `...HFR`）に限定して、圃場・タスク情報を日次で保存する。
- 保存済みスナップショットを `xHF for Rita 26タスク管理ダッシュボード`（ヘッダー: `xHF26ダッシュボード`）で可視化する。
- 運用は「日次自動 + 必要時手動更新（許可ユーザーのみ）」を前提とする。

## 2. 定期実行（スケジュール）
### 2.1 実行トリガー
- GitHub Actions: `HFR Snapshot Daily`
- Cron: `0 22 * * *`（UTC）= JST 07:00 毎日
- 手動実行: `workflow_dispatch`

### 2.2 実行内容
Workflowは以下をPOSTする。
- URL: `${{ secrets.SNAPSHOT_URL }}`
- Header: `X-Job-Secret: ${{ secrets.SNAPSHOT_JOB_SECRET }}`
- Body: `{"dryRun":false}`

対象API:
- `POST /api/jobs/hfr-snapshot`

## 3. ジョブ実行モード
`/api/jobs/hfr-snapshot` は2モード。

### 3.1 scheduledモード（通常）
条件:
- `login_token/api_token` 未指定
- `X-Job-Secret` が `SNAPSHOT_JOB_SECRET` と一致

認証:
- `.env` の `SNAPSHOT_USER_EMAIL`, `SNAPSHOT_USER_PASSWORD` でログイン

処理:
1. FarmsOverview で農場一覧取得
2. `FieldsNameScanByFarms` で `suffix=HFR` 圃場を持つ農場のみ抽出
3. 抽出農場をチャンク分割して `CombinedFieldData` 取得
4. HFR圃場に再フィルタ
5. fields/tasks を抽出しDBへ upsert
6. 同日・別run_idデータを prune（同日1スナップショットに収束）

### 3.2 manualモード（UIからの手動）
条件:
- `login_token` と `api_token` を指定
- `email` が `HFR_SNAPSHOT_MANUAL_EMAIL`（未設定時 `am@shonai.inc`）と一致
- `farm_uuids` 指定必須

特徴:
- 選択農場のみ対象
- modeはレスポンスで `manual`

## 4. 環境変数（主要）
### 4.1 必須級
- `HFR_SNAPSHOT_DATABASE_URL`（または `DATABASE_URL`）
- `SNAPSHOT_JOB_SECRET`
- `SNAPSHOT_USER_EMAIL`
- `SNAPSHOT_USER_PASSWORD`

### 4.2 動作調整
- `HFR_SNAPSHOT_CHUNK_SIZE`（既定5）
- `HFR_SNAPSHOT_CHUNK_ATTEMPTS`（既定6）
- `HFR_SNAPSHOT_CHUNK_RETRY_BACKOFF_SEC`（既定3.0）
- `HFR_SNAPSHOT_REQUIRE_COMPLETE_CHUNK`（既定 true）
- `HFR_SNAPSHOT_CACHE_TTL`（API側キャッシュ秒、既定600）

## 5. DBスキーマ（PostgreSQL）
保存テーブル:
- `hfr_snapshot_runs`
- `hfr_snapshot_fields`
- `hfr_snapshot_tasks`

### 5.1 主キー
- runs: `run_id`
- fields: `(snapshot_date, field_uuid, season_uuid)`
- tasks: `(snapshot_date, task_uuid)`

### 5.2 同日データ保持ルール
- upsert後、`prune_snapshot_date(snapshot_date, keep_run_id)` を実施
- 同一 `snapshot_date` の旧 `run_id` データ（runs/fields/tasks）を削除
- つまり、原則「日付ごとに最新1run」を保持

## 6. API仕様（ダッシュボード用）
### 6.1 `GET /api/hfr-snapshots`
用途:
- スナップショット明細取得

主パラメータ:
- `snapshot_date=YYYY-MM-DD`
- `farm_uuid`（任意）
- `include_fields` / `include_tasks`
- `limit`, `field_limit`, `task_limit`（最大50000）
- `refresh=true` でAPIキャッシュ無視

### 6.2 `GET /api/hfr-snapshots/summary`
用途:
- KPI/ランキング/分布/トレンドの集計返却

主パラメータ:
- `snapshot_date`
- `families`（カンマ区切り）
- `action_filter` (`none|overdue|due_today|upcoming_3days|future|incomplete`)
- `refresh`

### 6.3 `GET /api/hfr-snapshots/dates`
用途:
- 保存日一覧（直近日付セレクタ用）

### 6.4 `GET /api/hfr-snapshots/fields-csv`
用途:
- field_uuid単位の集約CSV出力

### 6.5 `GET /api/hfr-snapshots/compare`
用途:
- 2日比較（追加/削除/状態変化）

## 7. ダッシュボードUI仕様（現行）
対象ページ:
- `xHF for Rita 26タスク管理ダッシュボード`

### 7.1 初期ロード
- 先に summary + dates を取得
- tasks は必要時に取得（ただし現行ダッシュボード計算で利用）
- ローディングオーバーレイで進捗表示

### 7.2 フィルタ
- 表示タスク（単一選択）
- アクションフィルタ（遅延/本日期限/今後など）
- 農業者検索

### 7.3 タスク名/分類（UI表示）
基本マッピング:
- Harvest: `収穫タスク`
- Spraying: `散布タスク`（ただし creationFlowHint と農薬カテゴリで再分類）
- WaterManagement: `水管理タスク`
- Scouting: `観察記録タスク`
- CropEstablishment: `播種タスク`
- LandPreparation: `土壌管理タスク`
- SeedTreatment: `種子処理タスク`
- SeedBoxTreatment: `育苗箱処理タスク`

Spraying再分類:
- `creation_flow_hint=WEED_MANAGEMENT` → `雑草管理タスク`
- `creation_flow_hint=NUTRITION` → `施肥タスク`
- `creation_flow_hint=CROP_PROTECTION` → `防除タスク（除草剤/殺菌剤/殺虫剤/その他）`

### 7.4 画面要素
- KPIカード
- 農業者遅延マップ（散布図）
- 農業者ランキング
- タスクタイプ別進捗（回数別）
- 遅延率分布
- 圃場スナップショット表（必要時ロード）
- タスク一覧表（必要時ロード）

## 8. キャッシュ仕様
### 8.1 API側（FastAPIメモリ）
- `_hfr_snapshot_cache`（TTL: `HFR_SNAPSHOT_CACHE_TTL`）
- 対象: `hfr-snapshots`, `summary`, `dates`, `compare`, `fields-csv`
- スナップショット保存成功時に対象日キーをinvalidate

### 8.2 Web側
- メモリMap + `sessionStorage` 併用
- summary/dates/tasksの一部をセッション内再利用
- フィルタ変更時は原則クライアント再計算で即時反映

## 9. 障害時の典型エラー
### 9.1 `/api/jobs/hfr-snapshot`
- `unauthorized_job`: `X-Job-Secret` 不一致
- `snapshot_credentials_missing`: scheduled用認証情報不足
- `snapshot_store_not_ready`: DB接続不可/未設定
- `snapshot_chunk_upstream_failed`: Xarvio上流失敗(503等)

### 9.2 `/api/farms` 502
- Xarvio上流の一時不調で発生しうる
- フロント側は短いリトライ + 詳細エラー表示対応済み

## 10. 運用チェックリスト
### 10.1 毎日確認
1. GitHub Actions `HFR Snapshot Daily` が success
2. `/api/hfr-snapshots/dates` に当日が追加
3. `/api/hfr-snapshots?snapshot_date=当日` で run/fields/tasks が返る

### 10.2 手動リカバリ
1. Render環境変数確認（DB URL / snapshot creds / secret）
2. APIへ手動POST実行
3. `refresh=true` で summary/dates再取得

## 11. 今後の改善候補
- DB集計のマテビュー化（summary高速化）
- tasksのサーバーサイドページング標準化
- run監査ログ（再実行理由、起動元、所要時間）
- 圧縮アーカイブポリシー（長期保持向け）
