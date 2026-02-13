| 役割 | コマンド | 実行ディレクトリ | 備考 |
|---|---|---|---|
| FastAPI（バックエンド） | uvicorn main:app --reload --port 8080 | xhf-app/apps/api | Python の API サーバ |
| Vite（フロントエンド） | npm run dev | xhf-app/apps/web | React の開発サーバ |
※ Vite のポートは空き状況で 5173 / 5174…に変わることがあります（表示された URL を使用）。

## ローカルでテストする手順

1. **バックエンド (FastAPI) を起動**
   ```bash
   cd /Users/takuya/Desktop/xhf-app/apps/api
   uvicorn main:app --reload --port 8080
   ```

2. **フロントエンド (Vite) を起動**
   ```bash
   cd /Users/takuya/Desktop/xhf-app/apps/web
   npm run dev
   ```

3. ブラウザで `http://localhost:5173` を開き、本番同様に機能を確認する。

## フロントエンドのセッションキャッシュ（同一セッションで二重取得しない）

フロント側では **同一ブラウザタブのセッション中**、同じ内容の API レスポンスを再取得しないために `sessionStorage` を使ったキャッシュを利用しています（タブを閉じると消えます）。

- 共通ユーティリティ: `apps/web/src/utils/cachedJsonFetch.ts`
  - `sessionStorage` キャッシュ（キー一致で即返却）
  - 同一キーの **in-flight (進行中) リクエストを共有**して二重 fetch を抑止（React StrictMode の effect 再実行対策にも有効）
- キャッシュ対象は主に「読み取り系」API（GraphQL ライクな POST も含む）で、ログイン・更新系・ポーリング・添付 ZIP / 画像 blob などは都度取得のままです。

## 都道府県・市区町村を緯度経度から取得する仕組み（JP）

圃場の「都道府県」「市区町村」は、API から常に返ってくるとは限らないため、フロントエンド側で **緯度経度から逆ジオコード**（推定）するフォールバックを持っています。

### データソース

- `apps/web/public/pref_city_p5.topo.json.gz`
  - 日本の行政界ポリゴン（TopoJSON / gzip）
  - geometry の `properties` に `prefecture` / `municipality` / `subMunicipality` / `cityCode` を保持

### 実装（Worker）

- `apps/web/src/workers/prefCityReverseGeocode.ts`
  - gzip を `DecompressionStream('gzip')` で展開して TopoJSON を読み込み
  - ポリゴンをタイルインデックス化して、(lat, lon) から候補ポリゴンを絞り込み → point-in-polygon でヒットを判定

### UI 側の流れ（表示できた時に何が違ったか）

表示が安定して出るようになったポイント（＝「出たり出なかったり」の原因潰し）:

1. **Worker 初期化の順序**
   - `worker.onmessage` を先に設定してから `dataset` を送る
   - `dataset_ack` を取り逃がすと `warmup` が走らず `prefCityDatasetReady=false` のままになり、`lookup` が一切走らない（緯度経度は出ていても都道府県/市区町村だけ空になる）
2. **location のマージ**
   - Worker で得た `prefecture/municipality` を `field.location` に重ねるとき、`null/空文字` が既存値を潰さないように「空でない値だけ上書き」する
3. **緯度経度の補正**
   - まれに緯度経度が入れ替わっているデータがあるため、範囲チェックして入れ替えられそうなら補正して `lookup` する

### 動作確認（開発）

- 圃場ページ（`apps/web/src/pages/FarmsPage.tsx`）には開発用の確認 UI があり、
  - `pref-city: ready/not_ready`
  - `resolved / pending`
  - `Test reverse geocode (Tokyo)`（固定座標の逆ジオコード）
  を見て「逆ジオコード自体が動いているか / 初期化が完了しているか」を切り分けできます。

### 期待する結果例

- 緯度 `37.70739` / 経度 `138.83850` → `新潟県 西蒲原郡 弥彦村`（`cityCode: 15342`）

## 新しい GraphQL リクエストを追加する手順

1. **クエリを定義する**  
   `apps/api/graphql/queries.py` にクエリ文字列を追加する。

2. **リクエスト型を定義する**  
   `apps/api/schemas.py` に Pydantic のリクエストモデルを追加する。

3. **エンドポイントを追加する**  
   `apps/api/main.py` に `@app.post("/xxx")` を追加し、  
   `make_payload` と `call_graphql` を使って実行する。

4. **Vite プロキシに追記する**  
   `apps/web/vite.config.ts` の `server.proxy` にエンドポイントを追加する。

5. **フロントから呼び出す**  
   `fetch("/xxx", {...})` でバックエンド API を叩く処理を追加し、  
   画面にレスポンスを表示する。

---

✅ この 5 ステップを繰り返せば、他の GraphQL リクエストも同じ流れで追加できます。

## 変更を本番へ反映する手順 (Render + Cloudflare Pages)

### 中学生でもわかる版（まずはこれだけ）

このアプリの本番反映（デプロイ）は、ざっくり **2つ** です。

- **バックエンド(API)**: Render が担当（GitHub に push すると自動で更新される）
- **フロント(画面)**: Cloudflare Pages が担当（コマンドでアップロードして更新する）

#### 0) 事前にそろっていること（最初の1回だけ）

- Render に「このリポジトリを見てビルドする設定」ができている
  - Root Directory: `apps/api`
  - Environment: `Docker`
- Cloudflare Pages に `xhf-app-tt` プロジェクトがある
- ローカルPCで `wrangler` にログイン済み（`npx wrangler whoami` で確認できる）

#### 1) GitHub に push（ここがスタート）

まずは変更を GitHub に送ります（= push）。

```bash
cd /Users/takuya/Desktop/xhf-app
git status
git add -A
git commit -m "your message"
git push origin main
```

これで **Render 側のバックエンドは自動でデプロイが始まります**（数分かかることがあります）。

#### 2) フロントをビルド（= 公開用ファイルを作る）

フロントは「どの API にアクセスするか」を決めてからビルドします。
本番 API は Render の URL なので、`VITE_API_BASE` にそれを入れます。

```bash
cd /Users/takuya/Desktop/xhf-app
VITE_API_BASE=https://xhf-app-tt.onrender.com/api npm run build --workspace web
```

#### 3) Cloudflare Pages にアップロード（= 画面の更新）

```bash
cd /Users/takuya/Desktop/xhf-app
npx wrangler pages deploy apps/web/dist --project-name xhf-app-tt --branch main
```

#### 4) 動作確認（最低これだけ）

1. バックエンドが生きているか確認:
   ```bash
   curl https://xhf-app-tt.onrender.com/api/healthz
   ```
   `{ "ok": true }` が返ればOK。
2. 画面を開いてログインしてみる:
   - `https://xhf-app-tt.pages.dev`

#### 困ったとき（よくある原因）

- `wrangler` がログイン切れ: `npx wrangler login` を実行してからやり直す
- 画面は更新されたのに API が失敗する:
  - `VITE_API_BASE` が間違っている（`.../api` まで含める）
  - Render のデプロイがまだ終わっていない（少し待って再確認）

---

1. **バックエンド (Render Web Service)**

   - Root Directory: `apps/api`
   - Environment: `Docker`
   - Environment Variables:
     - `GIGYA_BASE=https://accounts.eu1.gigya.com`
     - `GIGYA_API_KEY=...`
     - `XARVIO_TOKEN_API_URL=https://fm-api.xarvio.com/api/users/tokens`
     - `XARVIO_GRAPHQL_ENDPOINT=https://fm-api.xarvio.com/api/graphql/data`
     - `PREF_CITY_ENABLED=false` (pref/city データを使わない場合)
   - Deploy して URL を控える (例: `https://xhf-app-tt.onrender.com`)

2. **フロントエンド (Cloudflare Pages + Wrangler)**

   - ビルド時に API の向き先を指定してデプロイする（この構成では Pages 側で `/api` をプロキシしないため、フロントは `VITE_API_BASE` に指定した API に直接アクセスします）
   ```bash
   cd /Users/takuya/Desktop/xhf-app
   VITE_API_BASE=https://xhf-app-tt.onrender.com/api npm run build --workspace web
   # 本番ドメイン (xhf-app-tt.pages.dev) に反映する場合
   npx wrangler pages deploy apps/web/dist --project-name xhf-app-tt --branch main
   ```

デプロイ後の動作確認:

1. `curl https://xhf-app-tt.onrender.com/api/healthz` で `{ "ok": true }` が返ることを確認。
2. `https://xhf-app-tt.pages.dev` でログインし、画面操作で API 呼び出しが正常に動くことを確認（通信先は `VITE_API_BASE` で指定した Render の `.../api`）。

キャッシュの削除
curl -X POST https://xhf-app-tt.onrender.com/api/cache/graphql/clear

---
## デプロイ運用方針

このプロジェクトの本番反映は **Render + Cloudflare Pages（GitHub 連携）** を前提にします。Firebase / Cloud Run は使用しません。
