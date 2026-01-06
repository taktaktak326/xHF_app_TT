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

1. **バックエンド (Render Web Service)**

   - リポジトリを接続して新しい Web Service を作成
   - Root Directory: `apps/api`
   - Environment: `Docker`
   - Environment Variables:
     - `GIGYA_BASE=https://accounts.eu1.gigya.com`
     - `GIGYA_API_KEY=...`
     - `XARVIO_TOKEN_API_URL=https://fm-api.xarvio.com/api/users/tokens`
     - `XARVIO_GRAPHQL_ENDPOINT=https://fm-api.xarvio.com/api/graphql/data`
     - `PREF_CITY_ENABLED=false` (pref/city データを使わない場合)
   - Deploy して URL を控える (例: `https://xhf-api.onrender.com`)

2. **フロントエンド (Cloudflare Pages)**

   - リポジトリを接続して新しい Pages を作成
   - Build command: `npm run build --workspace=web`
   - Build output directory: `apps/web/dist`
   - Environment Variables:
     - `VITE_API_BASE=https://xhf-api.onrender.com/api`

デプロイ後の動作確認:

1. `curl https://xhf-api.onrender.com/api/healthz` で `{ "ok": true }` が返ることを確認。
2. Cloudflare Pages の URL でログインし、`/api/*` の機能が正常に動くか確認。

キャッシュの削除
curl -X POST https://xhf-api.onrender.com/api/cache/graphql/clear
