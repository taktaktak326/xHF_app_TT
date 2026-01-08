| 役割 | コマンド | 実行ディレクトリ | 備考 |
|---|---|---|---|
| FastAPI（バックエンド） | uvicorn main:app --reload --port 8080 | xhf-app/apps/api | Python の API サーバ |
| Vite（フロントエンド） | npm run dev | xhf-app/apps/web | React の開発サーバ |
※ Vite のポートは空き状況で 5173 / 5174…に変わることがあります（表示された URL を使用）。


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
