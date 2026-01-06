# ベースとなるPythonの公式イメージを選択
FROM python:3.11-slim

# 環境変数でポートを指定できるようにする
ENV PORT 8080

# 作業ディレクトリを設定
WORKDIR /app

# 依存関係ファイルをコピーしてインストール
# poetryやpipenvを使っている場合は、その設定に合わせて変更してください
COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードをコピー
COPY . .

# アプリケーションを起動
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "$PORT"]
