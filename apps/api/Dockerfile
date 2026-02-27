FROM node:20-bookworm-slim AS web_builder

WORKDIR /src

COPY package.json package-lock.json ./
COPY pref_city_p5.topo.json.gz ./pref_city_p5.topo.json.gz
COPY apps/web ./apps/web

RUN npm ci
RUN npm run build --workspace web


FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

EXPOSE 8080

WORKDIR /app

RUN apt-get update \
  && apt-get install --no-install-recommends -y build-essential \
  && rm -rf /var/lib/apt/lists/*

COPY apps/api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY apps/api/ .
COPY --from=web_builder /src/apps/web/dist ./web_dist

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
