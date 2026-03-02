# xhf-app API 構成ドキュメント

このドキュメントは、xhf-app（`apps/web` + `apps/api`）が発行する API リクエストを「フロント → バックエンド → 外部（Gigya / Xarvio）」の観点で整理し、URL とペイロード例をコードブロックでまとめたものです。

---

## 全体構成（通信の向き）

```
Browser (apps/web)
  └─(HTTP)→ Backend API (apps/api, FastAPI, /api/*)
              ├─(HTTP)→ Gigya API (accounts.login)
              ├─(HTTP)→ Xarvio Token API (/api/users/tokens)
              ├─(HTTP)→ Xarvio GraphQL API (/api/graphql/data)
              └─(HTTP)→ Xarvio REST APIs (tasks, master-data, biomass, crop-seasons, etc.)
```

---

## ベース URL / 環境変数

### フロント（Vite）
- API ベース（優先順）: `VITE_API_BASE` → DEV では `http://localhost:8080/api` → PROD では `/api`
  - 実装: `apps/web/src/utils/apiBase.ts`
- DEV のプロキシ: `/api` → `http://127.0.0.1:8080`（例）
  - 実装: `apps/web/vite.config.ts`

### バックエンド（FastAPI）
- Gigya:
  - `GIGYA_BASE`（例: `https://accounts.eu1.gigya.com`）
  - `GIGYA_API_KEY`
  - 実装: `apps/api/settings.py`, `apps/api/services/gigya.py`
- Xarvio:
  - `XARVIO_TOKEN_API_URL`（例: `https://fm-api.xarvio.com/api/users/tokens`）
  - `XARVIO_GRAPHQL_ENDPOINT`（省略時は既定 `https://fm-api.xarvio.com/api/graphql/data`）
  - 実装: `apps/api/settings.py`, `apps/api/services/xarvio.py`

---

## 認証トークン（このアプリでの扱い）

- `login_token`（Gigya ログイン由来）: Xarvio API へは Cookie `LOGIN_TOKEN=<login_token>` として送る
- `api_token`（Xarvio の DF_TOKEN）: Xarvio API へは Cookie `DF_TOKEN=<api_token>` として送る
- フロントは `POST /api/login-and-token` の戻り値を `sessionStorage` に保存（`apps/web/src/context/AuthContext.tsx`）

---

## Python で直接呼ぶ外部 API（Gigya / Xarvio）

このアプリは通常 `apps/api`（FastAPI）を経由しますが、**Python から直接 Gigya / Xarvio を呼ぶ**場合は以下の URL / ペイロード（Python dict）を使います。

### 0) 共通（httpx）
```py
"""
✅ コピペして動くサンプル（Python 直叩き）

使い方:
1) `pip install httpx`
2) 環境変数をセット:
   - GIGYA_API_KEY=...
   - XARVIO_EMAIL=...
   - XARVIO_PASSWORD=...
   （必要なら GIGYA_BASE / XARVIO_* も上書き可）
3) このブロックを `direct_xarvio.py` などに保存して `python direct_xarvio.py`
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import httpx

GIGYA_BASE = os.getenv("GIGYA_BASE", "https://accounts.eu1.gigya.com")
GIGYA_API_KEY = os.getenv("GIGYA_API_KEY", "")

XARVIO_TOKEN_API_URL = os.getenv("XARVIO_TOKEN_API_URL", "https://fm-api.xarvio.com/api/users/tokens")
XARVIO_GRAPHQL_ENDPOINT = os.getenv("XARVIO_GRAPHQL_ENDPOINT", "https://fm-api.xarvio.com/api/graphql/data")

EMAIL = os.getenv("XARVIO_EMAIL", "")
PASSWORD = os.getenv("XARVIO_PASSWORD", "")


def _maybe_import_repo_queries() -> Dict[str, str]:
    """
    repo 直下 or その配下で実行した場合、apps/api/graphql/queries.py からクエリを import して利用する。
    repo 外で実行しても動くように、失敗したら最小クエリを内蔵して返す。
    """
    # Try locate repo root by walking up from cwd.
    root: Optional[Path] = None
    cur = Path.cwd().resolve()
    for p in [cur, *cur.parents]:
        if (p / "apps" / "api" / "graphql" / "queries.py").exists():
            root = p
            break

    if root is not None:
        sys.path.insert(0, str(root / "apps" / "api"))
        try:
            from graphql import queries as q  # type: ignore
            # Extract string constants we care about.
            out: Dict[str, str] = {}
            for name in [
                "FARMS_OVERVIEW",
                "FIELDS_BY_FARM",
                "FIELD_DATA_LAYER_IMAGES",
                "BIOMASS_NDVI",
                "FIELD_NOTES_BY_FARMS",
                "WEATHER_HISTORIC_FORECAST_DAILY",
                "WEATHER_CLIMATOLOGY_DAILY",
                "SPRAY_WEATHER",
                "WEATHER_HISTORIC_FORECAST_HOURLY",
                "CROP_PROTECTION_TASK_CREATION_PRODUCTS",
            ]:
                val = getattr(q, name, None)
                if isinstance(val, str) and val.strip():
                    out[name] = val
            if out:
                return out
        except Exception:
            pass

    # Fallback: minimal set (enough to smoke-test).
    return {
        "FARMS_OVERVIEW": """
query FarmsOverview {
  farms: farmsV2(uuids: []) {
    uuid
    name
    latitude
    longitude
    owner { firstName lastName email }
    currentUserPermission { access }
  }
}
""".strip()
    }


QUERIES = _maybe_import_repo_queries()


def gigya_login(*, email: str, password: str) -> Dict[str, Any]:
    url = f"{GIGYA_BASE}/accounts.login"
    data = {
        "apiKey": GIGYA_API_KEY,
        "loginID": email,
        "password": password,
        "format": "json",
    }
    with httpx.Client(timeout=15) as c:
        r = c.post(url, data=data)
        r.raise_for_status()
        j = r.json()
    if (j.get("errorCode") or 0) != 0:
        raise RuntimeError({"gigya_errorCode": j.get("errorCode"), "gigya_errorMessage": j.get("errorMessage")})

    session = j.get("sessionInfo", {}) or {}
    return {
        "login_token": session.get("cookieValue"),
        "gigya_uuid": j.get("UID"),
        "gigya_uuid_signature": j.get("UIDSignature"),
        "gigya_signature_timestamp": j.get("signatureTimestamp"),
        "raw": {"callId": j.get("callId"), "errorCode": j.get("errorCode")},
    }


def xarvio_issue_token(*, login_token: str, gigya_uuid: str, gigya_uuid_signature: str, gigya_signature_timestamp: str) -> str:
    url = XARVIO_TOKEN_API_URL
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Cookie": f"LOGIN_TOKEN={login_token}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
    }
    payload = {
        "gigyaUuid": gigya_uuid,
        "gigyaUuidSignature": gigya_uuid_signature,
        "gigyaSignatureTimestamp": gigya_signature_timestamp,
    }
    with httpx.Client(timeout=20) as c:
        r = c.post(url, json=payload, headers=headers)
        r.raise_for_status()
        j = r.json()
    token = (j or {}).get("token")
    if not token:
        raise RuntimeError({"reason": "no token in response", "raw": j})
    return token


def call_xarvio_graphql(*, operation_name: str, query: str, variables: dict, login_token: str, api_token: str) -> dict:
    url = XARVIO_GRAPHQL_ENDPOINT
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
        "User-Agent": "xhf-app/1.0",
    }
    payload = {"operationName": operation_name, "query": query, "variables": variables}
    timeout = httpx.Timeout(connect=10.0, read=120.0, write=120.0, pool=10.0)
    with httpx.Client(timeout=timeout, follow_redirects=True) as c:
        r = c.post(url, json=payload, headers=headers)
        r.raise_for_status()
        return r.json()


def require_env() -> None:
    missing = []
    if not GIGYA_API_KEY:
        missing.append("GIGYA_API_KEY")
    if not EMAIL:
        missing.append("XARVIO_EMAIL")
    if not PASSWORD:
        missing.append("XARVIO_PASSWORD")
    if missing:
        raise SystemExit(f"Missing env vars: {', '.join(missing)}")


if __name__ == "__main__":
    require_env()
    four = gigya_login(email=EMAIL, password=PASSWORD)
    login_token = four["login_token"]
    api_token = xarvio_issue_token(
        login_token=login_token,
        gigya_uuid=four["gigya_uuid"],
        gigya_uuid_signature=four["gigya_uuid_signature"],
        gigya_signature_timestamp=four["gigya_signature_timestamp"],
    )
    farms = call_xarvio_graphql(
        operation_name="FarmsOverview",
        query=QUERIES["FARMS_OVERVIEW"],
        variables={},
        login_token=login_token,
        api_token=api_token,
    )
    print("FarmsOverview ok:", "data" in farms, "farms:", len((farms.get("data") or {}).get("farms", []) or []))
```

### 1) Gigya ログイン（メール/パスワード → login_token + 4値）
```py
url = f"{GIGYA_BASE}/accounts.login"
data = {
    "apiKey": GIGYA_API_KEY,
    "loginID": EMAIL,
    "password": PASSWORD,
    "format": "json",
}

with httpx.Client(timeout=15) as c:
    r = c.post(url, data=data)
    r.raise_for_status()
    j = r.json()

login_token = (j.get("sessionInfo") or {}).get("cookieValue")
gigya_uuid = j.get("UID")
gigya_uuid_signature = j.get("UIDSignature")
gigya_signature_timestamp = j.get("signatureTimestamp")
```

### 2) Xarvio トークン発行（Gigya 4値 → api_token / DF_TOKEN）
```py
url = XARVIO_TOKEN_API_URL
headers = {
    "Accept": "application/json, text/plain, */*",
    "Content-Type": "application/json",
    "Cookie": f"LOGIN_TOKEN={login_token}",
    "Origin": "https://fm.xarvio.com",
    "Referer": "https://fm.xarvio.com/",
}
payload = {
    "gigyaUuid": gigya_uuid,
    "gigyaUuidSignature": gigya_uuid_signature,
    "gigyaSignatureTimestamp": gigya_signature_timestamp,
}

with httpx.Client(timeout=20) as c:
    r = c.post(url, json=payload, headers=headers)
    r.raise_for_status()
    api_token = (r.json() or {}).get("token")
```

### 3) Xarvio GraphQL（共通呼び出し）

クエリ文字列は `apps/api/graphql/queries.py` の各定数（例: `FARMS_OVERVIEW` など）をそのまま使います。

```py
def call_xarvio_graphql(*, operation_name: str, query: str, variables: dict, login_token: str, api_token: str):
    url = XARVIO_GRAPHQL_ENDPOINT
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
        "User-Agent": "xhf-app/1.0",
    }
    payload = {"operationName": operation_name, "query": query, "variables": variables}
    with httpx.Client(timeout=60) as c:
        r = c.post(url, json=payload, headers=headers)
        r.raise_for_status()
        return r.json()
```

#### FarmsOverview（農場一覧）
```py
url = XARVIO_GRAPHQL_ENDPOINT
payload = {
    "operationName": "FarmsOverview",
    "query": FARMS_OVERVIEW,
    "variables": {},
}

data = call_xarvio_graphql(
    operation_name=payload["operationName"],
    query=payload["query"],
    variables=payload["variables"],
    login_token=login_token,
    api_token=api_token,
)
```

#### FieldsByFarm（農場 → 圃場）
```py
url = XARVIO_GRAPHQL_ENDPOINT
payload = {
    "operationName": "FieldsByFarm",
    "query": FIELDS_BY_FARM,
    "variables": {"farmUuid": "<FARM_UUID>"},
}
```

#### FieldDataLayerImages（衛星レイヤ）
```py
url = XARVIO_GRAPHQL_ENDPOINT
payload = {
    "operationName": "FieldDataLayerImages",
    "query": FIELD_DATA_LAYER_IMAGES,
    "variables": {
        "fieldUuid": "<FIELD_UUID>",
        "types": ["BIOMASS_NDVI", "BIOMASS_SINGLE_IMAGE_LAI"],
    },
}
```

#### Biomass（NDVI）
```py
url = XARVIO_GRAPHQL_ENDPOINT
payload = {
    "operationName": "Biomass",
    "query": BIOMASS_NDVI,
    "variables": {
        "uuids": ["<CROP_SEASON_UUID_1>", "<CROP_SEASON_UUID_2>"],
        "from": "2025-01-01T00:00:00Z",
        "till": "2026-02-14T23:59:59.999Z",
    },
}
```

#### FieldNotesByFarms（圃場メモ）
```py
url = XARVIO_GRAPHQL_ENDPOINT
payload = {
    "operationName": "FieldNotesByFarms",
    "query": FIELD_NOTES_BY_FARMS,
    "variables": {"farmUuids": ["<FARM_UUID_1>", "<FARM_UUID_2>"]},
}
```

#### Weather（daily / climatology / spray / hourly）
```py
url = XARVIO_GRAPHQL_ENDPOINT
payload_daily = {
    "operationName": "WeatherHistoricForecastDaily",
    "query": WEATHER_HISTORIC_FORECAST_DAILY,
    "variables": {"fieldUuid": "<FIELD_UUID>", "fromDate": "2025-01-01", "tillDate": "2026-12-31"},
}
payload_climatology = {
    "operationName": "WeatherClimatologyDaily",
    "query": WEATHER_CLIMATOLOGY_DAILY,
    "variables": {"fieldUuid": "<FIELD_UUID>", "fromDate": "2025-01-01", "tillDate": "2026-12-31"},
}
payload_spray = {
    "operationName": "SprayWeather",
    "query": SPRAY_WEATHER,
    "variables": {"fieldUuid": "<FIELD_UUID>", "fromDate": "2025-01-01", "tillDate": "2026-12-31"},
}
payload_hourly = {
    "operationName": "WeatherHistoricForecastHourly",
    "query": WEATHER_HISTORIC_FORECAST_HOURLY,
    "variables": {"fieldUuid": "<FIELD_UUID>", "fromDate": "2025-01-01", "tillDate": "2026-12-31"},
}
```

#### CropProtectionTaskCreationProducts（散布資材候補）
```py
url = XARVIO_GRAPHQL_ENDPOINT
payload = {
    "operationName": "CropProtectionTaskCreationProducts",
    "query": CROP_PROTECTION_TASK_CREATION_PRODUCTS,
    "variables": {
        "farmUuids": ["<FARM_UUID_1>", "<FARM_UUID_2>"],
        "cropUuid": "<CROP_UUID>",
        "countryUuid": "<COUNTRY_UUID_JP>",
        "taskTypeCode": "FIELDTREATMENT",
    },
}
```

#### Combined（base / insights / predictions / tasks）
```py
url = XARVIO_GRAPHQL_ENDPOINT

payload_base = {
    "operationName": "CombinedDataBase",
    "query": COMBINED_DATA_BASE,
    "variables": {
        "farmUuids": ["<FARM_UUID_1>", "<FARM_UUID_2>"],
        "languageCode": "ja",
        "cropSeasonLifeCycleStates": ["ACTIVE", "PLANNED"],
        "withBoundary": True,
    },
}

payload_insights = {
    "operationName": "CombinedDataInsights",
    "query": COMBINED_DATA_INSIGHTS,
    "variables": {
        "farmUuids": ["<FARM_UUID_1>", "<FARM_UUID_2>"],
        "fromDate": "2026-02-14",
        "tillDate": "2026-03-16",
        "cropSeasonLifeCycleStates": ["ACTIVE", "PLANNED"],
        "withrisk": True,
    },
}

payload_predictions = {
    "operationName": "CombinedDataPredictions",
    "query": COMBINED_DATA_PREDICTIONS,
    "variables": {
        "farmUuids": ["<FARM_UUID_1>", "<FARM_UUID_2>"],
        "languageCode": "ja",
        "countryCode": "JP",
        "cropSeasonLifeCycleStates": ["ACTIVE", "PLANNED"],
    },
}

payload_tasks = {
    "operationName": "CombinedFieldData",
    "query": COMBINED_FIELD_DATA_TASKS,
    "variables": {
        "farmUuids": ["<FARM_UUID_1>", "<FARM_UUID_2>"],
        "languageCode": "ja",
        "cropSeasonLifeCycleStates": ["ACTIVE", "PLANNED"],
        "fromDate": "2026-02-14",
        "tillDate": "2026-03-16",
        "withrisk": True,
        "withCropSeasonsV2": True,
        "withHarvests": True,
        "withCropEstablishments": True,
        "withLandPreparations": True,
        "withDroneFlights": False,
        "withSeedTreatments": True,
        "withSeedBoxTreatments": True,
        "withSmartSprayingTasks": False,
        "withWaterManagementTasks": True,
        "withScoutingTasks": True,
        "withObservations": False,
        "withSprayingsV2": True,
        "withSoilSamplingTasks": False,
        "withBoundary": True,
    },
}
```

### 4) Xarvio REST（このアプリが直接叩くもの）

#### 散布タスク更新（予定日など）
```py
task_uuid = "<TASK_UUID>"
url = f"https://fm-api.xarvio.com/api/tasks/v2/sprayings/{task_uuid}"
headers = {
    "Content-Type": "application/json",
    "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
    "Origin": "https://fm.xarvio.com",
    "Referer": "https://fm.xarvio.com/",
}
payload = {"plannedDate": "2026-02-14T00:00:00Z", "executionDate": None}

with httpx.Client(timeout=30) as c:
    r = c.post(url, json=payload, headers=headers)
    r.raise_for_status()
    data = r.json()
```

#### LAI（biomass-analysis）取得（GET）
```py
url = "https://fm-api.xarvio.com/api/agronomic-index-analysis/biomass-analysis"
headers = {"Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}"}
params = {
    "cropSeasonUuid": ",".join(["<CROP_SEASON_UUID_1>", "<CROP_SEASON_UUID_2>"]),
    "fromDate": "2025-01-01T00:00:00Z",
    "tillDate": "2026-02-14T23:59:59Z",
}

with httpx.Client(timeout=30) as c:
    r = c.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()
```

#### 作物マスタ（md2/crops）
```py
url = "https://fm-api.xarvio.com/api/md2/crops"
headers = {
    "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
    "Origin": "https://fm.xarvio.com",
    "Referer": "https://fm.xarvio.com/",
}
params = {"locale": "JA-JP"}

with httpx.Client(timeout=30) as c:
    r = c.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()
```

#### 品種マスタ（md2/varieties）
```py
url = "https://fm-api.xarvio.com/api/md2/varieties"
headers = {
    "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
    "Origin": "https://fm.xarvio.com",
    "Referer": "https://fm.xarvio.com/",
}
params = {"locale": "JA-JP", "countryCode": "JP", "cropUuid": "<CROP_UUID>"}

with httpx.Client(timeout=30) as c:
    r = c.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()
```

#### パートナー耕起（master-data/partners/tillages）
```py
url = "https://fm-api.xarvio.com/api/master-data/partners/tillages"
headers = {
    "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
    "Origin": "https://fm.xarvio.com",
    "Referer": "https://fm.xarvio.com/",
}
params = {"locale": "JA-JP"}

with httpx.Client(timeout=30) as c:
    r = c.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()
```

#### 耕起体系（md2/tillage-systems）
```py
url = "https://fm-api.xarvio.com/api/md2/tillage-systems"
headers = {
    "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
    "Origin": "https://fm.xarvio.com",
    "Referer": "https://fm.xarvio.com/",
}
params = {"locale": "JA-JP"}

with httpx.Client(timeout=30) as c:
    r = c.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json()
```

#### 作付（farms/v2/crop-seasons）作成（POST）
```py
url = "https://fm-api.xarvio.com/api/farms/v2/crop-seasons"
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
    "Authorization": f"Bearer {api_token}",
    "X-Login-Token": login_token,
    "Origin": "https://fm.xarvio.com",
    "Referer": "https://fm.xarvio.com/",
}
payload = {
    "fieldUuid": "<FIELD_UUID>",
    "cropUuid": "<CROP_UUID>",
    "varietyUuid": "<VARIETY_UUID>",
    "startDate": "2026-02-14",
    "yieldExpectation": 6000,
}

with httpx.Client(timeout=45) as c:
    r = c.post(url, json=payload, headers=headers)
    r.raise_for_status()
    data = r.json()
```

#### Cross Farm Dashboard 検索（POST）
```py
url = "https://fm-api.xarvio.com/api/cross-farm-dashboard/_search"
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
    "X-Login-Token": login_token,
    "Origin": "https://fm.xarvio.com",
    "Referer": "https://fm.xarvio.com/",
    "User-Agent": "xhf-app/1.0",
}
params = {"includeClosedCropSeasons": "true"}  # 必要な場合だけ
payload = {"query": {"match_all": {}}, "size": 10}

with httpx.Client(timeout=60) as c:
    r = c.post(url, params=params, json=payload, headers=headers)
    r.raise_for_status()
    data = r.json()
```

### 5) 画像・添付・任意 URL の取得（GET）

任意 URL（ダウンロード対象）:
```py
url = "https://example.com/file.png"
# payload: None
```

添付 URL（Field Notes の attachment.url など）:
```py
url = "https://fm-api.xarvio.com/.../attachment1.jpg"
# payload: None
```

衛星画像 URL（fieldDataLayers の imageUrl）:
```py
url = "https://fm-api.xarvio.com/.../some-image.png"
headers = {"Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}"}
# payload: None
```

---

## ブラウザが直接叩く外部リソース（/api 以外）

OpenStreetMap タイル（地図表示）:
```text
https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png

# payload: (none)
```
