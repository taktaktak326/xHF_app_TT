# AIエージェント用：APIリクエストフロー設計（ログイン→農場選択→質問応答）

このファイルは、**xhf-app の API / 外部 API を使って**、AIエージェントが
1) ログインし、2) 農場（farm）を選び、3) ユーザーの質問内容に応じて必要なデータを取得して回答する
ための「実装に落とせる構成」をまとめたものです。

前提として、API の payload/URL は `docs/api-architecture.md` にあります（本ファイルでは「どの順で・どの条件で」呼ぶかに焦点を置きます）。

---

## 1. エージェントの状態（State）

最低限、以下の state をセッションに保持します。

```py
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

@dataclass
class AgentState:
    login_token: Optional[str] = None
    api_token: Optional[str] = None
    farms: Optional[List[Dict[str, Any]]] = None  # FarmsOverview の data.farms
    selected_farm_uuids: Optional[List[str]] = None
    selected_field_uuid: Optional[str] = None  # 天気/衛星などの代表圃場
    language_code: str = "ja"
    country_code: str = "JP"
```

---

## 2. 推奨フロー（最短）

### 2.1 ログイン（Gigya → Xarvio token）
1) Gigya: `accounts.login` → `login_token` + 4値
2) Xarvio: `/api/users/tokens` → `api_token`（DF_TOKEN）

### 2.2 農場一覧取得（FarmsOverview）
3) Xarvio GraphQL: `FarmsOverview` で farms を取得し、エージェントに選択肢を提示

### 2.3 農場選択（1つ or 複数）
4) ユーザーに farm を選ばせる（曖昧なら name 部分一致で候補提示）

### 2.4 以降は質問に応じて取得
5) 質問内容→Intent を分類し、必要な GraphQL/REST を叩いて回答

---

## 3. Intent ルーティング（質問→どのAPIを叩くか）

エージェントはユーザーの質問を以下の intent に分類し、必要な API を呼び出します。

| Intent | 例 | 必要なデータ | 主に使う API |
|---|---|---|---|
| `list_farms` | 「農場一覧」 | farms | GraphQL `FarmsOverview` |
| `select_farm` | 「〇〇農場を選択」 | selected_farm_uuids | 既存 farms から決定 |
| `fields_overview` | 「圃場一覧」「作期一覧」 | fields + cropSeasons +（必要なら tasks） | GraphQL `CombinedDataBase` / `CombinedFieldData`（統合） |
| `tasks` | 「散布予定」「タスク一覧」 | sprayingsV2 等 | GraphQL `CombinedFieldData`（統合） |
| `ndvi` | 「NDVI推移」 | biomassAnalysisNdvi | GraphQL `Biomass` |
| `lai` | 「LAI推移」 | biomassAnalysis（REST） | REST `agronomic-index-analysis/biomass-analysis` |
| `weather` | 「天気」「散布適性」 | weatherV2 / sprayWeather | GraphQL weather 系 |
| `satellite_layers` | 「衛星画像」「レイヤ一覧」 | fieldDataLayers | GraphQL `FieldDataLayerImages` |
| `satellite_image` | 「この画像を見せて」「画像URLの中身を取得して」 | image bytes | REST/GET `imageUrl`（Cookie付き） |
| `field_notes` | 「圃場メモ」「添付」 | fieldNotes | GraphQL `FieldNotesByFarms` |
| `download_attachment` | 「この添付を落として」 | url | 添付URLへ GET（必要なら proxy） |
| `download_attachments_bulk` | 「添付をまとめて取得」 | attachments list | GraphQL `FieldNotesByFarms` +（複数GET→ローカルZIP化） |
| `masterdata` | 「作物/品種候補」「耕起体系」 | crops/varieties/tillage | REST `md2/*` 等 |
| `crop_protection_products_bulk` | 「散布で使える資材候補（複数作物）」 | productsV2 | GraphQL `CropProtectionTaskCreationProducts` を crop ごとに実行 |
| `cross_farm_search` | 「横断検索」 | OpenSearch 結果 | REST `cross-farm-dashboard/_search` |
| `insights_risks` | 「リスク」「推奨」「ウィンドウ」 | insights/risk | GraphQL `CombinedDataInsights` / `CombinedFieldData` |
| `growth_stage_predictions` | 「生育ステージ予測」 | predictions | GraphQL `CombinedDataPredictions` |

---

## 3.1 Xarvio から「情報取得」するAPIの網羅（GraphQL / REST）

ユーザー要望の「全て」は、**ザルビオ（xarvio Field Manager）から情報を直接取得する** API のみを対象にします。
そのため、以下はスコープ外です。

- Gigya ログイン（認証）
- Xarvio トークン発行（認証）
- 更新/作成系（例: 散布予定日の更新、crop-seasons 作成）
- OpenStreetMap など xarvio 以外の外部リソース

### GraphQL（`XARVIO_GRAPHQL_ENDPOINT`）

このアプリが参照する主な `operationName` は以下です（クエリ文字列は `apps/api/graphql/queries.py`）。

```text
FarmsOverview
FieldsByFarm
FieldDataLayerImages
Biomass
FieldNotesByFarms
WeatherHistoricForecastDaily
WeatherClimatologyDaily
SprayWeather
WeatherHistoricForecastHourly
CropProtectionTaskCreationProducts
CombinedDataBase
CombinedDataInsights
CombinedDataPredictions
CombinedFieldData
```

### REST（`https://fm-api.xarvio.com/api/...`）

このアプリが「情報取得」に使う REST は以下です。

```text
GET  /api/agronomic-index-analysis/biomass-analysis
GET  /api/md2/crops
GET  /api/md2/varieties
GET  /api/md2/tillage-systems
GET  /api/master-data/partners/tillages
POST /api/cross-farm-dashboard/_search
GET  <fieldDataLayers magnitudes[].imageUrl>   # Cookie付き
GET  <fieldNotes attachments[].url>           # URLはxarvio配下とは限らないが、添付取得として扱う
```

---

## 4. 実装スケルトン（Python / 直叩き）

`docs/api-architecture.md` の「コピペして動くサンプル」をベースに、以下の関数を足すと実運用しやすくなります。

### 4.0 前提（共通の API クライアント）

このファイル内のコード例は、以下が既に用意されている前提です（`docs/api-architecture.md` の Python サンプルをそのまま使えます）。

```py
# 例: docs/api-architecture.md のサンプルを `direct_xarvio.py` として保存しておく
from direct_xarvio import (
    QUERIES,  # GraphQL query strings dict
    call_xarvio_graphql,
    httpx,
)
```

### 4.1 farms を読み込み、farm を選ぶ
```py
def load_farms(state: AgentState) -> None:
    farms = call_xarvio_graphql(
        operation_name="FarmsOverview",
        query=QUERIES["FARMS_OVERVIEW"],
        variables={},
        login_token=state.login_token,
        api_token=state.api_token,
    )
    items = (farms.get("data") or {}).get("farms") or []
    state.farms = [f for f in items if isinstance(f, dict)]


def select_farms_by_name(state: AgentState, name_query: str) -> List[dict]:
    if not state.farms:
        raise RuntimeError("farms not loaded")
    q = (name_query or "").strip().lower()
    hits = []
    for f in state.farms:
        name = str(f.get("name") or "").lower()
        if q and q in name:
            hits.append(f)
    return hits
```

### 4.2 統合データ取得（複数 farm を前提）
「圃場一覧」「作期」「タスク」「リスク」をまとめて回答したい場合は、`Combined*` をまとめて叩く構成が扱いやすいです。

```py
def fetch_combined_base(state: AgentState) -> dict:
    return call_xarvio_graphql(
        operation_name="CombinedDataBase",
        query=QUERIES["COMBINED_DATA_BASE"],
        variables={
            "farmUuids": state.selected_farm_uuids,
            "languageCode": state.language_code,
            "cropSeasonLifeCycleStates": ["ACTIVE", "PLANNED"],
            "withBoundary": True,
        },
        login_token=state.login_token,
        api_token=state.api_token,
    )


def fetch_combined_tasks(state: AgentState, *, from_date: str, till_date: str) -> dict:
    return call_xarvio_graphql(
        operation_name="CombinedFieldData",
        query=QUERIES["COMBINED_FIELD_DATA_TASKS"],
        variables={
            "farmUuids": state.selected_farm_uuids,
            "languageCode": state.language_code,
            "cropSeasonLifeCycleStates": ["ACTIVE", "PLANNED"],
            "fromDate": from_date,
            "tillDate": till_date,
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
        login_token=state.login_token,
        api_token=state.api_token,
    )
```

### 4.2.1 Insights（recommendations / risks）
```py
def fetch_combined_insights(state: AgentState, *, from_date: str, till_date: str) -> dict:
    return call_xarvio_graphql(
        operation_name="CombinedDataInsights",
        query=QUERIES["COMBINED_DATA_INSIGHTS"],
        variables={
            "farmUuids": state.selected_farm_uuids,
            "fromDate": from_date,
            "tillDate": till_date,
            "cropSeasonLifeCycleStates": ["ACTIVE", "PLANNED"],
            "withrisk": True,
        },
        login_token=state.login_token,
        api_token=state.api_token,
    )
```

### 4.2.2 Predictions（growth stage predictions）
```py
def fetch_combined_predictions(state: AgentState) -> dict:
    return call_xarvio_graphql(
        operation_name="CombinedDataPredictions",
        query=QUERIES["COMBINED_DATA_PREDICTIONS"],
        variables={
            "farmUuids": state.selected_farm_uuids,
            "languageCode": state.language_code,
            "countryCode": state.country_code,
            "cropSeasonLifeCycleStates": ["ACTIVE", "PLANNED"],
        },
        login_token=state.login_token,
        api_token=state.api_token,
    )
```

### 4.3 NDVI / LAI
```py
def fetch_ndvi(state: AgentState, *, crop_season_uuids: list[str], from_iso: str, till_iso: str) -> dict:
    return call_xarvio_graphql(
        operation_name="Biomass",
        query=QUERIES["BIOMASS_NDVI"],
        variables={"uuids": crop_season_uuids, "from": from_iso, "till": till_iso},
        login_token=state.login_token,
        api_token=state.api_token,
    )


def fetch_lai_rest(state: AgentState, *, crop_season_uuids: list[str], from_iso: str, till_iso: str) -> list[dict]:
    url = "https://fm-api.xarvio.com/api/agronomic-index-analysis/biomass-analysis"
    headers = {"Cookie": f"LOGIN_TOKEN={state.login_token}; DF_TOKEN={state.api_token}"}
    params = {
        "cropSeasonUuid": ",".join(crop_season_uuids),
        "fromDate": from_iso,
        "tillDate": till_iso,
    }
    with httpx.Client(timeout=30) as c:
        r = c.get(url, params=params, headers=headers)
        r.raise_for_status()
        data = r.json()
    return data if isinstance(data, list) else []
```

### 4.4 天気（field_uuid が必要）
```py
def fetch_weather_daily(state: AgentState, *, field_uuid: str, from_date: str, till_date: str) -> dict:
    return call_xarvio_graphql(
        operation_name="WeatherHistoricForecastDaily",
        query=QUERIES["WEATHER_HISTORIC_FORECAST_DAILY"],
        variables={"fieldUuid": field_uuid, "fromDate": from_date, "tillDate": till_date},
        login_token=state.login_token,
        api_token=state.api_token,
    )
```

### 4.4.1 画像バイト列の取得（衛星画像など）
`fieldDataLayers.magnitudes[].imageUrl` の URL は、通常 Cookie（LOGIN_TOKEN/DF_TOKEN）が必要です。

```py
def fetch_private_image_bytes(state: AgentState, *, image_url: str) -> bytes:
    headers = {"Cookie": f"LOGIN_TOKEN={state.login_token}; DF_TOKEN={state.api_token}"}
    with httpx.Client(timeout=30, follow_redirects=True) as c:
        r = c.get(image_url, headers=headers)
        r.raise_for_status()
        return r.content
```

### 4.6 散布資材候補（複数 cropUuid をまとめて取得）
GraphQL は `cropUuid` が単数なので、`crop_uuids` を渡されたらループ/並列実行します（アプリの `/crop-protection-products/bulk` 相当）。

```py
def fetch_crop_protection_products_bulk(
    state: AgentState,
    *,
    farm_uuids: list[str],
    country_uuid: str,
    crop_uuids: list[str],
    task_type_code: str = "FIELDTREATMENT",
) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = {}
    for crop_uuid in crop_uuids:
        res = call_xarvio_graphql(
            operation_name="CropProtectionTaskCreationProducts",
            query=QUERIES["CROP_PROTECTION_TASK_CREATION_PRODUCTS"],
            variables={
                "farmUuids": farm_uuids,
                "countryUuid": country_uuid,
                "cropUuid": crop_uuid,
                "taskTypeCode": task_type_code,
            },
            login_token=state.login_token,
            api_token=state.api_token,
        )
        items = (((res.get("data") or {}).get("productsV2")) or [])
        out[crop_uuid] = items if isinstance(items, list) else []
    return out
```

### 4.7 添付をまとめて取得（ローカルでZIP化）
アプリの `/attachments/zip` はバックエンド機能なので、直叩き運用では「添付URLをGETしてローカルZIP化」が実装しやすいです。

```py
import io
import zipfile

def download_attachments_as_zip(state: AgentState, *, attachments: list[dict], zip_name: str = "attachments.zip") -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for idx, att in enumerate(attachments):
            url = att.get("url")
            if not url:
                continue
            filename = att.get("fileName") or f"attachment_{idx}"
            with httpx.Client(timeout=30, follow_redirects=True) as c:
                r = c.get(url)
                r.raise_for_status()
                z.writestr(filename, r.content)
    buf.seek(0)
    return buf.read()
```

---

## 4.A 参考：更新系（スコープ外）

本ドキュメントの主眼は「情報取得」ですが、参考として更新系の代表例だけ記載します（必要な場合にのみ実装）。

散布タスク更新（予定日変更など）:
```text
POST https://fm-api.xarvio.com/api/tasks/v2/sprayings/{task_uuid}
```

## 5. 農場・圃場の「選び方」ポリシー

AIエージェントは回答精度のため、選択が曖昧なときは質問で解決します。

- farm が未選択:
  - 「どの農場ですか？」→ farm候補を 5 件以内で提示
- farm が複数選択:
  - 質問が「横断」ならそのまま
  - 「特定の圃場」なら field_uuid を追加で確認
- weather / satellite は field_uuid が必須:
  - 代表圃場（最大面積 or 名前一致）を提案して確認

---

## 6. 回答生成のための取得（Retrieval Plan）

エージェントは「先に必要最小限を取得→不足なら追加取得」の順で動かすと、コストと遅延を抑えられます。

例:
- 「〇〇農場の散布予定を教えて」:
  1) farms 未ロードなら FarmsOverview
  2) farm 選択（uuid）
  3) CombinedDataBase（field/cropSeason の名前解決）
  4) CombinedFieldData（sprayingsV2 を含める）
  5) タスクを整形して回答（日時は JST/ISO の扱いを明示）

- 「リスクが高い圃場は？」:
  1) farms / farm選択
  2) CombinedDataInsights（withrisk=true）
  3) 必要なら CombinedDataBase（圃場名・作物名の解決）
  4) リスクを集計して回答（対象 farm を明記）

- 「生育ステージ予測は？」:
  1) farms / farm選択
  2) CombinedDataPredictions
  3) 必要なら CombinedDataBase（作物・作期の補足）

---

## 7. 運用上の注意（最低限）

- 認証情報はログに出さない（`login_token`/`api_token` のマスク）
- 429/5xx/timeout は指数バックオフでリトライ（特に GraphQL）
- 取得データは「どのfarm/fieldを対象にしたか」を回答に明記
- 直叩き運用が難しい場合は、エージェントが `apps/api` の `/api/*` を叩く構成に切り替える（CORS/秘匿/監査の観点で安全）

---

## 8. 網羅性について（このアプリで使う API を全部取得できるか）

本ファイルは、アプリで使われている主要な「情報取得」（farms/fields/combined/tasks/weather/ndvi/lai/satellite/notes/masterdata/cross-farm）を intent でカバーします。

ただし、ユーザー要望に合わせて「ザルビオから情報取得する GraphQL/REST の網羅」を優先し、以下は必須扱いにしていません（必要なら追加）。

- `/warmup`, `/warmup/status`（pref/city などの準備。UI体験向け）
- `/cache/graphql/*`（デバッグ用）
- `/healthz`（監視用）

---

## 9. 社内運用リリース判定チェックリスト（100 farm × 300圃場）

### 9.1 判定ルール
- `Go`: P0/P1 が全て `OK`、P2 は未完でも期限付きで許容
- `No-Go`: P0/P1 に 1 件でも `NG` がある

### 9.2 P0（必須: データ完全性）
- [ ] 大規模選択時（既定20 farm以上）に `requireComplete=true` が有効
- [ ] `failed_chunks > 0` のとき成功扱いしない（206を完了扱いしない）
- [ ] 失敗 farm UUID をレスポンス/ログで特定できる
- [ ] `withBoundarySvg=false` など payload 縮小が有効
- [ ] 100 farm × 300圃場の実負荷で「欠損0件」を連続10回達成

### 9.3 P1（必須: 運用・障害対応）
- [ ] retry 回数、失敗 chunk 数、最終成否を構造化ログに出力
- [ ] タイムアウト/429/5xx 時の自動再試行が有効
- [ ] API障害時にユーザーへ「再試行中/未完了」をUI表示できる
- [ ] 失敗時の再実行手順（runbook）がある
- [ ] トークン等の機密情報をログに出力しない

### 9.4 P2（推奨: 品質・保守）
- [ ] `npm run lint:web` をPRゲートに設定
- [ ] `npm run lint:web:strict` を定期実行し、警告を継続削減
- [ ] hooks依存警告の優先返済（`react-hooks/exhaustive-deps`）
- [ ] 重要フローのE2E/統合テスト（farm選択→圃場取得→表示）

### 9.5 リリース判定テンプレート
```text
対象バージョン:
判定日:
判定者:

P0: OK / NG
P1: OK / NG
P2: OK / 条件付きOK / NG

No-Go理由（あれば）:
リリース後の監視項目:
- 欠損率
- 再試行回数
- 取得完了までの時間(P95)
```
