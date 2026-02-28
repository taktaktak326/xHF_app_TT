# apps/api/main.py
import asyncio
import json
import re
import math
import random
from datetime import datetime, timedelta, timezone, date
from uuid import uuid4
import io
import zipfile
import os
import threading
import time
from pathlib import Path as FilePath
from typing import Optional, List, Any, Dict
from urllib.parse import urlparse, unquote, quote

import httpx
from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, Path, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, Response, FileResponse
from pydantic import BaseModel

from settings import settings
from schemas import (
    LoginReq,
    FourValues,
    FarmsReq,
    FieldsReq,
    HfrFarmCandidatesReq,
    HfrCsvFieldsReq,
    CombinedFieldDataTasksReq,
    CombinedFieldsReq,  # ★ 追加
    BiomassNdviReq,
    BiomassLaiReq,
    FieldNotesReq,
    WeatherByFieldReq,
    CropProtectionProductsReq,
    CropProtectionProductsBulkReq,
    FieldDataLayersReq,
    FieldDataLayerImageReq,
    SprayingTaskUpdateReq,
    MasterdataCropsReq,
    MasterdataVarietiesReq,
    MasterdataPartnerTillagesReq,
    MasterdataTillageSystemsReq,
    CropSeasonCreateReq,
    CrossFarmDashboardSearchReq,
    HfrSnapshotJobReq,
)
from pydantic import BaseModel
from services.gigya import gigya_login_impl
from services.xarvio import get_api_token_impl
from services.graphql_client import call_graphql
from services.field_location import enrich_field_with_location, get_pref_city_status, start_pref_city_warmup
from services.cache import get_last_response, get_by_operation, clear_cache, save_response
from services import hfr_snapshot_store
from graphql.queries import (
    FARMS_OVERVIEW,
    FIELDS_BY_FARM,
    FIELDS_NAME_SCAN_BY_FARMS,
    COMBINED_DATA_BASE,
    COMBINED_FIELD_DATA_TASKS,
    COMBINED_DATA_INSIGHTS,
    COMBINED_DATA_PREDICTIONS,
    BIOMASS_NDVI,
    FIELD_NOTES_BY_FARMS,
    WEATHER_HISTORIC_FORECAST_DAILY,
    WEATHER_CLIMATOLOGY_DAILY,
    SPRAY_WEATHER,
    WEATHER_HISTORIC_FORECAST_HOURLY,
    CROP_PROTECTION_TASK_CREATION_PRODUCTS,
    FIELD_DATA_LAYER_IMAGES,
    HFR_CSV_FIELDS_DATA,
)
from graphql.payloads import make_payload

# .env 読み込み
load_dotenv()


class AttachmentDownload(BaseModel):
    url: str
    fileName: Optional[str] = None
    farmUuid: Optional[str] = None
    farmName: Optional[str] = None


class AttachmentsZipReq(BaseModel):
    attachments: List[AttachmentDownload]
    zipName: Optional[str] = None


def _filename_from_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        name = parsed.path.split("/")[-1]
        name = unquote(name)
        return name or "attachment"
    except Exception:
        return "attachment"


def _sanitize_zip_name(name: Optional[str]) -> str:
    base = (name or "attachments").strip()
    if not base:
        base = "attachments"
    # allow unicode; just strip characters illegal in filenames on major OS
    safe = base
    for ch in ['\\', '/', ':', '*', '?', '"', '<', '>', '|']:
        safe = safe.replace(ch, '_')
    if not safe.lower().endswith(".zip"):
        safe = f"{safe}.zip"
    return safe


def _content_disposition(zip_name: str) -> str:
    ascii_fallback = zip_name
    try:
        ascii_fallback = zip_name.encode("ascii", "ignore").decode("ascii")
    except Exception:
        ascii_fallback = "attachments.zip"
    if not ascii_fallback:
        ascii_fallback = "attachments.zip"
    encoded = quote(zip_name)
    return f'attachment; filename="{ascii_fallback}"; filename*=UTF-8\'\'{encoded}'

IMAGE_CACHE_TTL_SEC = int(os.getenv("IMAGE_CACHE_TTL", "300"))
_image_cache: Dict[str, Dict[str, Any]] = {}
_image_cache_lock = threading.Lock()

DASHBOARD_CACHE_TTL_SEC = int(os.getenv("DASHBOARD_CACHE_TTL", "300"))
_dashboard_cache: Dict[str, Dict[str, Any]] = {}
_dashboard_cache_lock = threading.Lock()


def _image_cache_get(url: str):
    now = time.time()
    with _image_cache_lock:
        entry = _image_cache.get(url)
        if not entry:
            return None
        if entry["expires_at"] < now:
            _image_cache.pop(url, None)
            return None
        return entry


def _image_cache_set(url: str, content: bytes, content_type: str):
    expires_at = time.time() + IMAGE_CACHE_TTL_SEC
    with _image_cache_lock:
        _image_cache[url] = {
            "content": content,
            "content_type": content_type,
            "expires_at": expires_at,
        }


def _dashboard_cache_get(key: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    with _dashboard_cache_lock:
        entry = _dashboard_cache.get(key)
        if not entry:
            return None
        if entry.get("expires_at", 0) < now:
            _dashboard_cache.pop(key, None)
            return None
        return entry


def _dashboard_cache_set(key: str, data: Dict[str, Any]):
    with _dashboard_cache_lock:
        _dashboard_cache[key] = {
            "data": data,
            "cached_at": datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds"),
            "expires_at": time.time() + DASHBOARD_CACHE_TTL_SEC,
        }


def _dashboard_rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator * 100.0) / denominator, 1)


def _dashboard_status(rate: float) -> str:
    if rate < 15:
        return "good"
    if rate < 30:
        return "warn"
    return "bad"


def _dashboard_trend(rate: float, index: int) -> str:
    metric = (int(rate) + index * 7) % 9
    if metric >= 6:
        return "worsening"
    if metric <= 2:
        return "improving"
    return "stable"


def _build_dashboard_dataset() -> Dict[str, Any]:
    names = [
        "佐藤農園",
        "田中アグリ",
        "高橋ファーム",
        "伊藤ライス",
        "渡辺農場",
        "中村アグロ",
        "小林グリーン",
        "加藤ファーム",
        "吉田農園",
        "山本アグリ",
        "山田農産",
        "斎藤ファーム",
    ]
    task_type_names = ["土壌分析", "播種", "施肥", "防除", "生育調査", "収穫"]
    as_of = datetime.now(timezone(timedelta(hours=9))).isoformat(timespec="seconds")

    farmers: List[Dict[str, Any]] = []
    farmer_details: Dict[str, Dict[str, Any]] = {}

    for i, name in enumerate(names):
        farmer_id = f"farmer-{i + 1}"
        field_count = 18 + ((i * 5) % 32)
        due_task_count = field_count * 5 + ((i * 13) % 40)
        completion_seed = 58 + ((i * 9) % 35)
        completed_count = round((due_task_count * completion_seed) / 100)
        overdue_count = max(0, due_task_count - completed_count)
        due_today_count = 6 + ((i * 3) % 16)
        upcoming_3days_count = 10 + ((i * 7) % 24)
        future_task_count = 70 + ((i * 23) % 140)
        delay_rate = _dashboard_rate(overdue_count, due_task_count)
        completion_rate = _dashboard_rate(completed_count, due_task_count)

        farmer_row = {
            "id": farmer_id,
            "name": name,
            "field_count": field_count,
            "due_task_count": due_task_count,
            "completed_count": completed_count,
            "overdue_count": overdue_count,
            "due_today_count": due_today_count,
            "upcoming_3days_count": upcoming_3days_count,
            "future_task_count": future_task_count,
            "delay_rate": delay_rate,
            "completion_rate": completion_rate,
            "delay_status": _dashboard_status(delay_rate),
            "trend_direction": _dashboard_trend(delay_rate, i),
        }
        farmers.append(farmer_row)

        task_type_rows: List[Dict[str, Any]] = []
        for ti, task_name in enumerate(task_type_names):
            due_count = max(8, round((due_task_count * (12 + ti * 2)) / 100))
            type_completed = min(due_count, round(due_count * (0.55 + ((ti * 7) % 21) / 100)))
            type_overdue = max(0, due_count - type_completed)
            pending_count = round((future_task_count * (10 + ti * 4)) / 100)
            task_type_rows.append(
                {
                    "name": task_name,
                    "display_order": ti + 1,
                    "due_count": due_count,
                    "completed_count": type_completed,
                    "overdue_count": type_overdue,
                    "pending_count": pending_count,
                    "completion_rate": _dashboard_rate(type_completed, due_count),
                    "delay_rate": _dashboard_rate(type_overdue, due_count),
                }
            )

        farmer_details[farmer_id] = {
            "id": farmer_id,
            "name": name,
            "field_count": field_count,
            "summary": {
                "due": due_task_count,
                "completed": completed_count,
                "overdue": overdue_count,
                "pending": future_task_count,
                "delay_rate": delay_rate,
                "completion_rate": completion_rate,
            },
            "task_types": task_type_rows,
        }

    due_count = sum(f["due_task_count"] for f in farmers)
    completed_count = sum(f["completed_count"] for f in farmers)
    overdue_count = sum(f["overdue_count"] for f in farmers)
    due_today_count = sum(f["due_today_count"] for f in farmers)
    upcoming_3days_count = sum(f["upcoming_3days_count"] for f in farmers)
    future_task_count = sum(f["future_task_count"] for f in farmers)

    kpi = {
        "completion_rate": _dashboard_rate(completed_count, due_count),
        "completed_count": completed_count,
        "due_count": due_count,
        "overdue_count": overdue_count,
        "delay_rate": _dashboard_rate(overdue_count, due_count),
        "due_today_count": due_today_count,
        "upcoming_3days_count": upcoming_3days_count,
        "future_count": future_task_count,
        "total_task_count": due_count + future_task_count,
        "as_of": as_of,
    }

    task_types: List[Dict[str, Any]] = []
    for ti, task_name in enumerate(task_type_names):
        ratio = 0.1 + ti * 0.03
        type_due = sum(round(f["due_task_count"] * ratio) for f in farmers)
        type_completed = sum(round(f["completed_count"] * ratio) for f in farmers)
        type_overdue = max(0, type_due - type_completed)
        type_pending = sum(round(f["future_task_count"] * ratio) for f in farmers)
        task_types.append(
            {
                "task_type_name": task_name,
                "display_order": ti + 1,
                "due_count": type_due,
                "completed_count": type_completed,
                "overdue_count": type_overdue,
                "pending_count": type_pending,
                "completion_rate": _dashboard_rate(type_completed, type_due),
                "delay_rate": _dashboard_rate(type_overdue, type_due),
            }
        )

    buckets = [
        {"bucket": "0-5%", "min": 0, "max": 5, "color": "#22c55e"},
        {"bucket": "5-10%", "min": 5, "max": 10, "color": "#22c55e"},
        {"bucket": "10-15%", "min": 10, "max": 15, "color": "#22c55e"},
        {"bucket": "15-20%", "min": 15, "max": 20, "color": "#f59e0b"},
        {"bucket": "20-25%", "min": 20, "max": 25, "color": "#f59e0b"},
        {"bucket": "25-30%", "min": 25, "max": 30, "color": "#f59e0b"},
        {"bucket": "30%+", "min": 30, "max": 999, "color": "#ef4444"},
    ]
    distribution: List[Dict[str, Any]] = []
    for bucket in buckets:
        count = sum(1 for f in farmers if bucket["min"] <= f["delay_rate"] < bucket["max"])
        distribution.append(
            {
                "bucket": bucket["bucket"],
                "count": count,
                "color": bucket["color"],
            }
        )

    base_completion = _dashboard_rate(completed_count, due_count)
    trend: List[Dict[str, Any]] = []
    for idx in range(30):
        day = datetime.now(timezone(timedelta(hours=9))) - timedelta(days=29 - idx)
        drift = (math.sin((idx + 2) / 4) * 3) + (math.cos((idx + 4) / 5) * 1.4)
        completion_rate = max(40, min(98, round(base_completion + drift, 1)))
        delay_rate = round(100 - completion_rate, 1)
        trend.append(
            {
                "date": f"{day.month}/{day.day}",
                "completion_rate": completion_rate,
                "delay_rate": delay_rate,
            }
        )

    return {
        "as_of": as_of,
        "kpi": kpi,
        "farmers": farmers,
        "farmer_details": farmer_details,
        "task_types": task_types,
        "distribution": distribution,
        "trend": trend,
    }


def _get_dashboard_dataset(force_refresh: bool = False) -> Dict[str, Any]:
    cache_key = "dashboard:v1"
    if not force_refresh:
        cached = _dashboard_cache_get(cache_key)
        if cached:
            return {
                "source": "cache",
                "cached_at": cached.get("cached_at"),
                "data": cached["data"],
            }

    data = _build_dashboard_dataset()
    _dashboard_cache_set(cache_key, data)
    fresh = _dashboard_cache_get(cache_key)
    return {
        "source": "api",
        "cached_at": fresh.get("cached_at") if fresh else None,
        "data": data,
    }


api_app = FastAPI(title="xhf-app: Gigya login -> Xarvio API token")


@api_app.get("/dashboard/kpi")
async def dashboard_kpi(refresh: bool = False):
    result = _get_dashboard_dataset(force_refresh=refresh)
    return {
        **result["data"]["kpi"],
        "source": result["source"],
        "cached_at": result["cached_at"],
    }


@api_app.get("/dashboard/farmers")
async def dashboard_farmers(refresh: bool = False):
    result = _get_dashboard_dataset(force_refresh=refresh)
    return {
        "farmers": result["data"]["farmers"],
        "as_of": result["data"]["as_of"],
        "source": result["source"],
        "cached_at": result["cached_at"],
    }


@api_app.get("/dashboard/farmers/{farmer_id}")
async def dashboard_farmer_detail(farmer_id: str, refresh: bool = False):
    result = _get_dashboard_dataset(force_refresh=refresh)
    detail = result["data"]["farmer_details"].get(farmer_id)
    if not detail:
        raise HTTPException(404, {"reason": "farmer_not_found", "farmer_id": farmer_id})
    return {
        **detail,
        "as_of": result["data"]["as_of"],
        "source": result["source"],
        "cached_at": result["cached_at"],
    }


@api_app.get("/dashboard/task-types")
async def dashboard_task_types(refresh: bool = False):
    result = _get_dashboard_dataset(force_refresh=refresh)
    return {
        "task_types": result["data"]["task_types"],
        "as_of": result["data"]["as_of"],
        "source": result["source"],
        "cached_at": result["cached_at"],
    }


@api_app.get("/dashboard/distribution")
async def dashboard_distribution(refresh: bool = False):
    result = _get_dashboard_dataset(force_refresh=refresh)
    return {
        "distribution": result["data"]["distribution"],
        "as_of": result["data"]["as_of"],
        "source": result["source"],
        "cached_at": result["cached_at"],
    }


@api_app.get("/dashboard/trend")
async def dashboard_trend(refresh: bool = False):
    result = _get_dashboard_dataset(force_refresh=refresh)
    return {
        "data": result["data"]["trend"],
        "as_of": result["data"]["as_of"],
        "source": result["source"],
        "cached_at": result["cached_at"],
    }


@api_app.post("/dashboard/cache/clear")
async def dashboard_cache_clear():
    with _dashboard_cache_lock:
        _dashboard_cache.clear()
    return {"ok": True, "cleared": True}

# CORS（開発中は緩め / 本番は適切に制限してください）
api_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_origin_regex=".*",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Large JSON payloads are common for /combined-fields. Enable gzip to reduce
# transfer size and lower the chance of incomplete chunked responses.
api_app.add_middleware(
    GZipMiddleware,
    minimum_size=int(os.getenv("GZIP_MINIMUM_SIZE", "1000")),
)


def _sanitize_filename(raw_filename: Optional[str]) -> str:
    """
    Eliminate path traversal and empty names; fall back to 'download'.
    """
    if not raw_filename:
        return "download"
    cleaned = unquote(raw_filename).strip().replace("\\", "_").replace("/", "_")
    return cleaned or "download"


def _ascii_fallback(name: str) -> str:
    ascii_name = "".join(ch if ord(ch) < 128 else "_" for ch in name).strip("_")
    return ascii_name or "download"


def _summarize_response(label: str, res: Any) -> Dict[str, Any]:
    """
    Debug helper: return a token-safe summary of a cached/fetched response or exception.
    """
    summary: Dict[str, Any] = {"label": label}
    if res is None:
        summary["status"] = "missing"
        summary["ok"] = False
        return summary
    if isinstance(res, Exception):
        summary["ok"] = False
        summary["error"] = str(res)
        summary["exception_type"] = res.__class__.__name__
        return summary
    summary["ok"] = res.get("ok")
    summary["status"] = res.get("status")
    summary["reason"] = res.get("reason")
    summary["source"] = res.get("source")
    payload = (res.get("request") or {}).get("payload") or {}
    summary["operationName"] = payload.get("operationName")
    errors = (res.get("response") or {}).get("errors")
    if errors:
        summary["graphql_errors"] = errors[:3]
    return summary


def build_locale_candidates(raw_locale: Optional[str]) -> List[Optional[str]]:
    """
    Xarvio のマスターAPIは locale パラメータのバリデーションが厳しく、
    ドキュメントと実装に差異があるケースがあるため複数の候補を試す。
    優先順位:
      1. フロントから指定された値（そのまま・大文字化・小文字化）
      2. 言語コードと国コードの組み合わせ（大文字/小文字）
      3. 国コードのみ
      4. デフォルトの EN-GB
      5. locale なし
    """
    candidates: List[Optional[str]] = []
    if raw_locale:
        normalized = raw_locale.strip().replace("_", "-")
        if normalized:
            base_variants = [
                normalized,
                normalized.upper(),
                normalized.lower(),
            ]
            if "-" in normalized:
                lang, region = normalized.split("-", 1)
                lang = lang.strip()
                region = region.strip()
                combo_variants = [
                    f"{lang.lower()}-{region.upper()}",
                    f"{lang.upper()}-{region.upper()}",
                    f"{lang.lower()}-{region.lower()}",
                    f"{lang.upper()}-{region.lower()}",
                    lang.lower(),
                    lang.upper(),
                    region.upper(),
                    region.lower(),
                ]
                base_variants.extend([variant for variant in combo_variants if variant])
            for variant in base_variants:
                cleaned = variant.replace("_", "-")
                if cleaned and cleaned not in candidates:
                    candidates.append(cleaned)
    if "EN-GB" not in candidates:
        candidates.append("EN-GB")
    if None not in candidates:
        candidates.append(None)
    return candidates


def _merge_cropseason_payload(core: Optional[Dict[str, Any]], extra: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Merge cropSeasonsV2-level data from extra into core (field/season update).
    If core is None, return extra. If extra is None, return core.
    """
    if not extra:
        return core
    if not core:
        return extra
    try:
        core_fields = core["response"]["data"]["fieldsV2"]
        extra_fields = extra["response"]["data"]["fieldsV2"]
        extra_map = {f["uuid"]: f for f in extra_fields if f.get("uuid")}
        for field in core_fields:
            f_uuid = field.get("uuid")
            if not f_uuid or f_uuid not in extra_map:
                continue
            core_cs_map = {cs.get("uuid"): cs for cs in field.get("cropSeasonsV2", []) if cs.get("uuid")}
            for cs in extra_map[f_uuid].get("cropSeasonsV2", []):
                cs_uuid = cs.get("uuid")
                if cs_uuid and cs_uuid in core_cs_map:
                    core_cs_map[cs_uuid].update(cs)
        return core
    except Exception:
        return core

# ---------------------------
#        Health Check
# ---------------------------
@api_app.get("/healthz")
async def healthz():
    return {"ok": True, "graphql_endpoint": settings.GRAPHQL_ENDPOINT}


@api_app.post("/warmup")
async def warmup(force: bool = False):
    try:
        status = start_pref_city_warmup(force=force)
    except Exception as exc:  # pylint: disable=broad-except
        raise HTTPException(status_code=500, detail={"reason": "warmup_failed", "detail": str(exc)})
    return JSONResponse({"ok": True, **status})


@api_app.get("/warmup/status")
async def warmup_status():
    status = get_pref_city_status()
    return JSONResponse({"ok": True, **status})


# ---------------------------
#     Attachment Download
# ---------------------------
@api_app.get("/download-attachment")
async def download_attachment(url: str, filename: Optional[str] = None):
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(400, {"reason": "invalid_url", "detail": "Only http/https URLs are allowed."})

    safe_filename = _sanitize_filename(filename or parsed.path.rsplit("/", 1)[-1])
    ascii_filename = _ascii_fallback(safe_filename)

    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            resp = await client.get(url)
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                detail = exc.response.text[:200] if exc.response else str(exc)
                raise HTTPException(
                    status_code=exc.response.status_code if exc.response else 502,
                    detail={"reason": "download_failed", "detail": detail},
                )

        content_type = resp.headers.get("content-type") or "application/octet-stream"
        disposition = (
            f'attachment; filename="{ascii_filename}"; filename*=UTF-8\'\'{quote(safe_filename)}'
        )
        headers = {"Content-Disposition": disposition}
        return Response(content=resp.content, media_type=content_type, headers=headers)
    except HTTPException:
        raise
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail={"reason": "download_failed", "detail": str(exc)})
    except Exception as exc:  # pylint: disable=broad-except
        raise HTTPException(status_code=500, detail={"reason": "download_failed", "detail": str(exc)})

# ---------------------------
#        Auth & Token
# ---------------------------
@api_app.post("/login")
async def login(req: LoginReq):
    out = await gigya_login_impl(req.email, req.password)
    return JSONResponse(out)

@api_app.post("/get-api-token")
async def get_api_token(four: FourValues):
    token = await get_api_token_impl(four)
    return JSONResponse({"ok": True, "api_token": token})

@api_app.post("/login-and-token")
async def login_and_token(req: LoginReq):
    # 1) Gigya ログイン
    four_resp = await gigya_login_impl(req.email, req.password)
    if not four_resp.get("ok"):
        raise HTTPException(401, {"reason": "login failed", "detail": four_resp})

    four = FourValues(
        login_token=four_resp.get("login_token") or "",
        gigya_uuid=four_resp.get("gigya_uuid") or "",
        gigya_uuid_signature=four_resp.get("gigya_uuid_signature") or "",
        gigya_signature_timestamp=four_resp.get("gigya_signature_timestamp") or "",
    )

    # 2) Xarvio API トークン
    api_token = await get_api_token_impl(four)
    return JSONResponse({
        "ok": True,
        "login": four.dict(),
        "api_token": api_token,
    })

# ---------------------------
#        GraphQL Proxies
# ---------------------------
@api_app.post("/farms")
async def farms(req: FarmsReq, background_tasks: BackgroundTasks):
    payload = make_payload("FarmsOverview", FARMS_OVERVIEW)
    out = await call_graphql(payload, req.login_token, req.api_token)
    if not req.includeTokens:
        out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"
    background_tasks.add_task(start_pref_city_warmup)
    return JSONResponse(out)

@api_app.post("/fields")
async def fields(req: FieldsReq):
    variables = {"farmUuid": req.farm_uuid}
    payload = make_payload("FieldsByFarm", FIELDS_BY_FARM, variables)
    out = await call_graphql(payload, req.login_token, req.api_token)
    if not req.includeTokens:
        out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"
    return JSONResponse(out)


@api_app.post("/farms/hfr-candidates")
async def farms_hfr_candidates(req: HfrFarmCandidatesReq):
    farm_uuids = list(dict.fromkeys(str(u) for u in (req.farm_uuids or []) if str(u)))
    hard_max_farms = int(os.getenv("COMBINED_FIELDS_HARD_MAX_FARMS", "500"))
    if len(farm_uuids) > hard_max_farms:
        raise HTTPException(status_code=422, detail={
            "reason": "too_many_farms",
            "received_farms": len(farm_uuids),
            "max_farms": hard_max_farms,
        })

    suffix = str(req.suffix).strip() if req.suffix is not None else "HFR"
    pattern = re.compile(rf"{re.escape(suffix)}$", re.IGNORECASE) if suffix else None
    scan_chunk_size = int(os.getenv("HFR_SCAN_CHUNK_SIZE", "20"))

    matched_farm_uuid_set = set()
    matched_farm_name_by_uuid: Dict[str, str] = {}
    matched_field_count = 0
    scanned_field_count = 0

    for farm_chunk in _chunk_list(farm_uuids, scan_chunk_size):
        payload = make_payload("FieldsNameScanByFarms", FIELDS_NAME_SCAN_BY_FARMS, {
            "farmUuids": farm_chunk,
        })
        out = await call_graphql(payload, req.login_token, req.api_token)
        fields = (((out.get("response") or {}).get("data") or {}).get("fieldsV2") or [])
        if not isinstance(fields, list):
            continue
        scanned_field_count += len(fields)

        for field in fields:
            if not isinstance(field, dict):
                continue
            name = str(field.get("name") or "").strip()
            if not name:
                continue
            if pattern is not None and not pattern.search(name):
                continue
            farm = field.get("farmV2") or {}
            farm_uuid = str((farm or {}).get("uuid") or "")
            if not farm_uuid:
                continue
            matched_field_count += 1
            matched_farm_uuid_set.add(farm_uuid)
            farm_name = str((farm or {}).get("name") or "")
            if farm_name:
                matched_farm_name_by_uuid[farm_uuid] = farm_name

    matched_farm_uuids = [u for u in farm_uuids if u in matched_farm_uuid_set]
    matched_farms = [{"uuid": u, "name": matched_farm_name_by_uuid.get(u) or ""} for u in matched_farm_uuids]

    out: Dict[str, Any] = {
        "ok": True,
        "status": 200,
        "source": "api",
        "request": {
            "url": "/farms/hfr-candidates",
            "headers": {"Cookie": "LOGIN_TOKEN=***; DF_TOKEN=***"} if not req.includeTokens else {},
            "payload": {
                "farm_uuids": farm_uuids,
                "suffix": suffix,
                "chunkSize": scan_chunk_size,
            },
        },
        "response": {
            "data": {
                "suffix": suffix,
                "scannedFarmCount": len(farm_uuids),
                "scannedFieldCount": scanned_field_count,
                "matchedFarmCount": len(matched_farm_uuids),
                "matchedFieldCount": matched_field_count,
                "matchedFarmUuids": matched_farm_uuids,
                "matchedFarms": matched_farms,
            }
        },
    }
    return JSONResponse(out)


@api_app.post("/farms/hfr-csv-fields")
async def farms_hfr_csv_fields(req: HfrCsvFieldsReq):
    farm_uuids = list(dict.fromkeys(str(u) for u in (req.farm_uuids or []) if str(u)))
    hard_max_farms = int(os.getenv("COMBINED_FIELDS_HARD_MAX_FARMS", "500"))
    if len(farm_uuids) > hard_max_farms:
        raise HTTPException(status_code=422, detail={
            "reason": "too_many_farms",
            "received_farms": len(farm_uuids),
            "max_farms": hard_max_farms,
        })

    suffix = str(req.suffix).strip() if req.suffix is not None else "HFR"
    pattern = re.compile(rf"{re.escape(suffix)}$", re.IGNORECASE) if suffix else None

    payload = make_payload("HfrCsvFieldsData", HFR_CSV_FIELDS_DATA, {
        "farmUuids": farm_uuids,
        "languageCode": req.languageCode,
        "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
    })
    gql = await call_graphql(payload, req.login_token, req.api_token)
    fields = (((gql.get("response") or {}).get("data") or {}).get("fieldsV2") or [])

    matched_fields: List[dict] = []
    if isinstance(fields, list):
        for field in fields:
            if not isinstance(field, dict):
                continue
            name = str(field.get("name") or "").strip()
            if not name:
                continue
            if pattern is not None and not pattern.search(name):
                continue
            matched_fields.append(field)

    out: Dict[str, Any] = {
        "ok": True,
        "status": 200,
        "source": "api",
        "request": {
            "url": "/farms/hfr-csv-fields",
            "headers": {"Cookie": "LOGIN_TOKEN=***; DF_TOKEN=***"} if not req.includeTokens else {},
            "payload": {
                "farm_uuids": farm_uuids,
                "languageCode": req.languageCode,
                "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
                "suffix": suffix,
            },
        },
        "response": {
            "data": {
                "suffix": suffix,
                "scannedFarmCount": len(farm_uuids),
                "scannedFieldCount": len(fields) if isinstance(fields, list) else 0,
                "matchedFieldCount": len(matched_fields),
                "hfrFields": matched_fields,
            }
        },
    }
    return JSONResponse(out)


@api_app.post("/field-data-layers")
async def field_data_layers(req: FieldDataLayersReq):
    """
    特定圃場の衛星マップ用データレイヤを取得する。
    """
    default_types = [
        "BIOMASS_SINGLE_IMAGE_LAI",
        "BIOMASS_NDVI",
        "TRUE_COLOR_ANALYSIS",
        "BIOMASS_PROXY_MONITORING_VECTOR_ANALYSIS",
        "WEED_CLASSIFICATION_NDVI",
        "BIOMASS_MULTI_IMAGE_LAI",
    ]
    variables = {"fieldUuid": req.field_uuid, "types": req.types or default_types}
    payload = make_payload("FieldDataLayerImages", FIELD_DATA_LAYER_IMAGES, variables)

    cached_response = get_by_operation("FieldDataLayerImages", payload)
    if cached_response:
        cached = dict(cached_response)
        cached.setdefault("source", "cache")
        return JSONResponse(cached)

    out = await call_graphql(payload, req.login_token, req.api_token)
    out["source"] = "api"
    if not req.includeTokens:
        if out.get("request", {}).get("headers", {}).get("Cookie"):
            out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"

    save_response("FieldDataLayerImages", payload, out)
    print("💾 [CACHE] Saved response for operation: FieldDataLayerImages")
    return JSONResponse(out)


@api_app.post("/field-data-layer/image")
async def field_data_layer_image(req: FieldDataLayerImageReq):
    """
    fieldDataLayersの画像URLをバックエンド経由で取得し、トークン付きで代理取得する。
    """
    cached = _image_cache_get(req.image_url)
    if cached:
        return StreamingResponse(io.BytesIO(cached["content"]), media_type=cached.get("content_type") or "application/octet-stream")

    headers = {
        "Accept": "*/*",
        "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
        "User-Agent": "xhf-app/1.0",
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(req.image_url, headers=headers)
            resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"Image fetch failed: {exc.response.text}")
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Image fetch request error: {exc}")
    except Exception as exc:  # pylint: disable=broad-except
        raise HTTPException(status_code=500, detail=f"Unexpected error: {exc}")

    content_type = resp.headers.get("content-type", "application/octet-stream")
    _image_cache_set(req.image_url, resp.content, content_type)
    return StreamingResponse(io.BytesIO(resp.content), media_type=content_type)

def merge_fields_data(base_data, insights_data, predictions_data, tasks_data):
    """複数のレスポンスデータをマージする"""
    fields_map = {field['uuid']: field for field in base_data['data']['fieldsV2']}

    # insights, predictions, tasks をマージ
    for data_source in [insights_data, predictions_data, tasks_data]:
        if not data_source or not data_source.get('data') or not data_source['data'].get('fieldsV2'):
            continue
        for field_update in data_source['data']['fieldsV2']:
            field_uuid = field_update['uuid']
            if field_uuid in fields_map:
                if 'cropSeasonsV2' in field_update and field_update['cropSeasonsV2']:
                    cs_map = {cs['uuid']: cs for cs in fields_map[field_uuid].get('cropSeasonsV2', [])}
                    for cs_update in field_update['cropSeasonsV2']:
                        if cs_update['uuid'] in cs_map:
                            cs_map[cs_update['uuid']].update(cs_update)

    merged = list(fields_map.values())
    for field in merged:
        enrich_field_with_location(field)
    return merged


@api_app.post("/combined-fields")
async def combined_fields(req: CombinedFieldsReq):
    farms = list(dict.fromkeys(str(u) for u in (req.farm_uuids or []) if str(u)))
    req = req.copy(update={"farm_uuids": farms})

    sync_max_farms = int(os.getenv("COMBINED_FIELDS_SYNC_MAX_FARMS", "200"))
    hard_max_farms = int(os.getenv("COMBINED_FIELDS_HARD_MAX_FARMS", "500"))
    hard_max_farms = max(sync_max_farms, hard_max_farms)

    if len(farms) > hard_max_farms:
        raise HTTPException(status_code=422, detail={
            "reason": "too_many_farms",
            "received_farms": len(farms),
            "max_farms": hard_max_farms,
            "sync_max_farms": sync_max_farms,
        })

    # For large (but allowed) selections, force reliability mode:
    # - non-stream
    # - strict completeness
    # - chunked, lightweight payload
    if len(farms) > sync_max_farms:
        forced_req = req.copy(update={
            "stream": False,
            "requireComplete": True,
            "withBoundarySvg": False,
            "includeSubResponses": False,
        })
        return await _combined_fields_chunked(forced_req)

    # For very large farm selections, a single upstream GraphQL call (or a huge
    # combined response with duplicated debug payloads) becomes unreliable.
    # Chunk farms server-side and merge results to improve completeness.
    if not req.stream:
        threshold = int(os.getenv("COMBINED_FIELDS_SERVER_CHUNK_THRESHOLD", "20"))
        if len(farms) >= threshold:
            return await _combined_fields_chunked(req)

    return await _combined_fields_single(req)


def _chunk_list(items: List[str], size: int) -> List[List[str]]:
    n = max(1, int(size))
    return [items[i:i + n] for i in range(0, len(items), n)]


def _merge_fields_v2_by_uuid(field_lists: List[List[dict]]) -> List[dict]:
    merged: Dict[str, dict] = {}

    def _merge_crop_seasons(prev_list: Any, next_list: Any) -> List[dict]:
        out: Dict[str, dict] = {}
        for cs in (prev_list or []) if isinstance(prev_list, list) else []:
            if isinstance(cs, dict) and cs.get("uuid"):
                out[str(cs["uuid"])] = dict(cs)
        for cs in (next_list or []) if isinstance(next_list, list) else []:
            if not isinstance(cs, dict) or not cs.get("uuid"):
                continue
            key = str(cs["uuid"])
            prev = out.get(key, {})
            merged_cs = {**prev, **cs}
            out[key] = merged_cs
        return list(out.values())

    for fields in field_lists:
        if not isinstance(fields, list):
            continue
        for f in fields:
            if not isinstance(f, dict) or not f.get("uuid"):
                continue
            uuid = str(f["uuid"])
            prev = merged.get(uuid, {})
            combined = {**prev, **f}
            if prev.get("cropSeasonsV2") is not None or f.get("cropSeasonsV2") is not None:
                combined["cropSeasonsV2"] = _merge_crop_seasons(prev.get("cropSeasonsV2"), f.get("cropSeasonsV2"))
            merged[uuid] = combined
    return list(merged.values())


def _extract_farm_uuids_from_fields(fields: Any) -> List[str]:
    out: List[str] = []
    seen: set = set()
    if not isinstance(fields, list):
        return out
    for f in fields:
        if not isinstance(f, dict):
            continue
        farm = f.get("farmV2") or f.get("farm") or {}
        uuid = farm.get("uuid") if isinstance(farm, dict) else None
        if not uuid:
            continue
        key = str(uuid)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


async def _run_combined_chunk_pass(
    *,
    req: CombinedFieldsReq,
    farm_chunks: List[List[str]],
    per_chunk_attempts: int,
    backoff_base_sec: float,
) -> tuple[List[dict], List[dict]]:
    successes: List[dict] = []
    failures: List[dict] = []

    for farm_chunk in farm_chunks:
        subreq = req.copy(update={
            "farm_uuids": farm_chunk,
            "stream": False,
            # boundary payloads are large and frequently cause timeouts for big selections.
            "withBoundarySvg": False,
            # Sub responses duplicate field lists and can easily explode payload size.
            "includeSubResponses": False,
        })

        last_error: Any = None
        for attempt in range(1, per_chunk_attempts + 1):
            try:
                resp = await _combined_fields_single(subreq)
                if not isinstance(resp, JSONResponse):
                    last_error = {"reason": "unexpected_response_type", "type": resp.__class__.__name__}
                else:
                    status = int(getattr(resp, "status_code", 200))
                    body = getattr(resp, "body", b"") or b""
                    parsed = json.loads(body.decode("utf-8")) if body else {}
                    if status < 400 and isinstance(parsed, dict) and parsed.get("ok", True) is not False:
                        fields = (((parsed.get("response") or {}).get("data") or {}).get("fieldsV2") or [])
                        if isinstance(fields, list):
                            successes.append(parsed)
                            last_error = None
                            break
                    last_error = {"status": status, "body": parsed}
            except HTTPException as exc:
                last_error = {"status": exc.status_code, "detail": exc.detail}
            except Exception as exc:  # pylint: disable=broad-except
                last_error = {"status": 500, "detail": str(exc), "exception_type": exc.__class__.__name__}

            if attempt < per_chunk_attempts:
                await asyncio.sleep(min(5.0, backoff_base_sec * attempt))

        if last_error is not None:
            failures.append({"farmUuids": farm_chunk, "error": last_error})

    return successes, failures


async def _combined_fields_chunked(req: CombinedFieldsReq):
    farms = req.farm_uuids or []
    chunk_size = int(os.getenv("COMBINED_FIELDS_SERVER_CHUNK_SIZE", "1"))
    per_chunk_attempts = int(os.getenv("COMBINED_FIELDS_SERVER_PER_CHUNK_ATTEMPTS", "3"))
    backoff_base_sec = float(os.getenv("COMBINED_FIELDS_SERVER_RETRY_BACKOFF_SEC", "0.8"))

    require_complete = bool(getattr(req, "requireComplete", False))
    include_sub = bool(getattr(req, "includeSubResponses", False))

    warmup_status = start_pref_city_warmup()

    boundary_omitted = bool(getattr(req, "withBoundarySvg", False))
    initial_chunks = _chunk_list(farms, chunk_size)
    successes, failures = await _run_combined_chunk_pass(
        req=req,
        farm_chunks=initial_chunks,
        per_chunk_attempts=per_chunk_attempts,
        backoff_base_sec=backoff_base_sec,
    )

    retried_failed_farms: List[str] = []
    if failures:
        failed_farm_uuids: List[str] = []
        seen_failed: set = set()
        for f in failures:
            for uuid in (f.get("farmUuids") or []):
                key = str(uuid)
                if key in seen_failed:
                    continue
                seen_failed.add(key)
                failed_farm_uuids.append(key)

        if failed_farm_uuids:
            retried_failed_farms = failed_farm_uuids
            retry_successes, retry_failures = await _run_combined_chunk_pass(
                req=req,
                farm_chunks=_chunk_list(failed_farm_uuids, chunk_size),
                per_chunk_attempts=per_chunk_attempts,
                backoff_base_sec=backoff_base_sec,
            )
            successes.extend(retry_successes)
            failures = retry_failures

    if not successes:
        raise HTTPException(status_code=502, detail={
            "reason": "combined_fields_all_chunks_failed",
            "failed_chunks": failures[:50],
        })

    merged_fields = _merge_fields_v2_by_uuid([
        (((s.get("response") or {}).get("data") or {}).get("fieldsV2") or [])
        for s in successes
    ])
    requested_farm_uuids = list(dict.fromkeys(str(u) for u in farms if u))
    covered_farm_uuid_set = set(_extract_farm_uuids_from_fields(merged_fields))
    missing_farm_uuids = [u for u in requested_farm_uuids if u not in covered_farm_uuid_set]

    warnings: List[dict] = []
    for s in successes:
        for w in (s.get("warnings") or []):
            if isinstance(w, dict):
                warnings.append(w)
    if boundary_omitted:
        warnings.append({"reason": "boundary_omitted_for_large_request"})
    if include_sub:
        warnings.append({"reason": "sub_responses_omitted_for_large_request"})
    if failures:
        warnings.append({"reason": "chunked_fetch_partial", "failed_chunks": len(failures)})
    if retried_failed_farms:
        warnings.append({"reason": "failed_farms_retried", "retried_farm_uuids": retried_failed_farms[:100]})
    if missing_farm_uuids:
        warnings.append({"reason": "missing_farms_in_merged_response", "missing_farm_uuids": missing_farm_uuids[:100]})

    # If the caller requires a complete dataset, fail explicitly with diagnostics.
    if require_complete and (failures or missing_farm_uuids):
        raise HTTPException(status_code=502, detail={
            "reason": "combined_fields_incomplete",
            "failed_chunks": failures[:50],
            "partial_fields": len(merged_fields),
            "missing_farm_uuids": missing_farm_uuids[:100],
            "retried_failed_farm_uuids": retried_failed_farms[:100],
        })

    has_incomplete = len(failures) > 0 or len(missing_farm_uuids) > 0
    out: Dict[str, Any] = {
        "ok": not has_incomplete,
        "status": 200 if not has_incomplete else 206,
        "source": "api",
        "request": {
            "url": "/combined-fields",
            "headers": {"Cookie": "LOGIN_TOKEN=***; DF_TOKEN=***"} if not req.includeTokens else {},
            "payload": {
                "farm_uuids": farms,
                "languageCode": req.languageCode,
                "countryCode": req.countryCode,
                "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
                "withBoundarySvg": False,
                "includeTasks": getattr(req, "includeTasks", True),
                "requireComplete": require_complete,
                "chunked": True,
                "chunkSize": chunk_size,
            },
        },
        "diagnostics": {
            "requestedFarmCount": len(requested_farm_uuids),
            "coveredFarmCount": len(covered_farm_uuid_set),
            "missingFarmUuids": missing_farm_uuids[:100],
            "retriedFailedFarmUuids": retried_failed_farms[:100],
        },
        "response": {"data": {"fieldsV2": merged_fields}},
        "warmup": warmup_status,
        "locationEnrichmentPending": not warmup_status.get("loaded", False),
    }
    if warnings:
        out["warnings"] = warnings

    return JSONResponse(out)


async def _combined_fields_single(req: CombinedFieldsReq):
    """
    選択した複数の Farm UUID に対し、CombinedFieldData を実行して返す。
    body 例:
    {
      "login_token": "...",
      "api_token": "...",
      "farm_uuids": ["uuid1", "uuid2"],
      "languageCode": "ja",
      "cropSeasonLifeCycleStates": ["ACTIVE", "PLANNED"],
      "withBoundarySvg": true,
      "includeTokens": false
    }
    """
    JST = timezone(timedelta(hours=9))
    now_jst = datetime.now(JST)
    from_dt_utc = now_jst.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=9)
    till_dt_utc = (now_jst + timedelta(days=30)).replace(hour=23, minute=59, second=59, microsecond=999000) - timedelta(hours=9)
    from_date = from_dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    till_date = till_dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    # 各サブリクエスト設定を config 化
    include_tasks = getattr(req, "includeTasks", True)
    request_configs = {
        "base": {
            "optional": False,
            "payload": make_payload("CombinedDataBase", COMBINED_DATA_BASE, {
                "farmUuids": req.farm_uuids,
                "languageCode": req.languageCode,
                "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
                "withBoundary": req.withBoundarySvg,
            }),
        },
        "insights": {
            "optional": True,
            "timeout": 10.0,
            "payload": make_payload("CombinedDataInsights", COMBINED_DATA_INSIGHTS, {
                "farmUuids": req.farm_uuids,
                "fromDate": from_date,
                "tillDate": till_date,
                "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
                "withrisk": True,
            }),
        },
        "predictions": {
            "optional": True,
            "timeout": 10.0,
            "payload": make_payload("CombinedDataPredictions", COMBINED_DATA_PREDICTIONS, {
                "farmUuids": req.farm_uuids,
                "languageCode": req.languageCode,
                "countryCode": req.countryCode,
                "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
            }),
        },
        "risk1": {
            "optional": True,
            "payload": make_payload("CombinedFieldData", COMBINED_FIELD_DATA_TASKS, {
                "farmUuids": req.farm_uuids,
                "fromDate": from_date,
                "tillDate": till_date,
                "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
                "withCropSeasonsV2": True,
                "withactionRecommendations": True,
                "withnutritionRecommendations": True,
                "withwaterRecommendations": True,
                "withactionWindows": True,
                "withweedManagementRecommendations": True,
                "withCropSeasonStatus": False,
                "withNutritionStatus": False,
                "withWaterStatus": False,
                "withrisk": False,
            }),
        },
        "risk2": {
            "optional": True,
            "payload": make_payload("CombinedFieldData", COMBINED_FIELD_DATA_TASKS, {
                "farmUuids": req.farm_uuids,
                "fromDate": from_date,
                "tillDate": till_date,
                "languageCode": req.languageCode,
                "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
                "withCropSeasonsV2": True,
                "withactionRecommendations": False,
                "withnutritionRecommendations": False,
                "withwaterRecommendations": False,
                "withactionWindows": False,
                "withweedManagementRecommendations": False,
                "withCropSeasonStatus": True,
                "withNutritionStatus": True,
                "withWaterStatus": True,
                "withrisk": True,
                "withtimingStreessesInfo": True,
            }),
        },
    }

    if include_tasks:
        request_configs["tasks"] = {
            "optional": True,
            "payload": make_payload("CombinedFieldData", COMBINED_FIELD_DATA_TASKS, {
                "farmUuids": req.farm_uuids,
                "languageCode": req.languageCode,
                "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
                # boundary は base のみに限定
                "withBoundary": False,
                "withCropSeasonsV2": True,
                "withHarvests": req.withHarvests,
                "withCropEstablishments": req.withCropEstablishments,
                "withLandPreparations": req.withLandPreparations,
                "withDroneFlights": req.withDroneFlights,
                "withSeedTreatments": req.withSeedTreatments,
                "withSeedBoxTreatments": req.withSeedBoxTreatments,
                "withSmartSprayingTasks": req.withSmartSprayingTasks,
                "withWaterManagementTasks": req.withWaterManagementTasks,
                "withScoutingTasks": req.withScoutingTasks,
                "withObservations": req.withObservations,
                "withSprayingsV2": req.withSprayingsV2,
                "withSoilSamplingTasks": req.withSoilSamplingTasks,
            }),
        }
        request_configs["tasks_sprayings"] = {
            "optional": True,
            "condition": lambda rq: rq.withSprayingsV2,
            "payload": make_payload("CombinedFieldData", COMBINED_FIELD_DATA_TASKS, {
                "farmUuids": req.farm_uuids,
                "languageCode": req.languageCode,
                "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
                "withBoundary": False,
                "withCropSeasonsV2": True,
                "withHarvests": False,
                "withCropEstablishments": False,
                "withLandPreparations": False,
                "withDroneFlights": False,
                "withSeedTreatments": False,
                "withSeedBoxTreatments": False,
                "withSmartSprayingTasks": False,
                "withWaterManagementTasks": False,
                "withScoutingTasks": False,
                "withObservations": False,
                "withSprayingsV2": True,
                "withSoilSamplingTasks": False,
            }),
        }

    warmup_status = start_pref_city_warmup()
    operation_name_for_cache = "CombinedFieldData" # フロントエンドの互換性のため

    # ストリーミングモード: NDJSON で順次返す（フロント側が対応している場合のみ）
    if req.stream:
        # 先にキャッシュが揃っていれば一括で返す（ストリーム不要）
        cache_lookup_stream = {}
        for label, cfg in request_configs.items():
            if cfg.get("condition") and not cfg["condition"](req):
                continue
            payload = cfg["payload"]
            op_name = payload.get("operationName") or "CombinedFieldData"
            cache_lookup_stream[label] = get_by_operation(op_name, payload)
        def _ok(res: Any) -> bool:
            return isinstance(res, dict) and res.get("ok", True) is not False
        # 全ラベル（条件付きを含む）がキャッシュに揃っている場合のみストリームを省略
        all_labels_ready = all(
            _ok(cache_lookup_stream.get(label))
            for label, cfg in request_configs.items()
            if not cfg.get("condition") or cfg["condition"](req)
        )
        if all_labels_ready:
            base_res = cache_lookup_stream["base"]
            insights_res = cache_lookup_stream["insights"] if _ok(cache_lookup_stream["insights"]) else None
            predictions_res = cache_lookup_stream["predictions"] if _ok(cache_lookup_stream["predictions"]) else None
            tasks_res = cache_lookup_stream.get("tasks") if _ok(cache_lookup_stream.get("tasks")) else None
            tasks_sprayings_res = cache_lookup_stream.get("tasks_sprayings") if _ok(cache_lookup_stream.get("tasks_sprayings")) else None
            risk1_res = cache_lookup_stream["risk1"] if _ok(cache_lookup_stream["risk1"]) else None
            risk2_res = cache_lookup_stream["risk2"] if _ok(cache_lookup_stream["risk2"]) else None
            tasks_res = _merge_cropseason_payload(tasks_res, tasks_sprayings_res)
            tasks_res = _merge_cropseason_payload(tasks_res, risk1_res)
            tasks_res = _merge_cropseason_payload(tasks_res, risk2_res)
            merged_fields = merge_fields_data(
                base_res["response"],
                insights_res["response"] if insights_res else None,
                predictions_res["response"] if predictions_res else None,
                tasks_res["response"] if tasks_res else None,
            )
            warnings = []
            if not insights_res: warnings.append({"reason": "insights_pending"})
            if not predictions_res: warnings.append({"reason": "predictions_pending"})
            if not tasks_res: warnings.append({"reason": "tasks_pending"})
            out_cache = {
                "ok": True,
                "status": 200,
                "source": "cache",
                "response": {"data": {"fieldsV2": merged_fields}},
                "warnings": warnings,
                "_sub_responses": {
                    "base": base_res,
                    "insights": insights_res,
                    "predictions": predictions_res,
                    "tasks": tasks_res,
                    "tasks_sprayings": tasks_sprayings_res,
                    "risk1": risk1_res,
                    "risk2": risk2_res,
                },
                "warmup": warmup_status,
            }
            return JSONResponse(out_cache)

        def _log_stream_result(label: str, res: Any):
            try:
                if isinstance(res, Exception):
                    print(f"[STREAM][{label}] exception: {res}")
                    return
                if isinstance(res, dict) and res.get("ok") is False:
                    status = res.get("status")
                    reason = res.get("reason") or res.get("response", {}).get("errors")
                    print(f"[STREAM][{label}] ok:false status={status} reason={reason}")
                    return
                if isinstance(res, dict) and res.get("ok", True) is True:
                    status = res.get("status")
                    data = res.get("response", {}).get("data", {})
                    fields_count = len(data.get("fieldsV2") or data.get("fields") or [])
                    print(f"[STREAM][{label}] ok:true status={status} fields={fields_count}")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[STREAM][{label}] log error: {exc}")

        async def streamer():
            # キャッシュは使わず都度取得
            async def _run_labeled(label: str, payload: Any):
                try:
                    res = await call_graphql(payload, req.login_token, req.api_token)
                    return label, res
                except Exception as exc:  # pylint: disable=broad-except
                    return label, {"ok": False, "error": str(exc)}

            tasks_map = {}
            for label, cfg in request_configs.items():
                if cfg.get("condition") and not cfg["condition"](req):
                    continue
                tasks_map[label] = asyncio.create_task(call_graphql(cfg["payload"], req.login_token, req.api_token))

            # 先に base を返す
            base_res = await tasks_map["base"]
            try:
                for f in base_res.get("response", {}).get("data", {}).get("fieldsV2", []) or []:
                    enrich_field_with_location(f)
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[WARN] enrich location failed in stream base: {exc}")
            yield json.dumps({"type": "base", "data": base_res}) + "\n"

            # 残りは完了した順に返す
            stream_results = {"base": base_res}
            other_tasks = [
                asyncio.create_task(_run_labeled(label, cfg["payload"]))
                for label, cfg in request_configs.items()
                if label != "base" and (not cfg.get("condition") or cfg["condition"](req))
            ]
            for task in asyncio.as_completed(other_tasks):
                label, res = await task
                _log_stream_result(label, res)
                stream_results[label] = res
                yield json.dumps({"type": label, "data": res}) + "\n"

            try:
                summary = {
                    key: (
                        "missing" if key not in stream_results else
                        f"ok={stream_results[key].get('ok')}" if isinstance(stream_results[key], dict) else
                        stream_results[key].__class__.__name__
                    )
                    for key in request_configs.keys()
                }
                print(f"[STREAM] summary {summary}")
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[STREAM] summary log error: {exc}")

            yield json.dumps({"type": "done", "warmup": warmup_status}) + "\n"

        return StreamingResponse(streamer(), media_type="application/x-ndjson")

    # 1. まずキャッシュを確認し、不足分だけ API を呼び出す
    operation_name_for_cache = "CombinedFieldData" # フロントエンドの互換性のため
    payload_map = {label: cfg["payload"] for label, cfg in request_configs.items()}
    cache_lookup = {
        label: get_by_operation(operation_name_for_cache if label != "base" else "CombinedDataBase", cfg["payload"])
        for label, cfg in request_configs.items()
        if not cfg.get("condition") or cfg["condition"](req)
    }

    prepared_cache = {}
    for key, cached in cache_lookup.items():
        if cached:
            if cached.get("ok") is False:
                continue
            prepared = {**cached}
            prepared.setdefault("request", {"payload": payload_map[key]})
            prepared["source"] = "cache"
            prepared_cache[key] = prepared

    # combined-fields 由来のキャッシュなら、埋め込まれたサブレスポンスを流用して不足を補う
    tasks_cache = prepared_cache.get("tasks") or cache_lookup.get("tasks")
    if tasks_cache and tasks_cache.get("_sub_responses"):
        subs = tasks_cache.get("_sub_responses") or {}
        for key in ("base", "insights", "predictions", "tasks"):
            if subs.get(key) and (key not in prepared_cache or key == "tasks"):
                prepared_cache[key] = subs[key]

    fetch_plan = []
    if "base" not in prepared_cache:
        fetch_plan.append(("base", call_graphql(request_configs["base"]["payload"], req.login_token, req.api_token)))
    for label, cfg in request_configs.items():
        if cfg.get("condition") and not cfg["condition"](req):
            continue
        if label not in prepared_cache:
            fetch_plan.append((label, call_graphql(cfg["payload"], req.login_token, req.api_token)))

    # タイムアウト設定（7秒は短すぎて base まで落ちやすいので、用途別に分ける）
    # - base: 必須なので長め（デフォルト 30s）
    # - optional: 部分成功に回すため短め（デフォルト 20s）
    base_timeout_sec = float(os.getenv("COMBINED_FIELDS_BASE_TIMEOUT_SEC", "100"))
    optional_timeout_sec = float(os.getenv("COMBINED_FIELDS_OPTIONAL_TIMEOUT_SEC", "100"))
    optional_labels_with_timeout = {label for label, cfg in request_configs.items() if cfg.get("optional")}

    fetched: Dict[str, Any] = {}
    if fetch_plan:
        # Reliability-first:
        # Start with base only (critical). Optional sub-requests are started after base
        # so upstream isn't hit by a burst of concurrent GraphQL calls per request.
        loop = asyncio.get_running_loop()
        start_t = loop.time()
        plan_map: Dict[str, Any] = {label: coro for label, coro in fetch_plan}

        # base は必須：base だけは base_timeout_sec まで待つ
        if "base" in plan_map:
            try:
                fetched["base"] = await asyncio.wait_for(plan_map["base"], timeout=base_timeout_sec)
            except Exception as exc:  # pylint: disable=broad-except
                fetched["base"] = exc

            # If base fails (often timeout) and boundary was requested, retry once without boundary.
            if isinstance(fetched.get("base"), Exception) and req.withBoundarySvg:
                try:
                    retry_payload = make_payload("CombinedDataBase", COMBINED_DATA_BASE, {
                        "farmUuids": req.farm_uuids,
                        "languageCode": req.languageCode,
                        "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
                        "withBoundary": False,
                    })
                    fetched["base"] = await asyncio.wait_for(
                        call_graphql(retry_payload, req.login_token, req.api_token),
                        timeout=max(base_timeout_sec, 60.0),
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    fetched["base"] = exc

        # optional は optional_timeout_sec の残り時間で順番に取得し、残りは部分成功として打ち切る
        for label, cfg in request_configs.items():
            if label == "base":
                continue
            if label not in plan_map:
                continue
            if cfg.get("condition") and not cfg["condition"](req):
                continue
            remaining = optional_timeout_sec - (loop.time() - start_t)
            if remaining <= 0:
                if label in optional_labels_with_timeout:
                    fetched[label] = asyncio.TimeoutError(f"{label} timed out after {optional_timeout_sec}s")
                    continue
                try:
                    fetched[label] = await plan_map[label]
                except Exception as exc:  # pylint: disable=broad-except
                    fetched[label] = exc
                continue
            try:
                fetched[label] = await asyncio.wait_for(plan_map[label], timeout=remaining)
            except Exception as exc:  # pylint: disable=broad-except
                fetched[label] = exc

    # insights だけはタイムアウトが多いので個別にリトライする
    def _is_error(res: Any) -> bool:
        return isinstance(res, Exception) or (isinstance(res, dict) and res.get("ok") is False)

    if "insights" not in prepared_cache and _is_error(fetched.get("insights")):
        insights_attempts = 2
        insights_delay = 3  # seconds
        insights_result: Any = None
        payload_insights = request_configs["insights"]["payload"]
        for attempt in range(1, insights_attempts + 1):
            try:
                insights_result = await call_graphql(payload_insights, req.login_token, req.api_token)
                break
            except Exception as exc:  # pylint: disable=broad-except
                insights_result = exc
                if attempt >= insights_attempts:
                    break
                await asyncio.sleep(insights_delay)
        fetched["insights"] = insights_result

    # 取得状況をまとめておく（成功/失敗問わず）。フロントやログでデバッグに使う。
    diagnostics = {
        label: _summarize_response(
            label,
            prepared_cache.get(label) or fetched.get(label) or cache_lookup.get(label),
        )
        for label in (["base", "insights", "predictions"] + (["tasks"] if include_tasks else []))
    }

    # エラーチェック（Base は必須、他は警告を添えて部分成功を許容）
    errors_by_label = {
        label: res
        for label, res in {**prepared_cache, **fetched}.items()
        if _is_error(res)
    }

    critical_labels = {"base"}
    optional_labels = {label for label, cfg in request_configs.items() if cfg.get("optional")}

    critical_errors = [res for label, res in errors_by_label.items() if label in critical_labels]
    optional_errors = {label: res for label, res in errors_by_label.items() if label in optional_labels}

    if critical_errors:
        first_error = critical_errors[0]
        if isinstance(first_error, Exception):
            detail = {"reason": "combined_fields_failed", "detail": str(first_error), "diagnostics": diagnostics}
            raise HTTPException(status_code=500, detail=detail)
        error_payload = {
            **first_error,
            "reason": "combined_fields_failed",
            "diagnostics": diagnostics,
            "fetch_plan": [label for label, _ in fetch_plan],
        }
        return JSONResponse(status_code=first_error.get("status", 500), content=error_payload)

    base_res = prepared_cache.get("base") or fetched.get("base")
    insights_res_candidate = prepared_cache.get("insights") or fetched.get("insights")
    insights_res = insights_res_candidate if isinstance(insights_res_candidate, dict) and insights_res_candidate.get("ok", True) is not False else None
    predictions_res_candidate = prepared_cache.get("predictions") or fetched.get("predictions")
    predictions_res = predictions_res_candidate if isinstance(predictions_res_candidate, dict) and predictions_res_candidate.get("ok", True) is not False else None
    tasks_res_candidate = prepared_cache.get("tasks") or fetched.get("tasks")
    tasks_res = tasks_res_candidate if isinstance(tasks_res_candidate, dict) and tasks_res_candidate.get("ok", True) is not False else None
    tasks_sprayings_candidate = prepared_cache.get("tasks_sprayings") or fetched.get("tasks_sprayings")
    tasks_sprayings_res = tasks_sprayings_candidate if isinstance(tasks_sprayings_candidate, dict) and tasks_sprayings_candidate.get("ok", True) is not False else None
    risk1_candidate = prepared_cache.get("risk1") or fetched.get("risk1")
    risk1_res = risk1_candidate if isinstance(risk1_candidate, dict) and risk1_candidate.get("ok", True) is not False else None
    risk2_candidate = prepared_cache.get("risk2") or fetched.get("risk2")
    risk2_res = risk2_candidate if isinstance(risk2_candidate, dict) and risk2_candidate.get("ok", True) is not False else None

    # combined-fields 由来のキャッシュであれば、サブレスポンスを優先的に流用する
    if tasks_res and tasks_res.get("_sub_responses"):
        base_res = base_res or tasks_res["_sub_responses"].get("base")
        insights_res = insights_res or tasks_res["_sub_responses"].get("insights")
        predictions_res = predictions_res or tasks_res["_sub_responses"].get("predictions")
        tasks_res = tasks_res["_sub_responses"].get("tasks") or tasks_res

    if not base_res:
        raise HTTPException(status_code=500, detail="必要なデータの取得に失敗しました（base が欠落）。")

    warnings = []
    for label, err in optional_errors.items():
        if label == "insights" and not insights_res:
            warnings.append({"reason": "insights_unavailable", "detail": str(err)})
        if label == "predictions" and not predictions_res:
            warnings.append({"reason": "predictions_unavailable", "detail": str(err)})
        if label == "tasks" and not tasks_res:
            warnings.append({"reason": "tasks_unavailable", "detail": str(err)})
        if label == "tasks_sprayings" and not tasks_sprayings_res:
            warnings.append({"reason": "tasks_sprayings_unavailable", "detail": str(err)})
        if label == "risk1" and not risk1_res:
            warnings.append({"reason": "risk_recommendations_unavailable", "detail": str(err)})
        if label == "risk2" and not risk2_res:
            warnings.append({"reason": "risk_status_unavailable", "detail": str(err)})

    # Sprayings / Risk を別レスポンスからマージ
    tasks_res = _merge_cropseason_payload(tasks_res, tasks_sprayings_res)
    tasks_res = _merge_cropseason_payload(tasks_res, risk1_res)
    tasks_res = _merge_cropseason_payload(tasks_res, risk2_res)

    # 3つのレスポンスをマージ
    merged_fields = merge_fields_data(
        base_res['response'],
        insights_res['response'] if insights_res else None,
        predictions_res['response'] if predictions_res else None,
        tasks_res['response'] if tasks_res else None,
    )

    # 位置情報が欠落している場合は、強制ウォームアップして再付与を試みる
    def _has_complete_location(field: dict) -> bool:
        loc = field.get("location") or {}
        return bool(loc.get("prefecture")) and bool(loc.get("municipality")) and loc.get("latitude") is not None and loc.get("longitude") is not None

    if any(not _has_complete_location(f) for f in merged_fields):
        try:
            warmup_status = start_pref_city_warmup(force=True)
            for f in merged_fields:
                enrich_field_with_location(f)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[WARN] failed to force warmup for location enrichment: {exc}")
    else:
        warmup_status = warmup_status if 'warmup_status' in locals() else start_pref_city_warmup()


    source_label = "cache" if not fetch_plan else "api"

    # フロントエンドに返す最終的なレスポンスを構築
    out = {
        "ok": True,
        "status": 200,
        "source": source_label,
        "request": { # 代表としてbaseリクエストを格納
            "url": base_res["request"]["url"],
            "headers": base_res["request"]["headers"],
            "payload": request_configs["base"]["payload"],
        },
        "response": {
            "data": {
                "fieldsV2": merged_fields
            }
        },
        "warmup": warmup_status,
        "locationEnrichmentPending": not warmup_status.get("loaded", False),
    }
    # NOTE: _sub_responses duplicates large field lists (base/tasks/risk etc) and
    # can easily exceed browser/proxy limits for big selections.
    if getattr(req, "includeSubResponses", False):
        out["_sub_responses"] = {
            "base": base_res,
            "insights": insights_res,
            "predictions": predictions_res,
            "tasks": tasks_res,
            "tasks_sprayings": tasks_sprayings_res,
            "risk1": risk1_res,
            "risk2": risk2_res,
        }
    if warnings:
        out["warnings"] = warnings

    if not req.includeTokens:
        out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"

    def _has_complete_location(field: dict) -> bool:
        location = field.get("location") or {}
        prefecture = location.get("prefecture")
        municipality = location.get("municipality")
        return bool(prefecture) and bool(municipality)

    payload_for_cache = (
        request_configs.get("tasks", {}).get("payload")
        or request_configs["base"]["payload"]
    )
    save_response(operation_name_for_cache, payload_for_cache, out)
    print(f"💾 [CACHE] Saved response for operation: {operation_name_for_cache}")
    return JSONResponse(out)

@api_app.post("/combined-field-data-tasks")
async def combined_field_data_tasks(req: CombinedFieldDataTasksReq):
    """
    指定された farmUuids と各種フラグに基づいて、タスク関連のフィールドデータを取得する。
    """
    variables = {
        "farmUuids": req.farm_uuids,
        "languageCode": req.languageCode,
        "cropSeasonLifeCycleStates": req.cropSeasonLifeCycleStates,
        "withBoundary": req.withBoundary,
        "withCropSeasonsV2": req.withCropSeasonsV2,
        "withHarvests": req.withHarvests,
        "withCropEstablishments": req.withCropEstablishments,
        "withLandPreparations": req.withLandPreparations,
        "withDroneFlights": req.withDroneFlights,
        "withSeedTreatments": req.withSeedTreatments,
        "withSeedBoxTreatments": req.withSeedBoxTreatments,
        "withSmartSprayingTasks": req.withSmartSprayingTasks,
        "withWaterManagementTasks": req.withWaterManagementTasks,
        "withScoutingTasks": req.withScoutingTasks,
        "withObservations": req.withObservations,
        "withSprayingsV2": req.withSprayingsV2,
        "withSoilSamplingTasks": req.withSoilSamplingTasks,
    }
    # フロントエンドの互換性のため、オペレーション名は "CombinedFieldData" を使用
    operation_name = "CombinedFieldData"
    payload = make_payload(operation_name, COMBINED_FIELD_DATA_TASKS, variables)

    # 1. キャッシュを確認（snapshot job では forceRefresh で無効化）
    if not req.forceRefresh:
        cached_response = get_by_operation(operation_name, payload)
        if cached_response:
            cached_response["source"] = "cache"
            print(f"✅ [CACHE] Used cache for operation: {operation_name}")
            return JSONResponse(cached_response)

    # 2. キャッシュがなければAPIを呼び出す
    out = await call_graphql(payload, req.login_token, req.api_token)

    out["source"] = "api"
    if not req.includeTokens:
        # レスポンスにトークンが含まれている場合、マスクする
        if out.get("request", {}).get("headers", {}).get("Cookie"):
            out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"

    # 3. 最終的なレスポンスをキャッシュに保存（forceRefresh時は保存しない）
    # エラーレスポンスはキャッシュしない
    if out.get("ok") and not req.forceRefresh:
        save_response(operation_name, payload, out)
        print(f"💾 [CACHE] Saved response for operation: {operation_name}")

    return JSONResponse(out)

@api_app.post("/tasks/v2/sprayings/{task_uuid}")
async def update_spraying_task(task_uuid: str, req: SprayingTaskUpdateReq):
    """
    Sprayings タスクの予定日/実行日を更新するプロキシ。
    """
    if not req.plannedDate and not req.executionDate:
        raise HTTPException(400, {"reason": "missing_update_fields"})

    url = f"https://fm-api.xarvio.com/api/tasks/v2/sprayings/{task_uuid}"
    if_match = req.ifMatch
    if not if_match:
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                get_headers = {
                    "Accept": "application/json",
                    "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
                    "X-Login-Token": req.login_token,
                    "Origin": "https://fm.xarvio.com",
                    "Referer": "https://fm.xarvio.com/",
                    "User-Agent": "xhf-app/1.0",
                }
                etag_resp = await client.get(url, headers=get_headers)
                if_match = etag_resp.headers.get("ETag")
        except httpx.RequestError:
            if_match = None
    if not if_match:
        if_match = "*"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/merge-patch+json",
        "If-Match": if_match,
        "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
        "X-Login-Token": req.login_token,
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
        "User-Agent": "xhf-app/1.0",
    }
    payload = {
        "plannedDate": req.plannedDate,
        "executionDate": req.executionDate,
    }

    resp: httpx.Response | None = None
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.patch(url, headers=headers, json=payload)
            if resp.status_code == 405:
                resp = await client.put(url, headers=headers, json=payload)
    except httpx.RequestError as exc:
        raise HTTPException(502, {"reason": "xarvio request error", "detail": str(exc)})

    if resp is None:
        raise HTTPException(502, {"reason": "xarvio request error", "detail": "no response"})

    out: Dict[str, Any] = {
        "ok": resp.status_code < 400,
        "status": resp.status_code,
        "reason": resp.reason_phrase,
        "request": {
            "url": str(resp.request.url) if resp.request else url,
            "headers": {**headers, "Cookie": "MASKED"},
            "payload": payload,
        },
    }

    try:
        out["response"] = resp.json()
    except Exception:
        out["response_text"] = (resp.text or "")[:4000]

    if not req.includeTokens:
        if out.get("request", {}).get("headers", {}).get("Cookie"):
            out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"

    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, out)

    return JSONResponse(out)

@api_app.post("/biomass-ndvi")
async def biomass_ndvi(req: BiomassNdviReq):
    """
    複数の CropSeason UUID に対し、NDVI値を取得して返す。
    """
    # 毎回同じ日の終わりを `till` に設定することで、キャッシュキーを安定させる
    JST = timezone(timedelta(hours=9))
    now_jst = datetime.now(JST)
    till_dt_utc = now_jst.replace(hour=23, minute=59, second=59, microsecond=999000) - timedelta(hours=9)
    till_date = till_dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    variables = {
        "uuids": req.crop_season_uuids,
        "from": req.from_date,
        "till": till_date
    }
    payload = make_payload("Biomass", BIOMASS_NDVI, variables)

    # 1. まずキャッシュを確認
    cached_response = get_by_operation("Biomass", payload)
    if cached_response:
        cached_response["source"] = "cache"
        print(f"✅ [CACHE] Used cache for operation: Biomass")
        return JSONResponse(cached_response)

    # 2. キャッシュがなければAPIを呼び出す
    out = await call_graphql(payload, req.login_token, req.api_token)

    out["source"] = "api" # Explicitly set source for new API calls
    if not req.includeTokens:
        out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"

    # 3. 最終的なレスポンスをキャッシュに保存
    save_response("Biomass", payload, out)
    print(f"💾 [CACHE] Saved response for operation: Biomass")
    return JSONResponse(out)

@api_app.post("/biomass-lai")
async def biomass_lai(req: BiomassLaiReq):
    """
    複数の CropSeason UUID に対し、LAI値を取得して返す。(REST のみ)
    """
    operation_name = "BiomassLaiRest"
    variables = {
        "uuids": req.crop_season_uuids,
        "from": req.from_date,
        "till": req.till_date,
    }
    payload = make_payload(operation_name, "", variables)

    cached_response = get_by_operation(operation_name, payload)
    if cached_response:
        cached = dict(cached_response)
        cached.setdefault("source", "cache")
        return JSONResponse(cached)

    return await _biomass_lai_rest_proxy(req, payload, operation_name)


async def _biomass_lai_rest_proxy(req: BiomassLaiReq, payload: Dict[str, Any], operation_name: str = "BiomassLai"):
    """
    GraphQL が利用できない際のフォールバック（従来の REST エンドポイント）。
    レスポンス形式は GraphQL プロキシと同等に整形する。
    """
    rest_endpoint = "https://fm-api.xarvio.com/api/agronomic-index-analysis/biomass-analysis"
    headers = {
        "Accept": "application/json",
        "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
    }
    params = {
        "cropSeasonUuid": ",".join(req.crop_season_uuids),
        "fromDate": req.from_date,
        "tillDate": req.till_date,
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(rest_endpoint, headers=headers, params=params)
            response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"Xarvio API error: {exc.response.text}")
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Request to Xarvio API failed: {exc}")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {exc}")

    text = response.text or ""
    data = response.json() if text else []
    _sanitize_biomass_entries(data)

    cookie_for_response = "MASKED" if req.includeTokens else "LOGIN_TOKEN=***; DF_TOKEN=***"

    graph_like = {
        "ok": True,
        "status": response.status_code,
        "reason": response.reason_phrase,
        "source": "rest",
        "request": {
            "url": str(response.request.url),
            "headers": {"Cookie": cookie_for_response},
            "payload": payload,
            "method": "GET",
        },
        "response_meta": {
            "content_type": response.headers.get("content-type"),
            "url": str(response.url),
        },
        "response": {
            "data": {
                "biomassAnalysis": data or [],
            }
        },
    }
    save_response(operation_name, payload, graph_like)
    print(f"💾 [CACHE] Saved response for operation: {operation_name} (REST)")
    return JSONResponse(graph_like)


def _sanitize_biomass_entries(entries: Any) -> None:
    """
    Remove properties we do not want to expose to the frontend (e.g., dynamicZones).
    Works in-place for list[dict] payloads.
    """
    if not isinstance(entries, list):
        return
    for entry in entries:
        if isinstance(entry, dict):
            entry.pop("dynamicZones", None)
@api_app.post("/field-notes")
async def field_notes(req: FieldNotesReq):
    """
    複数の Farm UUID に基づいて、圃場のノート情報を取得する。
    """
    variables = {"farmUuids": req.farm_uuids}
    operation_name = "FieldNotesByFarms"
    payload = make_payload(operation_name, FIELD_NOTES_BY_FARMS, variables)

    # キャッシュは利用せず、常に最新の情報を取得
    out = await call_graphql(payload, req.login_token, req.api_token)

    out["source"] = "api"
    if not req.includeTokens:
        # レスポンスにトークンが含まれている場合、マスクする
        if out.get("request", {}).get("headers", {}).get("Cookie"):
            out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"

    return JSONResponse(out)


@api_app.post("/attachments/zip")
async def attachments_zip(req: AttachmentsZipReq):
    attachments = [att for att in req.attachments if att.url]
    if not attachments:
        raise HTTPException(400, {"reason": "no_attachments", "message": "添付ファイルがありません。"})

    zip_name = _sanitize_zip_name(req.zipName)

    async def fetch_one(idx: int, att: AttachmentDownload, client: httpx.AsyncClient, sem: asyncio.Semaphore):
        name = att.fileName or _filename_from_url(att.url) or f"attachment_{idx}"
        async with sem:
            if not att.url:
                raise ValueError("empty url")
            resp = await client.get(att.url)
            resp.raise_for_status()
            return name, resp.content

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            sem = asyncio.Semaphore(12)
            results = await asyncio.gather(
                *(fetch_one(idx, att, client, sem) for idx, att in enumerate(attachments, 1)),
                return_exceptions=True,
            )
    except Exception as exc:  # pylint: disable=broad-except
        print(f"[attachments_zip] failed before gather: {exc}")
        raise HTTPException(502, {"reason": "download_failed", "detail": str(exc)})

    name_counts: Dict[str, int] = {}

    def unique_name(folder: str, name: str) -> str:
        """
        同一フォルダ内での重複を検知し、_2, _3 ... と小さい連番でリネームする。
        """
        base_path = f"{folder}/{name}"
        if base_path not in name_counts:
            name_counts[base_path] = 1
            return base_path

        stem, dot, ext = name.partition(".")
        count = name_counts[base_path]
        while True:
            count += 1
            candidate_name = f"{stem}_{count}{dot}{ext}" if dot else f"{stem}_{count}"
            candidate_path = f"{folder}/{candidate_name}"
            if candidate_path not in name_counts:
                name_counts[base_path] = count
                name_counts[candidate_path] = 1
                return candidate_path

    for res in results:
        if isinstance(res, Exception):
            print(f"[attachments_zip] download error: {res}")
            raise HTTPException(502, {"reason": "download_failed", "detail": str(res)})

    buf = io.BytesIO()
    zf = zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED)
    name_counts.clear()
    try:
        for idx, att in enumerate(attachments, 1):
            name = att.fileName or _filename_from_url(att.url) or f"attachment_{idx}"
            folder = att.farmName or att.farmUuid or "farm"
            path = unique_name(folder, name)
            res = results[idx - 1]
            if isinstance(res, Exception):
                raise res
            _, content = res
            zf.writestr(path, content)
    except Exception as exc:  # pylint: disable=broad-except
        zf.close()
        print(f"[attachments_zip] zip write error: {exc}")
        raise HTTPException(500, {"reason": "zip_write_failed", "detail": str(exc)})

    zf.close()
    data = buf.getvalue()
    headers = {
        "Content-Disposition": _content_disposition(zip_name),
        "Content-Length": str(len(data)),
    }
    return Response(content=data, media_type="application/zip", headers=headers)


@api_app.post("/weather-by-field")
async def weather_by_field(req: WeatherByFieldReq):
    """
    指定された Field UUID に基づいて、各種の天気情報を取得する。
    """
    if req.from_date and req.till_date:
        from_date = req.from_date
        till_date = req.till_date
    else:
        # 日付範囲を今日から10日先に設定（従来の挙動）
        JST = timezone(timedelta(hours=9))
        now_jst = datetime.now(JST)
        from_dt_utc = now_jst.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(hours=9)
        till_dt_utc = (now_jst + timedelta(days=10)).replace(hour=23, minute=59, second=59, microsecond=999000) - timedelta(hours=9)
        from_date = from_dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        till_date = till_dt_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    variables = {
        "fieldUuid": req.field_uuid,
        "fromDate": from_date,
        "tillDate": till_date,
    }

    # 各クエリのペイロードを作成
    payloads = {
        "daily": make_payload("WeatherHistoricForecastDaily", WEATHER_HISTORIC_FORECAST_DAILY, variables),
        "climatology": make_payload("WeatherClimatologyDaily", WEATHER_CLIMATOLOGY_DAILY, variables),
        "spray": make_payload("SprayWeather", SPRAY_WEATHER, variables),
        "hourly": make_payload("WeatherHistoricForecastHourly", WEATHER_HISTORIC_FORECAST_HOURLY, variables),
    }

    # APIを並列で呼び出す
    tasks = {key: call_graphql(payload, req.login_token, req.api_token) for key, payload in payloads.items()}
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    # エラーチェック
    errors = [res for res in results if isinstance(res, Exception) or res.get("ok") is False]
    if errors:
        first_error = errors[0]
        if isinstance(first_error, Exception):
            raise HTTPException(status_code=500, detail=str(first_error))
        else:
            return JSONResponse(status_code=first_error.get("status", 500), content=first_error)

    # 結果をマージ
    daily_res, climatology_res, spray_res, hourly_res = results
    merged_data = {
        **daily_res['response']['data']['fieldV2'],
        **climatology_res['response']['data']['fieldV2'],
        **spray_res['response']['data']['fieldV2'],
        **hourly_res['response']['data']['fieldV2'],
    }

    # フロントエンドに返す最終的なレスポンスを構築
    out = {
        "ok": True,
        "status": 200,
        "source": "api",
        "request": {
            "url": daily_res["request"]["url"],
            "headers": daily_res["request"]["headers"],
            "payloads": payloads,
        },
        "response": {
            "data": {
                "fieldV2": merged_data
            }
        },
    }

    if not req.includeTokens:
        out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"

    return JSONResponse(out)


@api_app.post("/crop-protection-products")
async def crop_protection_products(req: CropProtectionProductsReq):
    """
    作物・国・農場に紐づく農薬マスターデータを取得する。
    categories.name が HERBICIDE のものが除草剤。
    """
    variables = {
        "farmUuids": req.farm_uuids,
        "countryUuid": req.country_uuid,
        "cropUuid": req.crop_uuid,
        "taskTypeCode": req.task_type_code,
    }
    payload = make_payload("CropProtectionTaskCreationProducts", CROP_PROTECTION_TASK_CREATION_PRODUCTS, variables)
    result = await call_graphql(payload, req.login_token, req.api_token)
    if isinstance(result, Exception):
        raise HTTPException(status_code=500, detail=str(result))
    if result.get("ok") is False:
        return JSONResponse(status_code=result.get("status", 500), content=result)
    return JSONResponse(result)


@api_app.post("/crop-protection-products/bulk")
async def crop_protection_products_bulk(req: CropProtectionProductsBulkReq):
    """
    複数 cropUuid をまとめて農薬マスターデータを取得する。
    categories.name が HERBICIDE のものが除草剤。
    """
    async def fetch_for_crop(crop_uuid: str):
        variables = {
            "farmUuids": req.farm_uuids,
            "countryUuid": req.country_uuid,
            "cropUuid": crop_uuid,
            "taskTypeCode": req.task_type_code,
        }
        payload = make_payload("CropProtectionTaskCreationProducts", CROP_PROTECTION_TASK_CREATION_PRODUCTS, variables)
        result = await call_graphql(payload, req.login_token, req.api_token)
        return crop_uuid, result

    tasks = [fetch_for_crop(crop_uuid) for crop_uuid in req.crop_uuids]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    out: Dict[str, Any] = {}
    for crop_uuid, result in results:
        if isinstance(result, Exception):
            out[crop_uuid] = {"ok": False, "detail": str(result)}
            continue
        if result.get("ok") is False:
            out[crop_uuid] = result
            continue
        out[crop_uuid] = result.get("response", {}).get("data", {}).get("productsV2", [])

    return JSONResponse({"ok": True, "items": out})


@api_app.post("/masterdata/crops")
async def masterdata_crops(req: MasterdataCropsReq):
    headers = {
        "Accept": "application/json",
        "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
        "X-Login-Token": req.login_token,
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
    }
    url = "https://fm-api.xarvio.com/api/md2/crops"

    async def fetch(locale: Optional[str], client: httpx.AsyncClient) -> httpx.Response:
        params = {}
        if locale:
            params["locale"] = locale
        return await client.get(url, headers=headers, params=params)

    primary_locale = req.locale or "EN-GB"
    fallbacks = build_locale_candidates(primary_locale)
    last_resp: Optional[httpx.Response] = None
    async with httpx.AsyncClient(timeout=30) as client:
        for locale in fallbacks:
            try:
                resp = await fetch(locale, client)
            except httpx.HTTPError as exc:
                raise HTTPException(502, {"reason": "xarvio request error", "detail": str(exc)})

            last_resp = resp
            if resp.status_code == 200:
                break
            if locale and resp.status_code == 400 and "locale" in resp.text.lower():
                # 試した locale が不正な場合は次の候補へ
                continue
            # それ以外のステータスはループを抜ける
            break

    if not last_resp:
        raise HTTPException(502, {"reason": "xarvio request error", "detail": "no response"})

    if last_resp.status_code >= 400:
        raise HTTPException(
            last_resp.status_code,
            {
                "reason": "xarvio http error",
                "status": last_resp.status_code,
                "url": url,
                "text": last_resp.text[:500],
            },
        )

    try:
        data = last_resp.json()
    except Exception as exc:
        raise HTTPException(
            502,
            {
                "reason": "invalid xarvio json",
                "detail": str(exc),
                "url": url,
                "raw": last_resp.text[:200],
            },
        )

    return JSONResponse({"ok": True, "items": data})


@api_app.post("/cross-farm-dashboard/_search")
async def cross_farm_dashboard_search(req: CrossFarmDashboardSearchReq):
    """
    Cross-Farm Dashboard の OpenSearch エンドポイントへプロキシする。
    """
    url = "https://fm-api.xarvio.com/api/cross-farm-dashboard/_search"
    params: Dict[str, Any] = {}
    if req.includeClosedCropSeasons:
        params["includeClosedCropSeasons"] = "true"

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
        "X-Login-Token": req.login_token,
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
        "User-Agent": "xhf-app/1.0",
    }

    try:
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.post(url, params=params, headers=headers, json=req.body)
    except httpx.RequestError as exc:
        raise HTTPException(502, {"reason": "crossfarm request error", "detail": str(exc)})

    out: Dict[str, Any] = {
        "ok": resp.status_code < 400,
        "status": resp.status_code,
        "reason": resp.reason_phrase,
        "request": {
            "url": str(resp.request.url) if resp and resp.request else url,
            "headers": {**headers, "Cookie": "MASKED"},
            "payload": req.body,
            "params": params,
        },
    }

    try:
        out["response"] = resp.json()
    except Exception:
        out["response_text"] = resp.text[:4000]

    if not req.includeTokens:
        if out.get("request", {}).get("headers", {}).get("Cookie"):
            out["request"]["headers"]["Cookie"] = "LOGIN_TOKEN=***; DF_TOKEN=***"

    if resp.status_code >= 400:
        raise HTTPException(resp.status_code, out)

    return JSONResponse(out)


@api_app.post("/masterdata/varieties")
async def masterdata_varieties(req: MasterdataVarietiesReq):
    headers = {
        "Accept": "application/json",
        "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
    }
    url = "https://fm-api.xarvio.com/api/md2/varieties"

    country_code = (req.countryCode or "").strip().upper()

    async def fetch(locale: Optional[str], client: httpx.AsyncClient) -> httpx.Response:
        params = {
            "cropUuid": req.cropUuid,
        }
        if country_code:
            params["countryCode"] = country_code
        if locale:
            params["locale"] = locale
        return await client.get(url, headers=headers, params=params)

    primary_locale = req.locale or "EN-GB"
    fallbacks = build_locale_candidates(primary_locale)
    last_resp: Optional[httpx.Response] = None
    async with httpx.AsyncClient(timeout=30) as client:
        for locale in fallbacks:
            try:
                resp = await fetch(locale, client)
            except httpx.HTTPError as exc:
                raise HTTPException(502, {"reason": "xarvio request error", "detail": str(exc)})

            last_resp = resp
            if resp.status_code == 200:
                break
            if locale and resp.status_code == 400 and "locale" in resp.text.lower():
                continue
            break

    if not last_resp:
        raise HTTPException(502, {"reason": "xarvio request error", "detail": "no response"})

    if last_resp.status_code >= 400:
        raise HTTPException(
            last_resp.status_code,
            {
                "reason": "xarvio http error",
                "status": last_resp.status_code,
                "url": url,
                "text": last_resp.text[:500],
            },
        )

    try:
        data = last_resp.json()
    except Exception as exc:
        raise HTTPException(
            502,
            {
                "reason": "invalid xarvio json",
                "detail": str(exc),
                "url": url,
                "raw": last_resp.text[:200],
            },
        )

    return JSONResponse({"ok": True, "items": data})


@api_app.post("/masterdata/partner-tillages")
async def masterdata_partner_tillages(req: MasterdataPartnerTillagesReq):
    headers = {
        "Accept": "application/json",
        "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
    }
    url = "https://fm-api.xarvio.com/api/master-data/partners/tillages"

    async def fetch(locale: Optional[str], client: httpx.AsyncClient) -> httpx.Response:
        params = {}
        if locale:
            params["locale"] = locale
        return await client.get(url, headers=headers, params=params)

    fallbacks = build_locale_candidates(req.locale)
    last_resp: Optional[httpx.Response] = None
    async with httpx.AsyncClient(timeout=30) as client:
        for locale in fallbacks:
            try:
                resp = await fetch(locale, client)
            except httpx.HTTPError as exc:
                raise HTTPException(502, {"reason": "xarvio request error", "detail": str(exc)})

            last_resp = resp
            if resp.status_code == 200:
                break
            if locale and resp.status_code == 400 and "locale" in resp.text.lower():
                continue
            break

    if not last_resp:
        raise HTTPException(502, {"reason": "xarvio request error", "detail": "no response"})

    if last_resp.status_code >= 400:
        raise HTTPException(
            last_resp.status_code,
            {
                "reason": "xarvio http error",
                "status": last_resp.status_code,
                "url": url,
                "text": last_resp.text[:500],
            },
        )

    try:
        data = last_resp.json()
    except Exception as exc:
        raise HTTPException(
            502,
            {
                "reason": "invalid xarvio json",
                "detail": str(exc),
                "url": url,
                "raw": last_resp.text[:200],
            },
        )

    return JSONResponse({"ok": True, "items": data})


@api_app.post("/masterdata/tillage-systems")
async def masterdata_tillage_systems(req: MasterdataTillageSystemsReq):
    headers = {
        "Accept": "application/json",
        "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
    }
    url = "https://fm-api.xarvio.com/api/md2/tillage-systems"

    async def fetch(locale: Optional[str], client: httpx.AsyncClient) -> httpx.Response:
        params = {}
        if locale:
            params["locale"] = locale
        return await client.get(url, headers=headers, params=params)

    fallbacks = build_locale_candidates(req.locale)
    last_resp: Optional[httpx.Response] = None
    async with httpx.AsyncClient(timeout=30) as client:
        for locale in fallbacks:
            try:
                resp = await fetch(locale, client)
            except httpx.HTTPError as exc:
                raise HTTPException(502, {"reason": "xarvio request error", "detail": str(exc)})

            last_resp = resp
            if resp.status_code == 200:
                break
            if locale and resp.status_code == 400 and "locale" in resp.text.lower():
                continue
            break

    if not last_resp:
        raise HTTPException(502, {"reason": "xarvio request error", "detail": "no response"})

    if last_resp.status_code >= 400:
        raise HTTPException(
            last_resp.status_code,
            {
                "reason": "xarvio http error",
                "status": last_resp.status_code,
                "url": url,
                "text": last_resp.text[:500],
            },
        )

    try:
        data = last_resp.json()
    except Exception as exc:
        raise HTTPException(
            502,
            {
                "reason": "invalid xarvio json",
                "detail": str(exc),
                "url": url,
                "raw": last_resp.text[:200],
            },
        )

    return JSONResponse({"ok": True, "items": data})


@api_app.post("/crop-seasons")
async def create_crop_seasons(req: CropSeasonCreateReq):
    if not req.payloads:
        return JSONResponse({"ok": True, "results": []})

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Cookie": f"LOGIN_TOKEN={req.login_token}; DF_TOKEN={req.api_token}",
        "Authorization": f"Bearer {req.api_token}",
        "X-Login-Token": req.login_token,
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
    }
    url = "https://fm-api.xarvio.com/api/farms/v2/crop-seasons"

    results: List[Dict[str, Any]] = []
    async with httpx.AsyncClient(timeout=45) as client:
        for payload in req.payloads:
            body = payload.dict(exclude_none=True)
            try:
                resp = await client.post(url, headers=headers, json=body)
            except httpx.HTTPError as exc:
                raise HTTPException(502, {"reason": "xarvio request error", "detail": str(exc)})

            if resp.status_code >= 400:
                detail_text = resp.text[:500]
                raise HTTPException(
                    resp.status_code,
                    {
                        "reason": "xarvio http error",
                        "status": resp.status_code,
                        "url": url,
                        "text": detail_text,
                        "payload": body,
                    },
                )

            try:
                resp_json = resp.json()
            except ValueError:
                resp_json = {"raw": resp.text[:200]}

            results.append({"status": resp.status_code, "body": resp_json})

    return JSONResponse({"ok": True, "results": results})


# ---------------------------
#        Cache Endpoints
# ---------------------------
@api_app.get("/cache/graphql/last")
async def cache_graphql_last():
    """
    直近に保存した GraphQL レスポンス（1件）を返す。
    保存前なら {ok: False, message: "..."} を返す。
    """
    resp = get_last_response()
    return resp or {"ok": False, "message": "no response cached yet"}

@api_app.get("/cache/graphql/op/{operation_name}")
async def cache_graphql_by_operation(operation_name: str = Path(..., min_length=1)):
    """
    operationName ごとの直近レスポンスを返す。
    例: /cache/graphql/op/FarmsOverview
    """
    resp = get_by_operation(operation_name)
    return resp or {"ok": False, "message": f"no cached response for operation '{operation_name}'"}

@api_app.delete("/cache/graphql")
@api_app.post("/cache/graphql/clear")  # POSTメソッドを追加
async def cache_graphql_clear():
    """
    キャッシュ全消去
    """
    clear_cache()
    return {"ok": True, "cleared": True}


def _response_to_json(resp: Response) -> Dict[str, Any]:
    raw = getattr(resp, "body", b"") or b""
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
    return json.loads(text) if text else {}


def _full_name(person: Any) -> str:
    if not isinstance(person, dict):
        return ""
    first = str(person.get("firstName") or "").strip()
    last = str(person.get("lastName") or "").strip()
    full = f"{last} {first}".strip()
    return full or str(person.get("email") or "").strip()


def _task_sort_time(task: Dict[str, Any]) -> float:
    for key in ("executionDate", "plannedDate"):
        raw = task.get(key)
        if not raw:
            continue
        try:
            return datetime.fromisoformat(str(raw).replace("Z", "+00:00")).timestamp()
        except Exception:
            continue
    return float("inf")


def _extract_snapshot_rows(
    *,
    run_id: str,
    snapshot_date: date,
    fields: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    field_rows: List[Dict[str, Any]] = []
    task_rows: List[Dict[str, Any]] = []

    for field in fields:
        if not isinstance(field, dict):
            continue
        field_uuid = str(field.get("uuid") or "")
        if not field_uuid:
            continue
        farm = field.get("farmV2") if isinstance(field.get("farmV2"), dict) else {}
        farm_uuid = str((farm or {}).get("uuid") or "")
        farm_name = str((farm or {}).get("name") or "")
        owner = (farm or {}).get("owner") or {}
        user_name = _full_name(owner)
        field_name = str(field.get("name") or "")
        area_m2 = field.get("area")
        crop_seasons = field.get("cropSeasonsV2") or []

        if not crop_seasons:
            field_rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "run_id": run_id,
                    "field_uuid": field_uuid,
                    "season_uuid": "",
                    "field_name": field_name,
                    "farm_uuid": farm_uuid,
                    "farm_name": farm_name,
                    "user_name": user_name,
                    "crop_name": "",
                    "variety_name": "",
                    "area_m2": area_m2,
                    "bbch_index": "",
                    "bbch_scale": "",
                }
            )
            continue

        for season in crop_seasons:
            if not isinstance(season, dict):
                continue
            season_uuid = str(season.get("uuid") or "")
            crop = season.get("crop") or {}
            variety = season.get("variety") or {}
            active = season.get("activeGrowthStage") or {}
            crop_name = str((crop or {}).get("name") or "")
            variety_name = str((variety or {}).get("name") or "")
            bbch_index = str((active or {}).get("index") or "")
            bbch_scale = str((active or {}).get("scale") or "")

            field_rows.append(
                {
                    "snapshot_date": snapshot_date,
                    "run_id": run_id,
                    "field_uuid": field_uuid,
                    "season_uuid": season_uuid,
                    "field_name": field_name,
                    "farm_uuid": farm_uuid,
                    "farm_name": farm_name,
                    "user_name": user_name,
                    "crop_name": crop_name,
                    "variety_name": variety_name,
                    "area_m2": area_m2,
                    "bbch_index": bbch_index,
                    "bbch_scale": bbch_scale,
                }
            )

            task_lists: List[tuple[str, str, List[Dict[str, Any]]]] = []
            task_lists.append(("Harvest", "収穫", list(season.get("harvests") or [])))
            task_lists.append(("Spraying", "防除", list(season.get("sprayingsV2") or [])))
            task_lists.append(("CropEstablishment", "播種", list(season.get("cropEstablishments") or [])))
            task_lists.append(("WaterManagement", "水管理", list(season.get("waterManagementTasks") or [])))
            task_lists.append(("Scouting", "生育調査", list(season.get("scoutingTasks") or [])))
            task_lists.append(("LandPreparation", "土づくり", list(season.get("landPreparations") or [])))
            task_lists.append(("SeedTreatment", "種子処理", list(season.get("seedTreatmentTasks") or [])))
            task_lists.append(("SeedBoxTreatment", "育苗箱処理", list(season.get("seedBoxTreatments") or [])))

            for task_type, task_name, tasks in task_lists:
                if not tasks:
                    continue
                sorted_tasks = sorted([t for t in tasks if isinstance(t, dict)], key=_task_sort_time)
                for idx, task in enumerate(sorted_tasks):
                    task_uuid = str(task.get("uuid") or f"{field_uuid}:{season_uuid}:{task_type}:{idx + 1}")
                    planned_date = task.get("plannedDate")
                    execution_date = task.get("executionDate")
                    task_date_raw = execution_date or planned_date
                    task_date = None
                    if task_date_raw:
                        try:
                            task_date = datetime.fromisoformat(str(task_date_raw).replace("Z", "+00:00")).date()
                        except Exception:
                            task_date = None

                    assignee_name = _full_name(task.get("assignee") or {})
                    status = str(task.get("state") or "")
                    product_names: List[str] = []
                    dosage_parts: List[str] = []

                    dosed = task.get("dosedMap") or {}
                    recipes = dosed.get("recipeV2") if isinstance(dosed, dict) else None
                    if isinstance(recipes, list):
                        for recipe in recipes:
                            if not isinstance(recipe, dict):
                                continue
                            r_name = str(recipe.get("name") or "")
                            if r_name:
                                product_names.append(r_name)
                            total = recipe.get("totalApplication")
                            unit = str(recipe.get("unit") or "")
                            if total is not None:
                                dosage_parts.append(f"{total}{unit}")
                    if not product_names:
                        products = task.get("products") or []
                        if isinstance(products, list):
                            for prod in products:
                                if not isinstance(prod, dict):
                                    continue
                                p_name = str(prod.get("name") or prod.get("uuid") or "")
                                if p_name:
                                    product_names.append(p_name)
                                app_rate = prod.get("applicationRate")
                                unit = str(prod.get("unit") or "")
                                if app_rate is not None:
                                    dosage_parts.append(f"{app_rate}{unit}")

                    task_rows.append(
                        {
                            "snapshot_date": snapshot_date,
                            "run_id": run_id,
                            "task_uuid": task_uuid,
                            "field_uuid": field_uuid,
                            "season_uuid": season_uuid,
                            "farm_uuid": farm_uuid,
                            "farm_name": farm_name,
                            "field_name": field_name,
                            "user_name": user_name,
                            "task_name": task_name,
                            "task_type": task_type,
                            "task_date": task_date,
                            "planned_date": planned_date,
                            "execution_date": execution_date,
                            "status": status,
                            "assignee_name": assignee_name,
                            "product": " / ".join(product_names),
                            "dosage": " / ".join(dosage_parts),
                            "bbch_index": bbch_index,
                            "bbch_scale": bbch_scale,
                            "occurrence": idx + 1,
                        }
                    )

    return {"fields": field_rows, "tasks": task_rows}


@api_app.post("/jobs/hfr-snapshot")
async def jobs_hfr_snapshot(
    req: HfrSnapshotJobReq,
    x_job_secret: Optional[str] = Header(default=None, alias="X-Job-Secret"),
):
    expected_secret = os.getenv("SNAPSHOT_JOB_SECRET", "").strip()
    if expected_secret and x_job_secret != expected_secret:
        raise HTTPException(401, {"reason": "unauthorized_job"})

    snapshot_date = datetime.now(timezone(timedelta(hours=9))).date()
    run_id = f"hfr-{snapshot_date.isoformat()}-{uuid4().hex[:8]}"
    farms_scanned = 0
    farms_matched = 0
    fields_saved = 0
    tasks_saved = 0
    should_persist = not req.dryRun
    started_at = time.perf_counter()

    def _progress(message: str):
        elapsed = time.perf_counter() - started_at
        print(f"[HFR_SNAPSHOT][{run_id}] +{elapsed:.1f}s {message}", flush=True)

    _progress(f"job started dryRun={req.dryRun} suffix={req.suffix} language={req.languageCode}")

    if should_persist:
        try:
            hfr_snapshot_store.ensure_schema()
        except Exception as exc:
            raise HTTPException(500, {"reason": "snapshot_store_not_ready", "detail": str(exc)})
        hfr_snapshot_store.start_run(run_id, snapshot_date, status="running", message="job started")
    try:
        email = os.getenv("SNAPSHOT_USER_EMAIL", "").strip()
        password = os.getenv("SNAPSHOT_USER_PASSWORD", "").strip()
        if not email or not password:
            raise HTTPException(500, {"reason": "snapshot_credentials_missing"})

        _progress("auth: gigya login started")
        login = await gigya_login_impl(email, password)
        _progress("auth: gigya login completed")
        four = FourValues(
            login_token=login.get("login_token") or "",
            gigya_uuid=login.get("gigya_uuid") or "",
            gigya_uuid_signature=login.get("gigya_uuid_signature") or "",
            gigya_signature_timestamp=login.get("gigya_signature_timestamp") or "",
        )
        _progress("auth: xarvio token started")
        api_token = await get_api_token_impl(four)
        login_token = four.login_token
        _progress("auth: xarvio token completed")

        _progress("step1: farms overview started")
        farms_out = await call_graphql(make_payload("FarmsOverview", FARMS_OVERVIEW), login_token, api_token)
        farms_data = (((farms_out.get("response") or {}).get("data") or {}).get("farms") or [])
        all_farm_uuids = [str(f.get("uuid")) for f in farms_data if isinstance(f, dict) and f.get("uuid")]
        farms_scanned = len(all_farm_uuids)
        _progress(f"step1: farms overview completed farms_scanned={farms_scanned}")

        _progress("step2: hfr candidate scan started")
        cand_resp = await farms_hfr_candidates(
            HfrFarmCandidatesReq(
                login_token=login_token,
                api_token=api_token,
                farm_uuids=all_farm_uuids,
                suffix=req.suffix,
                includeTokens=False,
            )
        )
        cand_json = _response_to_json(cand_resp)
        matched_farm_uuids = (((cand_json.get("response") or {}).get("data") or {}).get("matchedFarmUuids") or [])
        matched_farm_uuids = [str(u) for u in matched_farm_uuids if str(u)]
        farms_matched = len(matched_farm_uuids)
        _progress(f"step2: hfr candidate scan completed farms_matched={farms_matched}")

        if matched_farm_uuids:
            chunk_size = max(1, min(int(os.getenv("HFR_SNAPSHOT_CHUNK_SIZE", "5")), 100))
            chunk_attempts = max(1, min(int(os.getenv("HFR_SNAPSHOT_CHUNK_ATTEMPTS", "6")), 8))
            retry_backoff_sec = max(0.1, float(os.getenv("HFR_SNAPSHOT_CHUNK_RETRY_BACKOFF_SEC", "3.0")))
            require_complete_chunk = str(os.getenv("HFR_SNAPSHOT_REQUIRE_COMPLETE_CHUNK", "1")).lower() in (
                "1",
                "true",
                "yes",
            )
            suffix = str(req.suffix or "").strip()
            suffix_pattern = re.compile(rf"{re.escape(suffix)}$", re.IGNORECASE) if suffix else None
            collected_fields: List[Dict[str, Any]] = []
            farm_chunks = _chunk_list(matched_farm_uuids, chunk_size)
            total_chunks = len(farm_chunks)
            _progress(
                f"step3: task fetch started chunk_size={chunk_size} total_chunks={total_chunks} "
                f"attempts={chunk_attempts} require_complete_chunk={require_complete_chunk}"
            )

            for idx, farm_chunk in enumerate(farm_chunks, start=1):
                _progress(f"step3: chunk {idx}/{total_chunks} started farms={len(farm_chunk)}")
                combined_json = None
                last_http_exc: Optional[HTTPException] = None
                last_exc: Optional[Exception] = None
                for attempt in range(1, chunk_attempts + 1):
                    try:
                        combined_resp = await combined_field_data_tasks(
                            CombinedFieldDataTasksReq(
                                login_token=login_token,
                                api_token=api_token,
                                farm_uuids=farm_chunk,
                                languageCode=req.languageCode,
                                withBoundary=False,
                                withCropSeasonsV2=True,
                                withHarvests=True,
                                withCropEstablishments=True,
                                withLandPreparations=True,
                                withDroneFlights=False,
                                withSeedTreatments=True,
                                withSeedBoxTreatments=True,
                                withSmartSprayingTasks=False,
                                withWaterManagementTasks=True,
                                withScoutingTasks=True,
                                withObservations=False,
                                withSprayingsV2=True,
                                withSoilSamplingTasks=False,
                                forceRefresh=True,
                            )
                        )
                        combined_json = _response_to_json(combined_resp)
                        if not isinstance(combined_json, dict):
                            raise HTTPException(502, {"reason": "snapshot_chunk_invalid_response"})
                        if combined_json.get("ok", True) is False:
                            status_code = int(combined_json.get("status") or 502)
                            raise HTTPException(
                                status_code,
                                {
                                    "reason": "snapshot_chunk_upstream_failed",
                                    "chunk_index": idx,
                                    "chunk_total": total_chunks,
                                    "chunk_farm_count": len(farm_chunk),
                                    "upstream": combined_json,
                                },
                            )

                        chunk_fields = (((combined_json.get("response") or {}).get("data") or {}).get("fieldsV2") or [])
                        if not isinstance(chunk_fields, list):
                            raise HTTPException(502, {"reason": "snapshot_chunk_missing_fields"})

                        covered_farms = set(_extract_farm_uuids_from_fields(chunk_fields))
                        missing_farms = [u for u in farm_chunk if u not in covered_farms]
                        if missing_farms and require_complete_chunk:
                            raise HTTPException(
                                502,
                                {
                                    "reason": "snapshot_chunk_incomplete",
                                    "chunk_index": idx,
                                    "chunk_total": total_chunks,
                                    "missing_farm_uuids": missing_farms[:50],
                                    "covered_farm_count": len(covered_farms),
                                    "requested_farm_count": len(farm_chunk),
                                },
                            )
                        if missing_farms:
                            _progress(
                                f"step3: chunk {idx}/{total_chunks} warning missing_farms={len(missing_farms)}"
                            )
                        break
                    except HTTPException as exc:
                        last_http_exc = exc
                        retryable_status = exc.status_code in (429, 500, 502, 503, 504)
                        if attempt >= chunk_attempts or not retryable_status:
                            raise
                        sleep_s = retry_backoff_sec * attempt + random.uniform(0.0, 0.6)
                        _progress(
                            f"step3: chunk {idx}/{total_chunks} retry {attempt}/{chunk_attempts - 1} "
                            f"status={exc.status_code} wait={sleep_s:.1f}s"
                        )
                        await asyncio.sleep(sleep_s)
                    except Exception as exc:  # pylint: disable=broad-except
                        last_exc = exc
                        if attempt >= chunk_attempts:
                            raise
                        sleep_s = retry_backoff_sec * attempt + random.uniform(0.0, 0.6)
                        _progress(
                            f"step3: chunk {idx}/{total_chunks} retry {attempt}/{chunk_attempts - 1} "
                            f"error={exc.__class__.__name__} wait={sleep_s:.1f}s"
                        )
                        await asyncio.sleep(sleep_s)

                if combined_json is None:
                    if last_http_exc is not None:
                        raise last_http_exc
                    if last_exc is not None:
                        raise last_exc
                    raise HTTPException(500, {"reason": "snapshot_chunk_no_response"})

                fields = (((combined_json.get("response") or {}).get("data") or {}).get("fieldsV2") or [])
                if isinstance(fields, list):
                    collected_fields.extend([f for f in fields if isinstance(f, dict)])
                    _progress(
                        f"step3: chunk {idx}/{total_chunks} completed "
                        f"fields_in_chunk={len(fields)} collected_fields={len(collected_fields)}"
                    )
                else:
                    _progress(f"step3: chunk {idx}/{total_chunks} completed with no fieldsV2 list")

            _progress(f"step3: fetch completed collected_fields={len(collected_fields)}")
            filtered_fields = collected_fields
            if suffix_pattern is not None:
                filtered_fields = []
                for field in collected_fields:
                    name = str((field or {}).get("name") or "").strip()
                    if not name:
                        continue
                    if suffix_pattern.search(name):
                        filtered_fields.append(field)
                _progress(
                    f"step3.5: suffix filter suffix={suffix} "
                    f"before={len(collected_fields)} after={len(filtered_fields)}"
                )

            extracted = _extract_snapshot_rows(run_id=run_id, snapshot_date=snapshot_date, fields=filtered_fields)
            _progress(
                f"step4: extraction completed field_rows={len(extracted['fields'])} "
                f"task_rows={len(extracted['tasks'])}"
            )
            if not req.dryRun:
                fields_saved = hfr_snapshot_store.upsert_fields(extracted["fields"])
                tasks_saved = hfr_snapshot_store.upsert_tasks(extracted["tasks"])
                pruned = hfr_snapshot_store.prune_snapshot_date(snapshot_date, run_id)
                _progress(f"step5: persisted fields_saved={fields_saved} tasks_saved={tasks_saved}")
                _progress(
                    f"step5.5: pruned old runs runs_deleted={pruned.get('runs_deleted', 0)} "
                    f"fields_deleted={pruned.get('fields_deleted', 0)} "
                    f"tasks_deleted={pruned.get('tasks_deleted', 0)}"
                )
            else:
                fields_saved = len(extracted["fields"])
                tasks_saved = len(extracted["tasks"])
                _progress(f"step5: dry run counts fields={fields_saved} tasks={tasks_saved}")
        else:
            _progress("step3: skipped (no matched farms)")

        if should_persist:
            hfr_snapshot_store.finish_run(
                run_id,
                status="success",
                message="snapshot completed",
                farms_scanned=farms_scanned,
                farms_matched=farms_matched,
                fields_saved=fields_saved,
                tasks_saved=tasks_saved,
            )
        _progress(
            f"job completed farms_scanned={farms_scanned} farms_matched={farms_matched} "
            f"fields={fields_saved} tasks={tasks_saved}"
        )
        return {
            "ok": True,
            "run_id": run_id,
            "snapshot_date": str(snapshot_date),
            "farms_scanned": farms_scanned,
            "farms_matched": farms_matched,
            "fields_saved": fields_saved,
            "tasks_saved": tasks_saved,
            "dry_run": req.dryRun,
        }
    except HTTPException as exc:
        if should_persist:
            hfr_snapshot_store.finish_run(
                run_id,
                status="failed",
                message=str(exc.detail),
                farms_scanned=farms_scanned,
                farms_matched=farms_matched,
                fields_saved=fields_saved,
                tasks_saved=tasks_saved,
            )
        _progress(f"job failed (http) detail={exc.detail}")
        raise
    except Exception as exc:
        if should_persist:
            hfr_snapshot_store.finish_run(
                run_id,
                status="failed",
                message=str(exc),
                farms_scanned=farms_scanned,
                farms_matched=farms_matched,
                fields_saved=fields_saved,
                tasks_saved=tasks_saved,
            )
        _progress(f"job failed (exception) type={exc.__class__.__name__} detail={str(exc)}")
        raise HTTPException(500, {"reason": "snapshot_failed", "detail": str(exc)})


@api_app.get("/hfr-snapshots")
async def hfr_snapshots(snapshot_date: Optional[str] = None, farm_uuid: Optional[str] = None, limit: int = 2000):
    try:
        hfr_snapshot_store.ensure_schema()
        day = datetime.strptime(snapshot_date, "%Y-%m-%d").date() if snapshot_date else datetime.now(timezone(timedelta(hours=9))).date()
        data = hfr_snapshot_store.fetch_snapshot(day, farm_uuid=farm_uuid, limit=max(1, min(limit, 10000)))
        return {
            "ok": True,
            "snapshot_date": str(day),
            "run": data.get("run"),
            "fields": data.get("fields") or [],
            "tasks": data.get("tasks") or [],
        }
    except Exception as exc:
        raise HTTPException(500, {"reason": "snapshot_read_failed", "detail": str(exc)})


@api_app.get("/hfr-snapshots/compare")
async def hfr_snapshots_compare(from_date: str, to_date: str):
    try:
        hfr_snapshot_store.ensure_schema()
        from_day = datetime.strptime(from_date, "%Y-%m-%d").date()
        to_day = datetime.strptime(to_date, "%Y-%m-%d").date()
        data = hfr_snapshot_store.compare_snapshots(from_day, to_day)
        return {"ok": True, **data}
    except Exception as exc:
        raise HTTPException(500, {"reason": "snapshot_compare_failed", "detail": str(exc)})


app = FastAPI()


@app.get("/healthz")
async def root_healthz():
    return {"ok": True}


app.mount("/api", api_app)


def _resolve_web_dist_dir() -> Optional[FilePath]:
    env_dir = os.getenv("WEB_DIST_DIR")
    candidates: List[FilePath] = []
    if env_dir:
        candidates.append(FilePath(env_dir))

    # Docker image (apps/api/Dockerfile) copies frontend dist here.
    candidates.append(FilePath(__file__).resolve().parent / "web_dist")
    # Local development from repository root.
    candidates.append(FilePath(__file__).resolve().parents[1] / "web" / "dist")

    for candidate in candidates:
        if candidate.is_dir() and (candidate / "index.html").is_file():
            return candidate
    return None


WEB_DIST_DIR = _resolve_web_dist_dir()


@app.get("/", include_in_schema=False)
async def serve_spa_index():
    if WEB_DIST_DIR:
        return FileResponse(WEB_DIST_DIR / "index.html")
    return JSONResponse(
        status_code=404,
        content={
            "ok": False,
            "reason": "frontend_not_built",
            "detail": "Frontend dist not found. Build apps/web and provide WEB_DIST_DIR.",
        },
    )


@app.get("/{full_path:path}", include_in_schema=False)
async def serve_spa_assets(full_path: str):
    if not WEB_DIST_DIR:
        raise HTTPException(404, {"reason": "not_found"})

    candidate = (WEB_DIST_DIR / full_path).resolve()
    base = WEB_DIST_DIR.resolve()
    if base not in candidate.parents and candidate != base:
        raise HTTPException(400, {"reason": "invalid_path"})

    if candidate.is_file():
        return FileResponse(candidate)
    return FileResponse(WEB_DIST_DIR / "index.html")
