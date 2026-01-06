# apps/api/services/graphql_client.py
import httpx
from fastapi import HTTPException
from typing import Dict, Any
from settings import settings
from services.cache import save_response  # ★ 追加

async def call_graphql(payload: Dict[str, Any], login_token: str, api_token: str) -> Dict[str, Any]:
    endpoint = settings.GRAPHQL_ENDPOINT
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
        "User-Agent": "xhf-app/1.0",
    }

    try:
        timeout = httpx.Timeout(connect=10.0, read=60.0, write=60.0, pool=10.0)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as c:
            r = await c.post(endpoint, json=payload, headers=headers)
    except httpx.HTTPError as e:
        # 502 で呼び出し側へ返し、詳細を付与してデバッグしやすくする
        detail = {
            "reason": "graphql request error",
            "detail": str(e) or repr(e),
            "exception_type": e.__class__.__name__,
        }
        req = getattr(e, "request", None)
        if req is not None:
            detail["request_url"] = str(getattr(req, "url", endpoint))
            detail["method"] = getattr(req, "method", "POST")
        if isinstance(e, httpx.TimeoutException):
            detail["kind"] = "timeout"
        raise HTTPException(502, detail)

    out: Dict[str, Any] = {
        "ok": r.status_code < 400,
        "status": r.status_code,
        "reason": r.reason_phrase,
        "request": {
            "url": endpoint,
            "headers": {**headers, "Cookie": "MASKED"},
            "payload": payload,
        },
        "response_meta": {
            "content_type": r.headers.get("content-type"),
            "url": str(getattr(r, "url", endpoint)),
        },
    }

    try:
        out["response"] = r.json()
    except Exception:
        out["response_text"] = r.text[:2000]

    # ★ ここで直近レスポンスとして保存（新規が来たら上書き）
    operation = (payload.get("operationName") if isinstance(payload, dict) else "") or ""
    save_response(operation, payload, out)
    print(f"✅ [CACHE] Saved response for operation: {operation}")

    if r.status_code >= 400:
        raise HTTPException(r.status_code, out)

    return out
