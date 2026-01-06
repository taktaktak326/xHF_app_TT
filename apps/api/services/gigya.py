#Gigyaログイン処理（メール・パスワード → 4つの値を取得）

import httpx
from fastapi import HTTPException
from typing import Dict, Any
from settings import settings

async def gigya_login_impl(email: str, password: str) -> Dict[str, Any]:
    params = {
        "apiKey": settings.GIGYA_API_KEY,
        "loginID": email,
        "password": password,
        "format": "json",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post(f"{settings.GIGYA_BASE}/accounts.login", data=params)
    except httpx.HTTPError as e:
        raise HTTPException(502, {"reason": "gigya request error", "detail": str(e)})

    if r.status_code >= 400:
        raise HTTPException(502, {"reason": "gigya http error", "status": r.status_code, "text": r.text[:500]})

    try:
        j = r.json()
    except Exception as e:
        raise HTTPException(502, {"reason": "invalid gigya json", "detail": str(e), "raw": r.text[:200]})

    if j.get("errorCode") != 0:
        detail = {
            "gigya_errorCode": j.get("errorCode"),
            "gigya_errorMessage": j.get("errorMessage"),
            "callId": j.get("callId"),
        }
        raise HTTPException(status_code=401, detail=detail)

    session = j.get("sessionInfo", {}) or {}
    return {
        "ok": True,
        "login_token": session.get("cookieValue"),
        "gigya_uuid": j.get("UID"),
        "gigya_uuid_signature": j.get("UIDSignature"),
        "gigya_signature_timestamp": j.get("signatureTimestamp"),
        "raw": {"callId": j.get("callId"), "errorCode": j.get("errorCode")},
    }
