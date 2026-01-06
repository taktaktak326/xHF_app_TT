#Xarvio APIトークン発行処理（Gigyaの4値 → DF_TOKENを取得）

import httpx
from fastapi import HTTPException
from settings import settings
from schemas import FourValues

async def get_api_token_impl(four: FourValues) -> str:
    if not all([four.login_token, four.gigya_uuid, four.gigya_uuid_signature, four.gigya_signature_timestamp]):
        raise HTTPException(400, {
            "reason": "missing parameters",
            "need": ["login_token", "gigya_uuid", "gigya_uuid_signature", "gigya_signature_timestamp"]
        })

    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Cookie": f"LOGIN_TOKEN={four.login_token}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
    }
    payload = {
        "gigyaUuid": four.gigya_uuid,
        "gigyaUuidSignature": four.gigya_uuid_signature,
        "gigyaSignatureTimestamp": four.gigya_signature_timestamp,
    }

    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(settings.XARVIO_TOKEN_API_URL, json=payload, headers=headers)
    except httpx.HTTPError as e:
        raise HTTPException(502, {"reason": "xarvio request error", "detail": str(e)})

    if r.status_code >= 400:
        raise HTTPException(r.status_code, {
            "reason": "xarvio http error",
            "status": r.status_code,
            "text": r.text[:500]
        })

    try:
        j = r.json()
    except Exception as e:
        raise HTTPException(502, {"reason": "invalid xarvio json", "detail": str(e), "raw": r.text[:200]})

    api_token = j.get("token")
    if not api_token:
        raise HTTPException(502, {"reason": "no token in response", "raw": j})

    return api_token
