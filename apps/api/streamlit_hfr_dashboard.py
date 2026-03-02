import os
from typing import Any, Dict, Iterable, List

import requests
import streamlit as st
from dotenv import load_dotenv
from graphql.payloads import make_payload
from graphql.queries import FARMS_OVERVIEW, FIELDS_NAME_SCAN_BY_FARMS

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

DEFAULT_GIGYA_BASE = os.getenv("GIGYA_BASE", "https://accounts.eu1.gigya.com")
DEFAULT_GIGYA_API_KEY = os.getenv("GIGYA_API_KEY", "")
DEFAULT_TOKEN_URL = os.getenv("XARVIO_TOKEN_API_URL", "https://fm-api.xarvio.com/api/users/tokens")
DEFAULT_GRAPHQL_URL = os.getenv("XARVIO_GRAPHQL_ENDPOINT", "https://fm-api.xarvio.com/api/graphql/data")
REQUEST_TIMEOUT_SEC = 60
SCAN_CHUNK_SIZE = 50
FIELDS_BY_FARM_V2 = """
query FieldsByFarmV2($farmUuids: [UUID!]!) {
  fieldsV2(farmUuids: $farmUuids) {
    uuid
    name
    area
    farmV2 { uuid name }
  }
}
"""


def chunked(items: List[str], size: int) -> Iterable[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def gigya_login(gigya_base: str, gigya_api_key: str, email: str, password: str) -> Dict[str, str]:
    resp = requests.post(
        f"{gigya_base.rstrip('/')}/accounts.login",
        data={"apiKey": gigya_api_key, "loginID": email, "password": password, "format": "json"},
        timeout=REQUEST_TIMEOUT_SEC,
    )
    resp.raise_for_status()
    out = resp.json()
    if (out.get("errorCode") or 0) != 0:
        raise RuntimeError(
            f"Gigya login failed: errorCode={out.get('errorCode')} errorMessage={out.get('errorMessage')}"
        )

    session_info = out.get("sessionInfo") or {}
    return {
        "login_token": str(session_info.get("cookieValue") or ""),
        "gigya_uuid": str(out.get("UID") or ""),
        "gigya_uuid_signature": str(out.get("UIDSignature") or ""),
        "gigya_signature_timestamp": str(out.get("signatureTimestamp") or ""),
    }


def issue_xarvio_token(token_url: str, login_data: Dict[str, str]) -> str:
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Cookie": f"LOGIN_TOKEN={login_data['login_token']}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
        "User-Agent": "xhf-streamlit/1.0",
    }
    payload = {
        "gigyaUuid": login_data["gigya_uuid"],
        "gigyaUuidSignature": login_data["gigya_uuid_signature"],
        "gigyaSignatureTimestamp": login_data["gigya_signature_timestamp"],
    }
    resp = requests.post(token_url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT_SEC)
    resp.raise_for_status()
    out = resp.json()
    token = str(out.get("token") or "")
    if not token:
        raise RuntimeError(f"DF token missing in response: {out}")
    return token


def call_xarvio_graphql(
    graphql_url: str,
    login_token: str,
    api_token: str,
    operation_name: str,
    query: str,
    variables: Dict[str, Any],
) -> Dict[str, Any]:
    payload = make_payload(operation_name, query, variables)
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Cookie": f"LOGIN_TOKEN={login_token}; DF_TOKEN={api_token}",
        "Origin": "https://fm.xarvio.com",
        "Referer": "https://fm.xarvio.com/",
        "User-Agent": "xhf-streamlit/1.0",
    }
    resp = requests.post(graphql_url, json=payload, headers=headers, timeout=REQUEST_TIMEOUT_SEC)
    resp.raise_for_status()
    out = resp.json()
    if out.get("errors"):
        raise RuntimeError(f"GraphQL errors ({operation_name}): {out.get('errors')}")
    return out


def parse_farms(gql_out: Dict[str, Any]) -> List[Dict[str, str]]:
    farms = ((gql_out.get("data") or {}).get("farms") or [])
    result: List[Dict[str, str]] = []
    for farm in farms:
        if not isinstance(farm, dict):
            continue
        uuid = str(farm.get("uuid") or "").strip()
        if not uuid:
            continue
        result.append({"uuid": uuid, "name": str(farm.get("name") or "")})
    return result


def extract_hfr_farm_uuid_set(gql_out: Dict[str, Any], suffix: str) -> set[str]:
    suffix_lower = suffix.lower()
    fields = ((gql_out.get("data") or {}).get("fieldsV2") or [])
    matched: set[str] = set()
    for field in fields:
        if not isinstance(field, dict):
            continue
        name = str(field.get("name") or "").strip()
        if not name.lower().endswith(suffix_lower):
            continue
        farm_uuid = str(((field.get("farmV2") or {}).get("uuid") or "")).strip()
        if farm_uuid:
            matched.add(farm_uuid)
    return matched


def extract_hfr_fields(gql_out: Dict[str, Any], suffix: str) -> List[Dict[str, Any]]:
    suffix_lower = suffix.lower()
    fields = (gql_out.get("data") or {}).get("fieldsV2") or []
    matches: List[Dict[str, Any]] = []
    for field in fields:
        if not isinstance(field, dict):
            continue
        name = str(field.get("name") or "").strip()
        if not name.lower().endswith(suffix_lower):
            continue
        matches.append(
            {
                "field_uuid": str(field.get("uuid") or ""),
                "field_name": name,
                "area": field.get("area"),
            }
        )
    return matches


def ensure_session_defaults() -> None:
    st.session_state.setdefault("login_token", "")
    st.session_state.setdefault("api_token", "")
    st.session_state.setdefault("farms", [])
    st.session_state.setdefault("farm_name_by_uuid", {})
    st.session_state.setdefault("matched_farm_uuids", [])
    st.session_state.setdefault("per_farm_rows", [])
    st.session_state.setdefault("total_hfr_fields", 0)
    st.session_state.setdefault("scan_suffix", "")


def clear_drilldown_state() -> None:
    st.session_state.farms = []
    st.session_state.farm_name_by_uuid = {}
    st.session_state.matched_farm_uuids = []
    st.session_state.per_farm_rows = []
    st.session_state.total_hfr_fields = 0
    st.session_state.scan_suffix = ""


st.set_page_config(page_title="HFR圃場ダッシュボード", layout="wide")
st.title("HFR圃場ダッシュボード")
st.caption("FastAPIは使わず、Gigya + Xarvio API を直接呼び出して HFR 圃場を抽出します。")

ensure_session_defaults()

with st.sidebar:
    st.header("接続設定（直接API）")
    gigya_base = st.text_input("Gigya Base URL", value=DEFAULT_GIGYA_BASE)
    gigya_api_key = st.text_input("Gigya API Key", value=DEFAULT_GIGYA_API_KEY, type="password")
    token_url = st.text_input("Xarvio Token URL", value=DEFAULT_TOKEN_URL)
    graphql_url = st.text_input("Xarvio GraphQL URL", value=DEFAULT_GRAPHQL_URL)
    suffix = st.text_input("圃場名サフィックス", value="HFR")

    st.header("ログイン")
    email = st.text_input("Email", value="", key="login_email")
    password = st.text_input("Password", value="", type="password", key="login_password")

    if st.button("ログイン", use_container_width=True):
        if not email or not password:
            st.error("Email と Password を入力してください。")
        elif not gigya_api_key:
            st.error("Gigya API Key が必要です（.env の GIGYA_API_KEY か入力欄）。")
        else:
            try:
                login_data = gigya_login(gigya_base, gigya_api_key, email, password)
                st.session_state.login_token = login_data["login_token"]
                st.session_state.api_token = issue_xarvio_token(token_url, login_data)
                clear_drilldown_state()
                st.success("ログイン成功（Xarvio DF_TOKEN 取得済み）")
            except Exception as exc:
                st.error(f"ログイン失敗: {exc}")

    if st.session_state.login_token and st.session_state.api_token:
        st.info("ログイン済み")

st.subheader("取得操作")
col1, col2, col3 = st.columns(3)
fetch_hfr_farms_clicked = col1.button("HFR農場を取得", type="primary", use_container_width=True)
fetch_details_clicked = col2.button("HFR圃場詳細を取得", use_container_width=True)
clear_clicked = col3.button("結果をクリア", use_container_width=True)

if clear_clicked:
    clear_drilldown_state()
    st.success("表示データをクリアしました。")

if (fetch_hfr_farms_clicked or fetch_details_clicked) and (
    not st.session_state.login_token or not st.session_state.api_token
):
    st.error("先にログインしてください。")

if fetch_hfr_farms_clicked and st.session_state.login_token and st.session_state.api_token:
    try:
        with st.spinner("農場一覧を取得中..."):
            farms_out = call_xarvio_graphql(
                graphql_url,
                st.session_state.login_token,
                st.session_state.api_token,
                "FarmsOverview",
                FARMS_OVERVIEW,
                {},
            )
            farms = parse_farms(farms_out)
            with st.spinner("HFR候補の農場を抽出中..."):
                farm_uuids = [farm["uuid"] for farm in farms]
                matched_farm_uuid_set: set[str] = set()
                for chunk in chunked(farm_uuids, SCAN_CHUNK_SIZE):
                    scan_out = call_xarvio_graphql(
                        graphql_url,
                        st.session_state.login_token,
                        st.session_state.api_token,
                        "FieldsNameScanByFarms",
                        FIELDS_NAME_SCAN_BY_FARMS,
                        {"farmUuids": chunk},
                    )
                    matched_farm_uuid_set |= extract_hfr_farm_uuid_set(scan_out, suffix)
        st.session_state.farms = farms
        st.session_state.farm_name_by_uuid = {farm["uuid"]: farm["name"] for farm in farms}
        st.session_state.matched_farm_uuids = [uuid for uuid in farm_uuids if uuid in matched_farm_uuid_set]
        st.session_state.scan_suffix = suffix
        st.session_state.per_farm_rows = []
        st.session_state.total_hfr_fields = 0
        st.success(
            f"HFR農場の抽出が完了しました: 取得農場 {len(farms)}件 / HFR候補農場 {len(st.session_state.matched_farm_uuids)}件"
        )
    except Exception as exc:
        st.error(f"HFR農場取得失敗: {exc}")

if fetch_details_clicked and st.session_state.login_token and st.session_state.api_token:
    matched_farm_uuids = st.session_state.matched_farm_uuids
    if not matched_farm_uuids:
        st.warning("先に「HFR農場を取得」を実行してください。")
    elif st.session_state.scan_suffix != suffix:
        st.warning("圃場名サフィックスが変更されています。再度「HFR農場を取得」を実行してください。")
    else:
        try:
            with st.spinner("農場ごとのHFR圃場詳細を取得中..."):
                existing_row_by_farm_uuid = {row["farm_uuid"]: row for row in st.session_state.per_farm_rows}
                added_count = 0
                for farm_uuid in matched_farm_uuids:
                    if farm_uuid in existing_row_by_farm_uuid:
                        continue
                    fields_out = call_xarvio_graphql(
                        graphql_url,
                        st.session_state.login_token,
                        st.session_state.api_token,
                        "FieldsByFarmV2",
                        FIELDS_BY_FARM_V2,
                        {"farmUuids": [farm_uuid]},
                    )
                    hfr_fields = extract_hfr_fields(fields_out, suffix)
                    existing_row_by_farm_uuid[farm_uuid] = {
                        "farm_uuid": farm_uuid,
                        "farm_name": st.session_state.farm_name_by_uuid.get(farm_uuid, ""),
                        "hfr_field_count": len(hfr_fields),
                        "hfr_fields": hfr_fields,
                    }
                    added_count += 1
                st.session_state.per_farm_rows = [
                    existing_row_by_farm_uuid[uuid] for uuid in matched_farm_uuids if uuid in existing_row_by_farm_uuid
                ]
                st.session_state.total_hfr_fields = sum(
                    row["hfr_field_count"] for row in st.session_state.per_farm_rows
                )
            st.success(f"HFR圃場詳細を取得しました。追加取得: {added_count}農場")
        except Exception as exc:
            st.error(f"HFR圃場詳細取得失敗: {exc}")

farms = st.session_state.farms
matched_farm_uuids = st.session_state.matched_farm_uuids
per_farm_rows = st.session_state.per_farm_rows
per_farm_row_by_uuid = {row["farm_uuid"]: row for row in per_farm_rows}

metric1, metric2, metric3 = st.columns(3)
metric1.metric("取得農場数", len(farms))
metric2.metric("HFR候補農場数", len(matched_farm_uuids))
metric3.metric("HFR圃場数", st.session_state.total_hfr_fields)

if farms:
    st.subheader("農場一覧")
    st.dataframe(
        [{"farm_name": farm["name"], "farm_uuid": farm["uuid"]} for farm in farms],
        use_container_width=True,
        hide_index=True,
    )

if matched_farm_uuids:
    st.subheader("HFR圃場あり農場")
    summary_rows = [
        {
            "farm_name": st.session_state.farm_name_by_uuid.get(farm_uuid, ""),
            "farm_uuid": farm_uuid,
            "hfr_field_count": (
                per_farm_row_by_uuid[farm_uuid]["hfr_field_count"] if farm_uuid in per_farm_row_by_uuid else None
            ),
        }
        for farm_uuid in matched_farm_uuids
    ]
    st.dataframe(summary_rows, use_container_width=True, hide_index=True)

if per_farm_rows:
    st.subheader("農場ごとのHFR圃場詳細")
    for row in per_farm_rows:
        with st.expander(f"{row['farm_name']} ({row['hfr_field_count']}件)", expanded=False):
            st.write(f"farm_uuid: `{row['farm_uuid']}`")
            st.dataframe(row["hfr_fields"], use_container_width=True, hide_index=True)
