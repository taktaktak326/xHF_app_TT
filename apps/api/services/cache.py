# apps/api/services/graphql_cache.py
from __future__ import annotations
from typing import Any, Dict, Optional
from threading import RLock
from time import time
import json
import hashlib

_lock = RLock()

# 直近のレスポンス（最後に来たもの1件）
_last_response: Optional[Dict[str, Any]] = None

# operationName ごとの直近レスポンス（任意で参照用）
_by_operation: Dict[str, Dict[str, Any]] = {}

def _get_cache_key(operation: str, payload: Optional[Dict[str, Any]] = None) -> str:
    """キャッシュキーを生成する。ペイロードがあればハッシュ値もキーに含める。"""
    if not payload:
        return operation
    # 辞書のキーをソートして、順序に依存しない安定したJSON文字列を生成
    payload_str = json.dumps(payload, sort_keys=True)
    payload_hash = hashlib.md5(payload_str.encode()).hexdigest()
    return f"{operation}:{payload_hash}"

def save_response(operation_name: str, payload: Dict[str, Any], resp: Dict[str, Any]) -> None:
    """直近レスポンスを保存。operationName でも保持（いずれも上書き）。"""
    resp["saved_at"] = time()
    with _lock:
        global _last_response
        _last_response = resp
        key = _get_cache_key(operation_name, payload)
        _by_operation[key] = resp


def get_last_response() -> Optional[Dict[str, Any]]:
    """最後に保存したレスポンス（1件）"""
    with _lock:
        return _last_response


def get_by_operation(operation_name: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """operationName で最後に保存したレスポンス"""
    with _lock:
        key = _get_cache_key(operation_name, payload)
        return _by_operation.get(key)


def clear_cache() -> None:
    """キャッシュ全消去"""
    with _lock:
        global _last_response
        _last_response = None
        _by_operation.clear()
