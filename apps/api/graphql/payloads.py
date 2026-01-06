#GraphQLリクエストpayloadを組み立てる関数（operationName/variables/queryを一括生成）

from typing import Any, Dict, Optional

def make_payload(operation_name: str, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "operationName": operation_name,
        "variables": variables or {},
        "query": query,
    }
