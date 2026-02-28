from __future__ import annotations

import os
from typing import Any, Dict, List

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None


def _database_url() -> str:
    url = os.getenv("HFR_SNAPSHOT_DATABASE_URL") or os.getenv("DATABASE_URL") or ""
    url = url.strip()
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("ostgresql://"):
        url = "postgresql://" + url[len("ostgresql://"):]
    return url


def _require_driver_and_url() -> str:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed. Add psycopg[binary] to requirements.")
    url = _database_url()
    if not url:
        raise RuntimeError("HFR_SNAPSHOT_DATABASE_URL or DATABASE_URL is not configured.")
    return url


def _connect():
    return psycopg.connect(_require_driver_and_url(), row_factory=dict_row)


def ensure_schema() -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS crop_protection_product_cache (
                  country_uuid TEXT NOT NULL,
                  crop_uuid TEXT NOT NULL,
                  task_type_code TEXT NOT NULL,
                  product_uuid TEXT,
                  product_name TEXT NOT NULL,
                  category_code TEXT,
                  category_name TEXT,
                  cached_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (country_uuid, crop_uuid, task_type_code, product_name)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_crop_product_cache_lookup
                ON crop_protection_product_cache (country_uuid, crop_uuid, task_type_code)
                """
            )
        conn.commit()


def get_cached_products(country_uuid: str, crop_uuid: str, task_type_code: str) -> List[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  product_uuid,
                  product_name,
                  category_code,
                  category_name,
                  cached_at
                FROM crop_protection_product_cache
                WHERE country_uuid = %s
                  AND crop_uuid = %s
                  AND task_type_code = %s
                ORDER BY product_name
                """,
                (country_uuid, crop_uuid, task_type_code),
            )
            return cur.fetchall() or []


def replace_cached_products(
    country_uuid: str,
    crop_uuid: str,
    task_type_code: str,
    products: List[Dict[str, Any]],
) -> int:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM crop_protection_product_cache
                WHERE country_uuid = %s
                  AND crop_uuid = %s
                  AND task_type_code = %s
                """,
                (country_uuid, crop_uuid, task_type_code),
            )

            rows: List[Dict[str, Any]] = []
            for p in products:
                if not isinstance(p, dict):
                    continue
                product_name = str(p.get("name") or "").strip()
                if not product_name:
                    continue
                categories = p.get("categories") or []
                first = categories[0] if isinstance(categories, list) and categories else {}
                rows.append(
                    {
                        "country_uuid": country_uuid,
                        "crop_uuid": crop_uuid,
                        "task_type_code": task_type_code,
                        "product_uuid": str(p.get("uuid") or "").strip() or None,
                        "product_name": product_name,
                        "category_code": str((first or {}).get("code") or "").strip() or None,
                        "category_name": str((first or {}).get("name") or "").strip() or None,
                    }
                )

            if rows:
                cur.executemany(
                    """
                    INSERT INTO crop_protection_product_cache (
                      country_uuid, crop_uuid, task_type_code,
                      product_uuid, product_name, category_code, category_name
                    ) VALUES (
                      %(country_uuid)s, %(crop_uuid)s, %(task_type_code)s,
                      %(product_uuid)s, %(product_name)s, %(category_code)s, %(category_name)s
                    )
                    ON CONFLICT (country_uuid, crop_uuid, task_type_code, product_name)
                    DO UPDATE SET
                      product_uuid = EXCLUDED.product_uuid,
                      category_code = EXCLUDED.category_code,
                      category_name = EXCLUDED.category_name,
                      cached_at = NOW()
                    """,
                    rows,
                )
        conn.commit()
    return len(rows)
