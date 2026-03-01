from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Dict, List, Optional

try:
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover - dependency may be missing in local dev until installed
    psycopg = None
    dict_row = None


def _database_url() -> str:
    url = os.getenv("HFR_SNAPSHOT_DATABASE_URL") or os.getenv("DATABASE_URL") or ""
    url = url.strip()
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    return url


def _require_driver_and_url() -> str:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed. Add psycopg[binary] to requirements.")
    url = _database_url()
    if not url:
        raise RuntimeError("HFR_SNAPSHOT_DATABASE_URL or DATABASE_URL is not configured.")
    return url


def _connect():
    url = _require_driver_and_url()
    return psycopg.connect(url, row_factory=dict_row)


def ensure_schema() -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hfr_snapshot_runs (
                  run_id TEXT PRIMARY KEY,
                  snapshot_date DATE NOT NULL,
                  status TEXT NOT NULL,
                  message TEXT,
                  farms_scanned INTEGER NOT NULL DEFAULT 0,
                  farms_matched INTEGER NOT NULL DEFAULT 0,
                  fields_saved INTEGER NOT NULL DEFAULT 0,
                  tasks_saved INTEGER NOT NULL DEFAULT 0,
                  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  finished_at TIMESTAMPTZ
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hfr_snapshot_fields (
                  snapshot_date DATE NOT NULL,
                  run_id TEXT NOT NULL,
                  field_uuid TEXT NOT NULL,
                  season_uuid TEXT NOT NULL DEFAULT '',
                  field_name TEXT,
                  farm_uuid TEXT,
                  farm_name TEXT,
                  user_name TEXT,
                  crop_name TEXT,
                  variety_name TEXT,
                  area_m2 DOUBLE PRECISION,
                  bbch_index TEXT,
                  bbch_scale TEXT,
                  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (snapshot_date, field_uuid, season_uuid)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hfr_snapshot_tasks (
                  snapshot_date DATE NOT NULL,
                  run_id TEXT NOT NULL,
                  task_uuid TEXT NOT NULL,
                  field_uuid TEXT NOT NULL,
                  season_uuid TEXT NOT NULL DEFAULT '',
                  crop_uuid TEXT,
                  farm_uuid TEXT,
                  farm_name TEXT,
                  field_name TEXT,
                  user_name TEXT,
                  task_name TEXT,
                  task_type TEXT,
                  task_date DATE,
                  planned_date TIMESTAMPTZ,
                  execution_date TIMESTAMPTZ,
                  status TEXT,
                  assignee_name TEXT,
                  product TEXT,
                  dosage TEXT,
                  spray_category TEXT,
                  creation_flow_hint TEXT,
                  bbch_index TEXT,
                  bbch_scale TEXT,
                  occurrence INTEGER,
                  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (snapshot_date, task_uuid)
                )
                """
            )
            cur.execute(
                """
                ALTER TABLE hfr_snapshot_tasks
                ADD COLUMN IF NOT EXISTS crop_uuid TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE hfr_snapshot_tasks
                ADD COLUMN IF NOT EXISTS spray_category TEXT
                """
            )
            cur.execute(
                """
                ALTER TABLE hfr_snapshot_tasks
                ADD COLUMN IF NOT EXISTS creation_flow_hint TEXT
                """
            )
            # Performance indexes for common query patterns
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_snapshot_tasks_date_run
                ON hfr_snapshot_tasks (snapshot_date, run_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_snapshot_fields_date_run
                ON hfr_snapshot_fields (snapshot_date, run_id)
                """
            )
        conn.commit()


def start_run(run_id: str, snapshot_date: date, status: str = "running", message: Optional[str] = None) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO hfr_snapshot_runs (run_id, snapshot_date, status, message)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_id)
                DO UPDATE SET snapshot_date = EXCLUDED.snapshot_date,
                              status = EXCLUDED.status,
                              message = EXCLUDED.message,
                              started_at = NOW(),
                              finished_at = NULL
                """,
                (run_id, snapshot_date, status, message),
            )
        conn.commit()


def finish_run(
    run_id: str,
    *,
    snapshot_date: Optional[date] = None,
    status: str,
    message: Optional[str],
    farms_scanned: int,
    farms_matched: int,
    fields_saved: int,
    tasks_saved: int,
) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE hfr_snapshot_runs
                   SET status = %s,
                       message = %s,
                       farms_scanned = %s,
                       farms_matched = %s,
                       fields_saved = %s,
                       tasks_saved = %s,
                       finished_at = NOW()
                 WHERE run_id = %s
                """,
                (status, message, farms_scanned, farms_matched, fields_saved, tasks_saved, run_id),
            )
            updated = int(cur.rowcount or 0)
            if updated == 0:
                fallback_date = snapshot_date or date.today()
                cur.execute(
                    """
                    INSERT INTO hfr_snapshot_runs (
                      run_id, snapshot_date, status, message,
                      farms_scanned, farms_matched, fields_saved, tasks_saved,
                      started_at, finished_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (run_id)
                    DO UPDATE SET
                      snapshot_date = EXCLUDED.snapshot_date,
                      status = EXCLUDED.status,
                      message = EXCLUDED.message,
                      farms_scanned = EXCLUDED.farms_scanned,
                      farms_matched = EXCLUDED.farms_matched,
                      fields_saved = EXCLUDED.fields_saved,
                      tasks_saved = EXCLUDED.tasks_saved,
                      finished_at = NOW()
                    """,
                    (
                        run_id,
                        fallback_date,
                        status,
                        message,
                        farms_scanned,
                        farms_matched,
                        fields_saved,
                        tasks_saved,
                    ),
                )
        conn.commit()


def prune_snapshot_date(snapshot_date: date, keep_run_id: str) -> Dict[str, int]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM hfr_snapshot_tasks
                 WHERE snapshot_date = %s
                   AND run_id <> %s
                """,
                (snapshot_date, keep_run_id),
            )
            tasks_deleted = int(cur.rowcount or 0)
            cur.execute(
                """
                DELETE FROM hfr_snapshot_fields
                 WHERE snapshot_date = %s
                   AND run_id <> %s
                """,
                (snapshot_date, keep_run_id),
            )
            fields_deleted = int(cur.rowcount or 0)
            cur.execute(
                """
                DELETE FROM hfr_snapshot_runs
                 WHERE snapshot_date = %s
                   AND run_id <> %s
                """,
                (snapshot_date, keep_run_id),
            )
            runs_deleted = int(cur.rowcount or 0)
        conn.commit()
    return {
        "runs_deleted": runs_deleted,
        "fields_deleted": fields_deleted,
        "tasks_deleted": tasks_deleted,
    }


def upsert_fields(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO hfr_snapshot_fields (
                  snapshot_date, run_id, field_uuid, season_uuid,
                  field_name, farm_uuid, farm_name, user_name,
                  crop_name, variety_name, area_m2,
                  bbch_index, bbch_scale
                ) VALUES (
                  %(snapshot_date)s, %(run_id)s, %(field_uuid)s, %(season_uuid)s,
                  %(field_name)s, %(farm_uuid)s, %(farm_name)s, %(user_name)s,
                  %(crop_name)s, %(variety_name)s, %(area_m2)s,
                  %(bbch_index)s, %(bbch_scale)s
                )
                ON CONFLICT (snapshot_date, field_uuid, season_uuid)
                DO UPDATE SET
                  run_id = EXCLUDED.run_id,
                  field_name = EXCLUDED.field_name,
                  farm_uuid = EXCLUDED.farm_uuid,
                  farm_name = EXCLUDED.farm_name,
                  user_name = EXCLUDED.user_name,
                  crop_name = EXCLUDED.crop_name,
                  variety_name = EXCLUDED.variety_name,
                  area_m2 = EXCLUDED.area_m2,
                  bbch_index = EXCLUDED.bbch_index,
                  bbch_scale = EXCLUDED.bbch_scale,
                  fetched_at = NOW()
                """,
                rows,
            )
        conn.commit()
    return len(rows)


def upsert_tasks(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO hfr_snapshot_tasks (
                  snapshot_date, run_id, task_uuid, field_uuid, season_uuid, crop_uuid,
                  farm_uuid, farm_name, field_name, user_name,
                  task_name, task_type, task_date,
                  planned_date, execution_date, status, assignee_name,
                  product, dosage, spray_category, creation_flow_hint, bbch_index, bbch_scale, occurrence
                ) VALUES (
                  %(snapshot_date)s, %(run_id)s, %(task_uuid)s, %(field_uuid)s, %(season_uuid)s, %(crop_uuid)s,
                  %(farm_uuid)s, %(farm_name)s, %(field_name)s, %(user_name)s,
                  %(task_name)s, %(task_type)s, %(task_date)s,
                  %(planned_date)s, %(execution_date)s, %(status)s, %(assignee_name)s,
                  %(product)s, %(dosage)s, %(spray_category)s, %(creation_flow_hint)s, %(bbch_index)s, %(bbch_scale)s, %(occurrence)s
                )
                ON CONFLICT (snapshot_date, task_uuid)
                DO UPDATE SET
                  run_id = EXCLUDED.run_id,
                  field_uuid = EXCLUDED.field_uuid,
                  season_uuid = EXCLUDED.season_uuid,
                  crop_uuid = EXCLUDED.crop_uuid,
                  farm_uuid = EXCLUDED.farm_uuid,
                  farm_name = EXCLUDED.farm_name,
                  field_name = EXCLUDED.field_name,
                  user_name = EXCLUDED.user_name,
                  task_name = EXCLUDED.task_name,
                  task_type = EXCLUDED.task_type,
                  task_date = EXCLUDED.task_date,
                  planned_date = EXCLUDED.planned_date,
                  execution_date = EXCLUDED.execution_date,
                  status = EXCLUDED.status,
                  assignee_name = EXCLUDED.assignee_name,
                  product = EXCLUDED.product,
                  dosage = EXCLUDED.dosage,
                  spray_category = EXCLUDED.spray_category,
                  creation_flow_hint = EXCLUDED.creation_flow_hint,
                  bbch_index = EXCLUDED.bbch_index,
                  bbch_scale = EXCLUDED.bbch_scale,
                  occurrence = EXCLUDED.occurrence,
                  fetched_at = NOW()
                """,
                rows,
            )
        conn.commit()
    return len(rows)


def fetch_snapshot(
    snapshot_date: date,
    farm_uuid: Optional[str] = None,
    limit: int = 2000,
    *,
    include_fields: bool = True,
    include_tasks: bool = True,
    field_limit: Optional[int] = None,
    task_limit: Optional[int] = None,
    task_type_in: Optional[List[str]] = None,
    action_filter: Optional[str] = None,
    action_filter_today: Optional[str] = None,
    action_filter_in3days: Optional[str] = None,
) -> Dict[str, Any]:
    with _connect() as conn:
        with conn.cursor() as cur:
            run_q = """
                WITH task_last AS (
                  SELECT snapshot_date, run_id, MAX(fetched_at) AS last_at
                    FROM hfr_snapshot_tasks
                   WHERE snapshot_date = %s
                   GROUP BY snapshot_date, run_id
                ),
                field_last AS (
                  SELECT snapshot_date, run_id, MAX(fetched_at) AS last_at
                    FROM hfr_snapshot_fields
                   WHERE snapshot_date = %s
                   GROUP BY snapshot_date, run_id
                )
                SELECT r.*
                  FROM hfr_snapshot_runs
                  r
                  LEFT JOIN task_last t
                    ON t.snapshot_date = r.snapshot_date AND t.run_id = r.run_id
                  LEFT JOIN field_last f
                    ON f.snapshot_date = r.snapshot_date AND f.run_id = r.run_id
                 WHERE r.snapshot_date = %s
                 ORDER BY COALESCE(t.last_at, f.last_at, r.finished_at, r.started_at) DESC NULLS LAST,
                          r.started_at DESC
                 LIMIT 1
            """
            cur.execute(run_q, (snapshot_date, snapshot_date, snapshot_date))
            run = cur.fetchone()
            run_id = (run or {}).get("run_id")
            if not run_id:
                # Backward-compatibility: if runs table is empty but tasks/fields exist,
                # infer the latest run_id from data tables.
                cur.execute(
                    """
                    SELECT run_id
                      FROM (
                        SELECT run_id, MAX(fetched_at) AS last_at
                          FROM hfr_snapshot_tasks
                         WHERE snapshot_date = %s
                         GROUP BY run_id
                        UNION ALL
                        SELECT run_id, MAX(fetched_at) AS last_at
                          FROM hfr_snapshot_fields
                         WHERE snapshot_date = %s
                         GROUP BY run_id
                      ) x
                     ORDER BY last_at DESC NULLS LAST
                     LIMIT 1
                    """,
                    (snapshot_date, snapshot_date),
                )
                inferred = cur.fetchone() or {}
                run_id = inferred.get("run_id")
                if not run_id:
                    return {"run": run, "fields": [], "tasks": []}
                run = {
                    "run_id": run_id,
                    "snapshot_date": snapshot_date,
                    "status": "inferred",
                    "message": "run row missing; inferred from snapshot tables",
                }

            safe_field_limit = int(field_limit if field_limit is not None else limit)
            safe_task_limit = int(task_limit if task_limit is not None else limit)

            fields: List[Dict[str, Any]] = []
            tasks: List[Dict[str, Any]] = []

            # Build dynamic WHERE clauses for task filtering
            task_where_extra = ""
            task_params_extra: List[Any] = []
            if task_type_in:
                placeholders = ",".join(["%s"] * len(task_type_in))
                task_where_extra += f" AND task_type IN ({placeholders})"
                task_params_extra.extend(task_type_in)
            if action_filter and action_filter != "none" and action_filter_today:
                done_condition = "(execution_date IS NOT NULL OR status IN ('DONE','COMPLETED','EXECUTED'))"
                not_done_condition = f"(execution_date IS NULL AND (status IS NULL OR status NOT IN ('DONE','COMPLETED','EXECUTED')))"
                if action_filter == "overdue":
                    task_where_extra += f" AND COALESCE(planned_date::date, task_date) < %s::date AND {not_done_condition}"
                    task_params_extra.append(action_filter_today)
                elif action_filter == "due_today":
                    task_where_extra += f" AND COALESCE(planned_date::date, task_date) = %s::date AND {not_done_condition}"
                    task_params_extra.append(action_filter_today)
                elif action_filter == "upcoming_3days":
                    task_where_extra += f" AND COALESCE(planned_date::date, task_date) > %s::date AND COALESCE(planned_date::date, task_date) <= %s::date AND {not_done_condition}"
                    task_params_extra.extend([action_filter_today, action_filter_in3days or action_filter_today])
                elif action_filter == "future":
                    task_where_extra += f" AND COALESCE(planned_date::date, task_date) > %s::date AND {not_done_condition}"
                    task_params_extra.append(action_filter_today)
                elif action_filter == "incomplete":
                    task_where_extra += f" AND {not_done_condition}"

            if farm_uuid:
                field_q = """
                    SELECT *
                      FROM hfr_snapshot_fields
                     WHERE snapshot_date = %s AND run_id = %s AND farm_uuid = %s
                     ORDER BY farm_name, field_name, season_uuid
                     LIMIT %s
                """
                task_q = f"""
                    SELECT *
                      FROM hfr_snapshot_tasks
                     WHERE snapshot_date = %s AND run_id = %s AND farm_uuid = %s{task_where_extra}
                     ORDER BY farm_name, field_name, task_date NULLS LAST, task_name
                     LIMIT %s
                """
                if include_fields:
                    cur.execute(field_q, (snapshot_date, run_id, farm_uuid, safe_field_limit))
                    fields = cur.fetchall() or []
                if include_tasks:
                    cur.execute(task_q, (snapshot_date, run_id, farm_uuid, *task_params_extra, safe_task_limit))
                    tasks = cur.fetchall() or []
            else:
                field_q = """
                    SELECT *
                      FROM hfr_snapshot_fields
                     WHERE snapshot_date = %s AND run_id = %s
                     ORDER BY farm_name, field_name, season_uuid
                     LIMIT %s
                """
                task_q = f"""
                    SELECT *
                      FROM hfr_snapshot_tasks
                     WHERE snapshot_date = %s AND run_id = %s{task_where_extra}
                     ORDER BY farm_name, field_name, task_date NULLS LAST, task_name
                     LIMIT %s
                """
                if include_fields:
                    cur.execute(field_q, (snapshot_date, run_id, safe_field_limit))
                    fields = cur.fetchall() or []
                if include_tasks:
                    cur.execute(task_q, (snapshot_date, run_id, *task_params_extra, safe_task_limit))
                    tasks = cur.fetchall() or []

            return {
                "run": run,
                "fields": fields,
                "tasks": tasks,
            }


def list_snapshot_runs(limit: int = 90) -> List[Dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 1000))
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH task_last AS (
                  SELECT snapshot_date, run_id, MAX(fetched_at) AS last_at
                    FROM hfr_snapshot_tasks
                   GROUP BY snapshot_date, run_id
                ),
                field_last AS (
                  SELECT snapshot_date, run_id, MAX(fetched_at) AS last_at
                    FROM hfr_snapshot_fields
                   GROUP BY snapshot_date, run_id
                )
                SELECT DISTINCT ON (r.snapshot_date)
                       r.run_id,
                       r.snapshot_date,
                       r.status,
                       r.message,
                       r.farms_scanned,
                       r.farms_matched,
                       r.fields_saved,
                       r.tasks_saved,
                       r.started_at,
                       r.finished_at
                  FROM hfr_snapshot_runs r
                  LEFT JOIN task_last t
                    ON t.snapshot_date = r.snapshot_date AND t.run_id = r.run_id
                  LEFT JOIN field_last f
                    ON f.snapshot_date = r.snapshot_date AND f.run_id = r.run_id
                 ORDER BY r.snapshot_date DESC,
                          COALESCE(t.last_at, f.last_at, r.finished_at, r.started_at) DESC NULLS LAST,
                          r.started_at DESC
                 LIMIT %s
                """,
                (safe_limit,),
            )
            rows = cur.fetchall() or []
            if rows:
                return rows

            # Fallback when runs table is empty: reconstruct latest run per day
            # from tasks/fields.
            cur.execute(
                """
                WITH merged AS (
                  SELECT snapshot_date, run_id, MAX(fetched_at) AS last_at
                    FROM hfr_snapshot_tasks
                   GROUP BY snapshot_date, run_id
                  UNION ALL
                  SELECT snapshot_date, run_id, MAX(fetched_at) AS last_at
                    FROM hfr_snapshot_fields
                   GROUP BY snapshot_date, run_id
                ),
                ranked AS (
                  SELECT snapshot_date, run_id, MAX(last_at) AS last_at
                    FROM merged
                   GROUP BY snapshot_date, run_id
                ),
                picked AS (
                  SELECT DISTINCT ON (snapshot_date)
                         snapshot_date, run_id, last_at
                    FROM ranked
                   ORDER BY snapshot_date DESC, last_at DESC NULLS LAST
                )
                SELECT run_id,
                       snapshot_date,
                       'inferred'::TEXT AS status,
                       'run row missing; inferred from snapshot tables'::TEXT AS message,
                       0::INTEGER AS farms_scanned,
                       0::INTEGER AS farms_matched,
                       0::INTEGER AS fields_saved,
                       0::INTEGER AS tasks_saved,
                       NULL::TIMESTAMPTZ AS started_at,
                       last_at AS finished_at
                  FROM picked
                 ORDER BY snapshot_date DESC
                 LIMIT %s
                """,
                (safe_limit,),
            )
            rows = cur.fetchall() or []
    return rows


def compare_snapshots(from_date: date, to_date: date) -> Dict[str, Any]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id
                  FROM hfr_snapshot_runs
                 WHERE snapshot_date = %s
                 ORDER BY started_at DESC
                 LIMIT 1
                """,
                (from_date,),
            )
            from_run = cur.fetchone() or {}
            cur.execute(
                """
                SELECT run_id
                  FROM hfr_snapshot_runs
                 WHERE snapshot_date = %s
                 ORDER BY started_at DESC
                 LIMIT 1
                """,
                (to_date,),
            )
            to_run = cur.fetchone() or {}
            from_run_id = from_run.get("run_id")
            to_run_id = to_run.get("run_id")
            if not from_run_id or not to_run_id:
                return {
                    "from_date": str(from_date),
                    "to_date": str(to_date),
                    "summary": {},
                    "changed_tasks": [],
                }

            cur.execute(
                """
                WITH from_rows AS (
                  SELECT task_uuid, status, planned_date, execution_date
                    FROM hfr_snapshot_tasks
                   WHERE snapshot_date = %s AND run_id = %s
                ),
                to_rows AS (
                  SELECT task_uuid, status, planned_date, execution_date
                    FROM hfr_snapshot_tasks
                   WHERE snapshot_date = %s AND run_id = %s
                )
                SELECT
                  (SELECT COUNT(*) FROM to_rows) AS to_total_tasks,
                  (SELECT COUNT(*) FROM from_rows) AS from_total_tasks,
                  (SELECT COUNT(*) FROM to_rows t WHERE NOT EXISTS (SELECT 1 FROM from_rows f WHERE f.task_uuid = t.task_uuid)) AS added_tasks,
                  (SELECT COUNT(*) FROM from_rows f WHERE NOT EXISTS (SELECT 1 FROM to_rows t WHERE t.task_uuid = f.task_uuid)) AS removed_tasks,
                  (
                    SELECT COUNT(*)
                      FROM to_rows t
                      JOIN from_rows f ON f.task_uuid = t.task_uuid
                     WHERE COALESCE(t.status, '') <> COALESCE(f.status, '')
                  ) AS status_changed_tasks,
                  (
                    SELECT COUNT(*) FROM to_rows
                     WHERE planned_date::date < %s AND execution_date IS NULL
                  ) AS to_overdue_tasks,
                  (
                    SELECT COUNT(*) FROM from_rows
                     WHERE planned_date::date < %s AND execution_date IS NULL
                  ) AS from_overdue_tasks
                """,
                (from_date, from_run_id, to_date, to_run_id, to_date, from_date),
            )
            summary = cur.fetchone() or {}

            cur.execute(
                """
                SELECT t.task_uuid, t.farm_name, t.field_name, t.task_name,
                       f.status AS from_status, t.status AS to_status,
                       f.execution_date AS from_execution_date,
                       t.execution_date AS to_execution_date
                  FROM hfr_snapshot_tasks t
                  JOIN hfr_snapshot_tasks f
                    ON f.task_uuid = t.task_uuid
                   AND f.snapshot_date = %s
                   AND f.run_id = %s
                 WHERE t.snapshot_date = %s
                   AND t.run_id = %s
                   AND COALESCE(f.status, '') <> COALESCE(t.status, '')
                 ORDER BY t.farm_name, t.field_name, t.task_name
                 LIMIT 500
                """,
                (from_date, from_run_id, to_date, to_run_id),
            )
            changed_rows = cur.fetchall() or []

    return {
        "from_date": str(from_date),
        "to_date": str(to_date),
        "summary": summary,
        "changed_tasks": changed_rows,
    }


def dumps_for_debug(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)
