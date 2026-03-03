from __future__ import annotations

import json
import os
from datetime import date, timedelta
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
                CREATE TABLE IF NOT EXISTS hfr_snapshot_growth_stage_predictions (
                  snapshot_date DATE NOT NULL,
                  run_id TEXT NOT NULL,
                  field_uuid TEXT NOT NULL,
                  season_uuid TEXT NOT NULL DEFAULT '',
                  prediction_index TEXT NOT NULL DEFAULT '',
                  start_date TEXT NOT NULL DEFAULT '',
                  end_date TEXT,
                  scale TEXT,
                  gs_order TEXT,
                  stage_uuid TEXT,
                  stage_name TEXT,
                  stage_code TEXT,
                  fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (snapshot_date, field_uuid, season_uuid, prediction_index, start_date)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hfr_snapshot_field_notes (
                  note_uuid TEXT PRIMARY KEY,
                  snapshot_date DATE NOT NULL,
                  run_id TEXT NOT NULL,
                  field_uuid TEXT NOT NULL,
                  field_name TEXT,
                  season_uuid TEXT NOT NULL DEFAULT '',
                  farm_uuid TEXT,
                  farm_name TEXT,
                  note_text TEXT,
                  categories_json TEXT,
                  creation_date TIMESTAMPTZ,
                  location_type TEXT,
                  region TEXT,
                  location_json TEXT,
                  creator_uuid TEXT,
                  creator_name TEXT,
                  attachments_json TEXT,
                  image_urls_json TEXT,
                  audio_attachments_json TEXT,
                  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
                CREATE INDEX IF NOT EXISTS idx_snapshot_tasks_date_run_type
                ON hfr_snapshot_tasks (snapshot_date, run_id, task_type)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_snapshot_tasks_date_run_planned
                ON hfr_snapshot_tasks (snapshot_date, run_id, planned_date, task_date)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_snapshot_fields_date_run
                ON hfr_snapshot_fields (snapshot_date, run_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_snapshot_growth_stage_date_run
                ON hfr_snapshot_growth_stage_predictions (snapshot_date, run_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_snapshot_field_notes_date_run
                ON hfr_snapshot_field_notes (snapshot_date, run_id)
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_snapshot_field_notes_field_uuid
                ON hfr_snapshot_field_notes (field_uuid, creation_date DESC)
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hfr_snapshot_dashboard_summary_cache (
                  snapshot_date DATE NOT NULL,
                  run_id TEXT NOT NULL,
                  payload JSONB NOT NULL,
                  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  PRIMARY KEY (snapshot_date, run_id)
                )
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_snapshot_dashboard_summary_date
                ON hfr_snapshot_dashboard_summary_cache (snapshot_date, updated_at DESC)
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
                DELETE FROM hfr_snapshot_growth_stage_predictions
                 WHERE snapshot_date = %s
                   AND run_id <> %s
                """,
                (snapshot_date, keep_run_id),
            )
            growth_stage_deleted = int(cur.rowcount or 0)
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
        "growth_stage_deleted": growth_stage_deleted,
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

    # PK単位で重複を圧縮して無駄な conflict update を減らす。
    deduped: Dict[tuple, Dict[str, Any]] = {}
    for row in rows:
        key = (row.get("snapshot_date"), row.get("task_uuid"))
        deduped[key] = row
    deduped_rows = list(deduped.values())

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE tmp_hfr_snapshot_tasks (
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
                  occurrence INTEGER
                ) ON COMMIT DROP
                """,
            )
            with cur.copy(
                """
                COPY tmp_hfr_snapshot_tasks (
                  snapshot_date, run_id, task_uuid, field_uuid, season_uuid, crop_uuid,
                  farm_uuid, farm_name, field_name, user_name,
                  task_name, task_type, task_date,
                  planned_date, execution_date, status, assignee_name,
                  product, dosage, spray_category, creation_flow_hint, bbch_index, bbch_scale, occurrence
                ) FROM STDIN
                """
            ) as copy:
                for row in deduped_rows:
                    copy.write_row(
                        (
                            row.get("snapshot_date"),
                            row.get("run_id"),
                            row.get("task_uuid"),
                            row.get("field_uuid"),
                            row.get("season_uuid"),
                            row.get("crop_uuid"),
                            row.get("farm_uuid"),
                            row.get("farm_name"),
                            row.get("field_name"),
                            row.get("user_name"),
                            row.get("task_name"),
                            row.get("task_type"),
                            row.get("task_date"),
                            row.get("planned_date"),
                            row.get("execution_date"),
                            row.get("status"),
                            row.get("assignee_name"),
                            row.get("product"),
                            row.get("dosage"),
                            row.get("spray_category"),
                            row.get("creation_flow_hint"),
                            row.get("bbch_index"),
                            row.get("bbch_scale"),
                            row.get("occurrence"),
                        )
                    )

            cur.execute(
                """
                INSERT INTO hfr_snapshot_tasks (
                  snapshot_date, run_id, task_uuid, field_uuid, season_uuid, crop_uuid,
                  farm_uuid, farm_name, field_name, user_name,
                  task_name, task_type, task_date,
                  planned_date, execution_date, status, assignee_name,
                  product, dosage, spray_category, creation_flow_hint, bbch_index, bbch_scale, occurrence
                )
                SELECT
                  snapshot_date, run_id, task_uuid, field_uuid, season_uuid, crop_uuid,
                  farm_uuid, farm_name, field_name, user_name,
                  task_name, task_type, task_date,
                  planned_date, execution_date, status, assignee_name,
                  product, dosage, spray_category, creation_flow_hint, bbch_index, bbch_scale, occurrence
                  FROM tmp_hfr_snapshot_tasks
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
                """
            )
        conn.commit()
    return len(deduped_rows)


def upsert_growth_stage_predictions(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    # Primary key 単位で重複を圧縮し、不要な conflict/update を減らす。
    deduped: Dict[tuple, Dict[str, Any]] = {}
    for row in rows:
        key = (
            row.get("snapshot_date"),
            row.get("field_uuid"),
            row.get("season_uuid"),
            row.get("prediction_index"),
            row.get("start_date"),
        )
        deduped[key] = row
    deduped_rows = list(deduped.values())

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE tmp_hfr_growth_stage_predictions (
                  snapshot_date DATE NOT NULL,
                  run_id TEXT NOT NULL,
                  field_uuid TEXT NOT NULL,
                  season_uuid TEXT NOT NULL DEFAULT '',
                  prediction_index TEXT NOT NULL DEFAULT '',
                  start_date TEXT NOT NULL DEFAULT '',
                  end_date TEXT,
                  scale TEXT,
                  gs_order TEXT,
                  stage_uuid TEXT,
                  stage_name TEXT,
                  stage_code TEXT
                ) ON COMMIT DROP
                """,
            )
            with cur.copy(
                """
                COPY tmp_hfr_growth_stage_predictions (
                  snapshot_date, run_id, field_uuid, season_uuid,
                  prediction_index, start_date, end_date, scale, gs_order,
                  stage_uuid, stage_name, stage_code
                ) FROM STDIN
                """
            ) as copy:
                for row in deduped_rows:
                    copy.write_row(
                        (
                            row.get("snapshot_date"),
                            row.get("run_id"),
                            row.get("field_uuid"),
                            row.get("season_uuid"),
                            row.get("prediction_index"),
                            row.get("start_date"),
                            row.get("end_date"),
                            row.get("scale"),
                            row.get("gs_order"),
                            row.get("stage_uuid"),
                            row.get("stage_name"),
                            row.get("stage_code"),
                        )
                    )

            cur.execute(
                """
                INSERT INTO hfr_snapshot_growth_stage_predictions (
                  snapshot_date, run_id, field_uuid, season_uuid,
                  prediction_index, start_date, end_date, scale, gs_order,
                  stage_uuid, stage_name, stage_code
                )
                SELECT
                  snapshot_date, run_id, field_uuid, season_uuid,
                  prediction_index, start_date, end_date, scale, gs_order,
                  stage_uuid, stage_name, stage_code
                  FROM tmp_hfr_growth_stage_predictions
                ON CONFLICT (snapshot_date, field_uuid, season_uuid, prediction_index, start_date)
                DO UPDATE SET
                  run_id = EXCLUDED.run_id,
                  end_date = EXCLUDED.end_date,
                  scale = EXCLUDED.scale,
                  gs_order = EXCLUDED.gs_order,
                  stage_uuid = EXCLUDED.stage_uuid,
                  stage_name = EXCLUDED.stage_name,
                  stage_code = EXCLUDED.stage_code,
                  fetched_at = NOW()
                """
            )
        conn.commit()
    return len(deduped_rows)


def insert_new_field_notes(rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        note_uuid = str(row.get("note_uuid") or "").strip()
        if not note_uuid:
            continue
        deduped[note_uuid] = row
    deduped_rows = list(deduped.values())
    if not deduped_rows:
        return 0

    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE tmp_hfr_snapshot_field_notes (
                  note_uuid TEXT NOT NULL,
                  snapshot_date DATE NOT NULL,
                  run_id TEXT NOT NULL,
                  field_uuid TEXT NOT NULL,
                  field_name TEXT,
                  season_uuid TEXT NOT NULL DEFAULT '',
                  farm_uuid TEXT,
                  farm_name TEXT,
                  note_text TEXT,
                  categories_json TEXT,
                  creation_date TIMESTAMPTZ,
                  location_type TEXT,
                  region TEXT,
                  location_json TEXT,
                  creator_uuid TEXT,
                  creator_name TEXT,
                  attachments_json TEXT,
                  image_urls_json TEXT,
                  audio_attachments_json TEXT
                ) ON COMMIT DROP
                """
            )
            with cur.copy(
                """
                COPY tmp_hfr_snapshot_field_notes (
                  note_uuid, snapshot_date, run_id, field_uuid, field_name, season_uuid,
                  farm_uuid, farm_name, note_text, categories_json, creation_date,
                  location_type, region, location_json, creator_uuid, creator_name,
                  attachments_json, image_urls_json, audio_attachments_json
                ) FROM STDIN
                """
            ) as copy:
                for row in deduped_rows:
                    copy.write_row(
                        (
                            row.get("note_uuid"),
                            row.get("snapshot_date"),
                            row.get("run_id"),
                            row.get("field_uuid"),
                            row.get("field_name"),
                            row.get("season_uuid"),
                            row.get("farm_uuid"),
                            row.get("farm_name"),
                            row.get("note_text"),
                            row.get("categories_json"),
                            row.get("creation_date"),
                            row.get("location_type"),
                            row.get("region"),
                            row.get("location_json"),
                            row.get("creator_uuid"),
                            row.get("creator_name"),
                            row.get("attachments_json"),
                            row.get("image_urls_json"),
                            row.get("audio_attachments_json"),
                        )
                    )

            cur.execute(
                """
                WITH ins AS (
                  INSERT INTO hfr_snapshot_field_notes (
                    note_uuid, snapshot_date, run_id, field_uuid, field_name, season_uuid,
                    farm_uuid, farm_name, note_text, categories_json, creation_date,
                    location_type, region, location_json, creator_uuid, creator_name,
                    attachments_json, image_urls_json, audio_attachments_json
                  )
                  SELECT
                    note_uuid, snapshot_date, run_id, field_uuid, field_name, season_uuid,
                    farm_uuid, farm_name, note_text, categories_json, creation_date,
                    location_type, region, location_json, creator_uuid, creator_name,
                    attachments_json, image_urls_json, audio_attachments_json
                    FROM tmp_hfr_snapshot_field_notes
                  ON CONFLICT (note_uuid) DO NOTHING
                  RETURNING 1
                )
                SELECT COUNT(*)::INTEGER AS inserted_count FROM ins
                """
            )
            row = cur.fetchone() or {}
        conn.commit()
    return int(row.get("inserted_count") or 0)


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
    tasks_projection: str = "full",
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
            task_select_columns = "*"
            task_order_clause = "ORDER BY farm_name, field_name, task_date NULLS LAST, task_name"
            if tasks_projection == "lite":
                task_select_columns = """
                    snapshot_date, run_id, task_uuid, field_uuid, season_uuid, crop_uuid,
                    farm_uuid, farm_name, field_name, user_name,
                    task_name, task_type, task_date,
                    planned_date, execution_date, status, assignee_name,
                    product, dosage, spray_category, creation_flow_hint,
                    bbch_index, bbch_scale, occurrence, fetched_at
                """
                # For dashboard "lite" payload we do not require stable ordering.
                # Skipping ORDER BY avoids a costly sort on large snapshots.
                task_order_clause = ""

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
                    SELECT {task_select_columns}
                      FROM hfr_snapshot_tasks
                     WHERE snapshot_date = %s AND run_id = %s AND farm_uuid = %s{task_where_extra}
                     {task_order_clause}
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
                    SELECT {task_select_columns}
                      FROM hfr_snapshot_tasks
                     WHERE snapshot_date = %s AND run_id = %s{task_where_extra}
                     {task_order_clause}
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


def fetch_growth_stage_predictions(
    snapshot_date: date,
    run_id: str,
    *,
    field_uuid: Optional[str] = None,
    limit: int = 200000,
) -> List[Dict[str, Any]]:
    ensure_schema()
    with _connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            if field_uuid:
                cur.execute(
                    """
                    SELECT snapshot_date, run_id, field_uuid, season_uuid,
                           prediction_index, start_date, end_date, scale, gs_order,
                           stage_uuid, stage_name, stage_code, fetched_at
                      FROM hfr_snapshot_growth_stage_predictions
                     WHERE snapshot_date = %s
                       AND run_id = %s
                       AND field_uuid = %s
                     ORDER BY field_uuid, season_uuid, start_date, prediction_index
                     LIMIT %s
                    """,
                    (snapshot_date, run_id, field_uuid, int(limit)),
                )
            else:
                cur.execute(
                    """
                    SELECT snapshot_date, run_id, field_uuid, season_uuid,
                           prediction_index, start_date, end_date, scale, gs_order,
                           stage_uuid, stage_name, stage_code, fetched_at
                      FROM hfr_snapshot_growth_stage_predictions
                     WHERE snapshot_date = %s
                       AND run_id = %s
                     ORDER BY field_uuid, season_uuid, start_date, prediction_index
                     LIMIT %s
                    """,
                    (snapshot_date, run_id, int(limit)),
                )
            return cur.fetchall() or []


def fetch_field_aggregate(snapshot_date: date, run_id: Optional[str]) -> Dict[str, Any]:
    if not run_id:
        return {"field_count": 0, "total_area_m2": 0.0, "farmer_count": 0}
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH uniq AS (
                  SELECT field_uuid, MAX(COALESCE(area_m2, 0)) AS area_m2
                    FROM hfr_snapshot_fields
                   WHERE snapshot_date = %s
                     AND run_id = %s
                   GROUP BY field_uuid
                ),
                farmers AS (
                  SELECT COUNT(DISTINCT COALESCE(NULLIF(farm_uuid, ''), farm_name))::INTEGER AS farmer_count
                    FROM hfr_snapshot_fields
                   WHERE snapshot_date = %s
                     AND run_id = %s
                )
                SELECT COUNT(*)::INTEGER AS field_count,
                       COALESCE(SUM(area_m2), 0)::DOUBLE PRECISION AS total_area_m2,
                       (SELECT farmer_count FROM farmers) AS farmer_count
                  FROM uniq
                """,
                (snapshot_date, run_id, snapshot_date, run_id),
            )
            row = cur.fetchone() or {}
            return {
                "field_count": int(row.get("field_count") or 0),
                "total_area_m2": float(row.get("total_area_m2") or 0.0),
                "farmer_count": int(row.get("farmer_count") or 0),
            }


def fetch_no_task_farmers(snapshot_date: date, run_id: Optional[str]) -> List[Dict[str, Any]]:
    if not run_id:
        return []
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH fields_uniq AS (
                  SELECT
                    COALESCE(NULLIF(farm_uuid, ''), 'name:' || COALESCE(farm_name, '')) AS farmer_id,
                    COALESCE(farm_name, '') AS farmer_name,
                    field_uuid
                  FROM hfr_snapshot_fields
                  WHERE snapshot_date = %s
                    AND run_id = %s
                  GROUP BY 1, 2, 3
                ),
                task_fields AS (
                  SELECT
                    COALESCE(NULLIF(farm_uuid, ''), 'name:' || COALESCE(farm_name, '')) AS farmer_id,
                    field_uuid
                  FROM hfr_snapshot_tasks
                  WHERE snapshot_date = %s
                    AND run_id = %s
                  GROUP BY 1, 2
                )
                SELECT
                  f.farmer_id,
                  MAX(f.farmer_name) AS farmer_name,
                  COUNT(*)::INTEGER AS field_count,
                  COUNT(*) FILTER (WHERE t.field_uuid IS NULL)::INTEGER AS no_task_field_count
                FROM fields_uniq f
                LEFT JOIN task_fields t
                  ON t.farmer_id = f.farmer_id
                 AND t.field_uuid = f.field_uuid
                GROUP BY f.farmer_id
                HAVING COUNT(*) FILTER (WHERE t.field_uuid IS NULL) > 0
                ORDER BY no_task_field_count DESC, farmer_name
                """,
                (snapshot_date, run_id, snapshot_date, run_id),
            )
            rows = cur.fetchall() or []
    return [
        {
            "id": str(r.get("farmer_id") or ""),
            "name": str(r.get("farmer_name") or ""),
            "field_count": int(r.get("field_count") or 0),
            "no_task_field_count": int(r.get("no_task_field_count") or 0),
        }
        for r in rows
    ]


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


def fetch_dashboard_summary_cache(snapshot_date: date, run_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    with _connect() as conn:
        with conn.cursor() as cur:
            if run_id:
                cur.execute(
                    """
                    SELECT payload
                      FROM hfr_snapshot_dashboard_summary_cache
                     WHERE snapshot_date = %s
                       AND run_id = %s
                     LIMIT 1
                    """,
                    (snapshot_date, run_id),
                )
            else:
                cur.execute(
                    """
                    SELECT payload
                      FROM hfr_snapshot_dashboard_summary_cache
                     WHERE snapshot_date = %s
                     ORDER BY updated_at DESC
                     LIMIT 1
                    """,
                    (snapshot_date,),
                )
            row = cur.fetchone() or {}
            payload = row.get("payload")
            if isinstance(payload, dict):
                return payload
            return None


def upsert_dashboard_summary_cache(snapshot_date: date, run_id: str, payload: Dict[str, Any]) -> None:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO hfr_snapshot_dashboard_summary_cache (
                  snapshot_date, run_id, payload, updated_at
                )
                VALUES (%s, %s, %s::jsonb, NOW())
                ON CONFLICT (snapshot_date, run_id)
                DO UPDATE SET
                  payload = EXCLUDED.payload,
                  updated_at = NOW()
                """,
                (snapshot_date, run_id, json.dumps(payload, ensure_ascii=False, default=str)),
            )
        conn.commit()


def rebuild_dashboard_summary_cache(snapshot_date: date, run_id: str) -> Dict[str, Any]:
    with _connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id, snapshot_date, started_at, finished_at
                  FROM hfr_snapshot_runs
                 WHERE run_id = %s
                 LIMIT 1
                """,
                (run_id,),
            )
            run = cur.fetchone() or {
                "run_id": run_id,
                "snapshot_date": snapshot_date,
                "started_at": None,
                "finished_at": None,
            }
            cur.execute(
                """
                SELECT
                  farm_uuid,
                  farm_name,
                  field_uuid,
                  COALESCE(NULLIF(task_name, ''), CASE task_type
                    WHEN 'Harvest' THEN '収穫'
                    WHEN 'Spraying' THEN '防除'
                    WHEN 'WaterManagement' THEN '水管理'
                    WHEN 'Scouting' THEN '生育調査'
                    WHEN 'CropEstablishment' THEN '播種'
                    WHEN 'LandPreparation' THEN '土づくり'
                    WHEN 'SeedTreatment' THEN '種子処理'
                    WHEN 'SeedBoxTreatment' THEN '育苗箱処理'
                    ELSE COALESCE(task_type, 'その他')
                  END) AS family,
                  COALESCE(occurrence, 1) AS occurrence,
                  COALESCE(planned_date::date, task_date) AS planned_day,
                  (execution_date IS NOT NULL OR status IN ('DONE','COMPLETED','EXECUTED')) AS done
                FROM hfr_snapshot_tasks
                WHERE snapshot_date = %s
                  AND run_id = %s
                """,
                (snapshot_date, run_id),
            )
            rows = cur.fetchall() or []
            cur.execute(
                """
                SELECT farm_uuid, farm_name, field_uuid, area_m2
                  FROM hfr_snapshot_fields
                 WHERE snapshot_date = %s
                   AND run_id = %s
                """,
                (snapshot_date, run_id),
            )
            field_rows = cur.fetchall() or []

    field_area_by_uuid: Dict[str, float] = {}
    farmer_name_by_id: Dict[str, str] = {}
    farmer_fields: Dict[str, set] = {}
    for f in field_rows:
        field_uuid = str(f.get("field_uuid") or "")
        if field_uuid:
            area_val = f.get("area_m2")
            try:
                area_num = float(area_val) if area_val is not None else 0.0
            except Exception:
                area_num = 0.0
            current = field_area_by_uuid.get(field_uuid)
            if current is None or area_num > current:
                field_area_by_uuid[field_uuid] = area_num

        farm_uuid = str(f.get("farm_uuid") or "").strip()
        farm_name = str(f.get("farm_name") or "")
        farmer_id = farm_uuid if farm_uuid else f"name:{farm_name}"
        if farmer_id not in farmer_name_by_id:
            farmer_name_by_id[farmer_id] = farm_name
        if field_uuid:
            farmer_fields.setdefault(farmer_id, set()).add(field_uuid)

    field_agg = {
        "field_count": len(field_area_by_uuid),
        "total_area_m2": float(sum(field_area_by_uuid.values())),
        "farmer_count": len(farmer_fields),
    }
    day_key = snapshot_date.isoformat()
    in7days = (snapshot_date + timedelta(days=7)).isoformat()
    in3days = (snapshot_date + timedelta(days=3)).isoformat()

    def _rate(a: int, b: int) -> float:
        return round((a * 100.0 / b), 1) if b > 0 else 0.0

    def _counts(items: List[Dict[str, Any]]) -> Dict[str, int]:
        due = completed = overdue = due_today = upcoming3 = future = 0
        for t in items:
            planned = str(t.get("planned_day") or "")
            done = bool(t.get("done"))
            if not planned:
                if not done:
                    future += 1
                continue
            if planned <= day_key:
                due += 1
                if done:
                    completed += 1
            if planned < day_key and not done:
                overdue += 1
            if planned == day_key and not done:
                due_today += 1
            if day_key < planned <= in3days and not done:
                upcoming3 += 1
            if planned > day_key and not done:
                future += 1
        if due == 0 and items:
            due = len(items)
            completed = sum(1 for t in items if t.get("done"))
        return {
            "due": due,
            "completed": completed,
            "overdue": overdue,
            "due_today": due_today,
            "upcoming3": upcoming3,
            "future": future,
        }

    farmers_map: Dict[str, Dict[str, Any]] = {}
    task_fields_by_farmer: Dict[str, set] = {}
    for task in rows:
        farmer_id = str(task.get("farm_uuid") or f"name:{task.get('farm_name') or ''}")
        row = farmers_map.get(farmer_id)
        if not row:
            row = {"id": farmer_id, "name": str(task.get("farm_name") or ""), "field_set": set(), "tasks": []}
            farmers_map[farmer_id] = row
        field_uuid = str(task.get("field_uuid") or "")
        if field_uuid:
            row["field_set"].add(field_uuid)
            task_fields_by_farmer.setdefault(farmer_id, set()).add(field_uuid)
        row["tasks"].append(task)

    farmers: List[Dict[str, Any]] = []
    farmer_details: Dict[str, Any] = {}
    for idx, (farmer_id, row) in enumerate(farmers_map.items(), start=1):
        c = _counts(row["tasks"])
        delay_rate = _rate(c["overdue"], c["due"])
        completion_rate = _rate(c["completed"], c["due"])
        farmers.append({
            "id": farmer_id,
            "name": row["name"] or f"農業者{idx}",
            "field_count": len(row["field_set"]),
            "due_task_count": c["due"],
            "completed_count": c["completed"],
            "overdue_count": c["overdue"],
            "due_today_count": c["due_today"],
            "upcoming_3days_count": c["upcoming3"],
            "future_task_count": c["future"],
            "delay_rate": delay_rate,
            "completion_rate": completion_rate,
            "delay_status": "good" if delay_rate < 15 else ("warn" if delay_rate < 30 else "bad"),
            "trend_direction": "stable",
        })
        type_map: Dict[str, List[Dict[str, Any]]] = {}
        for t in row["tasks"]:
            label = f"{str(t.get('family') or '')} {int(t.get('occurrence') or 1)}回目"
            type_map.setdefault(label, []).append(t)
        type_rows = []
        for order, name in enumerate(sorted(type_map.keys()), start=1):
            tc = _counts(type_map[name])
            type_rows.append({
                "name": name,
                "display_order": order,
                "due_count": tc["due"],
                "completed_count": tc["completed"],
                "overdue_count": tc["overdue"],
                "pending_count": tc["future"],
                "completion_rate": _rate(tc["completed"], tc["due"]),
                "delay_rate": _rate(tc["overdue"], tc["due"]),
            })
        farmer_details[farmer_id] = {
            "id": farmer_id,
            "name": row["name"] or f"農業者{idx}",
            "field_count": len(row["field_set"]),
            "summary": {
                "due": c["due"],
                "completed": c["completed"],
                "overdue": c["overdue"],
                "pending": c["future"],
                "delay_rate": delay_rate,
                "completion_rate": completion_rate,
            },
            "task_types": type_rows,
        }

    total_due = sum(f["due_task_count"] for f in farmers)
    total_completed = sum(f["completed_count"] for f in farmers)
    total_overdue = sum(f["overdue_count"] for f in farmers)
    total_due_today = sum(f["due_today_count"] for f in farmers)
    total_upcoming = sum(f["upcoming_3days_count"] for f in farmers)
    total_future = sum(f["future_task_count"] for f in farmers)

    type_map_all: Dict[str, List[Dict[str, Any]]] = {}
    for t in rows:
        label = f"{str(t.get('family') or '')} {int(t.get('occurrence') or 1)}回目"
        type_map_all.setdefault(label, []).append(t)
    task_types = []
    for order, name in enumerate(sorted(type_map_all.keys()), start=1):
        tc = _counts(type_map_all[name])
        task_types.append({
            "task_type_name": name,
            "display_order": order,
            "due_count": tc["due"],
            "completed_count": tc["completed"],
            "overdue_count": tc["overdue"],
            "pending_count": tc["future"],
            "completion_rate": _rate(tc["completed"], tc["due"]),
            "delay_rate": _rate(tc["overdue"], tc["due"]),
        })

    distribution_buckets = [(0, 5, "#22c55e", "0-5%"), (5, 10, "#22c55e", "5-10%"), (10, 15, "#22c55e", "10-15%"),
                            (15, 20, "#f59e0b", "15-20%"), (20, 25, "#f59e0b", "20-25%"), (25, 30, "#f59e0b", "25-30%"),
                            (30, 1000, "#ef4444", "30%+")]
    distribution = []
    for lo, hi, color, label in distribution_buckets:
        cnt = sum(1 for f in farmers if lo <= float(f["delay_rate"]) < hi)
        distribution.append({"bucket": label, "count": cnt, "color": color})

    total_done_count = sum(1 for t in rows if t.get("done"))
    total_tasks = len(rows)
    trend_start = (snapshot_date - timedelta(days=29)).isoformat()
    planned_on_all: Dict[str, int] = {}
    planned_on_notdone: Dict[str, int] = {}
    completed_on: Dict[str, int] = {}
    for t in rows:
        planned = str(t.get("planned_day") or "")
        if not planned:
            continue
        done = bool(t.get("done"))
        planned_on_all[planned] = planned_on_all.get(planned, 0) + 1
        if not done:
            planned_on_notdone[planned] = planned_on_notdone.get(planned, 0) + 1
        else:
            completed_on[planned] = completed_on.get(planned, 0) + 1
    cum_due = 0
    cum_completed = 0
    cum_overdue = 0
    for t in rows:
        planned = str(t.get("planned_day") or "")
        if not planned or planned >= trend_start:
            continue
        cum_due += 1
        if bool(t.get("done")):
            cum_completed += 1
        else:
            cum_overdue += 1

    trend = []
    for offset in range(29, -1, -1):
        key = (snapshot_date - timedelta(days=offset)).isoformat()
        cum_due += planned_on_all.get(key, 0)
        cum_completed += completed_on.get(key, 0)
        effective_due = cum_due
        effective_completed = cum_completed
        effective_overdue = cum_overdue
        if effective_due == 0 and total_tasks > 0:
            effective_due = total_tasks
            effective_completed = total_done_count
            effective_overdue = 0
        trend.append({
            "date": f"{int(key[5:7])}/{int(key[8:10])}",
            "completion_rate": _rate(effective_completed, effective_due),
            "delay_rate": _rate(effective_overdue, effective_due),
        })
        cum_overdue += planned_on_notdone.get(key, 0)

    as_of = run.get("finished_at") or run.get("started_at") or day_key
    payload: Dict[str, Any] = {
        "ok": True,
        "snapshot_date": day_key,
        "farmer_count": int(field_agg.get("farmer_count") or 0),
        "field_count": int(field_agg.get("field_count") or 0),
        "total_area_m2": float(field_agg.get("total_area_m2") or 0.0),
        "kpi": {
            "completion_rate": _rate(total_completed, total_due),
            "completed_count": total_completed,
            "due_count": total_due,
            "overdue_count": total_overdue,
            "delay_rate": _rate(total_overdue, total_due),
            "due_today_count": total_due_today,
            "upcoming_3days_count": total_upcoming,
            "future_count": total_future,
            "total_task_count": total_tasks,
            "as_of": str(as_of),
        },
        "farmers": farmers,
        "task_types": task_types,
        "distribution": distribution,
        "trend": trend,
        "farmer_details": farmer_details,
        "no_task_farmers": [
            {
                "id": farmer_id,
                "name": farmer_name_by_id.get(farmer_id, ""),
                "field_count": len(fields_all),
                "no_task_field_count": len(fields_all - task_fields_by_farmer.get(farmer_id, set())),
            }
            for farmer_id, fields_all in sorted(
                farmer_fields.items(),
                key=lambda item: (
                    -len(item[1] - task_fields_by_farmer.get(item[0], set())),
                    farmer_name_by_id.get(item[0], ""),
                ),
            )
            if len(fields_all - task_fields_by_farmer.get(farmer_id, set())) > 0
        ],
        "as_of": str(as_of),
    }
    upsert_dashboard_summary_cache(snapshot_date, run_id, payload)
    return payload
