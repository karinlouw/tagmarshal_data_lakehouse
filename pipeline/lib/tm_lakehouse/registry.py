"""Ingestion registry for tracking pipeline state and preventing duplicates."""

import hashlib
import os
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor


def get_db_connection():
    """Create a connection to the Airflow Postgres database."""
    return psycopg2.connect(
        host=os.getenv("TM_POSTGRES_HOST", "postgres"),
        port=os.getenv("TM_POSTGRES_PORT", "5432"),
        database=os.getenv("TM_POSTGRES_DB", "airflow"),
        user=os.getenv("TM_POSTGRES_USER", "airflow"),
        password=os.getenv("TM_POSTGRES_PASSWORD", "airflow"),
    )


def compute_file_hash(file_path: str) -> Optional[str]:
    """Compute MD5 hash of a file for change detection."""
    if not os.path.exists(file_path):
        return None
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def is_already_ingested(filename: str, ingest_date: str, layer: str) -> bool:
    """Check if a file has already been successfully ingested for the given date and layer."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COUNT(*) FROM ingestion_log 
            WHERE filename = %s AND ingest_date = %s AND layer = %s AND status = 'success'
            """,
            (filename, ingest_date, layer),
        )
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        return count > 0
    except Exception as e:
        print(f"  ⚠️  Registry check failed: {e}")
        return False  # Proceed with ingestion if registry unavailable


def register_ingestion_start(
    filename: str,
    course_id: str,
    ingest_date: str,
    layer: str,
    dag_run_id: Optional[str] = None,
    task_id: Optional[str] = None,
) -> Optional[int]:
    """Register the start of an ingestion attempt. Returns the log ID."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO ingestion_log (filename, course_id, ingest_date, layer, status, dag_run_id, task_id, started_at)
            VALUES (%s, %s, %s, %s, 'running', %s, %s, NOW())
            ON CONFLICT (filename, ingest_date, layer) 
            DO UPDATE SET status = 'running', started_at = NOW(), retry_count = ingestion_log.retry_count + 1
            RETURNING id
            """,
            (filename, course_id, ingest_date, layer, dag_run_id, task_id),
        )
        log_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return log_id
    except Exception as e:
        print(f"  ⚠️  Registry start failed: {e}")
        return None


def register_ingestion_success(
    log_id: int,
    rows_processed: int,
    s3_path: str,
    file_size_bytes: Optional[int] = None,
    file_hash: Optional[str] = None,
) -> bool:
    """Register a successful ingestion."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE ingestion_log 
            SET status = 'success',
                rows_processed = %s,
                s3_path = %s,
                file_size_bytes = %s,
                file_hash = %s,
                completed_at = NOW(),
                duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
            WHERE id = %s
            """,
            (rows_processed, s3_path, file_size_bytes, file_hash, log_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"  ⚠️  Registry success update failed: {e}")
        return False


def register_ingestion_failure(log_id: int, error_message: str) -> bool:
    """Register a failed ingestion."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE ingestion_log 
            SET status = 'failed',
                error_message = %s,
                completed_at = NOW(),
                duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
            WHERE id = %s
            """,
            (error_message, log_id),
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"  ⚠️  Registry failure update failed: {e}")
        return False


def register_ingestion_skipped(
    filename: str,
    course_id: str,
    ingest_date: str,
    layer: str,
    reason: str = "Already ingested",
) -> bool:
    """Register that an ingestion was skipped."""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO ingestion_log (filename, course_id, ingest_date, layer, status, error_message, started_at, completed_at)
            VALUES (%s, %s, %s, %s, 'skipped', %s, NOW(), NOW())
            ON CONFLICT (filename, ingest_date, layer) DO NOTHING
            """,
            (filename, course_id, ingest_date, layer, reason),
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        print(f"  ⚠️  Registry skip update failed: {e}")
        return False


def get_ingestion_status(
    ingest_date: Optional[str] = None, layer: Optional[str] = None
) -> list:
    """Get ingestion status for a given date and/or layer."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        query = "SELECT course_id, layer, status, rows_processed, duration_seconds, s3_path, completed_at FROM ingestion_log WHERE 1=1"
        params = []

        if ingest_date:
            query += " AND ingest_date = %s"
            params.append(ingest_date)
        if layer:
            query += " AND layer = %s"
            params.append(layer)

        query += " ORDER BY completed_at DESC NULLS LAST"

        cur.execute(query, params)
        results = cur.fetchall()
        cur.close()
        conn.close()
        return results
    except Exception as e:
        print(f"  ⚠️  Registry query failed: {e}")
        return []


def get_missing_ingestions(layer: str = "bronze") -> list:
    """Get courses that haven't been ingested today but were ingested before."""
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            """
            SELECT DISTINCT course_id, MAX(ingest_date) as last_ingested
            FROM ingestion_log
            WHERE layer = %s AND status = 'success' AND ingest_date < CURRENT_DATE
              AND course_id NOT IN (
                  SELECT course_id FROM ingestion_log 
                  WHERE layer = %s AND status = 'success' AND ingest_date = CURRENT_DATE
              )
            GROUP BY course_id
            ORDER BY last_ingested DESC
            """,
            (layer, layer),
        )
        results = cur.fetchall()
        cur.close()
        conn.close()
        return results
    except Exception as e:
        print(f"  ⚠️  Registry query failed: {e}")
        return []
