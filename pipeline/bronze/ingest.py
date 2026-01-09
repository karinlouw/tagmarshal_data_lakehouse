"""Bronze layer upload utilities - validates and uploads CSV/JSON to Landing Zone bucket.

Supports two data formats:
1. CSV files (flattened columns like locations[0].hole)
2. JSON files (MongoDB export format with nested structures)

Both formats are validated and uploaded to S3/MinIO for Silver ETL processing.
"""

from __future__ import annotations

import csv
import json
import os
import re
from dataclasses import dataclass
from datetime import date

import sys
from pathlib import Path

# Add lib to path for shared utilities
sys.path.insert(0, str(Path(__file__).parent.parent / "lib"))

from tm_lakehouse.config import TMConfig
from tm_lakehouse.s3 import make_s3_client, object_exists


@dataclass(frozen=True)
class BronzeUploadResult:
    """Result of a Bronze upload operation."""

    bucket: str
    key: str
    row_count: int
    header_ok: bool
    skipped: bool  # True if file was already uploaded (idempotency)


def detect_file_format(path: str) -> str:
    """Detect file format based on extension and content. Returns: "csv" or "json"""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".json":
        return "json"
    if ext == ".csv":
        return "csv"

    # Try to detect from content
    with open(path, "r") as f:
        first_char = f.read(1).strip()
        if first_char in "[{":
            return "json"

    return "csv"  # Default to CSV


def validate_csv_header(path: str) -> None:
    """Validate CSV has minimal required columns (_id, course).

    We only check for the absolute minimum required fields to preserve all data.
    Missing locations, timestamps, or other fields are handled by the Silver ETL.
    """
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)

    required = {"_id", "course"}
    missing = [c for c in required if c not in header]
    if missing:
        raise ValueError(f"CSV header missing required columns: {missing}")


# Todo: Check how validation works
def validate_json_structure(path: str) -> None:
    """Validate JSON has minimal required fields (_id, course).

    We only check for the absolute minimum required fields to preserve all data.
    Missing locations, timestamps, or other fields are handled by the Silver ETL.
    """
    with open(path, "r") as f:
        data = json.load(f)

    # Handle both single object and array
    rounds = data if isinstance(data, list) else [data]

    if not rounds:
        raise ValueError("JSON file is empty")

    # Check first round for minimal required structure
    first = rounds[0]

    # _id can be string or {"$oid": "..."} (MongoDB format)
    if "_id" not in first:
        raise ValueError("JSON missing required field: _id")

    # course field is required
    if "course" not in first:
        raise ValueError("JSON missing required field: course")


def count_csv_rows(path: str) -> int:
    """Count data rows in CSV (excludes header)."""
    n = 0
    with open(path, "r", newline="") as f:
        next(f)  # skip header
        for _ in f:
            n += 1
    return n


def count_json_rows(path: str) -> int:
    """Count rounds in JSON file."""
    with open(path, "r") as f:
        data = json.load(f)

    if isinstance(data, list):
        return len(data)
    return 1  # Single object


def bronze_object_key(course_id: str, ingest_date: str, filename: str) -> str:
    """Build Bronze S3 key: course_id=.../ingest_date=YYYY-MM-DD/<filename>."""
    return f"course_id={course_id}/ingest_date={ingest_date}/{filename}"


def _validate_ingest_date(ingest_date: str) -> None:
    """Ensure ingest_date is ISO (YYYY-MM-DD)."""
    try:
        date.fromisoformat(ingest_date)
    except Exception:
        raise ValueError(f"Invalid ingest_date (expected YYYY-MM-DD): {ingest_date}")


def _sample_course_from_csv(path: str) -> str | None:
    """Sample first non-empty course value from CSV."""
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            course_val = (row.get("course") or "").strip()
            if course_val:
                return course_val
    return None


def _sample_course_from_json(path: str) -> str | None:
    """Sample course field from first JSON object."""
    with open(path, "r") as f:
        data = json.load(f)
    rounds = data if isinstance(data, list) else [data]
    for r in rounds:
        course_val = (r.get("course") or "").strip() if isinstance(r, dict) else ""
        if course_val:
            return course_val
    return None


def _normalize_course_text(value: str) -> str:
    """Normalize a course identifier or course name to a comparable slug.

    This is intentionally heuristic because source data often contains human-readable
    names (e.g. "American Falls Golf Course") while our pipeline uses stable slugs
    (e.g. "americanfalls").
    """
    v = (value or "").strip().lower()
    if not v:
        return ""
    # Normalize common punctuation/spacing
    v = v.replace("&", " and ")
    v = re.sub(r"[^a-z0-9]+", " ", v)
    tokens = [t for t in v.split() if t]
    # Remove very common/generic golf-related words that appear in names but not slugs
    stop = {"golf", "course", "club", "country", "the"}
    tokens = [t for t in tokens if t not in stop]
    return "".join(tokens)


def _courses_match(course_id: str, sampled_course: str) -> bool:
    """Best-effort match between a requested course_id and a sampled course name."""
    a_raw = (course_id or "").strip().lower()
    b_raw = (sampled_course or "").strip().lower()
    if not a_raw or not b_raw:
        return True

    # Exact match (fast path)
    if a_raw == b_raw:
        return True

    a = _normalize_course_text(a_raw)
    b = _normalize_course_text(b_raw)
    if not a or not b:
        return True

    if a == b:
        return True

    # Common case: our IDs sometimes end with "gc" (golf club/course shorthand)
    if a.endswith("gc") and len(a) > 2:
        a2 = a[:-2]
        if a2 == b or b.startswith(a2) or a2.startswith(b) or a2 in b or b in a2:
            return True

    # Containment heuristic (e.g. "americanfallsgolfcourse" vs "americanfalls")
    return a in b or b in a


def upload_file_to_bronze(
    cfg: TMConfig,
    course_id: str,
    local_path: str,
    ingest_date: str | None = None,
    skip_if_exists: bool = True,
) -> BronzeUploadResult:
    """Upload CSV or JSON file to Landing Zone bucket with minimal validation.

    This function:
    1. Detects file format (CSV or JSON)
    2. Validates minimal required fields (_id, course)
    3. Counts rows/rounds
    4. Uploads file to S3 exactly as-is (no transformation, no filtering)

    The file is uploaded completely unchanged - all rows, all columns, all NULL values
    are preserved. The Silver ETL handles missing fields and NULL values.

    Args:
        cfg: Configuration object with S3 credentials and bucket names
        course_id: Course identifier (e.g., "bradshawfarmgc")
        local_path: Path to local CSV or JSON file
        ingest_date: Date in YYYY-MM-DD format (defaults to today)
        skip_if_exists: If True, skip upload if file already exists in S3

    Returns:
        BronzeUploadResult with upload details

    Raises:
        FileNotFoundError: If local file doesn't exist
        ValueError: If validation fails or file has no data
    """
    if ingest_date is None:
        ingest_date = date.today().isoformat()
    _validate_ingest_date(ingest_date)

    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)

    # Detect format and validate
    file_format = detect_file_format(local_path)

    sampled_course = None

    if file_format == "csv":
        validate_csv_header(local_path)
        row_count = count_csv_rows(local_path)
        sampled_course = _sample_course_from_csv(local_path)
    else:
        validate_json_structure(local_path)
        row_count = count_json_rows(local_path)
        sampled_course = _sample_course_from_json(local_path)

    if sampled_course and not _courses_match(course_id, sampled_course):
        msg = f"Course mismatch: file contains course '{sampled_course}' but parameter is '{course_id}'"
        # Default: warn and continue. Strict mode can be enabled for CI / production if desired.
        strict = os.getenv("TM_STRICT_COURSE_MATCH", "false").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
        )
        if strict:
            raise ValueError(msg)
        print(f"  ⚠ {msg} (continuing)")

    if row_count <= 0:
        raise ValueError(f"{file_format.upper()} file has no data")

    filename = os.path.basename(local_path)
    key = bronze_object_key(
        course_id=course_id, ingest_date=ingest_date, filename=filename
    )
    s3 = make_s3_client(cfg)

    # Idempotency: skip if already uploaded
    if skip_if_exists and object_exists(s3, cfg.bucket_landing, key):
        print(f"  ⏭  SKIP: Already exists → s3://{cfg.bucket_landing}/{key}")
        return BronzeUploadResult(
            bucket=cfg.bucket_landing,
            key=key,
            row_count=0,
            header_ok=True,
            skipped=True,
        )

    s3.upload_file(local_path, cfg.bucket_landing, key)
    print(
        f"  ✓  DONE: {row_count:,} rows ({file_format}) → s3://{cfg.bucket_landing}/{key}"
    )

    return BronzeUploadResult(
        bucket=cfg.bucket_landing,
        key=key,
        row_count=row_count,
        header_ok=True,
        skipped=False,
    )


# Keep backward compatibility alias
def upload_csv_to_bronze(
    cfg: TMConfig,
    course_id: str,
    local_path: str,
    ingest_date: str | None = None,
    skip_if_exists: bool = True,
) -> BronzeUploadResult:
    """Upload CSV to Landing Zone bucket (backward compatible alias).

    Use upload_file_to_bronze() instead - it handles both CSV and JSON.
    """
    return upload_file_to_bronze(
        cfg, course_id, local_path, ingest_date, skip_if_exists
    )
