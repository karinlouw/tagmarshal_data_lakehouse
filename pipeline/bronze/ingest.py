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
    """Validate CSV has required columns (_id, course, locations[0].startTime)."""
    with open(path, "r", newline="") as f:
        reader = csv.reader(f)
        header = next(reader)
    required = {"_id", "course"}
    missing = [c for c in required if c not in header]
    if missing:
        raise ValueError(f"CSV header missing required columns: {missing}")
    if "locations[0].startTime" not in header:
        raise ValueError("CSV header missing required column: locations[0].startTime")


# Todo: Check how validation works
def validate_json_structure(path: str) -> None:
    """Validate JSON has required fields (_id, locations with startTime)."""
    with open(path, "r") as f:
        data = json.load(f)

    # Handle both single object and array
    rounds = data if isinstance(data, list) else [data]

    if not rounds:
        raise ValueError("JSON file is empty")

    # Check first round for required structure
    first = rounds[0]

    # _id can be string or {"$oid": "..."} (MongoDB format)
    if "_id" not in first:
        raise ValueError("JSON missing required field: _id")

    # locations must exist and have at least one entry with startTime
    locations = first.get("locations", [])
    if not locations:
        raise ValueError("JSON missing required field: locations")

    if "startTime" not in locations[0]:
        raise ValueError("JSON locations[0] missing required field: startTime")


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


def upload_file_to_bronze(
    cfg: TMConfig,
    course_id: str,
    local_path: str,
    ingest_date: str | None = None,
    skip_if_exists: bool = True,
) -> BronzeUploadResult:
    """Upload CSV or JSON file to Landing Zone bucket with validation.

    This is the main entry point - it auto-detects file format.
    """
    if ingest_date is None:
        ingest_date = date.today().isoformat()

    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)

    # Detect format and validate
    file_format = detect_file_format(local_path)

    if file_format == "csv":
        validate_csv_header(local_path)
        row_count = count_csv_rows(local_path)
    else:
        validate_json_structure(local_path)
        row_count = count_json_rows(local_path)

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
