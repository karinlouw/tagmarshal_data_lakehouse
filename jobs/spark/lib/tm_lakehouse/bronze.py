"""Bronze layer upload utilities - validates and uploads CSVs to Landing Zone bucket."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import date

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


def count_csv_rows(path: str) -> int:
    """Count data rows in CSV (excludes header)."""
    n = 0
    with open(path, "r", newline="") as f:
        next(f)  # skip header
        for _ in f:
            n += 1
    return n


def bronze_object_key(course_id: str, ingest_date: str, filename: str) -> str:
    """Build Bronze S3 key: course_id=.../ingest_date=YYYY-MM-DD/<filename>."""
    return f"course_id={course_id}/ingest_date={ingest_date}/{filename}"


def upload_csv_to_bronze(
    cfg: TMConfig,
    course_id: str,
    local_path: str,
    ingest_date: str | None = None,
    skip_if_exists: bool = True,
) -> BronzeUploadResult:
    """Upload CSV to Landing Zone bucket with validation and idempotency check."""
    if ingest_date is None:
        ingest_date = date.today().isoformat()

    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)

    filename = os.path.basename(local_path)
    key = bronze_object_key(
        course_id=course_id, ingest_date=ingest_date, filename=filename
    )
    s3 = make_s3_client(cfg)

    # Idempotency: skip if already uploaded
    if skip_if_exists and object_exists(s3, cfg.bucket_landing, key):
        print(f"  ⏭  SKIP: Already exists → s3://{cfg.bucket_landing}/{key}")
        return BronzeUploadResult(
            bucket=cfg.bucket_landing, key=key, row_count=0, header_ok=True, skipped=True
        )

    validate_csv_header(local_path)
    row_count = count_csv_rows(local_path)
    if row_count <= 0:
        raise ValueError("CSV has no data rows")

    s3.upload_file(local_path, cfg.bucket_landing, key)
    print(f"  ✓  DONE: {row_count:,} rows → s3://{cfg.bucket_landing}/{key}")

    return BronzeUploadResult(
        bucket=cfg.bucket_landing,
        key=key,
        row_count=row_count,
        header_ok=True,
        skipped=False,
    )
