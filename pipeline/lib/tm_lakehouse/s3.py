"""S3/MinIO client utilities for Tagmarshal lakehouse."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from tm_lakehouse.config import TMConfig


def make_s3_client(cfg: TMConfig):
    """Create boto3 S3 client configured for MinIO (local) or AWS S3."""
    kwargs: dict[str, Any] = {
        "region_name": cfg.s3_region,
        "config": BotoConfig(
            s3={"addressing_style": "path" if cfg.s3_force_path_style else "virtual"}
        ),
    }
    if cfg.s3_endpoint:
        kwargs["endpoint_url"] = cfg.s3_endpoint
    if cfg.s3_access_key and cfg.s3_secret_key:
        kwargs["aws_access_key_id"] = cfg.s3_access_key
        kwargs["aws_secret_access_key"] = cfg.s3_secret_key
    return boto3.client("s3", **kwargs)


def object_exists(s3, bucket: str, key: str) -> bool:
    """Check if an S3 object exists (for idempotency checks)."""
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def put_json(s3, bucket: str, key: str, obj: dict) -> None:
    """Write a dict as JSON to S3."""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(obj).encode("utf-8"),
        ContentType="application/json",
    )


def cfg_as_dict(cfg: TMConfig) -> dict:
    """Convert config to dict with secrets masked."""
    d = asdict(cfg)
    if d.get("s3_access_key"):
        d["s3_access_key"] = "****"
    d["s3_secret_key"] = "****" if d.get("s3_secret_key") else None
    return d
