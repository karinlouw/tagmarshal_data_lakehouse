"""Observability utilities - run summaries and artifact uploads."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from tm_lakehouse.config import TMConfig
from tm_lakehouse.s3 import make_s3_client, put_json


def obs_key(cfg: TMConfig, *parts: str) -> str:
    """Build an S3 key path under the observability prefix."""
    base = cfg.obs_prefix.strip("/") if cfg.obs_prefix else ""
    all_parts = [p.strip("/") for p in parts if p.strip("/")]
    if base:
        return "/".join([base, *all_parts])
    return "/".join(all_parts)


def write_run_summary(cfg: TMConfig, stage: str, run_id: str, payload: dict) -> str:
    """Write a run summary JSON to the observability bucket."""
    s3 = make_s3_client(cfg)
    payload = {
        "stage": stage,
        "run_id": run_id,
        "ts": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    # Path: <obs_prefix>/<stage>/run_id=<run_id>.json
    key = obs_key(cfg, stage, f"run_id={run_id}.json")
    put_json(s3, cfg.bucket_observability, key, payload)
    print(f"  → Run summary: s3://{cfg.bucket_observability}/{key}")
    return key


def upload_file(cfg: TMConfig, local_path: str, dest_key: str) -> None:
    """Upload a local file to the observability bucket."""
    if not os.path.exists(local_path):
        raise FileNotFoundError(local_path)
    s3 = make_s3_client(cfg)
    s3.upload_file(local_path, cfg.bucket_observability, dest_key)
    print(f"  → Artifact: s3://{cfg.bucket_observability}/{dest_key}")
