#!/usr/bin/env python3
"""
Backfill Orchestrator - Resumable bulk data processing

This script handles massive backfills with:
- Checkpoint tracking (know what's done)
- Automatic resume (continue from where you left off)
- Retry logic (handle transient failures)
- Batch processing (limit concurrent jobs)
- Duplicate prevention (idempotent operations)

Usage:
    python scripts/backfill.py bronze --start-date 2018-01-01 --end-date 2024-12-31
    python scripts/backfill.py silver --course bradshawfarmgc
    python scripts/backfill.py status
"""

import argparse
import subprocess
import time
import sys
from datetime import datetime, timedelta
from pathlib import Path
import json

# Database connection (uses same Postgres as Airflow)
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "airflow"
DB_USER = "airflow"
DB_PASS = "airflow"


def run_sql(query: str, fetch: bool = True) -> list:
    """Execute SQL against the registry database."""
    cmd = [
        "docker", "exec", "airflow-postgres",
        "psql", "-U", DB_USER, "-d", DB_NAME, "-t", "-c", query
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"SQL Error: {result.stderr}")
        return []
    if fetch:
        return [line.strip() for line in result.stdout.strip().split('\n') if line.strip()]
    return []


def get_pending_jobs(layer: str, course_id: str = None, start_date: str = None, end_date: str = None) -> list:
    """Get list of course/date combinations that need processing."""
    
    # Get all available data in Bronze
    if layer == "silver":
        # For Silver, look at what's in Bronze landing zone
        cmd = [
            "docker", "exec", "airflow", "python3", "-c", """
import boto3
s3 = boto3.client('s3', endpoint_url='http://minio:9000', 
    aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')

result = s3.list_objects_v2(
    Bucket='tm-lakehouse-landing-zone',
    Delimiter='/'
)

# Get all course_id prefixes
for prefix in result.get('CommonPrefixes', []):
    course = prefix['Prefix'].replace('course_id=', '').rstrip('/')
    
    # Get all ingest_dates for this course
    dates_result = s3.list_objects_v2(
        Bucket='tm-lakehouse-landing-zone',
        Prefix=prefix['Prefix'],
        Delimiter='/'
    )
    for date_prefix in dates_result.get('CommonPrefixes', []):
        date = date_prefix['Prefix'].split('ingest_date=')[1].rstrip('/')
        print(f'{course}|{date}')
"""
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        available = []
        for line in result.stdout.strip().split('\n'):
            if '|' in line:
                course, date = line.split('|')
                if course_id and course != course_id:
                    continue
                if start_date and date < start_date:
                    continue
                if end_date and date > end_date:
                    continue
                available.append((course, date))
        
        # Get already completed from registry
        completed_query = f"""
            SELECT DISTINCT course_id || '|' || ingest_date 
            FROM ingestion_log 
            WHERE layer = 'silver' AND status = 'success'
        """
        completed = set(run_sql(completed_query))
        
        # Return pending (available minus completed)
        pending = [(c, d) for c, d in available if f"{c}|{d}" not in completed]
        return pending
    
    return []


def log_job_start(course_id: str, ingest_date: str, layer: str) -> int:
    """Log job start in registry, return job ID."""
    query = f"""
        INSERT INTO ingestion_log (course_id, ingest_date, layer, filename, status, started_at)
        VALUES ('{course_id}', '{ingest_date}', '{layer}', '{course_id}_{ingest_date}', 'running', NOW())
        ON CONFLICT (filename, ingest_date, layer) 
        DO UPDATE SET status = 'running', started_at = NOW(), retry_count = ingestion_log.retry_count + 1
        RETURNING id
    """
    result = run_sql(query)
    return int(result[0]) if result else 0


def log_job_complete(job_id: int, status: str, rows: int = 0, error: str = None):
    """Log job completion in registry."""
    error_clause = f", error_message = '{error[:500]}'" if error else ""
    query = f"""
        UPDATE ingestion_log 
        SET status = '{status}', 
            completed_at = NOW(), 
            rows_processed = {rows},
            duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at))
            {error_clause}
        WHERE id = {job_id}
    """
    run_sql(query, fetch=False)


def trigger_silver_etl(course_id: str, ingest_date: str, max_retries: int = 3) -> bool:
    """Trigger Silver ETL with retry logic."""
    bronze_prefix = f"course_id={course_id}/ingest_date={ingest_date}/"
    
    for attempt in range(max_retries):
        if attempt > 0:
            wait_time = 2 ** attempt * 30  # Exponential backoff: 30s, 60s, 120s
            print(f"    Retry {attempt}/{max_retries} in {wait_time}s...")
            time.sleep(wait_time)
        
        # Trigger the DAG
        cmd = [
            "docker", "exec", "airflow",
            "airflow", "dags", "trigger", "silver_etl",
            "--conf", json.dumps({
                "course_id": course_id,
                "ingest_date": ingest_date,
                "bronze_prefix": bronze_prefix
            })
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            # Wait for job to complete
            run_id = None
            for line in result.stdout.split('\n'):
                if 'Created' in line or 'manual__' in line:
                    # Extract run_id
                    import re
                    match = re.search(r'(manual__[\w\-:.+]+)', line)
                    if match:
                        run_id = match.group(1)
                        break
            
            if run_id:
                # Poll for completion
                for _ in range(60):  # Max 30 minutes
                    time.sleep(30)
                    status_cmd = [
                        "docker", "exec", "airflow",
                        "airflow", "dags", "list-runs", "-d", "silver_etl", "-o", "plain"
                    ]
                    status_result = subprocess.run(status_cmd, capture_output=True, text=True)
                    
                    for line in status_result.stdout.split('\n'):
                        if run_id in line:
                            if 'success' in line:
                                return True
                            elif 'failed' in line:
                                break
                    else:
                        continue
                    break  # Failed
            
        # Job failed, will retry if attempts remain
        print(f"    ❌ Attempt {attempt + 1} failed")
    
    return False


def run_backfill(layer: str, course_id: str = None, start_date: str = None, 
                 end_date: str = None, batch_size: int = 5, dry_run: bool = False):
    """Run backfill for specified layer."""
    
    print(f"\n{'='*60}")
    print(f"BACKFILL: {layer.upper()}")
    print(f"{'='*60}")
    
    # Get pending jobs
    pending = get_pending_jobs(layer, course_id, start_date, end_date)
    
    if not pending:
        print("✅ Nothing to process - all caught up!")
        return
    
    print(f"\n📋 Found {len(pending)} jobs to process")
    
    if dry_run:
        print("\n[DRY RUN] Would process:")
        for course, date in pending[:20]:
            print(f"  - {course} / {date}")
        if len(pending) > 20:
            print(f"  ... and {len(pending) - 20} more")
        return
    
    # Process in batches
    success_count = 0
    fail_count = 0
    
    for i, (course, date) in enumerate(pending):
        print(f"\n[{i+1}/{len(pending)}] Processing {course} / {date}")
        
        # Log start
        job_id = log_job_start(course, date, layer)
        
        try:
            if layer == "silver":
                success = trigger_silver_etl(course, date)
            else:
                success = False
            
            if success:
                log_job_complete(job_id, "success")
                success_count += 1
                print(f"    ✅ Success")
            else:
                log_job_complete(job_id, "failed", error="Max retries exceeded")
                fail_count += 1
                print(f"    ❌ Failed after retries")
                
        except Exception as e:
            log_job_complete(job_id, "failed", error=str(e))
            fail_count += 1
            print(f"    ❌ Error: {e}")
        
        # Rate limiting between jobs
        if (i + 1) % batch_size == 0:
            print(f"\n⏳ Batch complete. Pausing 60s...")
            time.sleep(60)
    
    print(f"\n{'='*60}")
    print(f"BACKFILL COMPLETE")
    print(f"  ✅ Success: {success_count}")
    print(f"  ❌ Failed:  {fail_count}")
    print(f"{'='*60}")


def show_status():
    """Show current backfill status."""
    print("\n📊 BACKFILL STATUS")
    print("="*60)
    
    # Get counts by layer and status
    query = """
        SELECT layer, status, count(*) 
        FROM ingestion_log 
        GROUP BY layer, status 
        ORDER BY layer, status
    """
    results = run_sql(query)
    
    print("\nBy Layer and Status:")
    for row in results:
        parts = row.split('|')
        if len(parts) >= 3:
            print(f"  {parts[0].strip():10} {parts[1].strip():10} {parts[2].strip():>8}")
    
    # Get recent failures
    query = """
        SELECT course_id, ingest_date, layer, error_message 
        FROM ingestion_log 
        WHERE status = 'failed' 
        ORDER BY completed_at DESC 
        LIMIT 10
    """
    failures = run_sql(query)
    
    if failures:
        print("\nRecent Failures:")
        for row in failures:
            parts = row.split('|')
            if len(parts) >= 3:
                print(f"  {parts[0].strip()} / {parts[1].strip()} ({parts[2].strip()})")


def retry_failed(layer: str = None):
    """Retry all failed jobs."""
    where_clause = f"AND layer = '{layer}'" if layer else ""
    
    # Reset failed jobs to pending
    query = f"""
        UPDATE ingestion_log 
        SET status = 'pending', retry_count = retry_count + 1
        WHERE status = 'failed' {where_clause}
        RETURNING course_id, ingest_date, layer
    """
    results = run_sql(query)
    
    print(f"Reset {len(results)} failed jobs to pending")
    print("Run backfill again to process them")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill orchestrator for data lakehouse")
    subparsers = parser.add_subparsers(dest="command", help="Command")
    
    # Silver backfill
    silver_parser = subparsers.add_parser("silver", help="Run Silver ETL backfill")
    silver_parser.add_argument("--course", help="Specific course ID")
    silver_parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    silver_parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    silver_parser.add_argument("--batch-size", type=int, default=5, help="Jobs per batch")
    silver_parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    
    # Status
    subparsers.add_parser("status", help="Show backfill status")
    
    # Retry failed
    retry_parser = subparsers.add_parser("retry", help="Retry failed jobs")
    retry_parser.add_argument("--layer", help="Specific layer to retry")
    
    args = parser.parse_args()
    
    if args.command == "silver":
        run_backfill("silver", args.course, args.start_date, args.end_date, 
                     args.batch_size, args.dry_run)
    elif args.command == "status":
        show_status()
    elif args.command == "retry":
        retry_failed(args.layer)
    else:
        parser.print_help()

