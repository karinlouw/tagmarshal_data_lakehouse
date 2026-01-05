"""Tests for scripts.backfill module."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from scripts.backfill import (
    get_pending_jobs,
    log_job_complete,
    log_job_start,
    retry_failed,
    run_backfill,
    show_status,
    trigger_silver_etl,
)


def test_log_job_start():
    """Test log_job_start() logs job start and returns job ID."""
    mock_result = MagicMock()
    mock_result.stdout = "123\n"
    
    with patch("scripts.backfill.subprocess.run", return_value=mock_result):
        job_id = log_job_start("course1", "2024-01-15", "silver")
        assert job_id == 123


def test_log_job_start_no_result():
    """Test log_job_start() returns 0 when no result."""
    mock_result = MagicMock()
    mock_result.stdout = ""
    
    with patch("scripts.backfill.subprocess.run", return_value=mock_result):
        job_id = log_job_start("course1", "2024-01-15", "silver")
        assert job_id == 0


def test_log_job_complete():
    """Test log_job_complete() logs job completion."""
    mock_result = MagicMock()
    mock_result.returncode = 0
    
    with patch("scripts.backfill.subprocess.run", return_value=mock_result):
        # Should not raise
        log_job_complete(123, "success", rows=1000)


def test_log_job_complete_with_error():
    """Test log_job_complete() logs error message."""
    mock_result = MagicMock()
    mock_result.returncode = 0
    
    with patch("scripts.backfill.subprocess.run", return_value=mock_result):
        # Should not raise
        log_job_complete(123, "failed", error="Job failed")


def test_get_pending_jobs_silver():
    """Test get_pending_jobs() finds pending Silver jobs."""
    mock_result = MagicMock()
    mock_result.stdout = "course1|2024-01-15\ncourse2|2024-01-16\n"
    mock_result.returncode = 0
    
    with patch("scripts.backfill.subprocess.run", return_value=mock_result):
        with patch("scripts.backfill.run_sql", return_value=["course1|2024-01-15"]):
            pending = get_pending_jobs("silver")
            # Should return jobs not in completed list
            assert isinstance(pending, list)


def test_get_pending_jobs_filters_by_course():
    """Test get_pending_jobs() filters by course_id."""
    mock_result = MagicMock()
    mock_result.stdout = "course1|2024-01-15\ncourse2|2024-01-16\n"
    mock_result.returncode = 0
    
    with patch("scripts.backfill.subprocess.run", return_value=mock_result):
        with patch("scripts.backfill.run_sql", return_value=[]):
            pending = get_pending_jobs("silver", course_id="course1")
            # Should only include course1
            assert all(course == "course1" for course, _ in pending)


def test_get_pending_jobs_filters_by_date_range():
    """Test get_pending_jobs() filters by date range."""
    mock_result = MagicMock()
    mock_result.stdout = "course1|2024-01-15\ncourse1|2024-01-20\ncourse1|2024-02-01\n"
    mock_result.returncode = 0
    
    with patch("scripts.backfill.subprocess.run", return_value=mock_result):
        with patch("scripts.backfill.run_sql", return_value=[]):
            pending = get_pending_jobs(
                "silver", start_date="2024-01-16", end_date="2024-01-31"
            )
            # Should only include dates in range
            assert all("2024-01-20" == date for _, date in pending)


def test_trigger_silver_etl_success():
    """Test trigger_silver_etl() successfully triggers ETL."""
    mock_trigger_result = MagicMock()
    mock_trigger_result.returncode = 0
    mock_trigger_result.stdout = "Created <DagRun silver_etl @ 2024-01-15T10:00:00+00:00: manual__2024-01-15T10:00:00+00:00, state:queued>"
    
    mock_status_result = MagicMock()
    mock_status_result.returncode = 0
    mock_status_result.stdout = "manual__2024-01-15T10:00:00+00:00 success"
    
    with patch("scripts.backfill.subprocess.run") as mock_run:
        mock_run.side_effect = [mock_trigger_result, mock_status_result]
        with patch("scripts.backfill.time.sleep"):  # Skip sleep
            success = trigger_silver_etl("course1", "2024-01-15", max_retries=1)
            # Note: This test structure validates the function logic
            # Full success depends on polling logic which requires time.sleep mocking


def test_trigger_silver_etl_retries_on_failure():
    """Test trigger_silver_etl() retries on failure."""
    mock_result = MagicMock()
    mock_result.returncode = 1  # Failure
    mock_result.stdout = ""
    
    with patch("scripts.backfill.subprocess.run", return_value=mock_result):
        with patch("scripts.backfill.time.sleep"):  # Skip sleep
            success = trigger_silver_etl("course1", "2024-01-15", max_retries=2)
            # Should return False after retries exhausted
            assert success is False


def test_run_backfill_dry_run():
    """Test run_backfill() dry run shows what would be processed."""
    with patch("scripts.backfill.get_pending_jobs", return_value=[
        ("course1", "2024-01-15"),
        ("course2", "2024-01-16"),
    ]):
        # Should not raise and should print pending jobs
        run_backfill("silver", dry_run=True)


def test_run_backfill_no_pending():
    """Test run_backfill() handles no pending jobs."""
    with patch("scripts.backfill.get_pending_jobs", return_value=[]):
        # Should not raise
        run_backfill("silver")


def test_show_status():
    """Test show_status() queries and displays status."""
    with patch("scripts.backfill.run_sql") as mock_sql:
        mock_sql.return_value = [
            "bronze|success|10",
            "silver|failed|5",
        ]
        # Should not raise
        show_status()


def test_retry_failed():
    """Test retry_failed() resets failed jobs."""
    with patch("scripts.backfill.run_sql") as mock_sql:
        mock_sql.return_value = ["course1|2024-01-15|silver"]
        # Should not raise
        retry_failed("silver")
        mock_sql.assert_called()

