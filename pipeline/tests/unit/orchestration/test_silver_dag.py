"""Tests for orchestration.dags.silver_etl_dag module."""

import subprocess
from unittest.mock import MagicMock, patch

import pytest

from orchestration.dags.silver_etl_dag import log_to_registry, run_spark_etl


def test_log_to_registry_success():
    """Test log_to_registry() logs success to database."""
    with patch("orchestration.dags.silver_etl_dag.PostgresHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        
        log_to_registry("course1", "2024-01-15", "success", rows=1000, dag_run_id="run123")
        
        mock_hook.run.assert_called_once()
        sql = mock_hook.run.call_args[0][0]
        assert "INSERT INTO ingestion_log" in sql
        assert "status = 'success'" in sql
        assert "rows_processed = 1000" in sql


def test_log_to_registry_failure():
    """Test log_to_registry() logs failure with error message."""
    with patch("orchestration.dags.silver_etl_dag.PostgresHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook
        
        log_to_registry("course1", "2024-01-15", "failed", error="Spark job failed", dag_run_id="run123")
        
        mock_hook.run.assert_called_once()
        sql = mock_hook.run.call_args[0][0]
        assert "status = 'failed'" in sql
        assert "error_message" in sql


def test_log_to_registry_handles_exception():
    """Test log_to_registry() handles database errors gracefully."""
    with patch("orchestration.dags.silver_etl_dag.PostgresHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook.run.side_effect = Exception("DB error")
        mock_hook_class.return_value = mock_hook
        
        # Should not raise, just print warning
        log_to_registry("course1", "2024-01-15", "success")


def test_run_spark_etl_success():
    """Test run_spark_etl() executes Spark job successfully."""
    context = {
        "dag_run": MagicMock(conf={
            "course_id": "americanfalls",
            "ingest_date": "2024-01-15",
            "bronze_prefix": "course_id=americanfalls/ingest_date=2024-01-15/",
        }),
        "run_id": "test_run_123",
    }
    
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "→ Appended 1,234 rows"
    mock_result.stderr = ""
    
    with patch("orchestration.dags.silver_etl_dag.subprocess.run", return_value=mock_result):
        with patch("orchestration.dags.silver_etl_dag.log_to_registry"):
            with patch.dict("os.environ", {
                "TM_SPARK_MASTER": "local[2]",
                "TM_SPARK_DRIVER_MEMORY": "1g",
            }):
                result = run_spark_etl(**context)
                
                assert result["status"] == "success"
                assert result["course_id"] == "americanfalls"
                assert result["rows"] == 1234


def test_run_spark_etl_missing_params():
    """Test run_spark_etl() raises error for missing parameters."""
    context = {
        "dag_run": MagicMock(conf={}),
        "run_id": "test_run_123",
    }
    
    with pytest.raises(ValueError, match="Missing required params"):
        run_spark_etl(**context)


def test_run_spark_etl_spark_failure():
    """Test run_spark_etl() handles Spark job failure."""
    context = {
        "dag_run": MagicMock(conf={
            "course_id": "americanfalls",
            "ingest_date": "2024-01-15",
            "bronze_prefix": "course_id=americanfalls/ingest_date=2024-01-15/",
        }),
        "run_id": "test_run_123",
    }
    
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stdout = ""
    mock_result.stderr = "Spark job failed with error"
    
    with patch("orchestration.dags.silver_etl_dag.subprocess.run", return_value=mock_result):
        with patch("orchestration.dags.silver_etl_dag.log_to_registry") as mock_log:
            with patch.dict("os.environ", {"TM_SPARK_MASTER": "local[2]"}):
                with pytest.raises(RuntimeError, match="Spark job failed"):
                    run_spark_etl(**context)
                
                # Verify failure was logged
                mock_log.assert_called()
                call_kwargs = mock_log.call_args[1]
                assert call_kwargs["status"] == "failed"


def test_run_spark_etl_extracts_row_count():
    """Test run_spark_etl() extracts row count from Spark output."""
    context = {
        "dag_run": MagicMock(conf={
            "course_id": "americanfalls",
            "ingest_date": "2024-01-15",
            "bronze_prefix": "course_id=americanfalls/ingest_date=2024-01-15/",
        }),
        "run_id": "test_run_123",
    }
    
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "Some output\n→ Appended 67,660 rows\nMore output"
    mock_result.stderr = ""
    
    with patch("orchestration.dags.silver_etl_dag.subprocess.run", return_value=mock_result):
        with patch("orchestration.dags.silver_etl_dag.log_to_registry"):
            with patch.dict("os.environ", {"TM_SPARK_MASTER": "local[2]"}):
                result = run_spark_etl(**context)
                
                assert result["rows"] == 67660


def test_run_spark_etl_no_row_count_in_output():
    """Test run_spark_etl() handles missing row count in output."""
    context = {
        "dag_run": MagicMock(conf={
            "course_id": "americanfalls",
            "ingest_date": "2024-01-15",
            "bronze_prefix": "course_id=americanfalls/ingest_date=2024-01-15/",
        }),
        "run_id": "test_run_123",
    }
    
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "No row count here"
    mock_result.stderr = ""
    
    with patch("orchestration.dags.silver_etl_dag.subprocess.run", return_value=mock_result):
        with patch("orchestration.dags.silver_etl_dag.log_to_registry"):
            with patch.dict("os.environ", {"TM_SPARK_MASTER": "local[2]"}):
                result = run_spark_etl(**context)
                
                assert result["rows"] == 0  # Default when not found

