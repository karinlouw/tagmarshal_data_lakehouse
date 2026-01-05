"""Tests for orchestration.dags.bronze_ingest_dag module."""

from unittest.mock import MagicMock, patch

import pytest

from orchestration.dags.bronze_ingest_dag import bronze_upload_task


def test_bronze_upload_task_success(sample_csv_path, sample_config):
    """Test bronze_upload_task() successfully uploads CSV."""
    context = {
        "dag_run": MagicMock(conf={"course_id": "americanfalls", "local_path": sample_csv_path}),
        "run_id": "test_run_123",
        "params": {},
    }
    
    with patch("orchestration.dags.bronze_ingest_dag.TMConfig.from_env", return_value=sample_config):
        with patch("orchestration.dags.bronze_ingest_dag.is_already_ingested", return_value=False):
            with patch("orchestration.dags.bronze_ingest_dag.register_ingestion_start", return_value=1):
                with patch("orchestration.dags.bronze_ingest_dag.compute_file_hash", return_value="abc123"):
                    with patch("orchestration.dags.bronze_ingest_dag.upload_csv_to_bronze") as mock_upload:
                        from bronze.ingest import BronzeUploadResult
                        mock_upload.return_value = BronzeUploadResult(
                            bucket="tm-lakehouse-landing-zone",
                            key="course_id=americanfalls/ingest_date=2024-01-15/rounds.csv",
                            row_count=2,
                            header_ok=True,
                            skipped=False,
                        )
                        with patch("orchestration.dags.bronze_ingest_dag.register_ingestion_success"):
                            with patch("orchestration.dags.bronze_ingest_dag.write_run_summary"):
                                result = bronze_upload_task(**context)
                                
                                assert result["row_count"] == 2
                                assert result["skipped"] is False
                                mock_upload.assert_called_once()


def test_bronze_upload_task_already_ingested(sample_csv_path, sample_config):
    """Test bronze_upload_task() skips if already ingested."""
    context = {
        "dag_run": MagicMock(conf={"course_id": "americanfalls", "local_path": sample_csv_path}),
        "run_id": "test_run_123",
        "params": {},
    }
    
    with patch("orchestration.dags.bronze_ingest_dag.TMConfig.from_env", return_value=sample_config):
        with patch("orchestration.dags.bronze_ingest_dag.is_already_ingested", return_value=True):
            with patch("orchestration.dags.bronze_ingest_dag.register_ingestion_skipped"):
                result = bronze_upload_task(**context)
                
                assert result["skipped"] is True
                assert result["row_count"] == 0


def test_bronze_upload_task_missing_course_id(sample_csv_path, sample_config):
    """Test bronze_upload_task() raises error for missing course_id."""
    context = {
        "dag_run": MagicMock(conf={}),
        "run_id": "test_run_123",
        "params": {},
    }
    
    with patch("orchestration.dags.bronze_ingest_dag.TMConfig.from_env", return_value=sample_config):
        with pytest.raises(ValueError, match="Missing required param: course_id"):
            bronze_upload_task(**context)


def test_bronze_upload_task_missing_local_path(sample_config):
    """Test bronze_upload_task() raises error for missing local_path."""
    context = {
        "dag_run": MagicMock(conf={"course_id": "americanfalls"}),
        "run_id": "test_run_123",
        "params": {},
    }
    
    with patch("orchestration.dags.bronze_ingest_dag.TMConfig.from_env", return_value=sample_config):
        with pytest.raises(ValueError, match="Missing required param: local_path"):
            bronze_upload_task(**context)


def test_bronze_upload_task_params_from_context(sample_csv_path, sample_config):
    """Test bronze_upload_task() extracts params from DAG context."""
    context = {
        "dag_run": MagicMock(conf={
            "course_id": "americanfalls",
            "local_path": sample_csv_path,
            "ingest_date": "2024-01-15",
        }),
        "run_id": "test_run_123",
        "params": {},
    }
    
    with patch("orchestration.dags.bronze_ingest_dag.TMConfig.from_env", return_value=sample_config):
        with patch("orchestration.dags.bronze_ingest_dag.is_already_ingested", return_value=False):
            with patch("orchestration.dags.bronze_ingest_dag.register_ingestion_start", return_value=1):
                with patch("orchestration.dags.bronze_ingest_dag.compute_file_hash", return_value="abc123"):
                    with patch("orchestration.dags.bronze_ingest_dag.upload_csv_to_bronze") as mock_upload:
                        from bronze.ingest import BronzeUploadResult
                        mock_upload.return_value = BronzeUploadResult(
                            bucket="bucket", key="key", row_count=2, header_ok=True, skipped=False
                        )
                        with patch("orchestration.dags.bronze_ingest_dag.register_ingestion_success"):
                            with patch("orchestration.dags.bronze_ingest_dag.write_run_summary"):
                                bronze_upload_task(**context)
                                
                                # Verify ingest_date was passed
                                call_kwargs = mock_upload.call_args[1]
                                assert call_kwargs["ingest_date"] == "2024-01-15"


def test_bronze_upload_task_handles_upload_error(sample_csv_path, sample_config):
    """Test bronze_upload_task() registers failure on upload error."""
    context = {
        "dag_run": MagicMock(conf={"course_id": "americanfalls", "local_path": sample_csv_path}),
        "run_id": "test_run_123",
        "params": {},
    }
    
    with patch("orchestration.dags.bronze_ingest_dag.TMConfig.from_env", return_value=sample_config):
        with patch("orchestration.dags.bronze_ingest_dag.is_already_ingested", return_value=False):
            with patch("orchestration.dags.bronze_ingest_dag.register_ingestion_start", return_value=1):
                with patch("orchestration.dags.bronze_ingest_dag.compute_file_hash", return_value="abc123"):
                    with patch("orchestration.dags.bronze_ingest_dag.upload_csv_to_bronze") as mock_upload:
                        mock_upload.side_effect = Exception("Upload failed")
                        with patch("orchestration.dags.bronze_ingest_dag.register_ingestion_failure") as mock_fail:
                            with pytest.raises(Exception, match="Upload failed"):
                                bronze_upload_task(**context)
                            
                            mock_fail.assert_called_once()


def test_bronze_upload_task_skipped_in_s3(sample_csv_path, sample_config):
    """Test bronze_upload_task() handles file already in S3."""
    context = {
        "dag_run": MagicMock(conf={"course_id": "americanfalls", "local_path": sample_csv_path}),
        "run_id": "test_run_123",
        "params": {},
    }
    
    with patch("orchestration.dags.bronze_ingest_dag.TMConfig.from_env", return_value=sample_config):
        with patch("orchestration.dags.bronze_ingest_dag.is_already_ingested", return_value=False):
            with patch("orchestration.dags.bronze_ingest_dag.register_ingestion_start", return_value=1):
                with patch("orchestration.dags.bronze_ingest_dag.compute_file_hash", return_value="abc123"):
                    with patch("orchestration.dags.bronze_ingest_dag.upload_csv_to_bronze") as mock_upload:
                        from bronze.ingest import BronzeUploadResult
                        mock_upload.return_value = BronzeUploadResult(
                            bucket="bucket", key="key", row_count=0, header_ok=True, skipped=True
                        )
                        with patch("orchestration.dags.bronze_ingest_dag.register_ingestion_success"):
                            result = bronze_upload_task(**context)
                            
                            assert result["skipped"] is True

