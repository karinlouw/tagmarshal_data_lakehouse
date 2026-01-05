"""Tests for orchestration.dags.gold_dbt_dag module."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from orchestration.dags.gold_dbt_dag import upload_gold_observability


def test_upload_gold_observability_success(tmp_path, sample_config):
    """Test upload_gold_observability() uploads dbt artifacts."""
    # Create mock dbt target directory
    dbt_target = tmp_path / "target"
    dbt_target.mkdir()
    
    # Create run_results.json
    run_results = {
        "metadata": {"dbt_version": "1.8.2"},
        "elapsed_time": 10.5,
        "results": [
            {
                "unique_id": "model.tagmarshal.gold.pace_summary_by_round",
                "status": "success",
                "execution_time": 5.2,
                "adapter_response": {"rows_affected": 1000},
            },
            {
                "unique_id": "model.tagmarshal.gold.signal_quality_rounds",
                "status": "success",
                "execution_time": 3.1,
                "adapter_response": {"rows_affected": 500},
            },
        ],
    }
    (dbt_target / "run_results.json").write_text(json.dumps(run_results))
    
    # Create manifest.json
    manifest = {"nodes": {}, "sources": {}}
    (dbt_target / "manifest.json").write_text(json.dumps(manifest))
    
    context = {
        "dag_run": MagicMock(run_id="test_run_123"),
    }
    
    mock_s3 = MagicMock()
    
    with patch("orchestration.dags.gold_dbt_dag.boto3.client", return_value=mock_s3):
        with patch.dict("os.environ", {
            "TM_S3_ENDPOINT": "http://minio:9000",
            "TM_S3_ACCESS_KEY": "minioadmin",
            "TM_S3_SECRET_KEY": "minioadmin",
            "TM_BUCKET_OBSERVABILITY": "tm-lakehouse-observability",
        }):
            with patch("orchestration.dags.gold_dbt_dag.os.path.exists", return_value=True):
                with patch("orchestration.dags.gold_dbt_dag.os.environ.get", side_effect=lambda k, d=None: {
                    "TM_S3_ENDPOINT": "http://minio:9000",
                    "TM_S3_ACCESS_KEY": "minioadmin",
                    "TM_S3_SECRET_KEY": "minioadmin",
                    "TM_BUCKET_OBSERVABILITY": "tm-lakehouse-observability",
                }.get(k, d)):
                    # Mock the dbt_target path
                    with patch("orchestration.dags.gold_dbt_dag.os.path.exists") as mock_exists:
                        def exists_side_effect(path):
                            if "run_results.json" in path or "manifest.json" in path:
                                return True
                            return False
                        mock_exists.side_effect = exists_side_effect
                        
                        with patch("builtins.open", create=True) as mock_open:
                            mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(run_results)
                            
                            result = upload_gold_observability(**context)
                            
                            assert result["success"] == 2
                            assert result["error"] == 0
                            assert "run_id" in result


def test_upload_gold_observability_no_run_results(tmp_path):
    """Test upload_gold_observability() handles missing run_results.json."""
    context = {
        "dag_run": MagicMock(run_id="test_run_123"),
    }
    
    mock_s3 = MagicMock()
    
    with patch("orchestration.dags.gold_dbt_dag.boto3.client", return_value=mock_s3):
        with patch.dict("os.environ", {
            "TM_S3_ENDPOINT": "http://minio:9000",
            "TM_S3_ACCESS_KEY": "minioadmin",
            "TM_S3_SECRET_KEY": "minioadmin",
            "TM_BUCKET_OBSERVABILITY": "tm-lakehouse-observability",
        }):
            with patch("orchestration.dags.gold_dbt_dag.os.path.exists", return_value=False):
                result = upload_gold_observability(**context)
                
                # Should still return result dict
                assert "run_id" in result


def test_upload_gold_observability_extracts_model_stats(tmp_path):
    """Test upload_gold_observability() extracts model statistics."""
    run_results = {
        "metadata": {"dbt_version": "1.8.2"},
        "elapsed_time": 10.5,
        "results": [
            {
                "unique_id": "model.tagmarshal.gold.test_model",
                "status": "success",
                "execution_time": 5.0,
                "adapter_response": {"rows_affected": 100},
            },
            {
                "unique_id": "model.tagmarshal.gold.error_model",
                "status": "error",
                "execution_time": 0.0,
                "adapter_response": {},
            },
        ],
    }
    
    context = {
        "dag_run": MagicMock(run_id="test_run_123"),
    }
    
    mock_s3 = MagicMock()
    
    with patch("orchestration.dags.gold_dbt_dag.boto3.client", return_value=mock_s3):
        with patch.dict("os.environ", {
            "TM_S3_ENDPOINT": "http://minio:9000",
            "TM_S3_ACCESS_KEY": "minioadmin",
            "TM_S3_SECRET_KEY": "minioadmin",
            "TM_BUCKET_OBSERVABILITY": "tm-lakehouse-observability",
        }):
            with patch("orchestration.dags.gold_dbt_dag.os.path.exists", return_value=True):
                with patch("builtins.open", create=True) as mock_open:
                    mock_open.return_value.__enter__.return_value.read.return_value = json.dumps(run_results)
                    
                    result = upload_gold_observability(**context)
                    
                    assert result["success"] == 1
                    assert result["error"] == 1

