"""Tests for tm_lakehouse.observability module."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from tm_lakehouse.observability import obs_key, upload_file, write_run_summary


def test_obs_key_no_prefix(sample_config):
    """Test obs_key() without prefix."""
    key = obs_key(sample_config, "bronze", "run123.json")
    assert key == "bronze/run123.json"


def test_obs_key_with_prefix(sample_config):
    """Test obs_key() with prefix."""
    config_with_prefix = sample_config.__class__(
        **{**sample_config.__dict__, "obs_prefix": "prod/"}
    )
    key = obs_key(config_with_prefix, "bronze", "run123.json")
    assert key == "prod/bronze/run123.json"


def test_obs_key_strips_slashes(sample_config):
    """Test obs_key() strips leading/trailing slashes."""
    key = obs_key(sample_config, "/bronze/", "/run123.json/")
    assert key == "bronze/run123.json"


def test_obs_key_empty_parts(sample_config):
    """Test obs_key() filters out empty parts."""
    key = obs_key(sample_config, "", "bronze", "", "run123.json")
    assert key == "bronze/run123.json"


def test_write_run_summary(mock_s3_client, sample_config):
    """Test write_run_summary() uploads JSON with timestamp."""
    with patch("tm_lakehouse.observability.make_s3_client", return_value=mock_s3_client):
        key = write_run_summary(
            sample_config, "bronze", "run123", {"course_id": "course1", "rows": 100}
        )
        
        assert key == "bronze/run_id=run123.json"
        mock_s3_client.put_object.assert_called_once()
        call_kwargs = mock_s3_client.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "tm-lakehouse-observability"
        assert call_kwargs["Key"] == key
        assert b'"stage": "bronze"' in call_kwargs["Body"]
        assert b'"run_id": "run123"' in call_kwargs["Body"]
        assert b'"ts":' in call_kwargs["Body"]  # Timestamp added


def test_upload_file(mock_s3_client, sample_config, tmp_path):
    """Test upload_file() uploads local file."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    
    with patch("tm_lakehouse.observability.make_s3_client", return_value=mock_s3_client):
        upload_file(sample_config, str(test_file), "test/test.txt")
        
        mock_s3_client.upload_file.assert_called_once_with(
            str(test_file), "tm-lakehouse-observability", "test/test.txt"
        )


def test_upload_file_not_found(sample_config):
    """Test upload_file() raises FileNotFoundError for missing file."""
    with patch("tm_lakehouse.observability.make_s3_client"):
        with pytest.raises(FileNotFoundError):
            upload_file(sample_config, "/nonexistent/file.txt", "test.txt")

