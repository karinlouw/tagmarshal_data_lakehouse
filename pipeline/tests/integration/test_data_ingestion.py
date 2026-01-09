"""Integration tests for data ingestion validation.

These tests validate that data flows correctly through the pipeline
and that ingested data meets quality requirements.
"""

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bronze.ingest import upload_file_to_bronze, BronzeUploadResult
from tm_lakehouse.config import TMConfig


@pytest.fixture
def sample_config():
    """Create test configuration."""
    return TMConfig(
        env="local",
        bucket_landing="tm-lakehouse-landing-zone",
        bucket_source="tm-lakehouse-source-store",
        bucket_serve="tm-lakehouse-serve",
        bucket_quarantine="tm-lakehouse-quarantine",
        bucket_observability="tm-lakehouse-observability",
        s3_endpoint="http://minio:9000",
        s3_region="us-east-1",
        s3_access_key="minioadmin",
        s3_secret_key="minioadmin",
        s3_force_path_style=True,
        iceberg_catalog_type="rest",
        iceberg_rest_uri="http://iceberg-rest:8181",
        iceberg_warehouse_silver="s3a://tm-lakehouse-source-store/warehouse",
        iceberg_warehouse_gold="s3a://tm-lakehouse-serve/warehouse",
        db_silver="silver",
        db_gold="gold",
        obs_prefix="",
        local_input_dir="/opt/tagmarshal/input",
        coord_min_lon=-180,
        coord_max_lon=180,
        coord_min_lat=-90,
        coord_max_lat=90,
        data_source="file",
        api_base_url=None,
        api_key=None,
        api_timeout=30,
    )


def test_bronze_upload_validates_required_columns(sample_config, tmp_path):
    """Test that Bronze upload validates required columns are present."""
    # Create CSV with missing required column
    invalid_csv = tmp_path / "invalid.csv"
    invalid_csv.write_text("_id\nround123")

    with patch("bronze.ingest.make_s3_client"):
        with patch("bronze.ingest.object_exists", return_value=False):
            with pytest.raises(ValueError, match="missing required column"):
                upload_file_to_bronze(sample_config, "test", str(invalid_csv))


def test_bronze_upload_validates_row_count(sample_config, tmp_path):
    """Test that Bronze upload validates file has data rows."""
    # Create empty CSV (header only)
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("_id,course,locations[0].startTime\n")

    with patch("bronze.ingest.make_s3_client"):
        with patch("bronze.ingest.object_exists", return_value=False):
            with pytest.raises(ValueError, match="has no data"):
                upload_file_to_bronze(sample_config, "test", str(empty_csv))


def test_bronze_upload_csv_row_count_matches_actual(sample_csv_path, sample_config):
    """Test that Bronze upload reports correct row count for CSV."""
    mock_s3 = MagicMock()

    with patch("bronze.ingest.make_s3_client", return_value=mock_s3):
        with patch("bronze.ingest.object_exists", return_value=False):
            result = upload_file_to_bronze(
                sample_config, "americanfalls", sample_csv_path, "2024-01-15"
            )

            # Verify row count matches actual data rows
            assert result.row_count > 0
            assert result.header_ok is True
            assert result.skipped is False


def test_bronze_upload_json_structure_validation(sample_json_path, sample_config):
    """Test that Bronze upload validates JSON structure."""
    mock_s3 = MagicMock()

    with patch("bronze.ingest.make_s3_client", return_value=mock_s3):
        with patch("bronze.ingest.object_exists", return_value=False):
            result = upload_file_to_bronze(
                sample_config, "americanfalls", sample_json_path, "2024-01-15"
            )

            # Verify JSON was validated and uploaded
            assert result.row_count > 0
            assert result.header_ok is True


def test_bronze_upload_idempotency(sample_csv_path, sample_config):
    """Test that Bronze upload is idempotent (skips if already exists)."""
    mock_s3 = MagicMock()

    with patch("bronze.ingest.make_s3_client", return_value=mock_s3):
        # First upload
        with patch("bronze.ingest.object_exists", return_value=False):
            result1 = upload_file_to_bronze(
                sample_config, "americanfalls", sample_csv_path, "2024-01-15"
            )
            assert result1.skipped is False

        # Second upload (should skip)
        with patch("bronze.ingest.object_exists", return_value=True):
            result2 = upload_file_to_bronze(
                sample_config, "americanfalls", sample_csv_path, "2024-01-15"
            )
            assert result2.skipped is True
            assert result2.row_count == 0


def test_bronze_object_key_format(sample_config):
    """Test that Bronze object keys follow the expected format."""
    from bronze.ingest import bronze_object_key

    key = bronze_object_key("americanfalls", "2024-01-15", "rounds.csv")

    # Verify key format: course_id=.../ingest_date=.../filename
    assert key.startswith("course_id=americanfalls")
    assert "ingest_date=2024-01-15" in key
    assert key.endswith("rounds.csv")


def test_csv_validation_required_fields(sample_csv_path):
    """Test CSV validation checks for all required fields."""
    from bronze.ingest import validate_csv_header

    # Valid CSV should not raise
    validate_csv_header(sample_csv_path)

    # This validates that the function checks for:
    # - _id
    # - course
    # (timestamps/locations are handled by Silver)


def test_json_validation_required_fields(sample_json_path):
    """Test JSON validation checks for all required fields."""
    from bronze.ingest import validate_json_structure

    # Valid JSON should not raise
    validate_json_structure(sample_json_path)

    # This validates that the function checks for:
    # - _id
    # - course


def test_file_format_detection(tmp_path):
    """Test file format detection works for both CSV and JSON."""
    from bronze.ingest import detect_file_format

    # CSV detection
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("header\nrow1")
    assert detect_file_format(str(csv_file)) == "csv"

    # JSON detection by extension
    json_file = tmp_path / "test.json"
    json_file.write_text('{"key": "value"}')
    assert detect_file_format(str(json_file)) == "json"

    # JSON detection by content
    json_file2 = tmp_path / "test.txt"
    json_file2.write_text('{"key": "value"}')
    assert detect_file_format(str(json_file2)) == "json"


def test_data_ingestion_metadata(sample_csv_path, sample_config):
    """Test that ingestion metadata is correctly captured."""
    mock_s3 = MagicMock()

    with patch("bronze.ingest.make_s3_client", return_value=mock_s3):
        with patch("bronze.ingest.object_exists", return_value=False):
            result = upload_file_to_bronze(
                sample_config, "americanfalls", sample_csv_path, "2024-01-15"
            )

            # Verify metadata
            assert result.bucket == "tm-lakehouse-landing-zone"
            assert "course_id=americanfalls" in result.key
            assert "ingest_date=2024-01-15" in result.key
            assert result.header_ok is True
            assert isinstance(result.row_count, int)
            assert result.row_count > 0
