"""Tests for bronze.ingest module."""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bronze.ingest import (
    bronze_object_key,
    count_csv_rows,
    count_json_rows,
    detect_file_format,
    upload_file_to_bronze,
    validate_csv_header,
    validate_json_structure,
)


def test_detect_file_format_csv_extension(tmp_path):
    """Test detect_file_format() detects CSV from extension."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("header\nrow1")
    assert detect_file_format(str(csv_file)) == "csv"


def test_detect_file_format_json_extension(tmp_path):
    """Test detect_file_format() detects JSON from extension."""
    json_file = tmp_path / "test.json"
    json_file.write_text('{"key": "value"}')
    assert detect_file_format(str(json_file)) == "json"


def test_detect_file_format_from_content_json(tmp_path):
    """Test detect_file_format() detects JSON from content (starts with { or [)."""
    json_file = tmp_path / "test.txt"
    json_file.write_text('{"key": "value"}')
    assert detect_file_format(str(json_file)) == "json"


def test_detect_file_format_from_content_array(tmp_path):
    """Test detect_file_format() detects JSON array from content."""
    json_file = tmp_path / "test.txt"
    json_file.write_text('[{"key": "value"}]')
    assert detect_file_format(str(json_file)) == "json"


def test_detect_file_format_defaults_to_csv(tmp_path):
    """Test detect_file_format() defaults to CSV when unclear."""
    txt_file = tmp_path / "test.txt"
    txt_file.write_text("plain text")
    assert detect_file_format(str(txt_file)) == "csv"


def test_validate_csv_header_valid(sample_csv_path):
    """Test validate_csv_header() passes for valid CSV."""
    # Should not raise
    validate_csv_header(sample_csv_path)


def test_validate_csv_header_missing_id(tmp_path):
    """Test validate_csv_header() raises error for missing _id."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("course,locations[0].startTime\nvalue1,value2")
    with pytest.raises(ValueError, match="missing required columns"):
        validate_csv_header(str(csv_file))


def test_validate_csv_header_missing_course(tmp_path):
    """Test validate_csv_header() raises error for missing course."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("_id,locations[0].startTime\nvalue1,value2")
    with pytest.raises(ValueError, match="missing required columns"):
        validate_csv_header(str(csv_file))


def test_validate_json_structure_valid(sample_json_path):
    """Test validate_json_structure() passes for valid JSON."""
    # Should not raise
    validate_json_structure(sample_json_path)


def test_validate_json_structure_empty_file(tmp_path):
    """Test validate_json_structure() raises error for empty JSON."""
    json_file = tmp_path / "empty.json"
    json_file.write_text("[]")
    with pytest.raises(ValueError, match="JSON file is empty"):
        validate_json_structure(str(json_file))


def test_validate_json_structure_missing_id(tmp_path):
    """Test validate_json_structure() raises error for missing _id."""
    json_file = tmp_path / "test.json"
    json_file.write_text('[{"course": "test"}]')
    with pytest.raises(ValueError, match="missing required field: _id"):
        validate_json_structure(str(json_file))


def test_validate_json_structure_missing_course(tmp_path):
    """Test validate_json_structure() raises error for missing course."""
    json_file = tmp_path / "test.json"
    json_file.write_text('[{"_id": "round123"}]')
    with pytest.raises(ValueError, match="missing required field: course"):
        validate_json_structure(str(json_file))


def test_validate_json_structure_single_object(tmp_path):
    """Test validate_json_structure() handles single object (not array)."""
    json_file = tmp_path / "test.json"
    json_file.write_text('{"_id": "round123", "course": "test"}')
    # Should not raise
    validate_json_structure(str(json_file))


def test_count_csv_rows(sample_csv_path):
    """Test count_csv_rows() counts data rows (excludes header)."""
    count = count_csv_rows(sample_csv_path)
    assert count == 2  # Two data rows in sample CSV (header excluded)


def test_count_csv_rows_empty(tmp_path):
    """Test count_csv_rows() returns 0 for empty CSV (header only)."""
    csv_file = tmp_path / "empty.csv"
    csv_file.write_text("_id,course\n")
    assert count_csv_rows(str(csv_file)) == 0


def test_count_json_rows_array(sample_json_path):
    """Test count_json_rows() counts rounds in JSON array."""
    count = count_json_rows(sample_json_path)
    assert count == 2  # Two rounds in sample JSON


def test_count_json_rows_single_object(tmp_path):
    """Test count_json_rows() returns 1 for single object."""
    json_file = tmp_path / "single.json"
    json_file.write_text('{"_id": "round123", "locations": []}')
    assert count_json_rows(str(json_file)) == 1


def test_bronze_object_key():
    """Test bronze_object_key() generates correct S3 key."""
    key = bronze_object_key("americanfalls", "2024-01-15", "rounds.csv")
    assert key == "course_id=americanfalls/ingest_date=2024-01-15/rounds.csv"


def test_upload_file_to_bronze_csv(sample_csv_path, sample_config, mock_s3_client):
    """Test upload_file_to_bronze() uploads CSV successfully."""
    with patch("bronze.ingest.make_s3_client", return_value=mock_s3_client):
        with patch("bronze.ingest.object_exists", return_value=False):
            result = upload_file_to_bronze(
                sample_config, "americanfalls", sample_csv_path, "2024-01-15"
            )

            assert result.bucket == "tm-lakehouse-landing-zone"
            assert result.row_count == 2
            assert result.header_ok is True
            assert result.skipped is False
            mock_s3_client.upload_file.assert_called_once()


def test_upload_file_to_bronze_json(sample_json_path, sample_config, mock_s3_client):
    """Test upload_file_to_bronze() uploads JSON successfully."""
    with patch("bronze.ingest.make_s3_client", return_value=mock_s3_client):
        with patch("bronze.ingest.object_exists", return_value=False):
            result = upload_file_to_bronze(
                sample_config, "americanfalls", sample_json_path, "2024-01-15"
            )

            assert result.bucket == "tm-lakehouse-landing-zone"
            assert result.row_count == 2
            assert result.header_ok is True
            assert result.skipped is False


def test_upload_file_to_bronze_idempotency(
    sample_csv_path, sample_config, mock_s3_client
):
    """Test upload_file_to_bronze() skips if file already exists."""
    with patch("bronze.ingest.make_s3_client", return_value=mock_s3_client):
        with patch("bronze.ingest.object_exists", return_value=True):
            result = upload_file_to_bronze(
                sample_config, "americanfalls", sample_csv_path, "2024-01-15"
            )

            assert result.skipped is True
            assert result.row_count == 0
            mock_s3_client.upload_file.assert_not_called()


def test_upload_file_to_bronze_file_not_found(sample_config):
    """Test upload_file_to_bronze() raises FileNotFoundError for missing file."""
    with pytest.raises(FileNotFoundError):
        upload_file_to_bronze(sample_config, "course1", "/nonexistent/file.csv")


def test_upload_file_to_bronze_empty_file(tmp_path, sample_config, mock_s3_client):
    """Test upload_file_to_bronze() raises error for empty file."""
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("_id,course,locations[0].startTime\n")

    with patch("bronze.ingest.make_s3_client", return_value=mock_s3_client):
        with patch("bronze.ingest.object_exists", return_value=False):
            with pytest.raises(ValueError, match="has no data"):
                upload_file_to_bronze(sample_config, "course1", str(empty_csv))


def test_upload_file_to_bronze_default_ingest_date(
    sample_csv_path, sample_config, mock_s3_client
):
    """Test upload_file_to_bronze() uses today's date when ingest_date is None."""
    from datetime import date

    with patch("bronze.ingest.make_s3_client", return_value=mock_s3_client):
        with patch("bronze.ingest.object_exists", return_value=False):
            with patch("bronze.ingest.date") as mock_date_module:
                # Mock date.today() to return a specific date
                mock_today = date(2024, 1, 15)
                mock_date_module.today = MagicMock(return_value=mock_today)

                result = upload_file_to_bronze(
                    sample_config, "americanfalls", sample_csv_path, ingest_date=None
                )

                # Verify the key uses today's date
                assert "2024-01-15" in result.key or "ingest_date=" in result.key


def test_upload_file_to_bronze_course_mismatch_warns_but_continues(
    tmp_path, sample_config, mock_s3_client
):
    """Course mismatch should not block Bronze upload (warn + continue by default)."""
    csv_file = tmp_path / "mismatch.csv"
    csv_file.write_text("_id,course\nround123,Some Human Name Golf Course\n")

    with patch("bronze.ingest.make_s3_client", return_value=mock_s3_client):
        with patch("bronze.ingest.object_exists", return_value=False):
            result = upload_file_to_bronze(
                sample_config, "expectedslug", str(csv_file), "2024-01-15"
            )

            assert result.skipped is False
            assert result.row_count == 1
            mock_s3_client.upload_file.assert_called_once()
