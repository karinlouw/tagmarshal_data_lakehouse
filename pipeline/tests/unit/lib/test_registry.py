"""Tests for tm_lakehouse.registry module."""

import hashlib
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from tm_lakehouse.registry import (
    compute_file_hash,
    get_ingestion_status,
    get_missing_ingestions,
    is_already_ingested,
    register_ingestion_failure,
    register_ingestion_skipped,
    register_ingestion_start,
    register_ingestion_success,
)


def test_compute_file_hash_exists(tmp_path):
    """Test compute_file_hash() computes MD5 hash of file."""
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")
    
    hash_value = compute_file_hash(str(test_file))
    
    # Compute expected hash
    expected_hash = hashlib.md5(b"test content").hexdigest()
    assert hash_value == expected_hash


def test_compute_file_hash_nonexistent():
    """Test compute_file_hash() returns None for non-existent file."""
    assert compute_file_hash("/nonexistent/file.txt") is None


def test_is_already_ingested_true(mock_postgres_connection):
    """Test is_already_ingested() returns True when record exists."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.fetchone.return_value = (1,)  # Found 1 record
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        assert is_already_ingested("file.csv", "2024-01-01", "bronze") is True
        mock_cursor.execute.assert_called_once()


def test_is_already_ingested_false(mock_postgres_connection):
    """Test is_already_ingested() returns False when record doesn't exist."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.fetchone.return_value = (0,)  # Not found
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        assert is_already_ingested("file.csv", "2024-01-01", "bronze") is False


def test_is_already_ingested_handles_exception(mock_postgres_connection):
    """Test is_already_ingested() returns False on database error."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.execute.side_effect = Exception("DB error")
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        # Should return False and not raise
        assert is_already_ingested("file.csv", "2024-01-01", "bronze") is False


def test_register_ingestion_start(mock_postgres_connection):
    """Test register_ingestion_start() inserts record and returns log_id."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.fetchone.return_value = (123,)  # Return log ID
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        log_id = register_ingestion_start(
            "file.csv", "course1", "2024-01-01", "bronze", "run123", "task1"
        )
        assert log_id == 123
        mock_conn.commit.assert_called_once()


def test_register_ingestion_start_handles_exception(mock_postgres_connection):
    """Test register_ingestion_start() returns None on database error."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.execute.side_effect = Exception("DB error")
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        log_id = register_ingestion_start("file.csv", "course1", "2024-01-01", "bronze")
        assert log_id is None


def test_register_ingestion_success(mock_postgres_connection):
    """Test register_ingestion_success() updates record."""
    mock_conn, mock_cursor = mock_postgres_connection
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        result = register_ingestion_success(
            123, 1000, "s3://bucket/key", file_size_bytes=5000, file_hash="abc123"
        )
        assert result is True
        mock_conn.commit.assert_called_once()


def test_register_ingestion_success_handles_exception(mock_postgres_connection):
    """Test register_ingestion_success() returns False on database error."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.execute.side_effect = Exception("DB error")
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        result = register_ingestion_success(123, 1000, "s3://bucket/key")
        assert result is False


def test_register_ingestion_failure(mock_postgres_connection):
    """Test register_ingestion_failure() updates record with error."""
    mock_conn, mock_cursor = mock_postgres_connection
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        result = register_ingestion_failure(123, "Error message")
        assert result is True
        mock_conn.commit.assert_called_once()


def test_register_ingestion_failure_handles_exception(mock_postgres_connection):
    """Test register_ingestion_failure() returns False on database error."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.execute.side_effect = Exception("DB error")
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        result = register_ingestion_failure(123, "Error")
        assert result is False


def test_register_ingestion_skipped(mock_postgres_connection):
    """Test register_ingestion_skipped() inserts skip record."""
    mock_conn, mock_cursor = mock_postgres_connection
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        result = register_ingestion_skipped(
            "file.csv", "course1", "2024-01-01", "bronze", "Already ingested"
        )
        assert result is True
        mock_conn.commit.assert_called_once()


def test_register_ingestion_skipped_handles_exception(mock_postgres_connection):
    """Test register_ingestion_skipped() returns False on database error."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.execute.side_effect = Exception("DB error")
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        result = register_ingestion_skipped("file.csv", "course1", "2024-01-01", "bronze")
        assert result is False


def test_get_ingestion_status(mock_postgres_connection):
    """Test get_ingestion_status() queries with filters."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.fetchall.return_value = [
        {"course_id": "course1", "layer": "bronze", "status": "success"}
    ]
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        results = get_ingestion_status(ingest_date="2024-01-01", layer="bronze")
        assert len(results) == 1
        assert results[0]["course_id"] == "course1"


def test_get_ingestion_status_handles_exception(mock_postgres_connection):
    """Test get_ingestion_status() returns empty list on database error."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.execute.side_effect = Exception("DB error")
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        results = get_ingestion_status()
        assert results == []


def test_get_missing_ingestions(mock_postgres_connection):
    """Test get_missing_ingestions() finds courses not ingested today."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.fetchall.return_value = [
        {"course_id": "course1", "last_ingested": "2024-01-01"}
    ]
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        results = get_missing_ingestions(layer="bronze")
        assert len(results) == 1
        assert results[0]["course_id"] == "course1"


def test_get_missing_ingestions_handles_exception(mock_postgres_connection):
    """Test get_missing_ingestions() returns empty list on database error."""
    mock_conn, mock_cursor = mock_postgres_connection
    mock_cursor.execute.side_effect = Exception("DB error")
    
    with patch("tm_lakehouse.registry.get_db_connection", return_value=mock_conn):
        results = get_missing_ingestions()
        assert results == []

