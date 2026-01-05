"""Tests for tm_lakehouse.api module."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from tm_lakehouse.api import (
    fetch_rounds_from_api,
    load_json_file,
    normalize_mongodb_value,
    normalize_round,
    save_normalized_json,
)


def test_normalize_mongodb_value_oid():
    """Test normalize_mongodb_value() converts $oid."""
    value = {"$oid": "507f1f77bcf86cd799439011"}
    assert normalize_mongodb_value(value) == "507f1f77bcf86cd799439011"


def test_normalize_mongodb_value_date():
    """Test normalize_mongodb_value() converts $date."""
    value = {"$date": "2024-01-15T10:00:00Z"}
    assert normalize_mongodb_value(value) == "2024-01-15T10:00:00Z"


def test_normalize_mongodb_value_regular_dict():
    """Test normalize_mongodb_value() returns regular dict unchanged."""
    value = {"key": "value"}
    assert normalize_mongodb_value(value) == {"key": "value"}


def test_normalize_mongodb_value_non_dict():
    """Test normalize_mongodb_value() returns non-dict values unchanged."""
    assert normalize_mongodb_value("string") == "string"
    assert normalize_mongodb_value(123) == 123
    assert normalize_mongodb_value(None) is None


def test_normalize_round_flattens_locations():
    """Test normalize_round() flattens locations array."""
    raw_round = {
        "_id": "round123",
        "course": "americanfalls",
        "locations": [
            {
                "hole": 1,
                "sectionNumber": 1,
                "startTime": 1000.5,
                "fixCoordinates": [-122.123, 45.678],
                "paceGap": 0.5,
            },
            {
                "hole": 2,
                "sectionNumber": 2,
                "startTime": 2000.0,
                "fixCoordinates": [-122.124, 45.679],
                "paceGap": 0.6,
            },
        ],
    }
    
    normalized = normalize_round(raw_round)
    
    assert normalized["_id"] == "round123"
    assert normalized["course"] == "americanfalls"
    assert normalized["locations[0].hole"] == 1
    assert normalized["locations[0].startTime"] == 1000.5
    assert normalized["locations[0].fixCoordinates[0]"] == -122.123
    assert normalized["locations[0].fixCoordinates[1]"] == 45.678
    assert normalized["locations[1].hole"] == 2
    assert normalized["locations[1].startTime"] == 2000.0


def test_normalize_round_handles_mongodb_types():
    """Test normalize_round() handles MongoDB $oid and $date."""
    raw_round = {
        "_id": {"$oid": "507f1f77bcf86cd799439011"},
        "startTime": {"$date": "2024-01-15T10:00:00Z"},
        "locations": [
            {
                "_id": {"$oid": "loc123"},
                "hole": 1,
                "startTime": 1000.5,
            }
        ],
    }
    
    normalized = normalize_round(raw_round)
    assert normalized["_id"] == "507f1f77bcf86cd799439011"
    assert normalized["startTime"] == "2024-01-15T10:00:00Z"
    assert normalized["locations[0]._id"] == "loc123"


def test_load_json_file_array(sample_json_path):
    """Test load_json_file() loads JSON array."""
    rounds = load_json_file(sample_json_path)
    assert len(rounds) == 2
    assert rounds[0]["_id"] == "round123"
    assert "locations[0].hole" in rounds[0]


def test_load_json_file_single_object(tmp_path):
    """Test load_json_file() handles single object (not array)."""
    test_file = tmp_path / "single.json"
    test_file.write_text(json.dumps({
        "_id": "round123",
        "course": "test",
        "locations": []
    }))
    
    rounds = load_json_file(str(test_file))
    assert len(rounds) == 1
    assert rounds[0]["_id"] == "round123"


def test_fetch_rounds_from_api_success(sample_config):
    """Test fetch_rounds_from_api() fetches and normalizes rounds."""
    api_config = sample_config.__class__(
        **{**sample_config.__dict__, "api_base_url": "https://api.example.com", "api_key": "test-key"}
    )
    
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "rounds": [
            {
                "_id": "round123",
                "course": "test",
                "locations": [{"hole": 1, "startTime": 1000.5}]
            }
        ]
    }
    mock_response.raise_for_status = MagicMock()
    
    with patch("requests.get", return_value=mock_response):
        rounds = fetch_rounds_from_api(api_config, "course1", "2024-01-01", "2024-01-31")
        
        assert len(rounds) == 1
        assert rounds[0]["_id"] == "round123"
        assert "locations[0].hole" in rounds[0]


def test_fetch_rounds_from_api_missing_key(sample_config):
    """Test fetch_rounds_from_api() raises error when API key is missing."""
    api_config = sample_config.__class__(
        **{**sample_config.__dict__, "api_base_url": "https://api.example.com", "api_key": None}
    )
    
    with pytest.raises(ValueError, match="TM_API_KEY not configured"):
        fetch_rounds_from_api(api_config, "course1", "2024-01-01", "2024-01-31")


def test_fetch_rounds_from_api_http_error(sample_config):
    """Test fetch_rounds_from_api() raises on HTTP error."""
    api_config = sample_config.__class__(
        **{**sample_config.__dict__, "api_base_url": "https://api.example.com", "api_key": "test-key"}
    )
    
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
    
    with patch("requests.get", return_value=mock_response):
        with pytest.raises(requests.HTTPError):
            fetch_rounds_from_api(api_config, "course1", "2024-01-01", "2024-01-31")


def test_save_normalized_json(tmp_path):
    """Test save_normalized_json() saves rounds to JSON file."""
    rounds = [
        {"_id": "round123", "course": "test"},
        {"_id": "round456", "course": "test"},
    ]
    
    output_file = tmp_path / "output.json"
    count = save_normalized_json(rounds, str(output_file))
    
    assert count == 2
    assert output_file.exists()
    loaded = json.loads(output_file.read_text())
    assert len(loaded) == 2
    assert loaded[0]["_id"] == "round123"

