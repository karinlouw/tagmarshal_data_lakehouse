"""Data quality validation tests.

These tests validate that ingested data meets quality requirements:
- Required fields are present
- Data types are correct
- Value ranges are valid
- No unexpected nulls
"""

import json
from pathlib import Path

import pytest


def test_csv_has_required_columns(sample_csv_path):
    """Test that sample CSV has all required columns."""
    import csv
    
    with open(sample_csv_path, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
    
    required = {"_id", "course"}
    assert all(col in header for col in required), f"Missing required columns: {required - set(header)}"
    assert "locations[0].startTime" in header, "Missing locations[0].startTime column"


def test_json_has_required_structure(sample_json_path):
    """Test that sample JSON has required structure."""
    with open(sample_json_path, "r") as f:
        data = json.load(f)
    
    # Should be a list
    assert isinstance(data, list), "JSON should be an array"
    assert len(data) > 0, "JSON should not be empty"
    
    # Check first round
    first_round = data[0]
    assert "_id" in first_round, "Missing _id field"
    assert "locations" in first_round, "Missing locations field"
    assert isinstance(first_round["locations"], list), "locations should be an array"
    assert len(first_round["locations"]) > 0, "locations array should not be empty"
    assert "startTime" in first_round["locations"][0], "Missing startTime in first location"


def test_coordinates_in_valid_range(sample_json_path):
    """Test that coordinates are in valid ranges (longitude: -180 to 180, latitude: -90 to 90)."""
    with open(sample_json_path, "r") as f:
        data = json.load(f)
    
    for round_data in data:
        for location in round_data.get("locations", []):
            coords = location.get("fixCoordinates", [])
            if len(coords) >= 2:
                longitude, latitude = coords[0], coords[1]
                if longitude is not None:
                    assert -180 <= longitude <= 180, f"Invalid longitude: {longitude}"
                if latitude is not None:
                    assert -90 <= latitude <= 90, f"Invalid latitude: {latitude}"


def test_timestamps_are_present(sample_json_path):
    """Test that timestamps are present in location data."""
    with open(sample_json_path, "r") as f:
        data = json.load(f)
    
    for round_data in data:
        for location in round_data.get("locations", []):
            # startTime should be present (can be None, but field should exist)
            assert "startTime" in location, "Missing startTime field in location"


def test_location_indices_are_sequential(sample_csv_path):
    """Test that CSV location indices are sequential (0, 1, 2, ...)."""
    import csv
    import re
    
    with open(sample_csv_path, "r") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
    
    # Find all location indices
    pattern = re.compile(r"locations\[(\d+)\]\.startTime")
    indices = []
    for col in header:
        match = pattern.match(col)
        if match:
            indices.append(int(match.group(1)))
    
    if indices:
        # Indices should be sequential starting from 0
        assert indices == list(range(len(indices))), f"Location indices not sequential: {indices}"


def test_no_duplicate_round_ids(sample_json_path):
    """Test that there are no duplicate round IDs in the data."""
    with open(sample_json_path, "r") as f:
        data = json.load(f)
    
    round_ids = [round_data.get("_id") for round_data in data]
    unique_ids = set(round_ids)
    
    assert len(round_ids) == len(unique_ids), f"Duplicate round IDs found: {round_ids}"


def test_data_types_are_correct(sample_json_path):
    """Test that data types match expected schema."""
    with open(sample_json_path, "r") as f:
        data = json.load(f)
    
    for round_data in data:
        # _id should be string
        assert isinstance(round_data.get("_id"), str), "_id should be a string"
        
        # locations should be a list
        assert isinstance(round_data.get("locations"), list), "locations should be a list"
        
        for location in round_data.get("locations", []):
            # hole should be int or None
            hole = location.get("hole")
            assert hole is None or isinstance(hole, int), f"hole should be int or None, got {type(hole)}"
            
            # startTime should be number or None
            start_time = location.get("startTime")
            assert start_time is None or isinstance(start_time, (int, float)), \
                f"startTime should be number or None, got {type(start_time)}"


def test_pace_gap_values_are_reasonable(sample_json_path):
    """Test that pace gap values are within reasonable range."""
    with open(sample_json_path, "r") as f:
        data = json.load(f)
    
    for round_data in data:
        for location in round_data.get("locations", []):
            pace_gap = location.get("paceGap")
            if pace_gap is not None:
                # Pace gap should be a reasonable number (not extremely large)
                assert isinstance(pace_gap, (int, float)), "paceGap should be a number"
                # Assuming reasonable range: -100 to 100 seconds
                assert -100 <= pace_gap <= 100, f"Unreasonable paceGap value: {pace_gap}"


def test_battery_percentage_range(sample_json_path):
    """Test that battery percentage is in valid range (0-100)."""
    with open(sample_json_path, "r") as f:
        data = json.load(f)
    
    for round_data in data:
        for location in round_data.get("locations", []):
            battery = location.get("batteryPercentage")
            if battery is not None:
                assert isinstance(battery, (int, float)), "batteryPercentage should be a number"
                assert 0 <= battery <= 100, f"Invalid battery percentage: {battery}"

