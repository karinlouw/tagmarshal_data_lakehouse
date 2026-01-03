"""API client for TagMarshal rounds data - fetches JSON and normalizes MongoDB format.

This module handles:
1. Fetching rounds data from the TagMarshal API
2. Normalizing MongoDB JSON format (e.g., $oid, $date) to flat structures
3. Reading local JSON files in the same MongoDB format

Usage:
    # From API (when you have API access):
    rounds = fetch_rounds_from_api(cfg, course_id, start_date, end_date)
    
    # From local JSON file (for testing/development):
    rounds = load_json_file("/path/to/rounds.json")
"""

from __future__ import annotations

import json
from typing import Any


def normalize_mongodb_value(value: Any) -> Any:
    """Convert MongoDB extended JSON types to plain Python values.
    
    Examples:
        {"$oid": "abc123"} → "abc123"
        {"$date": "2024-01-01T00:00:00Z"} → "2024-01-01T00:00:00Z"
    """
    if not isinstance(value, dict):
        return value
    
    # MongoDB ObjectId: {"$oid": "..."}
    if "$oid" in value:
        return value["$oid"]
    
    # MongoDB Date: {"$date": "..."}
    if "$date" in value:
        return value["$date"]
    
    return value


def normalize_round(raw_round: dict) -> dict:
    """Normalize a single round from MongoDB JSON to flat structure.
    
    Converts nested MongoDB format to match the CSV column structure
    that the Silver ETL expects.
    """
    result = {}
    
    # Top-level fields (normalize MongoDB types)
    for key, value in raw_round.items():
        if key == "locations":
            continue  # Handle locations separately
        result[key] = normalize_mongodb_value(value)
    
    # Flatten locations array into indexed columns
    locations = raw_round.get("locations", [])
    for i, loc in enumerate(locations):
        prefix = f"locations[{i}]"
        
        # Basic location fields
        for field in ["hole", "holeSection", "sectionNumber", "startTime",
                      "isProjected", "isProblem", "isCache", "paceGap",
                      "positionalGap", "pace", "heldUp", "holdingUp",
                      "batteryPercentage"]:
            result[f"{prefix}.{field}"] = loc.get(field)
        
        # Handle nested _id and fix (ObjectId references)
        result[f"{prefix}._id"] = normalize_mongodb_value(loc.get("_id"))
        result[f"{prefix}.fix"] = normalize_mongodb_value(loc.get("fix"))
        
        # Flatten fixCoordinates array [longitude, latitude]
        coords = loc.get("fixCoordinates", [])
        result[f"{prefix}.fixCoordinates[0]"] = coords[0] if len(coords) > 0 else None
        result[f"{prefix}.fixCoordinates[1]"] = coords[1] if len(coords) > 1 else None
    
    return result


def load_json_file(path: str) -> list[dict]:
    """Load and normalize rounds from a local JSON file.
    
    Args:
        path: Path to JSON file (MongoDB export format)
        
    Returns:
        List of normalized round dictionaries
    """
    with open(path, "r") as f:
        data = json.load(f)
    
    # Handle both single object and array
    if isinstance(data, dict):
        data = [data]
    
    return [normalize_round(r) for r in data]


def fetch_rounds_from_api(
    cfg,  # TMConfig
    course_id: str,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """Fetch rounds data from TagMarshal API.
    
    This is a placeholder for when you get API access.
    Update the endpoint and parameters based on the actual API docs.
    
    Args:
        cfg: TMConfig with API settings
        course_id: Course identifier
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        
    Returns:
        List of normalized round dictionaries
    """
    # Import here to avoid dependency issues if requests isn't installed
    import requests
    
    if not cfg.api_key:
        raise ValueError("TM_API_KEY not configured. Set it in your .env file.")
    
    # Build API request (adjust endpoint based on actual API documentation)
    url = f"{cfg.api_base_url}/v1/rounds"
    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }
    params = {
        "course_id": course_id,
        "start_date": start_date,
        "end_date": end_date,
    }
    
    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=cfg.api_timeout,
    )
    response.raise_for_status()
    
    data = response.json()
    
    # Handle API response format (adjust based on actual API)
    rounds = data.get("rounds", data) if isinstance(data, dict) else data
    
    return [normalize_round(r) for r in rounds]


def save_normalized_json(rounds: list[dict], output_path: str) -> int:
    """Save normalized rounds to a JSON file (one JSON per line = JSON Lines).
    
    Args:
        rounds: List of normalized round dictionaries
        output_path: Path to save the JSON file
        
    Returns:
        Number of rounds saved
    """
    with open(output_path, "w") as f:
        # Write as JSON array (easier for Spark to read)
        json.dump(rounds, f)
    
    return len(rounds)

