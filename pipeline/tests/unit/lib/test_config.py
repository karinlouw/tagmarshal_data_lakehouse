"""Tests for tm_lakehouse.config module."""

import os
import pytest

from tm_lakehouse.config import TMConfig, _env, _env_bool


def test_env_required_var_raises_error(monkeypatch):
    """Test that _env() raises ValueError for missing required variables."""
    monkeypatch.delenv("TM_BUCKET_LANDING", raising=False)
    with pytest.raises(ValueError, match="Missing required env var: TM_BUCKET_LANDING"):
        _env("TM_BUCKET_LANDING")


def test_env_with_default():
    """Test that _env() returns default when variable is missing."""
    assert _env("NONEXISTENT_VAR", default="default_value") == "default_value"


def test_env_returns_value(monkeypatch):
    """Test that _env() returns the environment variable value."""
    monkeypatch.setenv("TEST_VAR", "test_value")
    assert _env("TEST_VAR", default="default") == "test_value"


def test_env_empty_string_raises_error(monkeypatch):
    """Test that _env() raises error for empty string."""
    monkeypatch.setenv("TEST_VAR", "")
    with pytest.raises(ValueError, match="Missing required env var: TEST_VAR"):
        _env("TEST_VAR")


def test_env_bool_true_values(monkeypatch):
    """Test that _env_bool() returns True for various true values."""
    for value in ["1", "true", "True", "TRUE", "yes", "YES", "y", "Y", "on", "ON"]:
        monkeypatch.setenv("TEST_BOOL", value)
        assert _env_bool("TEST_BOOL") is True


def test_env_bool_false_values(monkeypatch):
    """Test that _env_bool() returns False for false values."""
    for value in ["0", "false", "False", "no", "off", "anything_else"]:
        monkeypatch.setenv("TEST_BOOL", value)
        assert _env_bool("TEST_BOOL") is False


def test_env_bool_missing_returns_default(monkeypatch):
    """Test that _env_bool() returns default when variable is missing."""
    monkeypatch.delenv("TEST_BOOL", raising=False)
    assert _env_bool("TEST_BOOL", default=True) is True
    assert _env_bool("TEST_BOOL", default=False) is False


def test_tmconfig_from_env(mock_env_vars):
    """Test that TMConfig.from_env() creates config from environment variables."""
    config = TMConfig.from_env()
    assert config.env == "local"
    assert config.bucket_landing == "tm-lakehouse-landing-zone"
    assert config.s3_endpoint == "http://minio:9000"
    assert config.s3_force_path_style is True


def test_tmconfig_missing_required_var_raises_error(monkeypatch):
    """Test that TMConfig.from_env() raises error for missing required variables."""
    monkeypatch.delenv("TM_BUCKET_LANDING", raising=False)
    with pytest.raises(ValueError, match="Missing required env var: TM_BUCKET_LANDING"):
        TMConfig.from_env()


def test_tmconfig_optional_vars_use_defaults(monkeypatch):
    """Test that optional environment variables use defaults."""
    # Set required vars
    monkeypatch.setenv("TM_BUCKET_LANDING", "landing")
    monkeypatch.setenv("TM_BUCKET_SOURCE", "source")
    monkeypatch.setenv("TM_BUCKET_SERVE", "serve")
    monkeypatch.setenv("TM_BUCKET_QUARANTINE", "quarantine")
    monkeypatch.setenv("TM_BUCKET_OBSERVABILITY", "observability")
    monkeypatch.setenv("TM_ICEBERG_WAREHOUSE_SILVER", "s3a://warehouse")
    monkeypatch.setenv("TM_ICEBERG_WAREHOUSE_GOLD", "s3a://warehouse")
    
    # Remove optional vars to test defaults
    monkeypatch.delenv("TM_ENV", raising=False)
    monkeypatch.delenv("TM_S3_REGION", raising=False)
    monkeypatch.delenv("TM_ICEBERG_CATALOG_TYPE", raising=False)
    monkeypatch.delenv("TM_DB_SILVER", raising=False)
    monkeypatch.delenv("TM_DB_GOLD", raising=False)
    monkeypatch.delenv("TM_LOCAL_INPUT_DIR", raising=False)
    monkeypatch.delenv("TM_DATA_SOURCE", raising=False)
    monkeypatch.delenv("TM_API_TIMEOUT", raising=False)
    
    config = TMConfig.from_env()
    assert config.env == "local"  # Default
    assert config.s3_region == "us-east-1"  # Default
    assert config.iceberg_catalog_type == "rest"  # Default
    assert config.db_silver == "silver"  # Default
    assert config.db_gold == "gold"  # Default
    assert config.local_input_dir == "/opt/tagmarshal/input"  # Default
    assert config.data_source == "file"  # Default
    assert config.api_timeout == 30  # Default


def test_tmconfig_frozen_dataclass(sample_config):
    """Test that TMConfig is frozen (immutable)."""
    with pytest.raises(Exception):  # Frozen dataclass raises FrozenInstanceError
        sample_config.env = "aws"  # type: ignore

