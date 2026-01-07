"""Shared pytest fixtures for TagMarshal pipeline tests."""

import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

# Add pipeline directory to path for imports
_pipeline_dir = Path(__file__).parent.parent
if str(_pipeline_dir) not in sys.path:
    sys.path.insert(0, str(_pipeline_dir))

# Add lib to path (matching how bronze/ingest.py does it)
_lib_dir = _pipeline_dir / "lib"
if str(_lib_dir) not in sys.path:
    sys.path.insert(0, str(_lib_dir))

# Import config directly - need to handle the relative import in tm_lakehouse/__init__.py
# by importing the module directly
sys.path.insert(0, str(_lib_dir / "tm_lakehouse"))
from config import TMConfig  # noqa: E402


@pytest.fixture
def sample_config():
    """Create a sample TMConfig for testing."""
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


@pytest.fixture
def mock_s3_client():
    """Create a mocked boto3 S3 client."""
    mock_client = MagicMock()
    mock_client.head_object = MagicMock()
    mock_client.upload_file = MagicMock()
    mock_client.put_object = MagicMock()
    return mock_client


@pytest.fixture
def mock_postgres_connection():
    """Create a mocked psycopg2 connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_cursor.fetchone.return_value = (0,)  # Default: not found
    mock_cursor.fetchall.return_value = []
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    return mock_conn, mock_cursor


@pytest.fixture
def mock_spark_session():
    """Create a mocked SparkSession."""
    mock_spark = MagicMock()
    mock_df = MagicMock()
    mock_spark.read = MagicMock()
    mock_spark.read.format.return_value = mock_spark.read
    mock_spark.read.option.return_value = mock_spark.read
    mock_spark.read.json.return_value = mock_df
    mock_spark.read.csv.return_value = mock_df
    mock_spark.read.format.return_value.load.return_value = mock_df
    mock_df.count.return_value = 10
    mock_df.columns = ["_id", "course", "locations[0].startTime"]
    mock_df.schema = MagicMock()
    mock_spark.sql = MagicMock()
    mock_spark.sparkContext.setLogLevel = MagicMock()
    return mock_spark, mock_df


@pytest.fixture
def temp_file(tmp_path):
    """Create a temporary file for testing."""

    def _create_file(content: str, suffix: str = ".txt"):
        file_path = tmp_path / f"test_file{suffix}"
        file_path.write_text(content)
        return str(file_path)

    return _create_file


@pytest.fixture
def sample_csv_path():
    """Path to sample CSV fixture."""
    return str(Path(__file__).parent / "fixtures" / "sample_csv.csv")


@pytest.fixture
def sample_json_path():
    """Path to sample JSON fixture."""
    return str(Path(__file__).parent / "fixtures" / "sample_json.json")


@pytest.fixture
def sample_json_mongodb_path():
    """Path to MongoDB format JSON fixture."""
    return str(Path(__file__).parent / "fixtures" / "sample_json_mongodb.json")


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Set up mock environment variables."""
    env_vars = {
        "TM_ENV": "local",
        "TM_BUCKET_LANDING": "tm-lakehouse-landing-zone",
        "TM_BUCKET_SOURCE": "tm-lakehouse-source-store",
        "TM_BUCKET_SERVE": "tm-lakehouse-serve",
        "TM_BUCKET_QUARANTINE": "tm-lakehouse-quarantine",
        "TM_BUCKET_OBSERVABILITY": "tm-lakehouse-observability",
        "TM_S3_ENDPOINT": "http://minio:9000",
        "TM_S3_REGION": "us-east-1",
        "TM_S3_ACCESS_KEY": "minioadmin",
        "TM_S3_SECRET_KEY": "minioadmin",
        "TM_S3_FORCE_PATH_STYLE": "true",
        "TM_ICEBERG_CATALOG_TYPE": "rest",
        "TM_ICEBERG_REST_URI": "http://iceberg-rest:8181",
        "TM_ICEBERG_WAREHOUSE_SILVER": "s3a://tm-lakehouse-source-store/warehouse",
        "TM_ICEBERG_WAREHOUSE_GOLD": "s3a://tm-lakehouse-serve/warehouse",
        "TM_DB_SILVER": "silver",
        "TM_DB_GOLD": "gold",
        "TM_LOCAL_INPUT_DIR": "/opt/tagmarshal/input",
        "TM_DATA_SOURCE": "file",
        "TM_API_TIMEOUT": "30",
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    return env_vars
