"""Tests for tm_lakehouse.s3 module."""

from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from tm_lakehouse.config import TMConfig
from tm_lakehouse.s3 import cfg_as_dict, make_s3_client, object_exists, put_json


def test_make_s3_client_minio(sample_config):
    """Test S3 client creation for MinIO."""
    with patch("tm_lakehouse.s3.boto3.client") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.return_value = mock_client

        client = make_s3_client(sample_config)

        mock_boto3.assert_called_once()
        call_kwargs = mock_boto3.call_args[1]
        assert call_kwargs["endpoint_url"] == "http://minio:9000"
        assert call_kwargs["aws_access_key_id"] == "minioadmin"
        assert call_kwargs["aws_secret_access_key"] == "minioadmin"
        assert call_kwargs["region_name"] == "us-east-1"


def test_make_s3_client_aws(sample_config):
    """Test S3 client creation for AWS (no endpoint)."""
    aws_config = TMConfig(
        env="aws",
        bucket_landing="bucket-landing",
        bucket_source="bucket-source",
        bucket_serve="bucket-serve",
        bucket_quarantine="bucket-quarantine",
        bucket_observability="bucket-observability",
        s3_endpoint=None,  # AWS doesn't use endpoint
        s3_region="us-west-2",
        s3_access_key=None,  # Uses IAM role
        s3_secret_key=None,
        s3_force_path_style=False,
        iceberg_catalog_type="glue",
        iceberg_rest_uri=None,
        iceberg_warehouse_silver="s3a://bucket/warehouse",
        iceberg_warehouse_gold="s3a://bucket/warehouse",
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

    with patch("tm_lakehouse.s3.boto3.client") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.return_value = mock_client

        client = make_s3_client(aws_config)

        call_kwargs = mock_boto3.call_args[1]
        assert (
            "endpoint_url" not in call_kwargs or call_kwargs.get("endpoint_url") is None
        )
        assert call_kwargs["region_name"] == "us-west-2"


def test_object_exists_true(mock_s3_client):
    """Test object_exists() returns True when object exists."""
    mock_s3_client.head_object.return_value = {}
    assert object_exists(mock_s3_client, "bucket", "key") is True
    mock_s3_client.head_object.assert_called_once_with(Bucket="bucket", Key="key")


def test_object_exists_false_404(mock_s3_client):
    """Test object_exists() returns False for 404 errors."""
    error_response = {"Error": {"Code": "404"}}
    mock_s3_client.head_object.side_effect = ClientError(error_response, "HeadObject")
    assert object_exists(mock_s3_client, "bucket", "key") is False


def test_object_exists_raises_other_errors(mock_s3_client):
    """Test object_exists() raises non-404 errors."""
    error_response = {"Error": {"Code": "403"}}
    mock_s3_client.head_object.side_effect = ClientError(error_response, "HeadObject")
    with pytest.raises(ClientError):
        object_exists(mock_s3_client, "bucket", "key")


def test_put_json(mock_s3_client):
    """Test put_json() uploads JSON correctly."""
    obj = {"key": "value", "number": 123}
    put_json(mock_s3_client, "bucket", "key", obj)

    mock_s3_client.put_object.assert_called_once()
    call_kwargs = mock_s3_client.put_object.call_args[1]
    assert call_kwargs["Bucket"] == "bucket"
    assert call_kwargs["Key"] == "key"
    assert call_kwargs["ContentType"] == "application/json"
    assert b'"key": "value"' in call_kwargs["Body"]


def test_cfg_as_dict_masks_secrets(sample_config):
    """Test cfg_as_dict() masks secrets."""
    d = cfg_as_dict(sample_config)
    assert d["s3_access_key"] == "****"
    assert d["s3_secret_key"] == "****"
    assert d["bucket_landing"] == "tm-lakehouse-landing-zone"  # Other values preserved


def test_cfg_as_dict_none_secrets(sample_config):
    """Test cfg_as_dict() handles None secrets."""
    config_no_secrets = TMConfig(
        env="aws",
        bucket_landing="bucket",
        bucket_source="bucket",
        bucket_serve="bucket",
        bucket_quarantine="bucket",
        bucket_observability="bucket",
        s3_endpoint=None,
        s3_region="us-east-1",
        s3_access_key=None,
        s3_secret_key=None,
        s3_force_path_style=False,
        iceberg_catalog_type="glue",
        iceberg_rest_uri=None,
        iceberg_warehouse_silver="s3a://warehouse",
        iceberg_warehouse_gold="s3a://warehouse",
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
    d = cfg_as_dict(config_no_secrets)
    assert d["s3_access_key"] is None
    assert d["s3_secret_key"] is None or d["s3_secret_key"] == "****"
