import os
import json
import logging
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from splash.utils.logger import (
    setup_logger, upload_log_to_gcs, zip_and_upload_logs,
    log_status_to_gcs, log_status_to_bigquery
)

@pytest.fixture
def sample_job_status():
    return [{
        "run_id": "abc123",
        "run_time": "2025-05-15T00:00:00",
        "sync_mode": "incremental",
        "log_path": "gs://test-bucket/logs/2025/05/15/incremental/etl-incremental-20250515_000000-abc123.zip",
        "source": "splash",
        "object": "event",
        "status": "success",
        "timestamp": "2025-05-15T00:01:00",
        "records_loaded": 100,
        "records_merged": 50,
        "records_deleted": 0
    }]

def test_setup_logger_returns_logger():
    logger = setup_logger("test_logger", level="DEBUG")
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.DEBUG

@patch("splash.utils.logger.storage.Client")
def test_upload_log_to_gcs_success(mock_client):
    with tempfile.NamedTemporaryFile(suffix=".log", delete=False) as tmp_file:
        tmp_file.write(b"Test log content")
        tmp_file_path = tmp_file.name

    upload_log_to_gcs("test-bucket", "logs/test.log", source_file=tmp_file_path)
    mock_client().bucket().blob().upload_from_filename.assert_called_once_with(tmp_file_path)

    os.remove(tmp_file_path)

@patch("splash.utils.logger.storage.Client")
@patch("splash.utils.logger.glob.glob", return_value=["/tmp/test.log"])
@patch("splash.utils.logger.zipfile.ZipFile.write")
def test_zip_and_upload_logs(mock_zip_write, mock_glob, mock_client):
    with patch("splash.utils.logger.tempfile.NamedTemporaryFile") as mock_tmp:
        mock_tmp.return_value.__enter__.return_value.name = "/tmp/test_archive.zip"
        zip_and_upload_logs("test-bucket", "logs/etl_logs.zip")
        mock_zip_write.assert_called()

@patch("splash.utils.logger.storage.Client")
def test_log_status_to_gcs_uploads_json(mock_client, sample_job_status):
    blob_mock = mock_client().bucket().blob()
    log_status_to_gcs("test-bucket", sample_job_status)
    upload_str = json.dumps(sample_job_status, indent=2, default=str)
    blob_mock.upload_from_string.assert_called_with(upload_str, content_type="application/json")

def test_log_status_to_gcs_skips_empty():
    with patch("splash.utils.logger.logger") as mock_logger:
        log_status_to_gcs("bucket", [])
        mock_logger.warning.assert_called_with("Empty Job Status - No status log uploaded to GCS")

@patch("splash.utils.logger.generate_schema_from_class")
def test_log_status_to_bigquery_calls_loader(mock_schema, sample_job_status):
    mock_loader = MagicMock()
    mock_schema.return_value = [{"name": "run_id", "type": "STRING"}]
    log_status_to_bigquery(mock_loader, sample_job_status)

    mock_loader.load.assert_called_once()
    assert mock_loader.load.call_args[0][0] == "_job_status"
