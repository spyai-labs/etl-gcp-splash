import os
import json
import glob
import zipfile
import tempfile
import logging
import pandas as pd
from logging.handlers import RotatingFileHandler
from typing import List, Optional, TYPE_CHECKING
from google.cloud import storage  # type: ignore

from splash.defined_types import JobStatus
from splash.config.settings import Settings
from splash.utils.schema_utils import generate_schema_from_class

if TYPE_CHECKING:
    from splash.loader import BigQueryLoader
    
"""
Logging utility for Splash ETL framework.

Handles:
- Logger setup with rotation and dual output (file + stream)
- Uploading logs and job statuses to Google Cloud Storage (GCS)
- Logging job status to BigQuery

This module is used in both local development and Cloud Run environments.
"""

DEFAULT_LOG_FILE = "/tmp/etl.log"
DEFAULT_LOG_ARCHIVE = "logs/etl_logs.zip"
JOB_STATUS_TABLE = "_job_status"


def setup_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """
    Set up a logger with rotating file and stream handlers.

    Args:
        name (str): Name of the logger.
        level (Optional[str]): Logging level (e.g., "DEBUG", "INFO", etc.).

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        level_str = level or Settings.LOG_LEVEL
        log_level = getattr(logging, level_str.upper(), logging.INFO) # DEBUG for local dev vs WARNING for production
        logger.setLevel(log_level)

        # Formatter
        formatter = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # File handler (for GCS upload later)
        file_handler = RotatingFileHandler(f"/tmp/{name}.log", maxBytes=5_000_000, backupCount=3)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Stream handler (stdout for Cloud Run logs)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    # Enable log propagration to parent loggers (useful for testing frameworks like pytest caplog)
    logger.propagate = True

    return logger


logger = setup_logger(__name__)


def upload_log_to_gcs(bucket_name: str, dest_blob_name: str, source_file: Optional[str] = None) -> None:
    """
    Upload a local log file to a GCS bucket.

    Args:
        bucket_name (str): GCS bucket name.
        dest_blob_name (str): Destination blob path in GCS.
        source_file (Optional[str]): Local file path to upload. Defaults to DEFAULT_LOG_FILE.
    """
    if not bucket_name:
        return
    
    source_file = source_file or DEFAULT_LOG_FILE
    
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(dest_blob_name)
        
        if os.path.exists(source_file):
            blob.upload_from_filename(source_file)
            logger.info(f"Uploaded log to gs://{bucket_name}/{dest_blob_name}")
            
    except Exception as e:
        logger.error(f"Failed to upload log to GCS: {e}")

        
def zip_and_upload_logs(bucket_name: str, archive_name: str = DEFAULT_LOG_ARCHIVE) -> None:
    """
    Compress all log files in `/tmp/` into a ZIP archive and upload to GCS.

    Args:
        bucket_name (str): GCS bucket to upload the archive.
        archive_name (str): Name of the archive file in GCS.
    """
    log_files = glob.glob("/tmp/*.log")
    if not log_files:
        logger.warning("No log files found to zip.")
        return

    with tempfile.NamedTemporaryFile(suffix=".zip", delete=True) as tmp_zip:
        with zipfile.ZipFile(tmp_zip.name, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for log_path in log_files:
                arcname = os.path.basename(log_path)
                try:
                    zipf.write(log_path, arcname=arcname)
                except Exception as e:
                    logger.warning(f"Failed to zip `{log_path}`: {e}")

        # Upload zip to GCS
        upload_log_to_gcs(bucket_name, archive_name, source_file=tmp_zip.name)


def log_status_to_gcs(
    bucket_name: str,
    status_data: List[JobStatus]
) -> None:
    """
    Upload job status metadata (as JSON) to GCS.

    Args:
        bucket_name (str): Target GCS bucket.
        status_data (List[JobStatus]): List of job status dicts to serialize.
    """
    if not status_data:
        logger.warning("Empty Job Status - No status log uploaded to GCS")
        return
        
    try:
        log_path = status_data[0].get("log_path")
        if not log_path:
            logger.warning("No `log_path` found in status_data. Skipping GCS upload.")
            return
        status_path = log_path.replace(f"gs://{bucket_name}/", "").replace(".zip", "_status.json")
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(status_path)
        blob.upload_from_string(json.dumps(status_data, indent=2, default=str), content_type="application/json")
        logger.info(f"Job status uploaded to: 'gs://{bucket_name}/{status_path}'")
        
    except Exception as e:
        logger.error(f"Failed to upload job status: {e}")

        
def log_status_to_bigquery(
    loader: 'BigQueryLoader',
    status_data: List[JobStatus]
) -> None:
    """
    Load job status records to BigQuery using provided loader.

    Args:
        loader (BigQueryLoader): An instance of the BigQuery loader.
        status_data (List[JobStatus]): List of job status dictionaries.
    """
    if not status_data:
        logger.warning("Empty Job Status - No status log uploaded to BigQuery")
        return
    
    try:
        df = pd.DataFrame(status_data)
        table_name = JOB_STATUS_TABLE
        table_schema = generate_schema_from_class(JobStatus)
        loader.load(table_name, df, schema=table_schema, write_mode="WRITE_APPEND")
        logger.info(f"Job status uploaded to BigQuery: `{loader.generate_table_id(table_name)}`")
        
    except Exception as e:
        logger.error(f"Failed to load job status to BigQuery: {e}")
