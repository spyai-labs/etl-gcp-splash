import threading
from typing import cast, List

from splash.metadata import ETLMetaData
from splash.defined_types import JobStatusLiteral, JobStats, JobStatus
from splash.config.settings import Settings
from splash.utils.time_utils import time_now

# Thread-local storage for per-thread ETL job statuses
_thread_local = threading.local()


def get_etl_job_statuses() -> List[JobStatus]:
    """
    Retrieve the list of ETL job statuses stored in the current thread.

    Returns:
        List[JobStatus]: List of recorded job status dictionaries for the thread.
    """
    if not hasattr(_thread_local, "etl_job_statuses"):
        _thread_local.etl_job_statuses = []
    return cast(List[JobStatus], _thread_local.etl_job_statuses)


def add_job_stats(agg_stats: JobStats, new_stat: JobStats) -> JobStats:
    """
    Aggregate job statistics by summing values from two stat dictionaries.

    Args:
        agg_stats (JobStats): The current aggregated stats.
        new_stat (JobStats): The new stats to add.

    Returns:
        JobStats: Combined stats with updated values for loaded, merged, and deleted.
    """
    return {
        "loaded": agg_stats.get("loaded", 0) + new_stat.get("loaded", 0),
        "merged": agg_stats.get("merged", 0) + new_stat.get("merged", 0),
        "deleted": agg_stats.get("deleted", 0) + new_stat.get("deleted", 0)
    }


def generate_job_status(
    metadata: ETLMetaData, 
    source: str, 
    obj_name: str, 
    status: JobStatusLiteral, 
    job_stats: JobStats
) -> JobStatus:
    """
    Generate a structured ETL job status dictionary from metadata and stats.

    Args:
        metadata (ETLMetaData): Job metadata including run ID, time, etc.
        source (str): The source data name (e.g. event, group_contact).
        obj_name (str): The name of the object/entity processed.
        status (JobStatusLiteral): Final status of the job ('success', 'failure').
        job_stats (JobStats): Dictionary with counts for loaded, merged, and deleted records.

    Returns:
        JobStatus: Dictionary containing job status information.
    """
    job_status: JobStatus = {
        "run_id": metadata.run_id,
        "run_time": metadata.run_time,
        "sync_mode": metadata.sync_mode,
        "log_path": f"gs://{Settings.LOG_BUCKET}/{metadata.log_path}",
        "source": source,
        "object": obj_name,
        "status": status,
        "timestamp": time_now(Settings.LOCAL_TIMEZONE),
        "records_loaded": job_stats.get('loaded', 0),
        "records_merged": job_stats.get('merged', 0),
        "records_deleted": job_stats.get('deleted', 0),
    }
    return job_status
