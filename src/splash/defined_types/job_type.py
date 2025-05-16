from datetime import datetime
from typing import TypedDict, Literal

__all__ = [
    'SyncMode',
    'StatName',
    'JobStatusLiteral',
    'JobStats',
    'JobStatus',
    'JobMetaData'
]

SyncMode = Literal["incremental", "incremental_window", "historical_full"]
StatName = Literal["loaded", "merged", "deleted"]
JobStatusLiteral = Literal["success", "failure"]


class JobStats(TypedDict):
    loaded: int
    merged: int
    deleted: int

    
class JobStatus(TypedDict):
    run_id: str
    run_time: datetime
    sync_mode: SyncMode
    log_path: str
    source: str
    object: str
    status: JobStatusLiteral
    timestamp: datetime
    records_loaded: int
    records_merged: int
    records_deleted: int


class JobMetaData(TypedDict):
    run_id: str
    run_time: datetime
    sync_mode: SyncMode
    full_sync: bool
    log_path: str
