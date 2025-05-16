import uuid
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field

from splash.defined_types import SyncMode, JobMetaData
from splash.config.settings import Settings
from splash.utils.time_utils import time_now


@dataclass
class ETLMetaData:
    """
    Container class for capturing metadata associated with each ETL job run.
    This includes the run ID, execution time, sync mode, sync type, and log file path.
    """
    
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])  # Unique run ID
    run_time: datetime = field(default_factory=lambda: time_now(tz=Settings.LOCAL_TIMEZONE))  # Unique run_time (localized)
    sync_mode: SyncMode = field(default_factory=lambda: Settings.SYNC_MODE)
    full_sync: bool = field(init=False)
    log_path: str = field(init=False)
    
    def __post_init__(self) -> None:
        """Initializes derived fields after dataclass construction."""
        self.full_sync = self.sync_mode == "historical_full"
        self.log_path = self._build_log_path(self.sync_mode, self.run_id, self.run_time)
        
    def __str__(self) -> str:
        """Returns a string representation of the ETL metadata."""
        return self.to_string()
    
    @staticmethod
    def _build_log_path(sync_mode: str, run_id: str, run_time: datetime) -> str:
        """
        Builds a structured file path for the log archive based on the run timestamp and sync mode.

        Format:
        logs/YYYY/MM/DD/<sync_mode>/etl-<sync_mode>-<timestamp>-<run_id>.zip
        """
        timestamp = run_time.strftime('%Y%m%d_%H%M%S')
        folder = Path(str(run_time.year), f"{run_time.month:02}", f"{run_time.day:02}", sync_mode)
        filename = f"etl-{sync_mode}-{timestamp}-{run_id}.zip"
        return str(Path("logs") / folder / filename)
    
    def to_dict(self) -> JobMetaData:
        """
        Converts ETL metadata into a dictionary format for logging or persistence.

        Returns:
            JobMetaData: Dictionary representation of the ETL metadata.
        """
        metadata: JobMetaData = {
            "run_id": self.run_id,
            "run_time": self.run_time,
            "sync_mode": self.sync_mode,
            "full_sync": self.full_sync,
            "log_path": self.log_path
        }
        return metadata
        
    def to_string(self) -> str:
        """Returns a formatted string representation of the ETL metadata."""
        return (
            f"ETL metadata - RunID: {self.run_id} | "
            f"RunTime: {self.run_time} | "
            f"SyncMode: {self.sync_mode} | "
            f"IsFullSync: {self.full_sync} | "
            f"LogPath: {self.log_path}"
        )
    