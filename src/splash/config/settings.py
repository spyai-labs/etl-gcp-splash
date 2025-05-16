import os
from datetime import tzinfo
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from typing import cast, Optional, TYPE_CHECKING

from .validate import get_sync_mode
if TYPE_CHECKING:
    from splash.defined_types import SyncMode

DEFAULT_START_DATE = "2023-01-01"
DEFAULT_LOOKBACK_HOURS = "168"  # 7 days


@dataclass(frozen=True)
class Settings:
    """
    Global application configuration loaded from environment variables.

    Categories:
    - GCP project and BigQuery settings
    - API credentials for Splash authentication
    - ETL behavior flags (sync mode, schema gen, time zone)
    - Proxy and timeout configuration for HTTP calls
    """

    # --- GCP-related configuration ---
    GCP_PROJECT_ID: str = os.getenv("GCP_PROJECT_ID")
    BQ_DATASET_ID: str = os.getenv("BQ_DATASET_ID")
    LOG_BUCKET: str = os.getenv("LOG_BUCKET")
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")  # log level set to logger initialisation
    ENABLE_GCS_LOGS: bool = os.getenv("ENABLE_GCS_LOGS", "true").lower() == "true"
    ENABLE_BQ_LOGS: bool = os.getenv("ENABLE_BQ_LOGS", "true").lower() == "true"
    STAGING_PREFIX: str = os.getenv("STAGING_PREFIX", "_stg_")
    STAGING_SUFFIX: str = os.getenv("STAGING_SUFFIX", "")
    
    # --- Splash API credentials and HTTP config ---
    BASE_URL: str = os.getenv("BASE_URL", "https://api.splashthat.com")
    CLIENT_ID: str = cast(str, os.getenv("CLIENT_ID"))
    CLIENT_SECRET: str = cast(str, os.getenv("CLIENT_SECRET"))
    USERNAME: str = cast(str, os.getenv("USERNAME"))
    PASSWORD: str = cast(str, os.getenv("PASSWORD"))
    TOKEN_SECRET_ID: str = os.getenv("TOKEN_SECRET_ID")
    TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "10"))
    HTTP_PROXY: str = os.getenv("HTTP_PROXY")
    NO_PROXY: str = os.getenv("NO_PROXY")
    VERIFY: bool = os.getenv("VERIFY_CERT", "true").lower() == "true"  # toggle SSL cert check
    
    # --- ETL execution and transformation config ---
    SYNC_MODE: 'SyncMode' = get_sync_mode()  # e.g. incremental, incremental_window, historical_full
    SPLASH_ETL_SOURCES: str = os.getenv("SPLASH_ETL_SOURCES", "event,group_contact")
    GENERATE_SCHEMA: bool = os.getenv("GENERATE_SCHEMA", "false").lower() == "true"
    EVENT_LOOKBACK_HOURS: int = int(os.getenv("EVENT_LOOKBACK_HOURS", DEFAULT_LOOKBACK_HOURS))  # default: 168 hrs = 7 days
    GROUP_CONTACT_LOOKBACK_HOURS: int = int(os.getenv("GROUP_CONTACT_LOOKBACK_HOURS", DEFAULT_LOOKBACK_HOURS))
    START_DATE: str = os.getenv("START_DATE", DEFAULT_START_DATE)  # Australia/Sydney date in %Y-%m-%d, fallback to full historical
    END_DATE: Optional[str] = os.getenv("END_DATE")
    
    # --- Time zone configuration ---
    SPLASH_TIMEZONE: tzinfo = ZoneInfo(os.getenv("SPLASH_TIMEZONE", "US/Eastern"))  # Splash default timezone
    LOCAL_TIMEZONE: tzinfo = ZoneInfo(os.getenv("LOCAL_TIMEZONE", "Australia/Sydney"))  # local default timezone
    
