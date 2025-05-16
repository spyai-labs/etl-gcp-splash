from typing import Dict
from datetime import tzinfo, date, datetime, timedelta

from splash.defined_types import SyncMode
from splash.config.settings import Settings
from splash.utils.logger import setup_logger

logger = setup_logger(__name__)

HISTORICAL_START_DATE = "2023-01-01"

SUPPORTED_ENTITIES = {"event", "groupcontact"}


class SyncController:
    sync_mode: SyncMode
    splash_timezone: tzinfo
    local_timezone: tzinfo
    event_lookback_hours: int
    groupcontact_lookback_hours: int
    today: date
    start_date: str
    end_date: str
    
    def __init__(self, sync_mode: SyncMode) -> None:
        """
        Initializes a SyncController instance that determines the sync date window
        for various entity types based on the configured sync mode.

        Args:
            sync_mode (SyncMode): The type of synchronization to perform 
                                  ('incremental', 'incremental_window', 'historical_full').
        """
        self.sync_mode = sync_mode  # incremental, incremental_window, historical_full
        self.splash_timezone = Settings.SPLASH_TIMEZONE  # splash default: 'US/Eastern'
        self.local_timezone = Settings.LOCAL_TIMEZONE  # default: 'Australia/Sydney'
        self.event_lookback_hours = Settings.EVENT_LOOKBACK_HOURS  # default: 168 (i.e. 7 days)
        self.groupcontact_lookback_hours = Settings.GROUP_CONTACT_LOOKBACK_HOURS  # default: 168 (i.e. 7 days)
        self.today = self.now_local.date()
        self.start_date = Settings.START_DATE
        self.end_date = Settings.END_DATE or str(self.today)
    
    @property
    def now_splash(self) -> datetime:
        """
        Returns the current datetime in Splash's timezone (US/Eastern).
        """
        return datetime.now(tz=self.splash_timezone)
    
    @property
    def now_local(self) -> datetime:
        """
        Returns the current datetime in the local timezone (e.g., Australia/Sydney).
        """
        return datetime.now(tz=self.local_timezone)
    
    def _local_date_to_splash(self, date_str: str, day_end: bool = False) -> datetime:
        """
        Converts a date string from the local timezone to a timezone-aware Splash datetime.

        Args:
            date_str (str): The date string in 'YYYY-MM-DD' format.
            day_end (bool): Whether to set the time to 23:59:59.

        Returns:
            datetime: Converted datetime in Splash timezone.

        Raises:
            ValueError: If the input date string is not in the correct format.
        """
        try:
            dt_naive = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError as e:
            error_msg = f"Invalid date format: '{date_str}'. Expected '%Y-%m-%d'"
            raise ValueError(error_msg) from e
            
        if day_end:
            dt_naive = dt_naive.replace(hour=23, minute=59, second=59) 
            
        return dt_naive.replace(tzinfo=self.local_timezone).astimezone(self.splash_timezone)
    
    def _check_valid_entity(self, entity_type: str) -> None:
        """
        Validates if the provided entity type is supported for syncing.

        Args:
            entity_type (str): The type of entity to validate.

        Raises:
            ValueError: If the entity type is not supported.
        """
        if entity_type not in SUPPORTED_ENTITIES:
            error_msg = f"SyncController - Unsupported entity_type: {entity_type}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
    def get_window(self, entity_type: str) -> Dict[str, datetime]:
        """
        Returns the appropriate start and end datetime window for the given entity
        based on the configured SYNC_MODE.

        Args:
            entity_type (str): One of "event" or "groupcontact".

        Returns:
            Dict[str, datetime]: Dictionary containing 'start_date' and 'end_date' keys.
        """
        entity_type = entity_type.lower()
        self._check_valid_entity(entity_type)

        if self.sync_mode == "incremental":
            lookback_attr = f"{entity_type}_lookback_hours"
            lookback = getattr(self, lookback_attr)
            now = self.now_splash # Current time in Eastern Time Zone
            start = now - timedelta(hours=lookback)
            end = now

        elif self.sync_mode == "incremental_window": # convert Australia/Sydney date into US/Eastern date
            start = self._local_date_to_splash(self.start_date)
            end = self._local_date_to_splash(self.end_date, day_end=True)

        else: #  self.sync_mode == "historical_full"
            start = self._local_date_to_splash(HISTORICAL_START_DATE) # static start date
            end = self.now_splash
         
        start = start.replace(microsecond=0)
        end = end.replace(microsecond=0)
        
        logger.debug(f"Sync window for '{entity_type}' with SyncMode '{self.sync_mode}' - Start: {start}, End: {end}")

        return {
            "start_date": start,
            "end_date": end
        }

    def get_event_params(self) -> Dict[str, datetime]:
        """
        Returns the start and end datetime window for syncing events.

        Returns:
            Dict[str, datetime]: Contains 'start_date' and 'end_date' for event sync.
        """
        return self.get_window("event")

    def get_group_contact_params(self) -> Dict[str, datetime]:
        """
        Returns the start and end datetime window for syncing group contacts.

        Returns:
            Dict[str, datetime]: Contains 'start_date' and 'end_date' for group contact sync.
        """
        return self.get_window("groupcontact")
