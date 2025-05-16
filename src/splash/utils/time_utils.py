from datetime import tzinfo, datetime
from splash.config.settings import Settings


def str_to_dt(dt_str: str) -> datetime:
    """
    Converts an ISO format string to a timezone-aware datetime object
    using the SPLASH_TIMEZONE from settings.

    Args:
        dt_str (str): ISO format datetime string.

    Returns:
        datetime: Timezone-aware datetime object.

    Raises:
        ValueError: If the input string is not a valid ISO datetime format.
    """
    try:
        return datetime.fromisoformat(dt_str).astimezone(Settings.SPLASH_TIMEZONE)
    except ValueError as e:
        raise ValueError(f"Invalid datetime string: {dt_str}") from e

    
def date_in_range(dt_str: str, start_dt: datetime, end_dt: datetime) -> bool:
    """
    Checks if a datetime string (converted to timezone-aware) falls within a range.

    Args:
        dt_str (str): ISO format datetime string.
        start_dt (datetime): Start of the date range.
        end_dt (datetime): End of the date range.

    Returns:
        bool: True if dt_str falls within the range [start_dt, end_dt], False otherwise.
    """
    this_dt = str_to_dt(dt_str)
    if start_dt > this_dt or end_dt < this_dt:
        return False
    return True


def time_now(tz: tzinfo = Settings.SPLASH_TIMEZONE) -> datetime:
    """
    Returns the current datetime with the given timezone (default is SPLASH_TIMEZONE)
    and microseconds set to 0.

    Args:
        tz (tzinfo): Timezone information. Defaults to SPLASH_TIMEZONE.

    Returns:
        datetime: Current datetime with microsecond = 0.
    """
    return datetime.now(tz).replace(microsecond=0)


def get_time_suffix(time: datetime) -> str:
    """
    Formats a datetime object into a string in the format 'YYYYMMDD_HHMMSS'.

    Args:
        time (datetime): Datetime object to format.

    Returns:
        str: Formatted timestamp string.
    """
    return time.strftime("%Y%m%d_%H%M%S")
