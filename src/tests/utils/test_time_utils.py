import pytest
from datetime import datetime
from splash.utils.time_utils import str_to_dt, date_in_range, time_now, get_time_suffix
from splash.config.settings import Settings

def test_str_to_dt_valid():
    dt_str = "2023-01-01T12:00:00"
    dt = str_to_dt(dt_str)
    assert isinstance(dt, datetime)
    assert dt.tzinfo is not None

def test_str_to_dt_invalid():
    with pytest.raises(ValueError, match="Invalid datetime string"):
        str_to_dt("not-a-datetime")

def test_date_in_range_true():
    dt_str = "2023-01-01T12:00:00"
    start = str_to_dt("2023-01-01T00:00:00")
    end = str_to_dt("2023-01-01T23:59:59")
    assert date_in_range(dt_str, start, end)

def test_date_in_range_false():
    dt_str = "2023-01-01T12:00:00"
    start = str_to_dt("2023-01-01T13:00:00")
    end = str_to_dt("2023-01-01T23:59:59")
    assert not date_in_range(dt_str, start, end)

def test_time_now_default_tz():
    now = time_now()
    assert isinstance(now, datetime)
    assert now.microsecond == 0
    assert now.tzinfo == Settings.SPLASH_TIMEZONE

def test_get_time_suffix():
    dt = datetime(2024, 12, 25, 15, 30, 45)
    assert get_time_suffix(dt) == "20241225_153045"
