import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo
from typing import Dict, List, Any

from splash.extractor.event_extractor import BaseExtractor


# Patch SplashAuthManager globally for this module
@pytest.fixture(autouse=True)
def patch_auth_manager():
    with patch("splash.extractor.base_extractor.SplashAuthManager") as mock_auth:
        mock_instance = MagicMock()
        mock_instance.get_auth_header.return_value = {"Authorization": "Bearer dummy-token"}
        mock_auth.return_value = mock_instance
        yield


# Dummy subclass for testing
class MockEventExtractor(BaseExtractor):
    def _get_config(self) -> Dict[str, Any]:
        return {}

    def extract(self) -> Dict[str, List[Dict[str, Any]]]:
        return {}

    
@pytest.fixture
def extractor():
    return MockEventExtractor("https://mock.splashthat.com/api")


def test_data_filter_us_eastern_timezone(extractor):
    start = datetime(2025, 5, 14, 0, 0, 0, tzinfo=ZoneInfo("US/Eastern"))
    end = datetime(2025, 5, 15, 23, 59, 59, tzinfo=ZoneInfo("US/Eastern"))
    data = [
        {"id": 1, "created": "2025-05-15 00:18:58-04:00"},  # in range
        {"id": 2, "created": "2025-05-16 00:18:58-04:00"},  # out of range
    ]
    filtered = extractor._data_filter(data, date_col="created", start_dt=start, end_dt=end)
    assert len(filtered) == 1
    assert filtered[0]["id"] == 1

    
def test_page_filter_continue_and_exit(extractor):
    start = datetime(2025, 5, 14, 0, 0, 0, tzinfo=ZoneInfo("US/Eastern"))
    end = datetime(2025, 5, 15, 23, 59, 59, tzinfo=ZoneInfo("US/Eastern"))

    # All records before range
    data_early = [
        {"created": "2025-05-13 23:59:00-04:00"},
        {"created": "2025-05-13 23:59:59-04:00"},
    ]
    assert extractor._page_filter(data_early, date_col="created", start_dt=start, end_dt=end) == "continue"

    # All records after range
    data_late = [
        {"created": "2025-05-16 00:00:00-04:00"},
        {"created": "2025-05-17 00:00:00-04:00"},
    ]
    assert extractor._page_filter(data_late, date_col="created", start_dt=start, end_dt=end) == "exit"

    # In-range
    data_ok = [
        {"created": "2025-05-15 00:18:58-04:00"},
        {"created": "2025-05-14 09:00:00-04:00"},
    ]
    assert extractor._page_filter(data_ok, date_col="created", start_dt=start, end_dt=end) is None

    
@patch("splash.extractor.base_extractor.requests.Session.get")
def test_get_data_filters_correctly(mock_get, extractor):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "data": [
                {"id": 1, "created": "2025-05-15 00:18:58-04:00"},
                {"id": 2, "created": "2025-05-13 00:00:00-04:00"},
            ],
            "pagination": {"count": 2, "limit": 250}
        }
    )

    start = datetime(2025, 5, 14, 0, 0, 0, tzinfo=ZoneInfo("US/Eastern"))
    end = datetime(2025, 5, 15, 23, 59, 59, tzinfo=ZoneInfo("US/Eastern"))
    result = extractor.get_data("event", date_col="created", start_dt=start, end_dt=end)
    assert "event" in result
    assert len(result["event"]) == 1
    assert result["event"][0]["id"] == 1
