import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from typing import Dict, List, Any

from splash.extractor.base_extractor import BaseExtractor


# Patch SplashAuthManager globally for this module
@pytest.fixture(autouse=True)
def patch_auth_manager():
    with patch("splash.extractor.base_extractor.SplashAuthManager") as mock_auth:
        mock_instance = MagicMock()
        mock_instance.get_auth_header.return_value = {"Authorization": "Bearer dummy-token"}
        mock_auth.return_value = mock_instance
        yield


# Mock implementation to test abstract base class
class MockExtractor(BaseExtractor):
    def _get_config(self) -> Dict[str, Any]:
        return {"mock": "config"}

    def extract(self) -> Dict[str, List[Dict[str, Any]]]:
        return {"mock": []}


@pytest.fixture
def extractor():
    return MockExtractor("https://mock.api.com")


def test_data_filter_filters_correctly(extractor):
    start = datetime.fromisoformat("2023-01-01 00:00:00-04:00")
    end = datetime.fromisoformat("2023-01-31 23:59:59-04:00")
    data = [
        {"created": "2023-01-10 12:00:00-04:00"},
        {"created": "2023-02-10 12:00:00-04:00"},  # out of range
    ]
    filtered = extractor._data_filter(data, date_col="created", start_dt=start, end_dt=end)
    assert len(filtered) == 1
    assert filtered[0]["created"] == "2023-01-10 12:00:00-04:00"


def test_page_filter_continue_and_exit_logic(extractor):
    start = datetime.fromisoformat("2023-03-01 00:00:00-04:00")
    end = datetime.fromisoformat("2023-03-31 23:59:59-04:00")
    # Continue scenario
    data = [{"created": "2023-01-01 00:00:00-04:00"}, {"created": "2023-01-02 00:00:00-04:00"}]
    assert extractor._page_filter(data, date_col="created", start_dt=start, end_dt=end) == "continue"

    # Exit scenario
    data = [{"created": "2023-04-01 00:00:00-04:00"}, {"created": "2023-04-02 00:00:00-04:00"}]
    assert extractor._page_filter(data, date_col="created", start_dt=start, end_dt=end) == "exit"


@patch("splash.extractor.base_extractor.requests.Session.get")
def test_get_data_with_mocked_response(mock_get, extractor):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "data": [{"id": 1, "created": "2023-01-01 12:00:00-04:00"}],
            "pagination": {"count": 1, "limit": 250}
        }
    )

    result = extractor.get_data(
        "events", 
        date_col="created", 
        start_dt=datetime.fromisoformat("2023-01-01 00:00:00-04:00"),
        end_dt=datetime.fromisoformat("2023-01-02 23:59:59-04:00")
    )
    assert "events" in result
    assert len(result["events"]) == 1


@patch("splash.extractor.base_extractor.requests.Session.get")
def test_get_data_handles_empty_data(mock_get, extractor):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {"data": []}
    )

    result = extractor.get_data("empty")
    assert result == {"empty": []}
