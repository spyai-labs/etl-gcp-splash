import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

from splash.extractor.group_contact_extractor import GroupContactExtractor


# Patch SplashAuthManager globally for this module
@pytest.fixture(autouse=True)
def patch_auth_manager():
    with patch("splash.extractor.base_extractor.SplashAuthManager") as mock_auth:
        mock_instance = MagicMock()
        mock_instance.get_auth_header.return_value = {"Authorization": "Bearer dummy-token"}
        mock_auth.return_value = mock_instance
        yield


@pytest.fixture
def extractor():
    return GroupContactExtractor(sync_mode="historical")


@patch("splash.extractor.base_extractor.requests.Session.get")
def test_group_contact_get_data_filtering(mock_get, extractor):
    mock_get.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "data": [
                {
                    "id": 1,
                    "modified": "2025-05-15 00:18:58-04:00",
                    "contact": {
                        "id": 101,
                        "bounce_info": {
                            "event_title": "Spring Summit",
                            "sent_on": "2025-05-14 13:00:00-04:00",
                            "bounce_reason": "Soft bounce"
                        }
                    },
                    "event_rsvp": {
                        "id": 201,
                        "event_id": 301,
                        "first_name": "Jane",
                        "last_name": "Smith",
                        "email": "jane.smith@example.com"
                    },
                    "answers": [
                        {"question_id": 1, "answer": "Yes"}
                    ],
                    "email_campaign_statuses": [
                        {"id": "ecs_001", "status": "opened"}
                    ]
                }
            ],
            "pagination": {"count": 1, "limit": 250}
        }
    )

    start = datetime(2025, 5, 14, 0, 0, 0, tzinfo=ZoneInfo("US/Eastern"))
    end = datetime(2025, 5, 15, 23, 59, 59, tzinfo=ZoneInfo("US/Eastern"))

    result = extractor.get_data(
        endpoint="groupcontacts",
        date_col="modified",
        start_dt=start,
        end_dt=end
    )

    assert len(result["group_contacts"]) == 1
    assert result["group_contacts"][0]["email"] == "jane.smith@example.com"
    assert result["answers"][0]["answer"] == "Yes"
    assert result["contacts"][0]["bounced_event"] == "Spring Summit"
    assert result["email_campaign_statuses"][0]["status"] == "opened"
    assert result["rsvps"][0]["group_contact_id"] == 1
