import pytest

from splash.transformer.group_contact_transformer import (
    GroupContactTransformer,
    GroupContactEventRSVPTransformer,
    GroupContactAnswerTransformer,
    GroupContactEmailCampaignStatusTransformer
)

@pytest.fixture
def group_contact_data():
    return [{
        "id": 1,
        "event_id": 100,
        "contact_id": 2,
        "event_rsvp_id": 3,
        "first_name": "John",
        "last_name": "Doe",
        "email": "john@example.com",
        "salesforce_campaign_member_id": "scm_456",
        "status": "invited",
        "created": "2025-05-15 01:58:23-04:00",
        "modified": "2025-05-15 01:58:23-04:00",
        "deleted": False
    }]

@pytest.fixture
def rsvp_data():
    return [{
        "id": 300,
        "group_contact_id": 1,
        "event_id": 100,
        "attending": True,
        "date_rsvped": "2025-05-15 01:58:23-04:00",
        "first_name": "John",
        "last_name": "Doe",
        "email": "john@example.com",
        "checked_in": "2025-05-15 01:58:23-04:00",
        "checked_out": "2025-05-15 01:58:23-04:00",
        "plus_one": False,
        "ticket_sale_id": 2,
        "ticket_number": "T123",
        "vip": False,
        "waitlist": False,
        "qr_url": "http://example.com/qr",
        "unsub_tag": None,
        "unsubscribed": False,
        "created": "2025-05-15 01:58:23-04:00",
        "modified": "2025-05-15 01:58:23-04:00",
        "deleted": False
    }]

@pytest.fixture
def answer_data():
    return [
        {
            "id": "1",
            "group_contact_id": 2,
            "question_id": 3,
            "event_id": 100,
            "answer": "Yes",
            "created": "2025-05-15 01:58:23-04:00",
            "modified": "2025-05-15 01:58:23-04:00",
            "deleted": False
        },
        {
            "id": "2",
            "group_contact_id": 2,
            "question_id": 3,
            "event_id": 100,
            "answer": "Yes (duplicate)",
            "created": "2025-05-15 01:58:23-04:00",
            "modified": "2025-05-15 01:58:23-04:00",
            "deleted": False
        }
    ]

@pytest.fixture
def email_status_data():
    return [{
        "id": "1",
        "group_contact_id": 1,
        "event_id": 100,
        "email_campaign_id": 2,
        "status": "opened",
        "created": "2025-05-15 01:58:23-04:00",
        "modified": "2025-05-15 01:58:23-04:00",
        "deleted": False
    }]

def test_group_contact_transformer(group_contact_data):
    df = GroupContactTransformer(group_contact_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "status"] == "invited"

def test_group_contact_rsvp_transformer_name_combined(rsvp_data):
    df = GroupContactEventRSVPTransformer(rsvp_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "name"] == "John Doe"

def test_group_contact_answer_transformer_deduplicates(answer_data):
    df = GroupContactAnswerTransformer(answer_data).transform_to_df()
    assert df.shape[0] == 1

def test_group_contact_email_status_transformer(email_status_data):
    df = GroupContactEmailCampaignStatusTransformer(email_status_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "status"] == "opened"
