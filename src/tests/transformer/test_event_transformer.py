import pytest
from splash.transformer.event_transformer import (
    EventTransformer,
    EventTypeTransformer,
    EventStatsTransformer,
    EventSettingTransformer,
    EventTriggeredEmailTransformer,
    EventCustomQuestionTransformer,
    EventTicketTypeTransformer
)


# ---------------- Fixtures ----------------

@pytest.fixture
def event_data():
    return [{
        "id": 1001,
        "name": "Sample Event",
        "salesforce_campaign_id": "abc123",
        "splash_theme_id": 1,
        "splash_theme_name": "Dark Theme",
        "event_type_id": 2,
        "event_type_name": "Webinar",
        "event_setting_id": 10,
        "title": "Event Title",
        "description_text": "Event Description",
        "event_owner_first_name": "John",
        "event_owner_last_name": "Doe",
        "event_owner_email": "john@example.com",
        "event_start": "2023-01-01T10:00:00Z",
        "event_end": "2023-01-01T12:00:00Z",
        "hide_event_time": False,
        "venue_name": "Main Hall",
        "address": "123 Street",
        "city": "City",
        "state": "State",
        "zip_code": "12345",
        "country": "Country",
        "created_at": "2023-01-01T09:00:00Z",
        "modified_at": "2023-01-01T09:30:00Z",
        "domain": "example.com",
        "custom_domain": "custom.example.com",
        "paid_for_domain": False,
        "deleted": False,
        "published": True,
        "hub": False,
        "fq_url": "https://example.com/event",
        "mobile_check_in_url": "https://example.com/checkin",
        "registration_updating_enabled": False,
        "registration_updating_deadline": 0,
        "event_attendance_types": ["virtual", "in_person"],
        "group_ids": [1, 2]
    }]


@pytest.fixture
def event_type_data():
    return [{
        "id": 1,
        "name": "Conference",
        "code_name": "CONF",
        "splash_type": False,
        "public_type": False,
        "is_enterprise_type": False,
        "created": "2023-01-01T00:00:00Z"
    }]


@pytest.fixture
def event_stats_data():
    return [{
        "id": "stat_001",
        "event_id": 1001,
        "name": "registrations",
        "stat_type": "total",
        "count": 150
    }]


@pytest.fixture
def event_setting_data():
    return [{
        "id": 10,
        "event_id": 1001,
        "header_image": "https://example.com/image.jpg",
        "rsvp_open": True,
        "wait_list": False,
        "rsvp_method": "form",
        "lat": None,
        "lng": None,
        "event_hashtag": "#SampleEvent",
        "rsvp_max": 500,
        "venue_tbd": False,
        "rsvp_guest_display": False,
        "rsvp_closed_state": "open",
        "rsvp_closed_at": None,
        "rsvp_closed_team_notified": False,
        "page_privacy_type": "public",
        "event_host": "Jane Smith",
        "button_closed_message": "Closed",
        "autosave": True
    }]


@pytest.fixture
def triggered_email_data():
    return [{
        "id": "te_001",
        "event_id": 1001,
        "event_setting_id": 10,
        "template_id": "temp001",
        "trigger": "rsvp"
    }]


@pytest.fixture
def custom_question_data():
    return [{
        "id": "cq_001",
        "event_id": 1001,
        "event_setting_id": 10,
        "type": "text",
        "name": "Your Role",
        "column_name": "role",
        "required": True
    }]


@pytest.fixture
def ticket_type_data():
    return [{
        "ticket_type_id": 3001,
        "event_id": 1001,
        "name": "VIP",
        "price": 199.99,
        "description": "Premium ticket",
        "active": True
    }]


# ---------------- Tests ----------------

def test_event_transformer(event_data):
    df = EventTransformer(event_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "event_attendance_types"] == "virtual, in_person"


def test_event_type_transformer(event_type_data):
    df = EventTypeTransformer(event_type_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "code_name"] == "CONF"


def test_event_stats_transformer(event_stats_data):
    df = EventStatsTransformer(event_stats_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "name"] == "registrations"


def test_event_setting_transformer(event_setting_data):
    df = EventSettingTransformer(event_setting_data).transform_to_df()
    assert not df.empty
    assert "lat" in df.columns


def test_triggered_email_transformer(triggered_email_data):
    df = EventTriggeredEmailTransformer(triggered_email_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "trigger"] == "rsvp"


def test_custom_question_transformer(custom_question_data):
    df = EventCustomQuestionTransformer(custom_question_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "column_name"] == "role"


def test_ticket_type_transformer(ticket_type_data):
    df = EventTicketTypeTransformer(ticket_type_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "ticket_type_id"] == 3001
