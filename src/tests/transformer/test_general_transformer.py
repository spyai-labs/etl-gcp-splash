import pytest

from splash.transformer.general_transformer import (
    SplashThemeTransformer,
    ContactTransformer,
    CustomQuestionTransformer
)


@pytest.fixture
def splash_theme_data():
    return [{
        "id": "1",
        "name": "Dark Mode",
        "abbr": "DM",
        "image_url": "http://example.com/img.png",
        "thumbnail_url": "http://example.com/thumb.png",
        "sort": 1,
        "active": True,
        "created": "2024-01-01T00:00:00Z"
    }]

@pytest.fixture
def contact_data():
    return [{
        "id": "100",
        "first_name": "John",
        "last_name": "Smith",
        "title": "Manager",
        "primary_email": "john@example.com",
        "salesforce_id": "Lead:12345",
        "unsubscribed": False,
        "createdate": "2024-01-01T00:00:00Z",
        "modifydate": "2024-01-01T00:00:00Z",
        "deleted": False,
        "vip": False,
        "bounced": False,
        "bounced_event": None,
        "bounced_on": None,
        "bounced_reason": None,
        "invalid_email": False,
        "organization_name": "",
        "phone": ""
    }]

@pytest.fixture
def custom_question_event_data():
    return [{
        "id": "200",
        "event_id": 900,
        "type": "text",
        "name": "Your role?",
        "column_name": "role",
        "required": True,
        "values": ["Manager", "Engineer"],
        "selected_values": ["Engineer"]
    }]

@pytest.fixture
def custom_question_ticket_data():
    return [{
        "id": "201",
        "ticket_type_id": 910,
        "type": "select",
        "name": "Meal preference?",
        "column_name": "meal",
        "required": False,
        "values": [],
        "selected_values": []
    }]

# --- SplashThemeTransformer ---
def test_splash_theme_transformer(splash_theme_data):
    df = SplashThemeTransformer(splash_theme_data).transform_to_df()
    assert not df.empty
    assert "name" in df.columns
    assert df.loc[0, "name"] == "Dark Mode"

# --- ContactTransformer ---
def test_contact_transformer_salesforce_split_and_null(contact_data):
    df = ContactTransformer(contact_data).transform_to_df()
    assert "salesforce_object_type" in df.columns
    assert df.loc[0, "salesforce_object_type"] == "Lead"
    assert df.loc[0, "salesforce_id"] == "12345"
    assert df.loc[0, "organization_name"] is None
    assert df.loc[0, "phone"] is None

# --- CustomQuestionTransformer for Event ---
def test_custom_question_transformer_event(custom_question_event_data):
    df = CustomQuestionTransformer(custom_question_event_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "object_type"] == "event"
    assert df.loc[0, "object_id"] == 900
    assert isinstance(df.loc[0, "values"], str)
    assert "Engineer" in df.loc[0, "selected_values"]

# --- CustomQuestionTransformer for Ticket Type ---
def test_custom_question_transformer_ticket(custom_question_ticket_data):
    df = CustomQuestionTransformer(custom_question_ticket_data).transform_to_df()
    assert df.loc[0, "object_type"] == "ticket_type"
    assert df.loc[0, "object_id"] == 910
    assert df.loc[0, "values"] is None
    assert df.loc[0, "selected_values"] is None
