import pytest
from splash.transformer.ticket_transformer import (
    TicketTypeTransformer,
    TicketOrderTransformer,
    TicketSaleTransformer,
    TicketTypeDiscountTransformer,
    TicketOrderDiscountTransformer,
    TicketTypeCustomQuestionTransformer,
)


# Fixtures
@pytest.fixture
def ticket_type_data():
    return [{
        "id": 1,
        "event_id": 101,
        "name": "Standard",
        "price": 50.0,
        "quantity": 100,
        "quantity_sold": 50,
        "sold_out": False,
        "order_min": 1,
        "order_max": 10,
        "fee_paid_by_buyer": True,
        "sort": 1,
        "collect_rsvp_fields": True,
        "non_dollar": False,
        "expires_length": 0,
        "nested_ticket": False,
        "show_remaining": True,
        "open_price": False,
        "open_price_min": 0,
        "open_price_max": 0,
        "add_guest_name": False,
        "description": "Entry ticket",
        "active": True
    }]


@pytest.fixture
def ticket_order_data():
    return [{
        "id": 10,
        "ticket_type_id": 1,
        "ticket_type_name": "Standard",
        "contact": {"id": 999},
        "currency": {"id": 1, "code": "AUD", "name": "Australian Dollar"},
        "ticket_order_discount": {
            "flat_discount_amount": 10,
            "percent_discount": 5,
            "discount_code": "SAVE5"
        },
        "order_number": "ORD001",
        "status": "paid",
        "created": "2023-01-01T00:00:00Z",
        "placed": "2023-01-01T00:01:00Z",
        "quantity": 2,
        "price": 100.0,
        "total": 200.0,
        "foreign_price": 0.0,
        "foreign_total_price": 0.0,
        "ticket_order_discount_id": 111,
        "tax": 0,
        "stripe_fee": 0,
        "fees_owed": 0,
        "fees_paid": 0,
        "fee_refunded": 0,
        "amount_refunded": 0,
        "email": "test@example.com",
        "cardholder_name": "John Smith"
    }]


@pytest.fixture
def ticket_sale_data():
    return [{
        "id": 100,
        "ticket_type_id": 1,
        "ticket_type_name": "Standard",
        "ticket_order_id": 10,
        "quantity": 1,
        "price": 30.0,
        "foreign_price": 10.0,
        "total": 60.0,
        "foreign_total": 20.0,
        "ticket_order": {"amount_refunded": 0}
    }]


@pytest.fixture
def ticket_type_discount_data():
    return [{
        "ticket_order_discount_id": 1,
        "ticket_type_id": 1,
        "discount_code": "WELCOME"
    }]


@pytest.fixture
def ticket_order_discount_data():
    return [{
        "id": 2,
        "flat_discount_amount": 5.0,
        "percent_discount": 10,
        "discount_code": "NEW10",
        "name": "New Customer",
        "code": "NEW10"
    }]


@pytest.fixture
def ticket_type_question_data():
    return [{
        "id": "1",
        "ticket_type_id": 1,
        "custom_question_id": 101,
        "type": "text",
        "name": "Agreement",
        "column_name": "agree",
        "required": True
    }]


# Tests
def test_ticket_type_transformer(ticket_type_data):
    df = TicketTypeTransformer(ticket_type_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "name"] == "Standard"


def test_ticket_order_transformer(ticket_order_data):
    df = TicketOrderTransformer(ticket_order_data).transform_to_df()
    assert not df.empty
    row = df.loc[0]
    assert row["contact_id"] == 999
    assert row["currency_code"] == "AUD"
    assert row["flat_discount"] == 10
    assert row["discount_code"] == "SAVE5"
    assert "foreign_total" in row


def test_ticket_sale_transformer(ticket_sale_data):
    df = TicketSaleTransformer(ticket_sale_data).transform_to_df()
    assert not df.empty
    row = df.loc[0]
    assert row["unit_price"] == 40.0  # 30 + 10
    assert row["total_price"] == 80.0  # 60 + 20
    assert not row["is_refunded"]


def test_ticket_type_discount_transformer(ticket_type_discount_data):
    df = TicketTypeDiscountTransformer(ticket_type_discount_data).transform_to_df()
    assert not df.empty
    assert "ticket_order_discount_id" in df.columns
    assert df.loc[0, "ticket_order_discount_id"] == 1


def test_ticket_order_discount_transformer(ticket_order_discount_data):
    df = TicketOrderDiscountTransformer(ticket_order_discount_data).transform_to_df()
    assert not df.empty
    assert "flat_discount" in df.columns
    assert df.loc[0, "flat_discount"] == 5.0


def test_ticket_type_custom_question_transformer(ticket_type_question_data):
    df = TicketTypeCustomQuestionTransformer(ticket_type_question_data).transform_to_df()
    assert not df.empty
    assert df.loc[0, "custom_question_id"] == 101