from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from splash.utils.guid_utils import generate_hashed_guid

__all__ = ['TicketType', 'TicketOrder', 'TicketSale', 'TicketTypeDiscount', 'TicketOrderDiscount', 'TicketTypeCustomQuestion']


class TicketType(BaseModel):
    id: int
    name: str
    description: str
    price: int
    quantity: int
    quantity_sold: int
    sold_out: bool
    active: bool
    order_min: int
    order_max: int
    fee_paid_by_buyer: bool
    sort: int
    collect_rsvp_fields: int
    non_dollar: int
    expires_length: int
    nested_ticket: bool
    show_remaining: bool
    open_price: bool
    open_price_min: int
    open_price_max: int
    add_guest_name: bool

    
class TicketOrder(BaseModel):
    id: int
    contact_id: int
    ticket_type_id: int
    ticket_type_name: str
    currency_id: int
    currency_code: str
    currency_name: str
    order_number: str
    status: str
    created: datetime
    placed: datetime
    quantity: int
    price: int
    total: int
    foreign_price: int
    foreign_total: int
    ticket_order_discount_id: Optional[int]
    flat_discount: Optional[int]
    percent_discount: Optional[float]
    discount_code: Optional[str]
    tax: int
    stripe_fee: int
    fees_owed: int
    fees_paid: int
    fee_refunded: int
    amount_refunded: int
    email: str
    cardholder_name: Optional[str]

    
class TicketSale(BaseModel):
    id: int
    ticket_type_id: int
    ticket_type_name: str
    ticket_order_id: int
    quantity: int
    unit_price: int
    total_price: int
    is_refunded: bool

    
class TicketTypeDiscount(BaseModel):
    ticket_type_id: int
    ticket_order_discount_id: int
    id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['ticket_type_id', 'ticket_order_discount_id'])
    )  # GUID

    
class TicketOrderDiscount(BaseModel):
    id: int
    flat_discount: int
    percent_discount: float
    name: Optional[str]
    code: str

    
class TicketTypeCustomQuestion(BaseModel):
    ticket_type_id: int
    custom_question_id: Optional[int] = Field(default=None)
    type: str
    name: str
    column_name: str
    required: bool
    id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['ticket_type_id', 'column_name'])
    )  # GUID
