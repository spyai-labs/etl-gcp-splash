from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field
from splash.utils.guid_utils import generate_hashed_guid

__all__ = ['GroupContact', 'GroupContactEventRSVP', 'GroupContactAnswer', 'GroupContactEmailCampaignStatus']


class GroupContact(BaseModel):
    id: int
    contact_id: int
    salesforce_campaign_member_id: Optional[str]
    event_id: Optional[int]  # derived
    event_rsvp_id: Optional[int]  # derived
    first_name: Optional[str]  # derived
    last_name: Optional[str]  # derived
    email: Optional[str]  # derived
    status: str
    created: datetime
    modified: datetime
    deleted: bool
    
    
class GroupContactEventRSVP(BaseModel):
    id: int
    event_id: int
    group_contact_id: int  # derived
    name: str  # derived
    attending: bool
    date_rsvped: datetime
    checked_in: Optional[datetime]
    checked_out: Optional[datetime]
    plus_one: int
    created: datetime
    modified: datetime
    deleted: int
    ticket_sale_id: Optional[int]
    ticket_number: Optional[str]
    vip: bool
    waitlist: bool
    qr_url: str
    unsub_tag: Optional[str]
    unsubscribed: bool
    
    
class GroupContactAnswer(BaseModel):
    event_id: Optional[int]  # Optional for safety reasons but it should exist for all rsvp guests
    group_contact_id: int
    question_id: int
    answer: str
    id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['group_contact_id', 'question_id'])
    )  # GUID

    
class GroupContactEmailCampaignStatus(BaseModel):
    event_id: Optional[int]
    group_contact_id: int
    email_campaign_id: int
    status: str
    id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['group_contact_id', 'email_campaign_id'])
    )  # GUID
