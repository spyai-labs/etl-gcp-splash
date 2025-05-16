from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field
from splash.utils.guid_utils import generate_hashed_guid

__all__ = ['Event', 'EventType', 'EventStats', 'EventSetting', 'EventTriggeredEmail', 'EventCustomQuestion', 'EventTicketType']


class Event(BaseModel):
    id: int
    salesforce_campaign_id: Optional[str]
    splash_theme_id: int
    splash_theme_name: str
    event_type_id: int
    event_type_name: str
    event_setting_id: int
    title: str
    description_text: str
    event_owner_first_name: str
    event_owner_last_name: str
    event_owner_email: str
    event_start: Optional[datetime]
    event_end: Optional[datetime]
    hide_event_time: bool
    venue_name: str
    address: str
    city: str
    state: str
    zip_code: str
    country: str
    created_at: datetime
    modified_at: datetime
    domain: str
    custom_domain: str
    paid_for_domain: bool
    deleted: bool
    published: bool
    hub: int
    fq_url: str
    mobile_check_in_url: str
    event_attendance_types: str
    group_ids: str
    registration_updating_enabled: bool
    registration_updating_deadline: int

    
class EventType(BaseModel):
    id: int
    name: str
    code_name: Optional[str]
    splash_type: bool
    public_type: bool
    is_enterprise_type: bool

    
class EventStats(BaseModel):
    event_id: int  # derived
    name: str
    count: int
    id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['event_id', 'name'])
    )  # GUID

    
class EventSetting(BaseModel):
    id: int
    header_image: Optional[str]
    rsvp_open: bool
    wait_list: bool
    rsvp_method: str
    lat: Optional[float]
    lng: Optional[float]
    event_hashtag: Optional[str]
    rsvp_max: int
    venue_tbd: int
    rsvp_guest_display: bool
    rsvp_closed_state: str
    rsvp_closed_at: Optional[datetime]
    rsvp_closed_team_notified: bool
    page_privacy_type: str
    event_host: str
    button_closed_message: str
    autosave: bool

    
class EventTriggeredEmail(BaseModel):
    event_id: int  # derived
    event_setting_id: int  # derived
    trigger: str
    subject: Optional[str] = Field(default=None)
    content: Optional[str] = Field(default=None)
    include_calendar_attachment: Optional[int] = Field(default=None)
    include_pdf_attachment: Optional[int] = Field(default=None)
    include_invoice_pdf: Optional[int] = Field(default=None)
    pdf_content: Optional[str] = Field(default=None)
    use_default_confirmation: Optional[int] = Field(default=None)
    active: Optional[int] = Field(default=None)
    event_message_id: Optional[int] = Field(default=None)
    event_message_linked_to_theme: Optional[int] = Field(default=None)
    id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['event_id', 'trigger'])
    )  # GUID

    
class EventCustomQuestion(BaseModel):
    event_id: int  # derived
    event_setting_id: int  # derived
    custom_question_id: Optional[int] = Field(default=None)
    type: str
    name: str
    column_name: str
    required: bool
    id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['event_id', 'column_name'])
    )  # GUID

    
class EventTicketType(BaseModel):
    event_id: int
    ticket_type_id: int
    name: str  # derived
    description: str  # derived
    active: bool  # derived
    id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['event_id', 'ticket_type_id'])
    )  # GUID
