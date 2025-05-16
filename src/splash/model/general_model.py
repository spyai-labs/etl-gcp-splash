from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
from splash.utils.guid_utils import generate_hashed_guid

__all__ = ['SplashTheme', 'Contact', 'CustomQuestion']


class SplashTheme(BaseModel):
    id: int
    name: str
    abbr: Optional[str]
    image_url: Optional[str]
    thumbnail_url: Optional[str]
    sort: int
    active: bool
    created: datetime

    
class Contact(BaseModel):
    id: int
    salesforce_id: Optional[str]
    salesforce_object_type: Optional[str]
    first_name: str
    last_name: str
    title: Optional[str]
    primary_email: str
    organization_name: Optional[str]
    phone: Optional[str]
    unsubscribed: bool
    createdate: datetime
    modifydate: datetime
    deleted: bool
    vip: bool
    bounced: bool
    bounced_event: Optional[str]
    bounced_on: Optional[datetime]
    bounced_reason: Optional[str]
    invalid_email: bool

    
class CustomQuestion(BaseModel):
    object_id: int  # derived
    object_type: str  # derived
    type: str
    name: str
    column_name: str
    custom_question_id: Optional[int] = Field(default=None)
    required: bool
    values: Optional[str]
    selected_values: Optional[str]
    deleted: Optional[bool] = Field(default=None)
    external_link: Optional[str] = Field(default=None)
    protected: Optional[bool] = Field(default=None)
    source_id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['object_id', 'column_name'])
    )  # GUID
    id: str = Field(
        default_factory=lambda data: generate_hashed_guid(data, ['object_type', 'source_id'])
    )  # GUID
