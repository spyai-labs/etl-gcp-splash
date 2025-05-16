from typing import Dict, Type, Any, List, Union
from .base_transformer import BaseTransformer

__all__ = [
    'EVENT_TRANSFORMER_MAP', 
    'GROUPCONTACT_TRANSFORMER_MAP',
    'TransformerClass',
    'TransformerMap'
]

from .event_transformer import (
    EventTransformer,
    EventTypeTransformer,
    EventStatsTransformer,
    EventSettingTransformer,
    EventTriggeredEmailTransformer,
    EventCustomQuestionTransformer,
    EventTicketTypeTransformer
)

from .general_transformer import (
    SplashThemeTransformer,
    ContactTransformer,
    CustomQuestionTransformer
)

from .group_contact_transformer import (
    GroupContactTransformer,
    GroupContactEventRSVPTransformer,
    GroupContactAnswerTransformer,
    GroupContactEmailCampaignStatusTransformer
)

from .ticket_transformer import (
    TicketTypeTransformer,
    TicketOrderTransformer,
    TicketSaleTransformer,
    TicketTypeDiscountTransformer,
    TicketOrderDiscountTransformer,
    TicketTypeCustomQuestionTransformer
)

# Define transformer type alias
TransformerClass = Type[BaseTransformer[Any]]
TransformerMap = Dict[str, Union[TransformerClass, List[TransformerClass]]]


# Typed Event transformer maps
EVENT_TRANSFORMER_MAP: TransformerMap = {
    'events': EventTransformer, 
    'splash_themes': SplashThemeTransformer,
    'event_types': EventTypeTransformer, 
    'event_stats': EventStatsTransformer,
    'event_settings': EventSettingTransformer, 
    'triggered_emails': EventTriggeredEmailTransformer, 
    'custom_questions': [EventCustomQuestionTransformer, CustomQuestionTransformer],
    'ticket_types': [EventTicketTypeTransformer, TicketTypeTransformer], 
    'ticket_type_questions': [TicketTypeCustomQuestionTransformer, CustomQuestionTransformer],
}

# Typed GroupContact transformer maps
GROUPCONTACT_TRANSFORMER_MAP: TransformerMap = {
    'group_contacts': GroupContactTransformer,
    'contacts': ContactTransformer,
    'rsvps': GroupContactEventRSVPTransformer,
    'email_campaign_statuses': GroupContactEmailCampaignStatusTransformer,
    'answers': GroupContactAnswerTransformer,
    'ticket_sales': TicketSaleTransformer,
    'ticket_orders': TicketOrderTransformer,
    'ticket_order_discounts': [TicketTypeDiscountTransformer, TicketOrderDiscountTransformer],
}
