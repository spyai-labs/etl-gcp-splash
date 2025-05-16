import numpy as np
from typing import Dict, List, Any

from .base_transformer import BaseTransformer
from splash.utils.dict_utils import replace_null, stringify_list, change_key_name
from splash.model.event_model import (
    Event,
    EventType,
    EventStats,
    EventSetting,
    EventTriggeredEmail,
    EventCustomQuestion,
    EventTicketType
)

__all__ = [
    'EventTransformer', 
    'EventTypeTransformer', 
    'EventStatsTransformer', 
    'EventSettingTransformer', 
    'EventTriggeredEmailTransformer', 
    'EventCustomQuestionTransformer', 
    'EventTicketTypeTransformer'
]


class EventTransformer(BaseTransformer[Event]):
    """
    Transformer for Event entities. 
    Handles stringifying lists such as attendance types and group IDs.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, Event)
    
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # convert list of values into string
        for key in ['event_attendance_types', 'group_ids']:
            stringify_list(item, key)
        return item


class EventTypeTransformer(BaseTransformer[EventType]):
    """
    Transformer for EventType entities. No custom transformation logic.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, EventType)

    
class EventStatsTransformer(BaseTransformer[EventStats]):
    """
    Transformer for EventStats entities. No custom transformation logic.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, EventStats)
    

class EventSettingTransformer(BaseTransformer[EventSetting]):
    """
    Transformer for EventSetting entities.
    Replaces null strings in 'lat' and 'lng' with NaN for proper numeric handling.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, EventSetting)
    
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # Replace 'null' string values with numpy NaN for latitude and longitude
        for key in ['lat', 'lng']:
            replace_null(item, key, np.nan)
        return item
    
    
class EventTriggeredEmailTransformer(BaseTransformer[EventTriggeredEmail]):
    """
    Transformer for EventTriggeredEmail entities. No custom transformation logic.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, EventTriggeredEmail)


class EventCustomQuestionTransformer(BaseTransformer[EventCustomQuestion]):
    """
    Transformer for EventCustomQuestion entities. No custom transformation logic.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, EventCustomQuestion)


class EventTicketTypeTransformer(BaseTransformer[EventTicketType]):
    """
    Transformer for EventTicketType entities.
    Renames key 'id' to 'ticket_type_id' to ensure model schema compatibility.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, EventTicketType)
    
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # Rename 'id' field to 'ticket_type_id' for clarity and schema alignment
        change_key_name(item, change_map = {
            'id': 'ticket_type_id'
        })
        
        return item
        