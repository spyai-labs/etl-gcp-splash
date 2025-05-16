from typing import Dict, List, Any
from .base_transformer import BaseTransformer
from splash.utils.dict_utils import replace_null, stringify_list
from splash.model.general_model import (
    SplashTheme,
    Contact,
    CustomQuestion
)

__all__ = [
    'SplashThemeTransformer', 
    'ContactTransformer', 
    'CustomQuestionTransformer'
]


class SplashThemeTransformer(BaseTransformer[SplashTheme]):
    """
    Transformer for SplashTheme data.
    Applies Pydantic validation but no custom transformation logic.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, SplashTheme)

        
class ContactTransformer(BaseTransformer[Contact]):
    """
    Transformer for Contact records.
    - Splits salesforce_id into id and object type.
    - Normalizes certain null-like fields.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, Contact)
        
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Custom transformation logic for Contact data:
        - Splits `salesforce_id` (e.g., "Lead:12345") into:
            `salesforce_object_type` = "Lead", `salesforce_id` = "12345"
        - Replaces "null"/empty strings in select fields with None
        """
        sf_id = item.get('salesforce_id')
        item.setdefault("salesforce_id")
        item.setdefault("salesforce_object_type")
        
        if sf_id:
            sf_id_split = sf_id.split(":")
            id_val = sf_id_split[1] if len(sf_id_split) > 1 else sf_id
            type_val = sf_id_split[0] if len(sf_id_split) > 1 else 'Unspecified'
            
            item.update({
                'salesforce_id': id_val,
                'salesforce_object_type': type_val
            })
        
        # Replace string null values with None
        for key in ['organization_name', 'phone']:
            replace_null(item, key, None)
        
        return item


class CustomQuestionTransformer(BaseTransformer[CustomQuestion]):
    """
    Transformer for CustomQuestion records.
    - Identifies if question belongs to event or ticket_type and adds metadata.
    - Converts array-like fields into stringified representations.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, CustomQuestion)
        
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """
        Custom logic for unifying structure of event and ticket-type questions:
        - Adds `object_type` ("event" or "ticket_type") and `object_id`
        - Converts `values` and `selected_values` list fields into delimited strings
        """
        if 'event_id' in item:
            item['object_id'] = item.get('event_id')
            item['object_type'] = 'event'
        
        elif 'ticket_type_id' in item:
            item['object_id'] = item.get('ticket_type_id')
            item['object_type'] = 'ticket_type'
        
        # convert list of values into string
        for key in ['values', 'selected_values']:
            stringify_list(item, key)

        return item
