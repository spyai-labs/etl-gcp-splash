import pandas as pd
from typing import Dict, List, Any
from .base_transformer import BaseTransformer
from splash.model.group_contact_model import (
    GroupContact,
    GroupContactEventRSVP,
    GroupContactAnswer,
    GroupContactEmailCampaignStatus
)

__all__ = [
    'GroupContactTransformer', 
    'GroupContactEventRSVPTransformer', 
    'GroupContactAnswerTransformer', 
    'GroupContactEmailCampaignStatusTransformer'
]


class GroupContactTransformer(BaseTransformer[GroupContact]):
    """
    Transformer for GroupContact records.
    Applies base transformation and validation using the GroupContact Pydantic model.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, GroupContact)


class GroupContactEventRSVPTransformer(BaseTransformer[GroupContactEventRSVP]):
    """
    Transformer for Event RSVP data within a GroupContact.
    Includes logic to combine first and last name into a full name field.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, GroupContactEventRSVP)
        
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # Combine first_name and last_name into a single name field
        fn = item.get('first_name', '')
        ln = item.get('last_name', '')
        item.setdefault('name', f"{fn} {ln}".strip())
        return item


class GroupContactAnswerTransformer(BaseTransformer[GroupContactAnswer]):
    """
    Transformer for custom answers tied to a GroupContact.
    Deduplicates on (group_contact_id, question_id).
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, GroupContactAnswer)
    
    # Remove duplicate answers based on composite key (group_contact_id, question_id)
    def process_df(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=['group_contact_id', 'question_id'], keep='last', ignore_index=True)  # Deduplication of Rows on subset of key columns

        
class GroupContactEmailCampaignStatusTransformer(BaseTransformer[GroupContactEmailCampaignStatus]):
    """
    Transformer for email campaign engagement status records related to a GroupContact.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, GroupContactEmailCampaignStatus)
