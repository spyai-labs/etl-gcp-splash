from typing import TYPE_CHECKING, Literal, Type, TypedDict

if TYPE_CHECKING:
    from splash.extractor import BaseExtractor
    from splash.transformer import TransformerMap
    
__all__ = ['DataSource', 'ETLUtil']

DataSource = Literal['event', 'group_contact']

class ETLUtil(TypedDict):
    extractor: Type['BaseExtractor']
    transformer_map: 'TransformerMap'
