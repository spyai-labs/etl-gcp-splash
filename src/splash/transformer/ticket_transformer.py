from typing import Dict, List, Any
from .base_transformer import BaseTransformer
from splash.utils.dict_utils import change_key_name, nested_get
from splash.model.ticket_model import (
    TicketType,
    TicketOrder,
    TicketSale,
    TicketTypeDiscount,
    TicketOrderDiscount,
    TicketTypeCustomQuestion
)

__all__ = [
    'TicketTypeTransformer', 
    'TicketOrderTransformer', 
    'TicketSaleTransformer', 
    'TicketTypeDiscountTransformer', 
    'TicketOrderDiscountTransformer', 
    'TicketTypeCustomQuestionTransformer'
]


class TicketTypeTransformer(BaseTransformer[TicketType]):
    """
    Transformer for TicketType model.
    No custom transformation logic needed.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, TicketType)

        
class TicketOrderTransformer(BaseTransformer[TicketOrder]):
    """
    Transformer for TicketOrder model.
    Extracts nested fields and performs field renaming.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, TicketOrder)
        
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # Extract contact ID from nested dict
        item["contact_id"] = nested_get(item, ["contact", "id"])
        
        # Extract currency fields
        item["currency_id"] = nested_get(item, ["currency", "id"])
        item["currency_code"] = nested_get(item, ["currency", "code"])
        item["currency_name"] = nested_get(item, ["currency", "name"])
        
        # Extract discount-related fields
        item["flat_discount"] = nested_get(item, ["ticket_order_discount", "flat_discount_amount"])
        item["percent_discount"] = nested_get(item, ["ticket_order_discount", "percent_discount"])
        item["discount_code"] = nested_get(item, ["ticket_order_discount", "discount_code"])
        
        # Rename field for schema compatibility
        change_key_name(item, change_map={
            "foreign_total_price": "foreign_total"
        })
        
        return item

        
class TicketSaleTransformer(BaseTransformer[TicketSale]):
    """
    Transformer for TicketSale model.
    Computes price aggregates and refund status.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, TicketSale)
        
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # Combine local and foreign prices
        item["unit_price"] = item.get('price', 0) + item.get('foreign_price', 0)
        item["total_price"] = item.get('total', 0) + item.get('foreign_total', 0)
        
        # Flag whether sale is refunded based on ticket_order.amount_refunded
        refunds = nested_get(item, ["ticket_order", "amount_refunded"], default=0)
        item["is_refunded"] = refunds != 0
        
        return item

        
class TicketTypeDiscountTransformer(BaseTransformer[TicketTypeDiscount]):
    """
    Transformer for TicketTypeDiscount model.
    Renames ID key for consistency with ticket_order_discount schema.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, TicketTypeDiscount)
    
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # Change dict keys
        change_key_name(item, change_map={
            "id": "ticket_order_discount_id"
        })
        
        return item


class TicketOrderDiscountTransformer(BaseTransformer[TicketOrderDiscount]):
    """
    Transformer for TicketOrderDiscount model.
    Renames flat discount field for schema consistency.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, TicketOrderDiscount)
        
    def transform(self, item: Dict[str, Any]) -> Dict[str, Any]:
        # Change dict keys
        change_key_name(item, change_map={
            "flat_discount_amount": "flat_discount"
        })
        
        return item

        
class TicketTypeCustomQuestionTransformer(BaseTransformer[TicketTypeCustomQuestion]):
    """
    Transformer for TicketTypeCustomQuestion model.
    No custom transformation logic needed.
    """
    def __init__(self, data: List[Dict[str, Any]]) -> None:
        super().__init__(data, TicketTypeCustomQuestion)
