from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .base_extractor import BaseExtractor
from splash.defined_types import SyncMode
from splash.config.settings import Settings
from splash.utils.logger import setup_logger
from splash.utils.dict_utils import nested_get, safe_copy
from splash.utils.time_utils import date_in_range
from splash.sync_controller import SyncController

logger = setup_logger(__name__)


class GroupContactExtractor(BaseExtractor):
    """
    Extracts group contact data from Splash API.
    Includes contact RSVP, email campaign status, tickets, and answers.
    Handles nested extraction logic per contact.
    """
    sync_mode: SyncMode
    
    # class attributes
    END_POINT = "groupcontacts"
    KEY_DATE_COL = "modified"
    KEY_DATE_SORT = "modified DESC"
    DEFAULT_LIMIT = 250
    INCREMENTAL_SYNC_PAGE_STOP = 8
    SUPPORTED_STATUSES = ["rsvp_yes", "rsvp_no", "checkin_yes", "checkin_no"]
    SUPPORTED_VIEW_GROUPS = ["groupContactEmailCampaignStatuses", "bounceInfo"]
    
    def __init__(self, sync_mode: SyncMode) -> None:
        super().__init__(Settings.BASE_URL)
        self.sync_mode = sync_mode
    
    def _get_config(self) -> Dict[str, Any]:
        """
        Builds configuration used to query the Splash API for group contacts.
        Includes filtering by RSVP/checkin status and view groups.
        """
        sync_params = SyncController(self.sync_mode).get_group_contact_params()
        
        setting_dict: Dict[str, Any] = {
            "endpoint": self.END_POINT,
            "params": {
                "sort": self.KEY_DATE_SORT,
                "limit": self.DEFAULT_LIMIT,
                **{f"status[{i}]": status for i, status in enumerate(self.SUPPORTED_STATUSES)},
                **{f"viewGroups[{i}]": group for i, group in enumerate(self.SUPPORTED_VIEW_GROUPS)},
            },
            "date_col": self.KEY_DATE_COL,
            "start_dt": sync_params.get('start_date'),
            "end_dt": sync_params.get('end_date'),
            "page_stop": self.INCREMENTAL_SYNC_PAGE_STOP if self.sync_mode == "incremental" else BaseExtractor.NO_PAGE_STOP
        }
        
        return setting_dict
    
    def extract(self) -> Dict[str, List[Dict[str, Any]]]:
        setting = self._get_config()
        logger.info(
            f"Extracting '{str(setting.get('endpoint')).lower()}' | Sync Mode: {self.sync_mode} | "
            f"StartDate: {setting.get('start_dt')} | EndDate: {setting.get('end_dt')}"
        )
        logger.info(f"Params: {setting.get('params')}")
        
        return self.get_data(**setting)
    
    def _process_ticket_sale(
        self, 
        sale: Dict[str, Any]
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extracts ticket type and order structure from a given ticket_sale.
        Appends references for BigQuery joins.
        """
        sale = safe_copy(sale)
        ticket_types, tt_questions, ticket_orders, discounts = [], [], [], []
        
        sale.setdefault('ticket_type_id', None)
        sale.setdefault('ticket_order_id', None)
        
        # Ticket Type        
        tt = nested_get(sale, ["ticket_type"], {})
        tt_id = tt.get("id")
        if tt:
            sale["ticket_type_id"] = tt_id
            ticket_types.append(safe_copy(tt))
            
            # Ticket Type Custom Question
            for question in tt.get("custom_questions", []):
                question = safe_copy(question)
                question["ticket_type_id"] = tt_id
                tt_questions.append(question)
        
        # Ticket Order
        order = nested_get(sale, ["ticket_order"], {})
        if order:
            order = safe_copy(order)
            sale["ticket_order_id"] = order.get("id")
            order["ticket_type_id"] = tt_id
            order.setdefault("ticket_order_discount_id", None)
            for k in ["ticket_type_name", "quantity", "price", "foreign_price"]:
                order[k] = sale.get(k)
            
            # Ticket Order Discount
            discount = order.get("ticket_order_discount", {})
            if discount:
                discount = safe_copy(discount)
                order["ticket_order_discount_id"] = discount.get("id")
                discount["ticket_type_id"] = tt_id
                discounts.append(discount)

            ticket_orders.append(order)

        return sale, ticket_types, tt_questions, ticket_orders, discounts
    
    def _process_rsvp(
        self, 
        rsvp: Dict[str, Any], 
        group_contact_id: int
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Processes RSVP and its associated ticket_sale object.
        Aggregates ticket-related subentities using _process_ticket_sale.
        """
        rsvp = safe_copy(rsvp)
        rsvp["group_contact_id"] = group_contact_id
        rsvp.setdefault("ticket_sale_id", None)
        ticket_sales, ticket_types, tt_questions, ticket_orders, discounts = [], [], [], [], []
        
        # Ticket Sale
        sale = nested_get(rsvp, ["ticket_sale"], {})
        if sale:
            rsvp["ticket_sale_id"] = sale.get("id")
            sale, tt_list, tq_list, to_list, dis_list = self._process_ticket_sale(sale)
            ticket_sales.append(sale)
            ticket_types.extend(tt_list)
            tt_questions.extend(tq_list)
            ticket_orders.extend(to_list)
            discounts.extend(dis_list)

        return rsvp, ticket_sales, ticket_types, tt_questions, ticket_orders, discounts
    
    def get_data(
        self,
        endpoint: str,
        pathvar: str = "",
        params: Optional[Dict[str, Any]] = None,
        page_start: int = 1,
        page_stop: int = -1,
        date_col: Optional[str] = None,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        **kwargs: Any
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Performs full flattening of group contact response:
            - Contact info
            - RSVP & answers
            - Ticket and discount lineage
        Applies date filtering and ID checks.
        """
        
        # Initialise the list for group_contact data extraction
        group_contacts, email_campaign_statuses, contacts, rsvps, answers = [], [], [], [], []
        ticket_sales, ticket_orders, ticket_types, ticket_order_discounts, ticket_type_questions = [], [], [], [], []
        
        if not start_dt or not end_dt:
            raise ValueError(f"Invalid or missing datetime values: start_dt: {start_dt} and/or end_dt: {end_dt}")
        
        raw_data = super().get_data(
            endpoint, 
            pathvar, 
            params or {}, 
            page_start=page_start,
            page_stop=page_stop,
            date_col=date_col,
            start_dt=start_dt,
            end_dt=end_dt,
            **kwargs
        ).get(endpoint, [])
        
        logger.info(f"Total Extracted Records: {len(raw_data):,}")
        
        for record in raw_data:
            group_contact_id = record.get("id")
            event_id = nested_get(record, keys=["event_rsvp", "event_id"], default=None)
            modified = record.get("modified") or record.get("created")  # replace with created date if dt_col not found
                
            if not isinstance(group_contact_id, int):
                logger.error(f"Invalid group_contact_id: {group_contact_id}")
                continue
                
            if not isinstance(modified, str):
                logger.error("Invalid or missing key date columns")
                continue
            
            if not date_in_range(modified, start_dt, end_dt):
                logger.error(f"Skipping out-of-range event: {group_contact_id}")
                continue
            
            # Email Campaign Status
            for campaign in nested_get(record, ["email_campaign_statuses"], []):
                campaign = safe_copy(campaign)
                campaign["event_id"] = event_id
                campaign["group_contact_id"] = group_contact_id
                email_campaign_statuses.append(campaign)

            # Contact
            contact = nested_get(record, ["contact"], {})
            if contact:
                contact = safe_copy(contact)
                bounce_info = nested_get(contact, keys=["bounce_info"], default={})
                contact['bounced_event'] = bounce_info.get('event_title')
                contact['bounced_on'] = bounce_info.get('sent_on')
                contact['bounced_reason'] = bounce_info.get('bounce_reason')
                contacts.append(contact)

            # EVENT RSVP
            rsvp = nested_get(record, ["event_rsvp"], {})
            if rsvp:
                rsvp_out, sales, t_types, questions, t_orders, discounts = self._process_rsvp(rsvp, group_contact_id)
                rsvps.append(rsvp_out)
                ticket_sales.extend(sales)
                ticket_types.extend(t_types)
                ticket_type_questions.extend(questions)
                ticket_orders.extend(t_orders)
                ticket_order_discounts.extend(discounts)
            
             # Answers
            for ans in nested_get(record, ["answers"], []):
                ans = safe_copy(ans)
                ans["event_id"] = event_id
                ans["group_contact_id"] = group_contact_id
                answers.append(ans)
                
            # Final GroupContact
            record = safe_copy(record)
            record['contact_id'] = nested_get(contact, ["id"])
            record['event_id'] = nested_get(rsvp, ["event_id"])
            record['event_rsvp_id'] = nested_get(rsvp, ["id"])
            record['first_name'] = nested_get(rsvp, ["first_name"])
            record['last_name'] = nested_get(rsvp, ["last_name"])
            record['email'] = nested_get(rsvp, ["email"])
            group_contacts.append(record)
                
                
        return {
            "group_contacts": group_contacts,
            "email_campaign_statuses": email_campaign_statuses,
            "contacts": contacts,
            "rsvps": rsvps,
            "answers": answers,
            "ticket_sales": ticket_sales,
            "ticket_orders": ticket_orders,
            "ticket_types": ticket_types,
            "ticket_order_discounts": ticket_order_discounts,
            "ticket_type_questions": ticket_type_questions
        }
