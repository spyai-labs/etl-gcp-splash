from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime

from .base_extractor import BaseExtractor
from splash.defined_types import SyncMode
from splash.config.settings import Settings
from splash.utils.logger import setup_logger
from splash.utils.dict_utils import nested_get, safe_copy
from splash.utils.time_utils import date_in_range
from splash.sync_controller import SyncController

logger = setup_logger(__name__)


class EventExtractor(BaseExtractor):
    """
    Extracts event-related data from the Splash API.
    Includes event metadata, settings, ticket types, stats, and associated objects.
    Sync mode affects pagination and limit size.
    """
    sync_mode: SyncMode
    
    # class attributes
    END_POINT = "events"
    KEY_DATE_COL = "modified_at"
    KEY_DATE_SORT = "modified_desc"
    DEFAULT_LIMIT = 30
    HISTORICAL_SYNC_LIMIT = 250
    INCREMENTAL_SYNC_PAGE_STOP = 2
    SUPPORTED_VIEW_GROUPS = ["salesforceIntegration"]
    
    def __init__(self, sync_mode: SyncMode) -> None:
        super().__init__(Settings.BASE_URL)
        self.sync_mode = sync_mode
    
    def _get_config(self) -> Dict[str, Any]:
        """
        Generates API query configuration (endpoint, parameters, sync range, etc.) based on sync mode.
        """
        sync_params = SyncController(self.sync_mode).get_event_params()
        
        setting_dict: Dict[str, Any] = {
            "endpoint": self.END_POINT,
            "params": {
                "sort": self.KEY_DATE_SORT,
                "limit": self.DEFAULT_LIMIT if self.sync_mode != "historical_full" else self.HISTORICAL_SYNC_LIMIT,
                **{f"viewGroups[{i}]": group for i, group in enumerate(self.SUPPORTED_VIEW_GROUPS)},
            },
            "date_col": self.KEY_DATE_COL,
            "start_dt": sync_params.get('start_date'),
            "end_dt": sync_params.get('end_date'),
            "page_stop": self.INCREMENTAL_SYNC_PAGE_STOP if self.sync_mode == "incremental" else BaseExtractor.NO_PAGE_STOP,
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
    
    def _process_event_settings(
        self, 
        setting: Dict[str, Any], 
        event_id: int
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Extracts nested custom questions and triggered emails from event settings.
        Adds relational references to each item.
        """
        settings, custom_questions, triggered_emails = [], [], []
        setting_id = setting.get("id")
        
        if setting:
            settings.append(safe_copy(setting))

            for q in setting.get("custom_questions", []):
                q = safe_copy(q)
                q["event_id"] = event_id
                q["event_setting_id"] = setting_id
                custom_questions.append(q)

            emails = nested_get(setting, keys=["email_settings", "triggered_emails"], default=[])
            for email in emails:
                email = safe_copy(email)
                email["event_id"] = event_id
                email["event_setting_id"] = setting_id
                triggered_emails.append(email)

        return settings, custom_questions, triggered_emails
    
    def _process_ticket_types(
        self, 
        tickets: List[Dict[str, Any]],
        event_id: int
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Processes nested ticket types and their child objects:
            - Discounts
            - Custom questions
        Appends event_id for join references.
        """
        ticket_types, discounts, questions = [], [], []
        
        for tt in tickets:
            tt = safe_copy(tt)
            tt["event_id"] = event_id
            ticket_types.append(tt)
            tt_id = tt.get("id")

            for discount in tt.get("ticket_order_discounts", []):
                discount = safe_copy(discount)
                discount["ticket_type_id"] = tt_id
                discounts.append(discount)

            for question in tt.get("custom_questions", []):
                question = safe_copy(question)
                question["ticket_type_id"] = tt_id
                questions.append(question)

        return ticket_types, discounts, questions
        
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
        Calls BaseExtractor.get_data and flattens nested event JSON into
        multiple BigQuery-ready datasets with enriched references.
        """
        
        # Initialise the list for event data extraction
        events, splash_themes, event_stats, event_types = [], [], [], []
        event_settings, triggered_emails, custom_questions = [], [], []
        ticket_types, ticket_order_discounts, ticket_type_questions = [], [], []
        
        # start_dt: Optional[datetime] = kwargs.get("start_dt")
        # end_dt: Optional[datetime] = kwargs.get("end_dt")
        
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
        
        for event in raw_data:
            event_id = event.get("id")
            modified = event.get("modified_at") or event.get("created_at")  # replace with created date if dt_col not found
            
            if not isinstance(event_id, int):
                logger.error(f"Invalid event_id: {event_id}")
                continue
                
            if not isinstance(modified, str):
                logger.error("Invalid or missing key date columns")
                continue
            
            if not date_in_range(modified, start_dt, end_dt):
                logger.error(f"Skipping out-of-range event: {event_id}")
                continue
            
            # Event Setting
            setting = nested_get(event, ["event_setting"], {})
            es, cq, te = self._process_event_settings(setting, event_id)
            event_settings.extend(es)
            custom_questions.extend(cq)
            triggered_emails.extend(te)
            
            # Event Types
            etype = nested_get(event, ["event_type"], {})
            if etype:
                event_types.append(safe_copy(etype))

            # Event Stats
            for stat in nested_get(event, ["stats"], []):
                stat = safe_copy(stat)
                stat["event_id"] = event_id
                event_stats.append(stat)

            # Splash Theme
            theme = nested_get(event, ["splash_theme"], {})
            if theme:
                splash_themes.append(safe_copy(theme))
            
            # Ticket Types
            raw_ticket_types = nested_get(event, ["ticket_types"], [])
            tt, td, tq = self._process_ticket_types(raw_ticket_types, event_id)
            ticket_types.extend(tt)
            ticket_order_discounts.extend(td)
            ticket_type_questions.extend(tq)
                    
            # Filtered Events - Adding object references
            event = safe_copy(event)
            event["splash_theme_id"] = nested_get(theme, ["id"])
            event["splash_theme_name"] = nested_get(theme, ["name"])
            event["event_type_id"] = nested_get(etype, ["id"])
            event["event_type_name"] = nested_get(etype, ["name"])
            event["event_setting_id"] = nested_get(setting, ["id"])
            events.append(event)
            
            
        return {
            "events": events,
            "event_settings": event_settings,
            "triggered_emails": triggered_emails,
            "custom_questions": custom_questions,
            "event_types": event_types,
            "event_stats": event_stats,
            "splash_themes": splash_themes,
            "ticket_types": ticket_types,
            "ticket_order_discounts": ticket_order_discounts,
            "ticket_type_questions": ticket_type_questions
        }
