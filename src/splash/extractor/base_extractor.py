import time
import math
import requests
from datetime import datetime
from abc import ABC, abstractmethod
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry  # type: ignore
from urllib.parse import urljoin
from typing import List, Optional, Dict, Any, Union

from splash.auth import SplashAuthManager
from splash.utils.dict_utils import to_list
from splash.utils.logger import setup_logger
from splash.utils.requests_utils import get_proxy
from splash.utils.time_utils import str_to_dt, date_in_range
from splash.config.settings import Settings

logger = setup_logger(__name__)


class BaseExtractor(ABC):
    """
    Abstract base class for all Splash API extractors.
    Handles HTTP requests, pagination, filtering, retries, and rate-limiting.
    Subclasses must implement `_get_config()` and `extract()`.
    """
    base_url: str
    session: requests.Session
    auth: SplashAuthManager
    rate_limit: int
    
    # Class Attributes
    RETRY_TOTAL = 4
    RETRY_BACKOFF = 1.0
    RATE_LIMIT = 2  # requests per second
    DEFAULT_LIMIT = 250
    PAGE_START = 1
    NO_PAGE_STOP = -1 
    
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.session = requests.Session()
        self.auth = SplashAuthManager()
        self.session.headers.update({
            **self.auth.get_auth_header(), 
            **{'Content-Type': 'application/x-www-form-urlencoded'}
        })
        
        # Retry configuration for transient errors and backoff
        retry_strategy = Retry(
            total=self.RETRY_TOTAL,  # Allows up to 4 total retry attemps per request including the first failed one
            backoff_factor=self.RETRY_BACKOFF,  # Enables exponential backoff - the delay between each retries will be 1s, 2s, 4s, etc. 
            status_forcelist=[429, 500, 502, 503, 504],  # Retry only for rate limiting or server errors
            allowed_methods=frozenset(["GET"])
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        self.session.proxies = get_proxy()
    
    @abstractmethod
    def _get_config(self) -> Dict[str, Any]:
        pass
    
    @abstractmethod
    def extract(self) -> Dict[str, List[Dict[str, Any]]]:
        pass
    
    def _data_filter(
        self, 
        data: Union[List[Dict[str, Any]], Dict[str, Any]], 
        *,
        date_col: Optional[str] = None,
        start_dt: Optional[datetime] = None,
        end_dt: Optional[datetime] = None,
        **kwargs: Any
    ) -> List[Dict[str, Any]]:
        """
        Filters records by a date range if a valid date_col is provided.
        Skips records missing the required fields.
        """
        data_list = to_list(data)
        filtered_data = []
        
        for record in data_list:
            date_str_val = record.get(date_col) if date_col else None
            
            if not date_str_val or not start_dt or not end_dt:
                continue  # skip if any required part is missing
            
            if date_in_range(date_str_val, start_dt, end_dt):
                filtered_data.append(record)
        
        return filtered_data
    
    def _page_filter(
            self,
            data: Union[List[Dict[str, Any]], Dict[str, Any]],
            *,
            date_col: Optional[str] = None,
            start_dt: Optional[datetime] = None,
            end_dt: Optional[datetime] = None,
            **kwargs: Any
        ) -> Optional[str]:
        """
        Optional page-level filter that determines whether to skip or stop paging.
        This helps reduce unnecessary API calls outside the target date window.
        Returns:
            - 'continue': skip this page
            - 'exit': stop paging
            - None: keep page
        """
        
        if not isinstance(data, list) or not date_col: # single page or date_col not specified
            return None
        
        if start_dt is None or end_dt is None:  # can't compare if bounds are missing
            return None
        
        date_val1 = data[0].get(date_col)
        date_val2 = data[-1].get(date_col)
        
        if not isinstance(date_val1, str) or not isinstance(date_val2, str):
            logger.error(f"[{type(self).__name__}._page_filter] Invalid date values passed: date_val1: {date_val1} and/or date_val2: {date_val2}")
            return None
        
        dt1 = str_to_dt(date_val1)
        dt2 = str_to_dt(date_val2)
        
        is_asc = dt1 < dt2
        min_dt = dt1 if is_asc else dt2
        max_dt = dt2 if is_asc else dt1

        if is_asc:
            if max_dt < start_dt:
                return 'continue'
            if min_dt > end_dt:
                return 'exit'
        else:
            if min_dt > end_dt:
                return 'continue'
            if max_dt < start_dt:
                return 'exit'

        return None
    
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
        Core method to extract data from the API endpoint with support for:
            - Pagination
            - Rate-limiting
            - Retry/backoff
            - Optional date/page filters
        Returns all filtered records under the endpoint name.
        """
        interval = 1.0 / self.RATE_LIMIT  # minimum interval b/w subsequent GET calls
        last_request_time: float = 0.0
        params = params or {}
        page = params.get("page", page_start)
        params.setdefault("limit", self.DEFAULT_LIMIT)
        
        all_data: List[Dict[str, Any]] = []
        total_records: Optional[int] = None
        max_page: Optional[int] = None
        
        while True:
            params.update({"page": page})
            elapsed = time.time() - last_request_time
            if elapsed < interval:
                time.sleep(interval - elapsed)
            
            if not max_page:
                logger.info(f"Fetching /{endpoint} | page: {page:}")
            else:
                logger.info(f"Fetching /{endpoint} | page: {page:,} / {max_page:,}")
            
            try:
                response = self.session.get(
                    url = urljoin(f"{self.base_url}/", f"{endpoint}/{pathvar}".rstrip("/")),
                    params=params,
                    timeout=Settings.TIMEOUT,
                    verify=Settings.VERIFY
                )
                last_request_time = time.time()
                
                if response.status_code == 429: # Rate Limit Error
                    retry_after = int(response.headers.get("ratelimit-reset", 1))
                    logger.warning(f"Rate limit hit. Retrying after {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                
                response.raise_for_status()
                page_json = response.json()
                data = page_json.get("data", [])
                
                logger.info(f"Get Request has fetched {len(data)} records from {endpoint}/")
                
                if not isinstance(data, list):
                    logger.warning(f"Unexpected data structure: {page_json}")
                    break
                
                if not data: # Empty data
                    break
                
                # Page Filter for processing efficiency - Proceed or Continue or Break
                page_filter = self._page_filter(data, date_col=date_col, start_dt=start_dt, end_dt=end_dt, **kwargs)
                
                if page_filter:
                    if page_filter == 'continue':
                        logger.info("Skipping Data Extract - Date Out of Range.")
                        page += 1
                        continue
                    elif page_filter == 'exit':
                        logger.info("Stopping Data Extract - Date Out of Range.")
                        break
                
                filtered = self._data_filter(data, date_col=date_col, start_dt=start_dt, end_dt=end_dt, **kwargs)
                all_data.extend(filtered)
                num_records = len(all_data)
                
                if 'pagination' not in page_json or len(data) < params.get("limit", self.DEFAULT_LIMIT):
                    if not total_records:
                        total_records = num_records
                    logger.info(f"Extracted records: {num_records:,} of {total_records:,}")
                    logger.info("No more pages. Ending Extraction")
                    break
                    
                else: # Multiple pages
                    pagination = page_json.get('pagination', {})
                    if not max_page:  # Assign max_page
                        count = pagination.get("count")
                        limit = pagination.get("limit") or params.get("limit", self.DEFAULT_LIMIT)
                        max_page = int(math.ceil(count / limit)) if count else 1
                    if not total_records:
                        total_records = pagination.get('count', num_records)
                    logger.info(f"Extracted records: {num_records:,} of {total_records:,}")
                
                if page == page_stop: # Reached page_stop (if any), stop extracting
                    logger.info(f"Stopping Data Extract - reached the page_stop at page {page_stop}.")
                    break
                    
                page += 1
            
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching from '/{endpoint}' on page {page}: {e}")
                break
                
        return { 
            endpoint: all_data
        }
