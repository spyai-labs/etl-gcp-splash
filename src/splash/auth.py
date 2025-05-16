import time
import json
import requests
from typing import TypedDict, Any, Dict

from splash.config.settings import Settings
from splash.utils.logger import setup_logger
from splash.utils.requests_utils import get_proxy
from splash.secret import get_version, add_and_destroy_prev

logger = setup_logger(__name__)


class SplashAuthError(Exception):
    """Raised when authentication with Splash API fails."""


class TokenData(TypedDict):
    access_token: str
    expires_in: int
    token_type: str
    scope: str
    refresh_token: str


class SplashAuthManager:
    """
    Initializes the SplashAuthManager instance.
    Loads token information from Secret Manager into memory.
    """
    auth_url: str
    access_token: str
    refresh_token: str
    expires_at: int
    is_valid_token: bool
    
    def __init__(self) -> None:
        self.auth_url = f"{Settings.BASE_URL}/oauth/v2/token"
        self.access_token = ""
        self.refresh_token = ""
        self.expires_at = 0
        self.is_valid_token = False
        self._set_token_data()
    
    def _set_token_data(self) -> None:
        """
        Fetches token data from GCP Secret Manager and stores it in memory.
        Raises:
            SplashAuthError: If the token cannot be retrieved or parsed.
        """
        try:
            token_json = get_version(Settings.GCP_PROJECT_ID, Settings.TOKEN_SECRET_ID) or json.dumps({})
            token_data = json.loads(token_json)
            self.access_token = token_data.get("access_token", "")
            self.refresh_token = token_data.get("refresh_token", "")
            self.expires_at = token_data.get("expires_at", 0)
            
            logger.info("Fetched token data from Secret Manager")
            
        except Exception as e:
            logger.error("Failed to fetch token data from Secret Manager")
            raise SplashAuthError("Secret Manager fetch failed") from e
        
    def _update_token_data(self, data: TokenData) -> None:
        """
        Updates in-memory token data and persists it to Secret Manager.
        Args:
            data (TokenData): Newly fetched or refreshed token information.
        Raises:
            SplashAuthError: If storing token in Secret Manager fails.
        """
        try:
            self.access_token = data["access_token"]
            self.refresh_token = data.get("refresh_token", self.refresh_token)
            self.expires_at = int(time.time()) + data.get("expires_in", 28800) - 60  # refresh 1 min early
            self.is_valid_token = True
            
            add_and_destroy_prev(
                Settings.GCP_PROJECT_ID, 
                Settings.TOKEN_SECRET_ID, 
                json.dumps({
                    'access_token': self.access_token,
                    'refresh_token': self.refresh_token,
                    'expires_at': self.expires_at
                })
            )
            
            logger.info("New token updated to Secret Manager")
        
        except Exception as e:
            logger.error("Failed to store new token in Secret Manager")
            raise SplashAuthError("Secret Manager upload failed") from e

    def _is_token_expired(self) -> bool:
        """
        Checks if the current access token is expired.
        Returns:
            bool: True if token is expired, False otherwise.
        """
        if time.time() >= self.expires_at:
            self.is_valid_token = False
            return True
        return False
    
    def _test_valid_token(self) -> None:
        """
        Validates the current access token by calling a harmless endpoint.
        Raises:
            SplashAuthError: If the token is invalid or the request fails.
        """
        try:
            response = requests.get(
                f"{Settings.BASE_URL}/events",
                params={"limit": "1", "page": "1", "event_start_before": "2000-01-01 00:00:00"},
                headers={"Authorization": f"Bearer {self.access_token}"},
                timeout=Settings.TIMEOUT,
                proxies=get_proxy(), 
                verify=Settings.VERIFY
            )
            response.raise_for_status()
        
        except requests.exceptions.RequestException as err:
            self.is_valid_token = False
            logger.info(f"Token invalid: {err}")
            raise SplashAuthError("Invalid token") from err
        
        logger.info("Token validation passed")
        self.is_valid_token = True
    
    def _safe_extract_error(self, response: requests.Response) -> str:
        """
        Attempts to extract a readable error message from the HTTP response.
        Args:
            response (requests.Response): The response to inspect.
        Returns:
            str: Extracted error message or raw response body.
        """
        try:
            return str(response.json())
        except Exception:
            return response.text or "No response body"
    
    def _fetch_access_token(self) -> TokenData:
        """
        Fetches a new access token using username/password flow.
        Returns:
            TokenData: The fetched token information.
        Raises:
            SplashAuthError: If the request fails or the response is invalid.
        """
        payload = {
            "grant_type": "password",
            "client_id": Settings.CLIENT_ID,
            "client_secret": Settings.CLIENT_SECRET,
            "scope": "user",
            "username": Settings.USERNAME,
            "password": Settings.PASSWORD
        }
        
        try:
            response = requests.post(
                self.auth_url, 
                data=payload,
                timeout=Settings.TIMEOUT,
                proxies=get_proxy(), 
                verify=Settings.VERIFY
            )
            response.raise_for_status()
            data: TokenData = response.json()
            
        except requests.exceptions.HTTPError as http_err:
            self.is_valid_token = False
            msg = self._safe_extract_error(response)
            logger.error(f"Failed to fetch new token: {http_err} - {msg}")
            raise SplashAuthError("Token fetch failed") from http_err
        
        logger.info("Fetched new access token")
        self._update_token_data(data)
        
        return data
    
    def _refresh_token(self) -> TokenData:
        """
        Attempts to refresh the token using a refresh_token grant.
        Falls back to fetching a new token if refresh_token is missing.
        Returns:
            TokenData: The refreshed token information.
        Raises:
            SplashAuthError: If the refresh request fails.
        """
        if not self.refresh_token:
            return self._fetch_access_token()

        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": Settings.CLIENT_ID,
            "client_secret": Settings.CLIENT_SECRET,
            "scope": "user"
        }
        
        try:
            response = requests.get(
                self.auth_url, 
                params=payload,
                headers={}, 
                timeout=Settings.TIMEOUT,
                proxies=get_proxy(), 
                verify=Settings.VERIFY
            )
            response.raise_for_status()
            data: TokenData = response.json()
        
        except requests.exceptions.HTTPError as http_err:
            self.is_valid_token = False
            msg = self._safe_extract_error(response)
            logger.warning(f"Failed to refresh token: {http_err} - {msg}")
            raise SplashAuthError("Token refresh failed") from http_err
        
        logger.info("Refreshed access token")
        self._update_token_data(data)
        
        return data
    
    def get_token(self) -> str:
        """
        Retrieves a valid access token.
        Refreshes or fetches a new token if expired or invalid.
        Returns:
            str: A valid access token.
        """
        try:
            if not self.access_token:
                self._set_token_data()

            if self._is_token_expired():
                self._refresh_token()

            if not self.is_valid_token:
                self._test_valid_token()
                
        except SplashAuthError:
            self._fetch_access_token()

        return self.access_token
    
    def get_auth_header(self) -> Dict[str, str]:
        """
        Returns the Authorization header with a valid access token.
        Returns:
            Dict[str, str]: Bearer token in header format.
        """
        return {"Authorization": f"Bearer {self.get_token()}"}

    def debug_token(self) -> Dict[str, Any]:
        """
        Returns a redacted debug summary of the current token state.
        Returns:
            Dict[str, Any]: Debug info including partial token, expiry, and validity.
        """
        return {
            "access_token": f"{self.access_token[:6]}...",
            "expires_at": self.expires_at,
            "valid": self.is_valid_token
        }
