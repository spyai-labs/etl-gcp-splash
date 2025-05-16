import pytest
import time
import json
from unittest.mock import patch, MagicMock
from splash.auth import SplashAuthManager, SplashAuthError


@pytest.fixture
def fake_token_data():
    return {
        "access_token": "test-access-token",
        "refresh_token": "test-refresh-token",
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "user"
    }


def test_token_fetch_success(fake_token_data):
    with patch("splash.auth.get_version", return_value=json.dumps(fake_token_data)), \
        patch("splash.auth.requests.get") as mock_get, \
        patch("splash.auth.requests.post") as mock_post, \
        patch("splash.auth.add_and_destroy_prev"):

        # Simulate token validation success
        mock_get_resp = MagicMock()
        mock_get_resp.status_code = 200
        mock_get_resp.raise_for_status.return_value = None
        mock_get_resp.json.return_value = {"data": []}
        mock_get.return_value = mock_get_resp

        # Simulate token fetch success
        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 200
        mock_post_resp.raise_for_status.return_value = None
        mock_post_resp.json.return_value = fake_token_data
        mock_post.return_value = mock_post_resp

        manager = SplashAuthManager()
        token = manager.get_token()
        assert token == "test-access-token"
        assert manager.is_valid_token


def test_token_expired_triggers_refresh(fake_token_data):
    fake_token_data["expires_in"] = -1
    with patch("splash.auth.get_version", return_value=json.dumps(fake_token_data)), \
         patch("splash.auth.add_and_destroy_prev"), \
         patch("splash.auth.requests.get") as mock_get:

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"access_token": "new-token", "expires_in": 3600}
        mock_get.return_value = mock_response

        manager = SplashAuthManager()
        manager.expires_at = time.time() - 10  # Simulate expired token

        token = manager.get_token()

        assert token == "new-token"
        assert manager.access_token == "new-token"


def test_refresh_token_failure_fallback_to_fetch():
    # Valid fake token data to simulate success response
    fake_token_data = {
        "access_token": "test-access-token",
        "refresh_token": "test-refresh-token",
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "user"
    }

    with patch("splash.auth.get_version", return_value=json.dumps(fake_token_data)), \
         patch("splash.auth.add_and_destroy_prev"), \
         patch("splash.auth.requests.get", side_effect=SplashAuthError("refresh failed")), \
         patch("splash.auth.requests.post") as mock_post:

        # Fallback token fetch (requests.post) returns valid token
        mock_post_resp = MagicMock()
        mock_post_resp.status_code = 200
        mock_post_resp.raise_for_status.return_value = None
        mock_post_resp.json.return_value = fake_token_data
        mock_post.return_value = mock_post_resp

        # Instantiate and force expiry
        manager = SplashAuthManager()
        manager.expires_at = time.time() - 10  # token is expired

        # This will try to refresh, fail, then fetch token via POST
        token = manager.get_token()

        assert token == "test-access-token"
        assert manager.is_valid_token


def test_safe_extract_error_json():
    mock_response = MagicMock()
    mock_response.json.return_value = {"error": "unauthorized"}
    assert "unauthorized" in SplashAuthManager()._safe_extract_error(mock_response)


def test_safe_extract_error_text():
    mock_response = MagicMock()
    mock_response.json.side_effect = Exception("fail")
    mock_response.text = "error text"
    assert SplashAuthManager()._safe_extract_error(mock_response) == "error text"


def test_debug_token_format():
    token_data = {
        "access_token": "abc123xyz",
        "refresh_token": "dummy-refresh",
        "expires_at": time.time() + 3600
    }

    with patch("splash.auth.get_version", return_value=json.dumps(token_data)), \
         patch("splash.auth.add_and_destroy_prev"), \
         patch("splash.auth.requests.get") as mock_get:

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"data": []}
        mock_get.return_value = mock_response

        manager = SplashAuthManager()
        # 🔒 Ensure no refresh or fetch occurs
        manager.is_valid_token = True

        debug_info = manager.debug_token()

        assert debug_info["access_token"].startswith("abc123")
        assert debug_info["valid"] is True
        assert debug_info["expires_at"] > time.time()