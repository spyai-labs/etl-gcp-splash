import pytest
import importlib

from splash.config import validate

# --- Tests for validate.py ---

def test_get_sync_mode_valid(monkeypatch):
    monkeypatch.setenv("SYNC_MODE", "historical_full")
    assert validate.get_sync_mode() == "historical_full"

def test_get_sync_mode_invalid(monkeypatch):
    monkeypatch.setenv("SYNC_MODE", "invalid_mode")
    with pytest.raises(ValueError, match="Invalid SYNC_MODE: invalid_mode"):
        validate.get_sync_mode()

def test_check_secrets_missing(monkeypatch):
    for key in validate.REQUIRED_SECRETS:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(RuntimeError, match="Missing required secrets"):
        validate.check_secrets()

def test_check_secrets_present(monkeypatch):
    monkeypatch.setenv("CLIENT_ID", "abc")
    monkeypatch.setenv("CLIENT_SECRET", "xyz")
    monkeypatch.setenv("USERNAME", "user")
    monkeypatch.setenv("PASSWORD", "pass")
    validate.check_secrets()  # should not raise


# --- Tests for settings.py ---

def test_settings_env_parsing(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("ENABLE_GCS_LOGS", "false")
    monkeypatch.setenv("ENABLE_BQ_LOGS", "True")
    monkeypatch.setenv("REQUEST_TIMEOUT", "30")
    monkeypatch.setenv("SYNC_MODE", "incremental")
    monkeypatch.setenv("CLIENT_ID", "abc")
    monkeypatch.setenv("CLIENT_SECRET", "xyz")
    monkeypatch.setenv("USERNAME", "user")
    monkeypatch.setenv("PASSWORD", "pass")
    
    # Re-import Splash settings
    import splash.config.settings as settings_module
    importlib.reload(settings_module)
    settings = settings_module.Settings()

    assert settings.GCP_PROJECT_ID == "test-project"
    assert settings.ENABLE_GCS_LOGS is False
    assert settings.ENABLE_BQ_LOGS is True
    assert settings.TIMEOUT == 30
    assert settings.SYNC_MODE == "incremental"
    assert settings.CLIENT_ID == "abc"
