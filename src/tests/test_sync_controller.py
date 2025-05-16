import pytest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from splash.sync_controller import SyncController, HISTORICAL_START_DATE


@pytest.fixture
def fixed_datetime():
    return datetime(2025, 5, 15, 12, 0, 0, tzinfo=ZoneInfo("Australia/Sydney"))


@pytest.fixture
def patch_settings(monkeypatch, fixed_datetime):
    monkeypatch.setattr("splash.config.settings.Settings.SPLASH_TIMEZONE", ZoneInfo("US/Eastern"))
    monkeypatch.setattr("splash.config.settings.Settings.LOCAL_TIMEZONE", ZoneInfo("Australia/Sydney"))
    monkeypatch.setattr("splash.config.settings.Settings.EVENT_LOOKBACK_HOURS", 168)
    monkeypatch.setattr("splash.config.settings.Settings.GROUP_CONTACT_LOOKBACK_HOURS", 72)
    monkeypatch.setattr("splash.config.settings.Settings.START_DATE", "2025-05-10")
    monkeypatch.setattr("splash.config.settings.Settings.END_DATE", "2025-05-13")
    monkeypatch.setattr("splash.sync_controller.SyncController.now_local", fixed_datetime)


def test_incremental_event_window(patch_settings):
    controller = SyncController(sync_mode="incremental")
    window = controller.get_event_params()

    assert isinstance(window, dict)
    assert "start_date" in window and "end_date" in window
    delta = window["end_date"] - window["start_date"]
    assert delta == timedelta(hours=168)


def test_incremental_window_groupcontact_window(patch_settings):
    controller = SyncController(sync_mode="incremental_window")
    window = controller.get_group_contact_params()

    assert window["start_date"] < window["end_date"]
    assert window["start_date"].tzinfo.key == "US/Eastern"


def test_historical_full_window(patch_settings):
    controller = SyncController(sync_mode="historical_full")
    window = controller.get_event_params()

    expected_start = datetime.strptime(HISTORICAL_START_DATE, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, tzinfo=ZoneInfo("Australia/Sydney")
    ).astimezone(ZoneInfo("US/Eastern")).replace(microsecond=0)

    assert window["start_date"] == expected_start
    assert window["end_date"].tzinfo.key == "US/Eastern"
    assert window["end_date"] > window["start_date"]


def test_invalid_entity_raises_error(patch_settings):
    controller = SyncController(sync_mode="incremental")
    with pytest.raises(ValueError, match="Unsupported entity_type"):
        controller.get_window("unknown")


def test_invalid_date_format_raises_error(patch_settings):
    controller = SyncController(sync_mode="incremental_window")
    with pytest.raises(ValueError, match="Invalid date format"):
        controller._local_date_to_splash("2025/05/10")
