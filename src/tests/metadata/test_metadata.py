from datetime import datetime

from splash.metadata.etl_metadata import ETLMetaData
from splash.config.settings import Settings


def test_etl_metadata_initialization(monkeypatch):
    # Force sync mode to 'incremental' and patch time
    monkeypatch.setattr(Settings, "SYNC_MODE", "incremental")
    monkeypatch.setattr(Settings, "LOCAL_TIMEZONE", Settings.LOCAL_TIMEZONE)

    metadata = ETLMetaData()

    assert metadata.sync_mode == "incremental"
    assert metadata.full_sync is False
    assert isinstance(metadata.run_time, datetime)
    assert isinstance(metadata.run_id, str)
    assert len(metadata.run_id) == 8
    assert metadata.log_path.startswith("logs/")
    assert metadata.log_path.endswith(f"{metadata.run_id}.zip")


def test_etl_metadata_full_sync_flag(monkeypatch):
    monkeypatch.setattr(Settings, "SYNC_MODE", "historical_full")

    metadata = ETLMetaData()

    assert metadata.sync_mode == "historical_full"
    assert metadata.full_sync is True


def test_etl_metadata_to_dict_keys(monkeypatch):
    monkeypatch.setattr(Settings, "SYNC_MODE", "incremental")
    metadata = ETLMetaData()
    meta_dict = metadata.to_dict()

    expected_keys = {"run_id", "run_time", "sync_mode", "full_sync", "log_path"}
    assert expected_keys.issubset(set(meta_dict.keys()))


def test_etl_metadata_to_string_format(monkeypatch):
    monkeypatch.setattr(Settings, "SYNC_MODE", "incremental")
    metadata = ETLMetaData()
    string_repr = metadata.to_string()

    assert isinstance(string_repr, str)
    assert f"RunID: {metadata.run_id}" in string_repr
    assert f"SyncMode: {metadata.sync_mode}" in string_repr
