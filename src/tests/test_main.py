import pytest
import pandas as pd
from unittest import mock
from unittest.mock import MagicMock
from datetime import datetime

from splash.metadata import ETLMetaData
from main import (
    ETL_UTILS,
    add_system_defaults,
    transform_data,
    load_and_merge_objects,
    run_etl, 
    main
)


@pytest.fixture
def fake_metadata():
    return ETLMetaData()


def test_add_system_defaults():
    df = pd.DataFrame({"id": [1, 2]})
    now = datetime(2025, 5, 15, 12, 0, 0)
    result = add_system_defaults(df, now)
    assert '_sync_time' in result.columns
    assert '_deleted' in result.columns
    assert not result['_deleted'].all()


def test_transform_data_applies_transformer(fake_metadata):
    class DummyTransformer:
        def __init__(self, data):
            self.data = data

        def transform_to_df(self):
            return pd.DataFrame(self.data)

    data_dict = {"rsvps": [{"id": 1}, {"id": 2}]}
    transformer_map = {"rsvps": [DummyTransformer]}

    result = transform_data(fake_metadata, data_dict, transformer_map)

    assert "dummy" in result
    assert isinstance(result["dummy"], pd.DataFrame)
    assert len(result["dummy"]) == 2


def test_transform_data_handles_failure(fake_metadata, caplog):
    # Ensure we capture logs from the 'main' logger used in main.py
    caplog.set_level("ERROR", logger="main")

    class BrokenTransformer:
        def __init__(self, data): pass
        def transform_to_df(self): raise ValueError("bad input")

    data_dict = {"rsvps": [{"id": 1}]}
    transformer_map = {"rsvps": [BrokenTransformer]}

    result = transform_data(fake_metadata, data_dict, transformer_map)

    assert result == {}
    assert any("Transformation failed" in record.getMessage() for record in caplog.records if record.name == "main")



def test_load_and_merge_objects_success(fake_metadata):
    df = pd.DataFrame({"id": [1, 2]})
    objects = {"event": df}

    mock_loader = MagicMock()
    mock_loader.load_and_merge.return_value = {'loaded': 2, 'merged': 0, 'deleted': 0}

    stats = load_and_merge_objects(fake_metadata, "event", objects, mock_loader)
    assert stats == {'loaded': 2, 'merged': 0, 'deleted': 0}
    mock_loader.load_and_merge.assert_called_once()


def test_load_and_merge_objects_failure(fake_metadata, caplog):
    caplog.set_level("ERROR", logger="main")

    df = pd.DataFrame({"id": [1]})
    objects = {"event": df}

    mock_loader = MagicMock()
    mock_loader.load_and_merge.side_effect = Exception("DB error")

    stats = load_and_merge_objects(fake_metadata, "event", objects, mock_loader)

    assert stats == {'loaded': 0, 'merged': 0, 'deleted': 0}
    assert any("Load and merge failed" in record.getMessage() for record in caplog.records if record.name == "main")


def test_run_etl_success(monkeypatch, fake_metadata):
    mock_extractor = MagicMock()
    mock_extractor().extract.return_value = {"event": [{"id": 1}]}
    
    mock_loader = MagicMock()
    mock_loader.load_and_merge.return_value = {'loaded': 1, 'merged': 0, 'deleted': 0}

    monkeypatch.setitem(
        ETL_UTILS,
        'event',
        {'extractor': mock_extractor, 'transformer_map': {'event': [lambda x: MagicMock(transform_to_df=lambda: pd.DataFrame(x))]}}
    )

    run_etl(fake_metadata, 'event', mock_loader)
    mock_loader.load_and_merge.assert_called_once()


def test_run_etl_invalid_source(fake_metadata):
    mock_loader = MagicMock()
    with pytest.raises(ValueError):
        run_etl(fake_metadata, 'invalid_source', mock_loader)


@mock.patch("main.ETL_UTILS", new_callable=lambda: {
    "event": {
        "extractor": lambda sync_mode: mock.MagicMock(extract=mock.MagicMock(return_value={"events": [{"id": 1}]})),
        "transformer_map": {"events": [lambda x: mock.MagicMock(transform_to_df=lambda: pd.DataFrame([{"id": 1}]))]}
    },
    "group_contact": {
        "extractor": lambda sync_mode: mock.MagicMock(extract=mock.MagicMock(return_value={"group_contacts": [{"id": 2}]})),
        "transformer_map": {"group_contacts": [lambda x: mock.MagicMock(transform_to_df=lambda: pd.DataFrame([{"id": 2}]))]}
    }
})
@mock.patch("main.Settings.SPLASH_ETL_SOURCES", "event,group_contact")
@mock.patch("main.Settings.SYNC_MODE", "incremental")
@mock.patch("main.Settings.ENABLE_GCS_LOGS", False)
@mock.patch("main.Settings.ENABLE_BQ_LOGS", False)
@mock.patch("main.ETLMetaData")
@mock.patch("main.BigQueryLoader")
@mock.patch("main.get_etl_job_statuses", return_value=[])
@mock.patch("main.generate_job_status")
@mock.patch("main.load_and_merge_objects", return_value={"loaded": 1, "merged": 1, "deleted": 0})
@mock.patch("main.transform_data", return_value={"events": pd.DataFrame([{"id": 1}]), "group_contacts": pd.DataFrame([{"id": 2}])})
def test_main_runs_successfully(
    mock_transform,
    mock_load_merge,
    mock_generate_status,
    mock_get_statuses,
    mock_loader,
    mock_metadata,
    mock_etl_utils,
):
    # Run main()
    main()

    # Verify transform and load/merge were called for both sources
    assert mock_transform.called
    assert mock_load_merge.called
    assert mock_generate_status.call_count >= 2  # One for each source
