import pytest
from splash.utils.job_utils import _thread_local, get_etl_job_statuses, add_job_stats, generate_job_status
from splash.metadata.etl_metadata import ETLMetaData

@pytest.fixture
def dummy_metadata():
    return ETLMetaData()

def test_get_etl_job_statuses_thread_local():
    _thread_local.etl_job_statuses = []
    statuses = get_etl_job_statuses()
    assert isinstance(statuses, list)
    assert statuses == []

def test_add_job_stats_merges_correctly():
    agg = {"loaded": 5, "merged": 3, "deleted": 1}
    new = {"loaded": 2, "merged": 4, "deleted": 0}
    result = add_job_stats(agg, new)
    assert result == {"loaded": 7, "merged": 7, "deleted": 1}

def test_add_job_stats_handles_missing_keys():
    agg = {"loaded": 10}
    new = {"merged": 2}
    result = add_job_stats(agg, new)
    assert result == {"loaded": 10, "merged": 2, "deleted": 0}

def test_generate_job_status_creates_expected_format(dummy_metadata):
    stats = {"loaded": 100, "merged": 50, "deleted": 10}
    status = generate_job_status(
        metadata=dummy_metadata,
        source="splash",
        obj_name="event",
        status="success",
        job_stats=stats
    )

    assert status["run_id"] == dummy_metadata.run_id
    assert status["sync_mode"] == dummy_metadata.sync_mode
    assert status["object"] == "event"
    assert status["records_loaded"] == 100
    assert "timestamp" in status
    assert status["log_path"].startswith("gs://")
