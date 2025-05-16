import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

from google.cloud.exceptions import NotFound
from splash.loader.bigquery_loader import BigQueryLoader, BigQueryLoaderError


@pytest.fixture
def loader():
    """Fixture to return an instance of BigQueryLoader with mocked BigQuery client."""
    with patch("splash.loader.bigquery_loader.bigquery.Client") as mock_client:
        loader_instance = BigQueryLoader()
        loader_instance.client = mock_client()
        return loader_instance


def test_generate_table_id(loader):
    assert loader.generate_table_id("table") == f"{loader.project}.{loader.dataset}.table"


def test_get_staging_table_name(loader):
    name = loader.get_staging_table_name("table")
    assert name.startswith("_stg_") and name.endswith("table")


def test_ensure_table_exists_creates_if_missing(loader):
    loader.client.get_table.side_effect = NotFound("Table not found")
    loader.client.create_table = MagicMock()

    with patch("splash.loader.bigquery_loader.bigquery.Table"):
        loader.ensure_table_exists("my_table", schema=[])

    loader.client.create_table.assert_called_once()


def test_load_success(loader):
    df = pd.DataFrame({"id": [1, 2]})
    mock_job = MagicMock()
    mock_job.result.return_value = True
    mock_job.output_rows = 2
    loader.client.load_table_from_dataframe.return_value = mock_job

    rows = loader.load("my_table", df, schema=[])
    assert rows == 2


def test_load_empty_df(loader):
    df = pd.DataFrame()
    rows = loader.load("my_table", df)
    assert rows == 0


def test_load_failure(loader):
    df = pd.DataFrame({"id": [1]})
    loader.client.load_table_from_dataframe.side_effect = Exception("Load error")

    with pytest.raises(BigQueryLoaderError):
        loader.load("my_table", df)


def test_merge_success(loader):
    df = pd.DataFrame({"id": [1], "name": ["a"]})
    mock_job = MagicMock()
    mock_job.result.return_value = True
    mock_job.num_dml_affected_rows = 1
    loader.client.query.return_value = mock_job

    with patch("splash.loader.bigquery_loader.generate_merge_sql", return_value="MERGE SQL"):
        rows = loader.merge("src", "dest", df)
        assert rows == 1


def test_merge_failure(loader):
    df = pd.DataFrame({"id": [1]})
    loader.client.query.side_effect = Exception("Merge error")

    with patch("splash.loader.bigquery_loader.generate_merge_sql", return_value="SQL"):
        with pytest.raises(BigQueryLoaderError):
            loader.merge("src", "dest", df)


def test_update_success(loader):
    df = pd.DataFrame({"id": [1]})
    mock_job = MagicMock()
    mock_job.result.return_value = True
    mock_job.num_dml_affected_rows = 3
    loader.client.query.return_value = mock_job

    with patch("splash.loader.bigquery_loader.generate_update_sql", return_value="UPDATE SQL"):
        rows = loader.update("src", "dest", df)
        assert rows == 3


def test_update_failure(loader):
    df = pd.DataFrame({"id": [1]})
    loader.client.query.side_effect = Exception("Update error")

    with patch("splash.loader.bigquery_loader.generate_update_sql", return_value="UPDATE SQL"):
        with pytest.raises(BigQueryLoaderError):
            loader.update("src", "dest", df)


def test_load_and_merge_success(loader):
    df = pd.DataFrame({"id": [1]})

    # Patch methods within BigQueryLoader
    loader.load = MagicMock(return_value=1)
    loader.merge = MagicMock(return_value=1)
    loader.update = MagicMock(return_value=1)
    loader.ensure_table_exists = MagicMock()
    loader.schemas["my_table"] = []

    stats = loader.load_and_merge("my_table", df, is_full_sync=True)
    assert stats == {"loaded": 1, "merged": 1, "deleted": 1}


def test_load_and_merge_missing_schema(loader):
    df = pd.DataFrame({"id": [1]})
    loader.schemas = {}  # No schema

    with pytest.raises(ValueError):
        loader.load_and_merge("my_table", df)
