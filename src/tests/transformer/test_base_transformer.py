import pandas as pd
from typing import Dict
from pydantic import BaseModel

from splash.transformer.base_transformer import BaseTransformer


# Sample Pydantic model for testing
class SampleModel(BaseModel):
    id: int
    name: str


# Dummy transformer extending BaseTransformer to test custom transformation
class DummyTransformer(BaseTransformer[SampleModel]):
    def transform(self, item: Dict) -> Dict:
        # Add a default name if missing
        if "name" not in item:
            item["name"] = "Unknown"
        return item


def test_transform_valid_records():
    data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    transformer = DummyTransformer(data, SampleModel)
    df = transformer.transform_to_df()

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)
    assert list(df.columns) == ["id", "name"]
    assert set(df["name"]) == {"Alice", "Bob"}


def test_transform_with_missing_field():
    data = [{"id": 1}, {"id": 2, "name": "Charlie"}]  # first record missing "name"
    transformer = DummyTransformer(data, SampleModel)
    df = transformer.transform_to_df()

    assert df.shape == (2, 2)
    assert "Unknown" in df["name"].values
    assert "Charlie" in df["name"].values


def test_transform_invalid_record_skipped():
    data = [{"id": 1, "name": "Alice"}, {"id": "invalid", "name": "Bob"}]  # invalid id
    transformer = DummyTransformer(data, SampleModel)
    df = transformer.transform_to_df()

    assert df.shape == (1, 2)
    assert df["id"].tolist() == [1]


def test_deduplication_by_id():
    data = [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"},
        {"id": 1, "name": "Alice Duplicate"},
    ]
    transformer = DummyTransformer(data, SampleModel)
    df = transformer.transform_to_df()

    assert df.shape[0] == 2  # Deduplicated
    assert df["id"].nunique() == 2


def test_no_id_column_handling(caplog):
    # 'id' column missing, should log a warning and return as-is
    class NoIdModel(BaseModel):
        name: str

    data = [{"name": "Alice"}, {"name": "Bob"}]
    transformer = BaseTransformer(data, NoIdModel)
    df = transformer.transform_to_df()

    assert df.shape[0] == 2
    assert "id" not in df.columns
