from google.cloud import bigquery
from pydantic import BaseModel
from typing import Optional, List, Dict

from splash.utils.schema_utils import (
    get_bq_type,
    generate_schema_from_class,
    add_system_generated_schema,
    prioritize_schema_fields_by_index,
    classify_group,
    group_schemas,
    format_schema_field
)


class MockModel(BaseModel):
    id: int
    name: str
    active: Optional[bool] = None


def test_get_bq_type_mapping():
    assert get_bq_type(str) == "STRING"
    assert get_bq_type(int) == "INTEGER"
    assert get_bq_type(bool) == "BOOLEAN"
    assert get_bq_type(float) == "FLOAT"


def test_generate_schema_from_class():
    schema = generate_schema_from_class(MockModel)
    assert isinstance(schema, list)
    assert len(schema) == 3
    assert schema[0].name == "id"
    assert schema[0].field_type == "INTEGER"
    assert schema[1].name == "name"
    assert schema[2].name == "active"


def test_add_system_generated_schema():
    schema = generate_schema_from_class(MockModel)
    enriched = add_system_generated_schema(schema)
    field_names = [f.name for f in enriched]
    assert "_sync_time" in field_names
    assert "_deleted" in field_names


def test_prioritize_schema_fields_by_index():
    schema = generate_schema_from_class(MockModel)
    prioritized = prioritize_schema_fields_by_index(schema, {"name": 0, "id": 2})
    assert prioritized[0].name == "name"
    assert prioritized[2].name == "id"


def test_classify_group():
    assert classify_group("group_contact_event") == "group_contact"
    assert classify_group("event_log") == "event"
    assert classify_group("ticket_order") == "ticket"
    assert classify_group("misc_table") == "general"


def test_group_schemas():
    mock_schema: List[bigquery.SchemaField] = [bigquery.SchemaField("id", "INTEGER")]
    input_dict: Dict[str, List[bigquery.SchemaField]] = {
        "event_main": mock_schema,
        "group_contact_log": mock_schema,
        "ticket_sales": mock_schema,
        "other": mock_schema,
    }
    grouped = group_schemas(input_dict)
    assert "event" in grouped
    assert "group_contact" in grouped
    assert "ticket" in grouped
    assert "general" in grouped


def test_format_schema_field():
    field = bigquery.SchemaField("id", "INTEGER", mode="NULLABLE")
    formatted = format_schema_field(field)
    assert formatted == "bigquery.SchemaField('id', 'INTEGER', mode='NULLABLE')"
