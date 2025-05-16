import os
import importlib
import inspect
from typing import Any, Dict, List, Type, TextIO, Literal, Union, get_type_hints
from collections import defaultdict

from google.cloud import bigquery
from pydantic import BaseModel

from splash.defined_types import BigQuerySchema
from splash.utils.string_utils import get_object_name

"""
Utility functions for generating and managing BigQuery schema definitions
from Pydantic models across different domain modules (event, ticket, group_contact, general).

Includes:
- Mapping Python types to BigQuery types
- Generating schemas from models
- Writing grouped schema definitions to .py files
- Classifying table groups
- Prioritizing schema field order
"""

# Define the model modules to scan
MODEL_MODULES: List[str] = [
    "splash.model.event_model",
    "splash.model.ticket_model",
    "splash.model.group_contact_model",
    "splash.model.general_model",
]

# Map Python types to BigQuery field types
BQ_TYPE_MAP: Dict[str, str] = {
    "str": "STRING",
    "int": "INTEGER",
    "float": "FLOAT",
    "bool": "BOOLEAN",
    "datetime": "TIMESTAMP",
    "date": "DATE",
}

# System Generated Fields
SYSTEM_FIELDS: Dict[str, str] = {
    "_sync_time": "TIMESTAMP",
    "_deleted": "BOOLEAN",
}

# Field priority map
FIELD_PRIORITY: Dict[str, int] = {
    'id': 0,
    'source_id': 1
}

IMPORTS = ["from typing import Dict, List\n", "from google.cloud import bigquery\n\n"]
TABLE_HEADER = "TABLE_SCHEMAS: Dict[str, List[bigquery.SchemaField]] = {\n"


def get_bq_type(py_type: Type[Any]) -> str:
    """
    Maps a Python type to its corresponding BigQuery field type.

    Args:
        py_type (Type[Any]): The Python type to convert.

    Returns:
        str: Corresponding BigQuery field type (e.g., STRING, INTEGER).
    """
    return BQ_TYPE_MAP.get(py_type.__name__, "STRING")


def generate_schema_from_class(cls: Any) -> List[bigquery.SchemaField]:
    """
    Converts a Pydantic model or TypedDict-like class into a list of BigQuery SchemaField objects.

    Args:
        cls (Any): The model class to convert.

    Returns:
        List[bigquery.SchemaField]: BigQuery schema representation of the class fields.
    """
    schema: List[bigquery.SchemaField] = []
    
    if not isinstance(cls, type):
        raise TypeError(f"Expected a class, got {type(cls).__name__}")
    
    if not issubclass(cls, BaseModel) and not hasattr(cls, "__annotations__"):
        raise TypeError(f"{cls.__name__} must be a subclass of BaseModel or a TypedDict-like class")
    
    # Adding SchemaField for object fields
    for name, annotation in get_type_hints(cls).items():
        base_type = annotation
        
        # Handle Literal types (e.g., Literal["a", "b"])
        if getattr(annotation, '__origin__', None) is Literal:
            base_type = str
        
        # Handle Optional[T]
        elif getattr(annotation, '__origin__', None) is Union:
            args = annotation.__args__
            if len(args) == 2 and type(None) in args:
                base_type = args[0] if args[1] is type(None) else args[1]
        
        schema.append(bigquery.SchemaField(name=name, field_type=get_bq_type(base_type)))

    return schema


def add_system_generated_schema(schema: List[bigquery.SchemaField]) -> List[bigquery.SchemaField]:
    """
    Appends predefined system-level fields (e.g., `_sync_time`, `_deleted`) to the schema.

    Args:
        schema (List[bigquery.SchemaField]): The original schema.

    Returns:
        List[bigquery.SchemaField]: Schema with system fields appended.
    """
    for name, bq_type in SYSTEM_FIELDS.items():
        schema.append(bigquery.SchemaField(name=name, field_type=bq_type))
    return schema


def prioritize_schema_fields_by_index(
    schema: List[bigquery.SchemaField],
    priority_map: Dict[str, int]
) -> List[bigquery.SchemaField]:
    """
    Reorders schema fields based on priority indices provided in a map.

    Args:
        schema (List[bigquery.SchemaField]): List of fields to reorder.
        priority_map (Dict[str, int]): Field name to index position mapping.

    Returns:
        List[bigquery.SchemaField]: Reordered schema.
    """
    total_fields = len(schema)
    output: List[bigquery.SchemaField] = [None] * total_fields  # type: ignore
    used_names = set()

    # Place prioritized fields at or after the requested index
    for field in sorted(schema, key=lambda f: priority_map.get(f.name, float('inf'))):
        if field.name in priority_map:
            target = priority_map[field.name]
            pos = min(target, total_fields - 1)
            while pos < len(output) and output[pos] is not None:
                pos += 1
            if pos >= len(output):
                output.append(field)
            else:
                output[pos] = field
            used_names.add(field.name)

    # Place remaining fields in first available slots
    insert_idx = 0
    for field in schema:
        if field.name in used_names:
            continue
        while insert_idx < len(output) and output[insert_idx] is not None:
            insert_idx += 1
        if insert_idx >= len(output):
            output.append(field)
        else:
            output[insert_idx] = field

    return output

        
def load_model_classes() -> Dict[str, Type[BaseModel]]:
    """
    Dynamically loads and returns all BaseModel subclasses from predefined model modules.

    Returns:
        Dict[str, Type[BaseModel]]: Mapping of table names to Pydantic model classes.
    """
    models: Dict[str, Type[BaseModel]] = {}
    for module_path in MODEL_MODULES:
        module = importlib.import_module(module_path)
        for name, obj in inspect.getmembers(module):
            if isinstance(obj, type) and issubclass(obj, BaseModel) and name.lower() != 'basemodel':
                table_name = get_object_name(obj)
                models[table_name] = obj
    return models


def generate_all_table_schemas() -> Dict[str, List[bigquery.SchemaField]]:
    """
    Generates BigQuery schemas for all model classes including system fields and prioritized ordering.

    Returns:
        Dict[str, List[bigquery.SchemaField]]: Mapping of table names to schema lists.
    """
    models = load_model_classes()
    table_schemas: Dict[str, List[bigquery.SchemaField]] = {}
    for table_name, cls in models.items():
        schema = generate_schema_from_class(cls)
        schema = add_system_generated_schema(schema)
        schema = prioritize_schema_fields_by_index(schema, priority_map=FIELD_PRIORITY)
        table_schemas[table_name] = schema
    return table_schemas


def classify_group(table_name: str) -> str:
    """
    Categorizes a table name into a logical group based on keywords.

    Args:
        table_name (str): The table name.

    Returns:
        str: Group name (e.g., "event", "ticket", "group_contact", "general").
    """
    for keyword in ("group_contact", "event", "ticket"):
        if keyword in table_name:
            return keyword
    return "general"


def group_schemas(
    schema_dict: Dict[str, List[bigquery.SchemaField]]
) -> Dict[str, Dict[str, List[Any]]]:
    """
    Groups schema definitions by category (event, ticket, etc.).

    Args:
        schema_dict (Dict[str, List[bigquery.SchemaField]]): Mapping of table names to schema fields.

    Returns:
        Dict[str, Dict[str, List[Any]]]: Nested dictionary grouped by category.
    """
    grouped: Dict[str, Dict[str, List[Any]]] = defaultdict(lambda: {"table": [], "schema": []})

    for table_name, schema in schema_dict.items():
        group = classify_group(table_name)
        grouped[group]["table"].append(table_name)
        grouped[group]["schema"].append(schema)

    return dict(grouped)


def write_lines_to_handlers(lines: List[str], handlers: List[TextIO]) -> None:
    """
    Writes lines of text to multiple file handlers.

    Args:
        lines (List[str]): Lines of text to write.
        handlers (List[TextIO]): List of open file handles.
    """
    for handler in handlers:
        handler.writelines(lines)


def format_schema_field(field: bigquery.SchemaField) -> str:
    """
    Converts a BigQuery SchemaField into a string suitable for Python source code.

    Args:
        field (bigquery.SchemaField): The schema field to format.

    Returns:
        str: Python string representation of the field.
    """
    return f"bigquery.SchemaField('{field.name}', '{field.field_type}', mode='{field.mode}')"


def write_schema_file(grouped_schemas: Dict[str, Dict[str, List[Any]]], output_dir: str = "splash/schema") -> None:
    """
    Writes grouped BigQuery schema definitions to Python files under the provided directory.
    Generates both group-specific and consolidated `all.py` schema files.
    
    Args:
        grouped_schemas (Dict[str, Dict[str, List[Any]]]): Grouped schema definitions.
        output_dir (str): Directory to output schema files. Defaults to "splash/schema".
    """
    os.makedirs(output_dir, exist_ok=True)
    all_file_path = os.path.join(output_dir, "all.py")
    all_lines = IMPORTS + [TABLE_HEADER]

    for group, data in grouped_schemas.items():
        group_file_path = os.path.join(output_dir, f"{group}.py")
        group_lines = IMPORTS + [TABLE_HEADER]

        for table, schema in zip(data['table'], data['schema']):
            lines = [f"    '{table}': [\n"]
            lines += [f"        {format_schema_field(field)},\n" for field in schema]
            lines += ["    ],\n"]
            group_lines.extend(lines)
            all_lines.extend(lines)

        group_lines.append("}\n")
        with open(group_file_path, "w") as f:
            f.writelines(group_lines)

    all_lines.append("}\n")
    with open(all_file_path, "w") as f:
        f.writelines(all_lines)

        
def generate_schema_file() -> Dict[str, List[bigquery.SchemaField]]:
    """
    Generates grouped schema files and writes them to disk.

    Returns:
        Dict[str, List[bigquery.SchemaField]]: Mapping of table names to BigQuery schema lists.
    """
    schemas = generate_all_table_schemas()
    grouped_schemas = group_schemas(schemas)
    write_schema_file(grouped_schemas)
    return schemas

    
def fetch_schemas(generate_new: bool = False) -> Dict[str, BigQuerySchema]:
    """
    Loads table schemas from splash.schema.all if available.
    Falls back to generating schema files if module is missing or `generate_new` is True.
    
    Args:
        generate_new (bool): Whether to regenerate schema definitions.

    Returns:
        Dict[str, BigQuerySchema]: Dictionary mapping table names to their schema.
    """
    if generate_new:
        return generate_schema_file()

    try:
        from splash.schema.all import TABLE_SCHEMAS
        return TABLE_SCHEMAS
        
    except ModuleNotFoundError:
        # Fallback: generate schemas dynamically
        return generate_schema_file()

        
if __name__ == "__main__":
    print("Generating schema files...")
    generate_schema_file()
    print("Schema files written to splash/schema/")
