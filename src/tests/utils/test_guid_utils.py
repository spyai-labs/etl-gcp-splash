import pytest
from uuid import UUID

from splash.utils.guid_utils import generate_hashed_guid

def test_generate_guid_default_namespace():
    data = {"id": 123, "email": "test@example.com"}
    keys = ["id", "email"]
    guid = generate_hashed_guid(data, keys)
    
    assert isinstance(guid, str)
    assert str(UUID(guid)) == guid  # Valid UUID format
    assert guid == generate_hashed_guid(data, keys)  # Deterministic output

def test_generate_guid_custom_namespace():
    data = {"key1": "abc", "key2": "xyz"}
    keys = ["key1", "key2"]
    guid1 = generate_hashed_guid(data, keys, namespace="Custom")
    guid2 = generate_hashed_guid(data, keys, namespace="custom")  # Same uppercased
    assert guid1 == guid2
    assert isinstance(UUID(guid1), UUID)

def test_generate_guid_missing_keys_should_fail():
    data = {"unrelated": "value"}
    keys = ["id", "email"]
    with pytest.raises(ValueError, match="All keys missing from input data"):
        generate_hashed_guid(data, keys)

def test_generate_guid_partial_keys():
    data = {"id": "123"}
    keys = ["id", "email"]
    guid = generate_hashed_guid(data, keys)
    assert isinstance(UUID(guid), UUID)
    assert guid == generate_hashed_guid(data, keys)  # Still deterministic

def test_generate_guid_ignores_whitespace():
    data = {"id": "  123  ", "email": "  test@example.com "}
    keys = ["id", "email"]
    guid = generate_hashed_guid(data, keys)
    assert isinstance(UUID(guid), UUID)
