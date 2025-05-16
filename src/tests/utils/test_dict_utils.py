from splash.utils.dict_utils import (
    safe_copy,
    nested_get,
    replace_null,
    stringify_list,
    change_key_name,
    list_to_dict,
    to_list
)


def test_safe_copy_returns_copy_of_dict():
    original = {"a": 1}
    result = safe_copy(original)
    assert result == original
    assert result is not original  # ensure it's a copy


def test_safe_copy_with_none_returns_empty_dict():
    result = safe_copy(None)
    assert result == {}


def test_nested_get_with_existing_keys():
    data = {"a": {"b": {"c": 42}}}
    assert nested_get(data, ["a", "b", "c"]) == 42


def test_nested_get_with_missing_key_returns_default():
    data = {"a": {"b": {}}}
    assert nested_get(data, ["a", "b", "x"], default="missing") == "missing"


def test_replace_null_replaces_none_and_empty():
    item = {"x": None, "y": "", "z": "value"}
    replace_null(item, "x", 1)
    replace_null(item, "y", 2)
    replace_null(item, "z", 3)
    assert item == {"x": 1, "y": 2, "z": "value"}


def test_stringify_list_transforms_list_to_string():
    item = {"roles": ["admin", "editor"]}
    stringify_list(item, "roles")
    assert item["roles"] == "admin, editor"


def test_stringify_list_sets_none_if_not_list_or_empty():
    item1 = {"tags": ""}
    item2 = {"tags": None}
    item3 = {"tags": []}
    stringify_list(item1, "tags")
    stringify_list(item2, "tags")
    stringify_list(item3, "tags")
    assert item1["tags"] is None
    assert item2["tags"] is None
    assert item3["tags"] is None


def test_change_key_name_renames_keys():
    item = {"old_key": 123, "keep": "yes"}
    change_key_name(item, {"old_key": "new_key"})
    assert "new_key" in item
    assert "old_key" not in item
    assert item["new_key"] == 123


def test_list_to_dict_converts_correctly():
    input_list = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    result = list_to_dict(input_list)
    assert result == {"a": [1, 3], "b": [2, 4]}


def test_to_list_with_none_returns_empty():
    assert to_list(None) == []


def test_to_list_with_scalar_wraps_in_list():
    assert to_list("x") == ["x"]


def test_to_list_with_list_returns_same():
    original = [1, 2, 3]
    assert to_list(original) == original
