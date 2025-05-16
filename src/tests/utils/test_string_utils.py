from splash.utils.string_utils import camel_to_snake, get_object_name

def test_camel_to_snake_basic():
    assert camel_to_snake("MyClassName") == "my_class_name"
    assert camel_to_snake("HTTPRequest") == "http_request"
    assert camel_to_snake("CamelCaseString") == "camel_case_string"
    assert camel_to_snake("Already_snake_case") == "already_snake_case"

def test_camel_to_snake_with_numbers():
    assert camel_to_snake("Version1Controller") == "version1_controller"
    assert camel_to_snake("HTML5Parser") == "html5_parser"

def test_get_object_name_regular_class():
    class EventStats:
        pass
    assert get_object_name(EventStats) == "event_stats"

def test_get_object_name_transformer_class():
    class EventStatsTransformer:
        pass
    assert get_object_name(EventStatsTransformer) == "event_stats"

def test_get_object_name_short():
    class A:
        pass
    assert get_object_name(A) == "a"
