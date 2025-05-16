import re


def camel_to_snake(text: str) -> str:
    """
    Converts a CamelCase or PascalCase string into snake_case.

    Args:
        text (str): The CamelCase input string.

    Returns:
        str: The converted snake_case string.

    Examples:
        camel_to_snake("MyClassName") -> "my_class_name"
        camel_to_snake("HTTPRequest") -> "http_request"
    """
    text = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', text)
    text = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', text)
    return text.lower()


def get_object_name(obj: type) -> str:
    """
    Converts the name of a class object into a normalized snake_case name,
    stripping the suffix "Transformer" if it exists.

    Args:
        obj (type): A Python class/type object.

    Returns:
        str: The normalized snake_case name for the object.

    Examples:
        class MyCustomTransformer: pass
        get_object_name(MyCustomTransformer) -> "my_custom"

        class EventStats: pass
        get_object_name(EventStats) -> "event_stats"
    """
    name = obj.__name__
    if name.endswith("Transformer"):
        name = name[:-len("Transformer")]
    return camel_to_snake(name)
