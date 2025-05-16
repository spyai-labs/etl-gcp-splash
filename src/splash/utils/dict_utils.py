from typing import Any, Dict, List, Optional, Union


# Util method to safely copy dictionary if dictionary exists is not None
def safe_copy(obj: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return obj.copy() if isinstance(obj, dict) else {}


# Util method to find a nested dictionary item. Nested keys provided as list
def nested_get(d: Dict[str, Any], keys: List[str], default: Optional[Any] = None) -> Any:
    current: Any = d
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return default
    return current if current is not None else default


# Transform helper util to replace null values with a specific value
def replace_null(item: Dict[str, Any], key: str, null_val: Any) -> None:
    raw_val = item.get(key)
    if raw_val is None or raw_val == '':
        item.update({key: null_val})

        
# Transform helper util to convert list of values into a string
def stringify_list(item: Dict[str, Any], key: str, delimiter: str = ", ") -> None:
    list_vals = item.get(key, [])
    if not isinstance(list_vals, list) or not list_vals:
        item[key] = None
    else:
        item[key] = delimiter.join(str(v) for v in list_vals)

        
# Transform helper util to change key names of a dictionary
def change_key_name(item: Dict[str, Any], change_map: Dict[str, str]) -> None:
    for old_key, new_key in change_map.items():
        if old_key in item:
            item[new_key] = item.get(old_key)
            item.pop(old_key, None)

            
# Util to convert list of dictionaries into a dictionary of lists
def list_to_dict(list_of_dicts: List[Dict[str, Any]]) -> Dict[str, List[Any]]:
    dict_of_lists: Dict[str, List[Any]] = {}
    for dictionary in list_of_dicts:
        for key, value in dictionary.items():
            if key in dict_of_lists:
                dict_of_lists[key].append(value)
            else:
                dict_of_lists[key] = [value]
    return dict_of_lists

    
# Ensure item is a list
def to_list(item: Union[Any, List[Any]]) -> List[Any]:
    if item is None:
        return []
    return [item] if not isinstance(item, list) else item
