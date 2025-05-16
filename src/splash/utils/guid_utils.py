from typing import Dict, Any, List, Optional
from uuid import NAMESPACE_OID, uuid5


def generate_hashed_guid(
    data: Dict[str, Any], 
    keys: List[str], 
    namespace: Optional[str] = None
) -> str:
    """
    Generate a deterministic UUIDv5 based on selected values from a dictionary.

    This function constructs a GUID by:
    - Extracting and joining the values corresponding to the provided `keys`.
    - Using a provided `namespace` (optional) or falling back to UUID's default NAMESPACE_OID.
    - Hashing the combined string using UUIDv5 for consistent GUID generation.

    Args:
        data (Dict[str, Any]): Dictionary containing the source values.
        keys (List[str]): List of keys to extract from the dictionary.
        namespace (Optional[str]): Optional string namespace for UUIDv5. 
            If not provided, uses NAMESPACE_OID.

    Returns:
        str: A deterministic UUID string.

    Raises:
        ValueError: If all specified keys are missing or empty in the input `data`.

    Example:
        >>> generate_hashed_guid({"id": 123, "email": "test@example.com"}, ["id", "email"])
        '84e5a4fc-bf10-51d6-a7db-04a5b70cf9c5'
    """
    custom_namespace = uuid5(NAMESPACE_OID, namespace.upper()) if namespace else NAMESPACE_OID
    identifiers = [str(data.get(key, "")).strip() for key in keys]
    combined = "-".join(identifiers)
    
    if not any(identifiers):
        raise ValueError("All keys missing from input data - cannot generate GUID.")
    
    return str(uuid5(custom_namespace, combined))
