from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, NoReturn

from crawlee._utils.file import ContentType, is_content_type

if TYPE_CHECKING:
    from crawlee.storages.types import StorageTypes


def filter_out_none_values_recursively(dictionary: dict, *, remove_empty_dicts: bool = False) -> dict | None:
    """Recursively filters out None values from a dictionary.

    Args:
        dictionary: The dictionary to filter.
        remove_empty_dicts: Flag indicating whether to remove empty nested dictionaries.

    Returns:
        A copy of the dictionary with all None values (and potentially empty dictionaries) removed.
    """
    result = {}
    for k, v in dictionary.items():
        # If the value is a dictionary, apply recursion
        if isinstance(v, dict):
            nested = filter_out_none_values_recursively(v, remove_empty_dicts=remove_empty_dicts)
            if nested or not remove_empty_dicts:
                result[k] = nested
        elif v is not None:
            result[k] = v

    # If removing empty dictionaries and result is empty, return None
    if remove_empty_dicts and not result:
        return None
    return result


def maybe_extract_enum_member_value(maybe_enum_member: Any) -> Any:
    """Extract the value of an enumeration member if it is an Enum, otherwise return the original value."""
    if isinstance(maybe_enum_member, Enum):
        return maybe_enum_member.value
    return maybe_enum_member


def maybe_parse_body(body: bytes, content_type: str) -> Any:
    """Parse the response body based on the content type."""
    if is_content_type(ContentType.JSON, content_type):
        s = body.decode('utf-8')
        return json.loads(s)

    if is_content_type(ContentType.XML, content_type) or is_content_type(ContentType.TEXT, content_type):
        return body.decode('utf-8')

    return body


def maybe_parse_bool(val: str | None) -> bool:
    """Parse a string value to a boolean."""
    if val in {'true', 'True', '1'}:
        return True
    return False


def maybe_parse_datetime(val: str) -> datetime | str:
    """Parse a string value to a datetime object."""
    try:
        return datetime.strptime(val, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    except ValueError:
        return val


def maybe_parse_float(val: str) -> float | None:
    """Parse a string value to a float."""
    try:
        return float(val)
    except ValueError:
        return None


def maybe_parse_int(val: str) -> int | None:
    """Parse a string value to an integer."""
    try:
        return int(val)
    except ValueError:
        return None


def raise_on_duplicate_storage(client_type: StorageTypes, key_name: str, value: str) -> NoReturn:
    """Raise an error indicating that a storage with the provided key name and value already exists."""
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with {key_name} "{value}" already exists.')


def raise_on_non_existing_storage(client_type: StorageTypes, id_: str | None) -> NoReturn:
    """Raise an error indicating that a storage with the provided id does not exist."""
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with id "{id_}" does not exist.')
