from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, NoReturn

from crawlee._utils.file import is_content_type_json, is_content_type_text, is_content_type_xml

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
    if is_content_type_json(content_type):
        return json.loads(body.decode('utf-8'))
    if is_content_type_xml(content_type) or is_content_type_text(content_type):
        return body.decode('utf-8')
    return body


def maybe_parse_bool(val: str | None) -> bool:
    if val in {'true', 'True', '1'}:
        return True
    return False


def maybe_parse_datetime(val: str) -> datetime | str:
    try:
        return datetime.strptime(val, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    except ValueError:
        return val


def maybe_parse_float(val: str) -> float | None:
    try:
        return float(val)
    except ValueError:
        return None


def maybe_parse_int(val: str) -> int | None:
    try:
        return int(val)
    except ValueError:
        return None


def raise_on_duplicate_storage(client_type: StorageTypes, key_name: str, value: str) -> NoReturn:
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with {key_name} "{value}" already exists.')


def raise_on_non_existing_storage(client_type: StorageTypes, id_: str | None) -> NoReturn:
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with id "{id_}" does not exist.')
