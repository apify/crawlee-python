from __future__ import annotations

import json
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, NoReturn, cast

from crawlee._utils.file import is_content_type_json, is_content_type_text, is_content_type_xml

if TYPE_CHECKING:
    from crawlee.storages.types import StorageTypes


def filter_out_none_values_recursively(dictionary: dict) -> dict:
    """Return copy of the dictionary, recursively omitting all keys for which values are None."""
    return cast(dict, filter_out_none_values_recursively_internal(dictionary))


def filter_out_none_values_recursively_internal(
    dictionary: dict,
    remove_empty_dicts: bool | None = None,
) -> dict | None:
    """Recursively filters out None values from a dictionary.

    Unfortunately, it's necessary to have an internal function for the correct result typing,
    without having to create complicated overloads
    """
    result = {}
    for k, v in dictionary.items():
        if isinstance(v, dict):
            v = filter_out_none_values_recursively_internal(v, remove_empty_dicts is True or remove_empty_dicts is None)  # noqa: PLW2901
        if v is not None:
            result[k] = v
    if not result and remove_empty_dicts:
        return None
    return result


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


def maybe_extract_enum_member_value(maybe_enum_member: Any) -> Any:
    """Extract the value of an enumeration member if it is an Enum, otherwise return the original value."""
    if isinstance(maybe_enum_member, Enum):
        return maybe_enum_member.value
    return maybe_enum_member


def maybe_parse_body(body: bytes, content_type: str) -> Any:
    if is_content_type_json(content_type):
        return json.loads(body.decode('utf-8'))  # Returns any
    if is_content_type_xml(content_type) or is_content_type_text(content_type):
        return body.decode('utf-8')
    return body


def raise_on_non_existing_storage(client_type: StorageTypes, id_: str | None) -> NoReturn:
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with id "{id_}" does not exist.')


def raise_on_duplicate_storage(client_type: StorageTypes, key_name: str, value: str) -> NoReturn:
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with {key_name} "{value}" already exists.')
