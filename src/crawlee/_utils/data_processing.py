from __future__ import annotations

import json
from enum import Enum
from typing import TYPE_CHECKING, Any, NoReturn

from crawlee._utils.file import ContentType, is_content_type

if TYPE_CHECKING:
    from crawlee._types import StorageTypes


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


def raise_on_duplicate_storage(client_type: StorageTypes, key_name: str, value: str) -> NoReturn:
    """Raise an error indicating that a storage with the provided key name and value already exists."""
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with {key_name} "{value}" already exists.')


def raise_on_non_existing_storage(client_type: StorageTypes, id: str | None) -> NoReturn:
    """Raise an error indicating that a storage with the provided id does not exist."""
    client_type = maybe_extract_enum_member_value(client_type)
    raise ValueError(f'{client_type} with id "{id}" does not exist.')
