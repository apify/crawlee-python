from __future__ import annotations

import codecs
import json
import re
from typing import Any

from crawlee._utils.file import infer_mime_type, json_dumps, parse_content_type_charset

NAME_REGEX = re.compile(r'^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9-]*[a-zA-Z0-9])$')


def validate_storage_name(name: str | None) -> None:
    if name and not NAME_REGEX.match(name):
        raise ValueError(
            f'Invalid storage name "{name}". Name can only contain letters "a" through "z", the digits "0" through'
            '"9", and the hyphen ("-") but only in the middle of the string (e.g. "my-value-1")'
        )


def validate_storage_alias(alias: str | None) -> None:
    """Validate a storage alias that is used as an on-disk subdirectory name.

    Unlike storage names, aliases may contain underscores and dots (e.g. the reserved `__default__` alias),
    but must not contain path separators or parent-directory references, so an alias always maps to a single
    directory under the storage directory.
    """
    if alias is None:
        return

    if not alias or '/' in alias or '\\' in alias or '\x00' in alias or alias in {'.', '..'}:
        raise ValueError(
            f'Invalid storage alias "{alias}". Alias must not be empty, contain path separators ("/", "\\") or '
            f'null bytes, or be a directory reference (".", "..").'
        )


_JSON_MEDIA_TYPE = 'application/json'

_TEXT_MEDIA_TYPE_PATTERN = re.compile(r'text/.*|application/.*xml')

_V1_NONE_MEDIA_TYPE = 'application/x-none'
"""The content type Crawlee v1 stored a `None` value under, instead of deleting the record."""


async def serialize_value(value: Any, content_type: str | None) -> tuple[bytes, str]:
    """Serialize a value into the bytes a KVS backend stores, and resolve its content type.

    Bytes-like and string values are stored as they are, under the given content type. Any other value is stored as
    JSON. Without a content type, bytes-like values get `application/octet-stream`, strings `text/plain` and other
    values `application/json`.

    Raises:
        TypeError: If the value is neither a string nor bytes-like, and the content type isn't JSON.
    """
    content_type = content_type or infer_mime_type(value)

    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value), content_type

    charset = parse_content_type_charset(content_type) or 'utf-8'

    if isinstance(value, str):
        return value.encode(charset), content_type

    if _get_media_type(content_type) != _JSON_MEDIA_TYPE:
        raise TypeError(
            f'A value of type {type(value).__name__} can only be stored as JSON, but the content type is '
            f'"{content_type}". Pass the value as `str` or `bytes`, or use a JSON content type.'
        )

    serialized_value = await json_dumps(value)
    return serialized_value.encode(charset), content_type


def parse_value(value: bytes, content_type: str) -> Any:
    """Parse the bytes read from a KVS backend according to their content type.

    JSON, text and XML are decoded with the charset of the content type, replacing the bytes it can't decode. JSON is
    then parsed, and text and XML are returned as a string. A record that Crawlee v1 stored for a `None` value is
    read as `None`. A value with any other content type, or with a charset Python can't decode text with, is returned
    as bytes.

    Raises:
        ValueError: If the value has a JSON content type but isn't valid JSON.
    """
    media_type = _get_media_type(content_type)
    if media_type == _V1_NONE_MEDIA_TYPE:
        return None

    is_json = media_type == _JSON_MEDIA_TYPE

    if not is_json and not _TEXT_MEDIA_TYPE_PATTERN.fullmatch(media_type):
        return value

    charset = parse_content_type_charset(content_type) or 'utf-8'
    try:
        # `bytes.decode` doesn't look the charset up for an empty value.
        codec_name = codecs.lookup(charset).name
        # A UTF-8 byte order mark isn't part of a JSON document.
        if is_json and codec_name == 'utf-8':
            charset = 'utf-8-sig'
        text = value.decode(charset, errors='replace')
    except LookupError:
        return value

    return json.loads(text) if is_json else text


def _get_media_type(content_type: str) -> str:
    return content_type.partition(';')[0].strip().lower()
