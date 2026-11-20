from __future__ import annotations

from typing import Any

import pytest

from crawlee.storages._utils import parse_value, serialize_value


@pytest.mark.parametrize(
    ('value', 'content_type', 'expected_bytes', 'expected_content_type'),
    [
        pytest.param(b'\x00\x01', None, b'\x00\x01', 'application/octet-stream', id='bytes'),
        pytest.param(b'\x00\x01', 'image/png', b'\x00\x01', 'image/png', id='bytes with content type'),
        pytest.param('žluť', None, 'žluť'.encode(), 'text/plain; charset=utf-8', id='str'),
        pytest.param(
            'žluť',
            'text/html; charset=windows-1250',
            'žluť'.encode('windows-1250'),
            'text/html; charset=windows-1250',
            id='str with charset',
        ),
        pytest.param(
            {'key': 'žluť'},
            None,
            '{\n  "key": "žluť"\n}'.encode(),
            'application/json; charset=utf-8',
            id='object',
        ),
        pytest.param(
            [1, 2],
            'Application/JSON',
            b'[\n  1,\n  2\n]',
            'Application/JSON',
            id='object with JSON content type',
        ),
        pytest.param(
            {'key': 'žluť'},
            'application/json; charset=utf-16',
            '{\n  "key": "žluť"\n}'.encode('utf-16'),
            'application/json; charset=utf-16',
            id='object with charset',
        ),
    ],
)
async def test_serialize_value(
    value: Any, content_type: str | None, expected_bytes: bytes, expected_content_type: str
) -> None:
    """A value is serialized to bytes by its type, and its content type is inferred when it isn't given."""
    serialized_value, resolved_content_type = await serialize_value(value, content_type)

    assert serialized_value == expected_bytes
    assert resolved_content_type == expected_content_type


async def test_serialize_value_rejects_unknown_charset() -> None:
    """A `str` value can't be encoded with a charset Python doesn't know."""
    with pytest.raises(LookupError, match='no-such-charset'):
        await serialize_value('text', 'text/plain; charset=no-such-charset')


async def test_serialize_value_rejects_unencodable_text() -> None:
    """A `str` value can't be stored with a charset that can't encode its characters."""
    with pytest.raises(UnicodeEncodeError):
        await serialize_value('žluť', 'text/plain; charset=ascii')


@pytest.mark.parametrize(
    ('value', 'content_type', 'expected_value'),
    [
        pytest.param(b'{"key": [1, 2]}', 'application/json', {'key': [1, 2]}, id='JSON'),
        pytest.param(b'null', 'APPLICATION/JSON; charset=UTF-8', None, id='JSON null with parameters'),
        pytest.param(
            '{"key": "žluť"}'.encode('utf-16'),
            'application/json; charset=utf-16',
            {'key': 'žluť'},
            id='JSON with charset',
        ),
        pytest.param('žluť'.encode(), 'text/plain', 'žluť', id='text'),
        pytest.param('žluť'.encode('utf-16'), 'text/html; charset="utf-16"', 'žluť', id='text with charset'),
        pytest.param(b'<feed/>', 'application/atom+xml', '<feed/>', id='XML'),
        pytest.param(b'\x00\x01', 'image/png', b'\x00\x01', id='binary'),
        pytest.param(b'\xff', 'text/plain; charset=no-such-charset', b'\xff', id='unknown charset'),
        pytest.param(b'text \xff', 'text/plain', 'text \ufffd', id='undecodable bytes'),
        pytest.param(b'', 'text/plain; charset=no-such-charset', b'', id='empty value with unknown charset'),
        pytest.param(b'aGk=', 'text/plain; charset=base64', b'aGk=', id='charset that is not a text encoding'),
    ],
)
def test_parse_value(value: bytes, content_type: str, expected_value: Any) -> None:
    """JSON is parsed, text and XML are decoded, and other content types are returned as bytes."""
    assert parse_value(value, content_type) == expected_value
