from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path

import pytest

from crawlee._utils.file import (
    ContentType,
    determine_file_extension,
    force_remove,
    force_rename,
    is_content_type,
    is_file_or_bytes,
    json_dumps,
)


async def test_json_dumps() -> None:
    assert await json_dumps({'key': 'value'}) == '{\n  "key": "value"\n}'
    assert await json_dumps(['one', 2, 3.0]) == '[\n  "one",\n  2,\n  3.0\n]'
    assert await json_dumps('string') == '"string"'
    assert await json_dumps(123) == '123'
    assert await json_dumps(datetime(2022, 1, 1, tzinfo=timezone.utc)) == '"2022-01-01 00:00:00+00:00"'


def test_is_file_or_bytes() -> None:
    assert is_file_or_bytes(b'bytes') is True
    assert is_file_or_bytes(bytearray(b'bytearray')) is True
    assert is_file_or_bytes(io.BytesIO(b'some bytes')) is True
    assert is_file_or_bytes(io.StringIO('string')) is True
    assert is_file_or_bytes('just a regular string') is False
    assert is_file_or_bytes(12345) is False


@pytest.mark.parametrize(
    ('content_type_enum', 'content_type', 'expected_result'),
    [
        (ContentType.JSON, 'application/json', True),
        (ContentType.JSON, 'application/json; charset=utf-8', True),
        (ContentType.JSON, 'text/plain', False),
        (ContentType.JSON, 'application/xml', False),
        (ContentType.XML, 'application/xml', True),
        (ContentType.XML, 'application/xhtml+xml', True),
        (ContentType.XML, 'text/xml; charset=utf-8', False),
        (ContentType.XML, 'application/json', False),
        (ContentType.TEXT, 'text/plain', True),
        (ContentType.TEXT, 'text/html; charset=utf-8', True),
        (ContentType.TEXT, 'application/json', False),
        (ContentType.TEXT, 'application/xml', False),
    ],
    ids=[
        'json_valid_simple',
        'json_valid_charset',
        'json_invalid_text',
        'json_invalid_xml',
        'xml_valid_simple',
        'xml_valid_xhtml',
        'xml_invalid_text_charset',
        'xml_invalid_json',
        'text_valid_plain',
        'text_valid_html_charset',
        'text_invalid_json',
        'text_invalid_xml',
    ],
)
def test_is_content_type(content_type_enum: ContentType, content_type: str, *, expected_result: bool) -> None:
    result = is_content_type(content_type_enum, content_type)
    assert expected_result == result


def test_is_content_type_json() -> None:
    assert is_content_type(ContentType.JSON, 'application/json') is True
    assert is_content_type(ContentType.JSON, 'application/json; charset=utf-8') is True
    assert is_content_type(ContentType.JSON, 'text/plain') is False
    assert is_content_type(ContentType.JSON, 'application/xml') is False


def test_is_content_type_xml() -> None:
    assert is_content_type(ContentType.XML, 'application/xml') is True
    assert is_content_type(ContentType.XML, 'application/xhtml+xml') is True
    assert is_content_type(ContentType.XML, 'text/xml; charset=utf-8') is False
    assert is_content_type(ContentType.XML, 'application/json') is False


def test_is_content_type_text() -> None:
    assert is_content_type(ContentType.TEXT, 'text/plain') is True
    assert is_content_type(ContentType.TEXT, 'text/html; charset=utf-8') is True
    assert is_content_type(ContentType.TEXT, 'application/json') is False
    assert is_content_type(ContentType.TEXT, 'application/xml') is False


def test_determine_file_extension() -> None:
    # Can determine common types properly
    assert determine_file_extension('application/json') == 'json'
    assert determine_file_extension('application/xml') == 'xml'
    assert determine_file_extension('text/plain') == 'txt'

    # Can handle unusual formats
    assert determine_file_extension(' application/json ') == 'json'
    assert determine_file_extension('APPLICATION/JSON') == 'json'
    assert determine_file_extension('application/json;charset=utf-8') == 'json'

    # Return None for non-existent content types
    assert determine_file_extension('clearly not a content type') is None
    assert determine_file_extension('') is None


async def test_force_remove(tmp_path: Path) -> None:
    test_file_path = Path(tmp_path, 'test.txt')
    # Does not crash/raise when the file does not exist
    assert test_file_path.exists() is False
    await force_remove(test_file_path)
    assert test_file_path.exists() is False

    # Remove the file if it exists
    with open(test_file_path, 'a', encoding='utf-8'):  # noqa: ASYNC230
        pass
    assert test_file_path.exists() is True
    await force_remove(test_file_path)
    assert test_file_path.exists() is False


async def test_force_rename(tmp_path: Path) -> None:
    src_dir = Path(tmp_path, 'src')
    dst_dir = Path(tmp_path, 'dst')
    src_file = Path(src_dir, 'src_dir.txt')
    dst_file = Path(dst_dir, 'dst_dir.txt')
    # Won't crash if source directory does not exist
    assert src_dir.exists() is False
    await force_rename(src_dir, dst_dir)

    # Will remove dst_dir if it exists (also covers normal case)
    # Create the src_dir with a file in it
    src_dir.mkdir()
    with open(src_file, 'a', encoding='utf-8'):  # noqa: ASYNC230
        pass
    # Create the dst_dir with a file in it
    dst_dir.mkdir()
    with open(dst_file, 'a', encoding='utf-8'):  # noqa: ASYNC230
        pass
    assert src_file.exists() is True
    assert dst_file.exists() is True
    await force_rename(src_dir, dst_dir)
    assert src_dir.exists() is False
    assert dst_file.exists() is False
    # src_dir.txt should exist in dst_dir
    assert (dst_dir / 'src_dir.txt').exists() is True
