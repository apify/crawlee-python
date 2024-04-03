from __future__ import annotations

import io
import json
import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aiofiles
from aiofiles.os import mkdir

from crawlee._utils.file import (
    determine_file_extension,
    force_remove,
    force_rename,
    is_content_type_json,
    is_content_type_text,
    is_content_type_xml,
    is_file_or_bytes,
    json_dumps,
    persist_metadata_if_enabled,
)

if TYPE_CHECKING:
    from pathlib import Path


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


def test_is_content_type_json() -> None:
    assert is_content_type_json('application/json') is True
    assert is_content_type_json('application/json; charset=utf-8') is True
    assert is_content_type_json('text/plain') is False
    assert is_content_type_json('application/xml') is False


def test_is_content_type_xml() -> None:
    assert is_content_type_xml('application/xml') is True
    assert is_content_type_xml('application/xhtml+xml') is True
    assert is_content_type_xml('text/xml; charset=utf-8') is False
    assert is_content_type_xml('application/json') is False


def test_is_content_type_text() -> None:
    assert is_content_type_text('text/plain') is True
    assert is_content_type_text('text/html; charset=utf-8') is True
    assert is_content_type_text('application/json') is False
    assert is_content_type_text('application/xml') is False


def test_determine_file_extension() -> None:
    # Can determine common types properly
    assert determine_file_extension('application/json') == 'json'
    assert determine_file_extension('application/xml') == 'xml'
    assert determine_file_extension('text/plain') == 'txt'

    # Can handle unusual formats
    assert determine_file_extension(' application/json ') == 'json'
    assert determine_file_extension('APPLICATION/JSON') == 'json'
    assert determine_file_extension('application/json;charset=utf-8') == 'json'

    # Returns None for non-existent content types
    assert determine_file_extension('clearly not a content type') is None
    assert determine_file_extension('') is None


async def test_force_remove(tmp_path: Path) -> None:
    test_file_path = os.path.join(tmp_path, 'test.txt')
    # Does not crash/raise when the file does not exist
    assert os.path.exists(test_file_path) is False
    await force_remove(test_file_path)
    assert os.path.exists(test_file_path) is False

    # Removes the file if it exists
    with open(test_file_path, 'a', encoding='utf-8'):  # noqa: ASYNC101
        pass
    assert os.path.exists(test_file_path) is True
    await force_remove(test_file_path)
    assert os.path.exists(test_file_path) is False


async def test_force_rename(tmp_path: Path) -> None:
    src_dir = os.path.join(tmp_path, 'src')
    dst_dir = os.path.join(tmp_path, 'dst')
    src_file = os.path.join(src_dir, 'src_dir.txt')
    dst_file = os.path.join(dst_dir, 'dst_dir.txt')
    # Won't crash if source directory does not exist
    assert os.path.exists(src_dir) is False
    await force_rename(src_dir, dst_dir)

    # Will remove dst_dir if it exists (also covers normal case)
    # Create the src_dir with a file in it
    await mkdir(src_dir)
    with open(src_file, 'a', encoding='utf-8'):  # noqa: ASYNC101
        pass
    # Create the dst_dir with a file in it
    await mkdir(dst_dir)
    with open(dst_file, 'a', encoding='utf-8'):  # noqa: ASYNC101
        pass
    assert os.path.exists(src_file) is True
    assert os.path.exists(dst_file) is True
    await force_rename(src_dir, dst_dir)
    assert os.path.exists(src_dir) is False
    assert os.path.exists(dst_file) is False
    # src_dir.txt should exist in dst_dir
    assert os.path.exists(os.path.join(dst_dir, 'src_dir.txt')) is True


async def test_persist_metadata_skips_when_disabled(tmp_path: Path) -> None:
    await persist_metadata_if_enabled(data={'key': 'value'}, entity_directory=str(tmp_path), write_metadata=False)
    assert not list(tmp_path.iterdir())  # The directory should be empty since write_metadata is False


async def test_persist_metadata_creates_files_and_directories_when_enabled(tmp_path: Path) -> None:
    data = {'key': 'value'}
    entity_directory = os.path.join(tmp_path, 'new_dir')
    await persist_metadata_if_enabled(data=data, entity_directory=entity_directory, write_metadata=True)
    assert os.path.exists(entity_directory)  # Check if directory was created
    assert os.path.isfile(os.path.join(entity_directory, '__metadata__.json'))  # Check if file was created


async def test_persist_metadata_correctly_writes_data(tmp_path: Path) -> None:
    data = {'key': 'value'}
    entity_directory = os.path.join(tmp_path, 'data_dir')
    await persist_metadata_if_enabled(data=data, entity_directory=entity_directory, write_metadata=True)
    metadata_path = os.path.join(entity_directory, '__metadata__.json')
    async with aiofiles.open(metadata_path, 'r') as f:
        content = await f.read()
    assert json.loads(content) == data  # Check if correct data was written
