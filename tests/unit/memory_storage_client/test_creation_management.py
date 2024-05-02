from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING

import aiofiles

from crawlee.memory_storage_client._creation_management import persist_metadata_if_enabled

if TYPE_CHECKING:
    from pathlib import Path


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
