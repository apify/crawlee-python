from __future__ import annotations

import json
from pathlib import Path

from crawlee._consts import METADATA_FILENAME
from crawlee.storage_clients._memory._creation_management import persist_metadata_if_enabled


async def test_persist_metadata_skips_when_disabled(tmp_path: Path) -> None:
    await persist_metadata_if_enabled(data={'key': 'value'}, entity_directory=str(tmp_path), write_metadata=False)
    assert not list(tmp_path.iterdir())  # The directory should be empty since write_metadata is False


async def test_persist_metadata_creates_files_and_directories_when_enabled(tmp_path: Path) -> None:
    data = {'key': 'value'}
    entity_directory = Path(tmp_path, 'new_dir')
    await persist_metadata_if_enabled(data=data, entity_directory=str(entity_directory), write_metadata=True)
    assert entity_directory.exists() is True  # Check if directory was created
    assert (entity_directory / METADATA_FILENAME).is_file()  # Check if file was created


async def test_persist_metadata_correctly_writes_data(tmp_path: Path) -> None:
    data = {'key': 'value'}
    entity_directory = Path(tmp_path, 'data_dir')
    await persist_metadata_if_enabled(data=data, entity_directory=str(entity_directory), write_metadata=True)
    metadata_path = entity_directory / METADATA_FILENAME
    with open(metadata_path) as f:  # noqa: ASYNC230
        content = f.read()
    assert json.loads(content) == data  # Check if correct data was written
