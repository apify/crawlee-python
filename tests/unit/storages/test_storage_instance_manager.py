import asyncio
import sys
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock

import pytest

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient
from crawlee.storages import Dataset, KeyValueStore, RequestQueue
from crawlee.storages._base import Storage


@pytest.fixture(autouse=True)
def clean_storage_instance_manager() -> None:
    """Helper function to clean the storage instance manager before each test."""
    service_locator.storage_instance_manager.clear_cache()


@pytest.fixture(params=[KeyValueStore, Dataset, RequestQueue])
def storage_type(request: pytest.FixtureRequest) -> type[Storage]:
    return cast('type[Storage]', request.param)


async def test_unique_storage_by_storage_client(tmp_path: Path, storage_type: type[Storage]) -> None:
    config = Configuration(purge_on_start=True, storage_dir=str(tmp_path))

    storage_1 = await storage_type.open(storage_client=MemoryStorageClient(), configuration=config)
    storage_2 = await storage_type.open(storage_client=FileSystemStorageClient(), configuration=config)
    assert storage_1 is not storage_2


async def test_same_storage_when_different_client(tmp_path: Path, storage_type: type[Storage]) -> None:
    config = Configuration(purge_on_start=True, storage_dir=str(tmp_path))

    storage_1 = await storage_type.open(storage_client=MemoryStorageClient(), configuration=config)
    storage_2 = await storage_type.open(storage_client=MemoryStorageClient(), configuration=config)
    assert storage_1 is storage_2


async def test_unique_storage_by_storage_type(tmp_path: Path) -> None:
    config = Configuration(purge_on_start=True, storage_dir=str(tmp_path))
    storage_client = MemoryStorageClient()

    kvs = await KeyValueStore.open(storage_client=storage_client, configuration=config)
    dataset = await Dataset.open(storage_client=storage_client, configuration=config)
    assert kvs is not dataset


async def test_unique_storage_by_name(storage_type: type[Storage]) -> None:
    """Test that StorageInstanceManager support different storage clients at the same time."""
    storage_client = MemoryStorageClient()

    storage_1 = await storage_type.open(storage_client=storage_client, name='kvs1')
    storage_2 = await storage_type.open(storage_client=storage_client, name='kvs2')
    assert storage_1 is not storage_2


async def test_unique_storage_by_unique_cache_key_different_path(tmp_path: Path, storage_type: type[Storage]) -> None:
    """Test that StorageInstanceManager support unique cache key. Difference in storage_dir."""
    path_1 = tmp_path / 'dir1'
    path_2 = tmp_path / 'dir2'
    path_1.mkdir()
    path_2.mkdir()

    config_1 = Configuration(storage_dir=str(path_1))

    config_2 = Configuration(storage_dir=str(path_2))

    storage_client = FileSystemStorageClient()

    storage_1 = await storage_type.open(storage_client=storage_client, configuration=config_1)
    storage_2 = await storage_type.open(storage_client=storage_client, configuration=config_2)
    assert storage_1 is not storage_2


async def test_unique_storage_by_unique_cache_key_same_path(tmp_path: Path, storage_type: type[Storage]) -> None:
    """Test that StorageInstanceManager support unique cache key. Different configs with same storage_dir create same
    storage."""
    config_1 = Configuration(storage_dir=str(tmp_path))

    config_2 = Configuration(storage_dir=str(tmp_path))

    storage_client = FileSystemStorageClient()

    storage_1 = await storage_type.open(storage_client=storage_client, configuration=config_1)
    storage_2 = await storage_type.open(storage_client=storage_client, configuration=config_2)
    assert storage_1 is storage_2


async def test_identical_storage_default_config(storage_type: type[Storage]) -> None:
    """Test that StorageInstanceManager correctly caches storage based on the storage client."""
    storage_client = MemoryStorageClient()

    storage_1 = await storage_type.open(storage_client=storage_client)
    storage_2 = await storage_type.open(storage_client=storage_client)
    assert storage_1 is storage_2


async def test_identical_storage_default_storage(storage_type: type[Storage]) -> None:
    """Test that StorageInstanceManager correctly caches storage based on the storage client."""
    storage_1 = await storage_type.open()
    storage_2 = await storage_type.open()
    assert storage_1 is storage_2


async def test_identical_storage_clear_cache(storage_type: type[Storage]) -> None:
    storage_1 = await storage_type.open()
    service_locator.storage_instance_manager.clear_cache()
    storage_2 = await storage_type.open()
    assert storage_1 is not storage_2


async def test_identical_storage_remove_from_cache(storage_type: type[Storage]) -> None:
    storage_1 = await storage_type.open()
    service_locator.storage_instance_manager.remove_from_cache(storage_1)
    storage_2 = await storage_type.open()
    assert storage_1 is not storage_2


async def test_preexisting_unnamed_storage_open_by_id(storage_type: type[Storage]) -> None:
    """Test that persisted pre-existing unnamed storage can be opened by ID."""
    storage_client = FileSystemStorageClient()
    storage_1 = await storage_type.open(alias='custom_name', storage_client=storage_client)

    # Make service_locator unaware of this storage
    service_locator.storage_instance_manager.clear_cache()

    storage_1_again = await storage_type.open(id=storage_1.id, storage_client=storage_client)

    assert storage_1.id == storage_1_again.id


@pytest.mark.skipif(sys.version_info[:3] < (3, 11), reason='asyncio.Barrier was introduced in Python 3.11.')
async def test_concurrent_open_datasets() -> None:
    """Test that concurrent open datasets with the same name return the same instance."""
    from asyncio import Barrier  # type:ignore[attr-defined] # noqa: PLC0415

    barrier = Barrier(2)

    async def push_data(data: dict) -> None:
        await barrier.wait()
        dataset = await Dataset.open(name='concurrent-storage')
        await dataset.push_data(data)

    await asyncio.gather(
        push_data({'test_1': '1'}),
        push_data({'test_2': '2'}),
    )

    dataset = await Dataset.open(name='concurrent-storage')

    items = await dataset.get_data()
    assert len(items.items) == 2

    await dataset.drop()


@pytest.mark.skipif(sys.version_info[:3] < (3, 11), reason='asyncio.Barrier was introduced in Python 3.11.')
async def test_concurrent_open_datasets_with_same_name_and_alias() -> None:
    """Test that concurrent open requests for the same storage return the same instance."""
    from asyncio import Barrier  # type:ignore[attr-defined] # noqa: PLC0415

    valid_kwargs: dict[str, str | None] = {}

    exception_calls = AsyncMock()

    barrier = Barrier(2)

    async def open_dataset(name: str | None, alias: str | None) -> None:
        await barrier.wait()
        try:
            await Dataset.open(name=name, alias=alias)
            valid_kwargs['name'] = name
            valid_kwargs['alias'] = alias
        except ValueError:
            exception_calls()

    await asyncio.gather(
        open_dataset(name=None, alias='concurrent-storage'),
        open_dataset(name='concurrent-storage', alias=None),
    )

    # Ensure that a ValueError was raised due to name/alias conflict
    exception_calls.assert_called_once()

    dataset = await Dataset.open(name=valid_kwargs.get('name'), alias=valid_kwargs.get('alias'))

    await dataset.drop()
