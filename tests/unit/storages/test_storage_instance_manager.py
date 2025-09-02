from pathlib import Path

import pytest

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient
from crawlee.storages import Dataset, KeyValueStore


@pytest.fixture(autouse=True)
def clean_storage_instance_manager() -> None:
    """Helper function to clean the storage instance manager before each test."""
    service_locator.storage_instance_manager.clear_cache()


async def test_unique_storage_by_storage_client(tmp_path: Path) -> None:
    config = Configuration(
        purge_on_start=True,
    )
    config.storage_dir = str(tmp_path)

    kvs1 = await KeyValueStore.open(storage_client=MemoryStorageClient(), configuration=config)
    kvs2 = await KeyValueStore.open(storage_client=FileSystemStorageClient(), configuration=config)
    assert kvs1 is not kvs2


async def test_same_storage_when_different_client(tmp_path: Path) -> None:
    config = Configuration(
        purge_on_start=True,
    )
    config.storage_dir = str(tmp_path)

    kvs1 = await KeyValueStore.open(storage_client=MemoryStorageClient(), configuration=config)
    kvs2 = await KeyValueStore.open(storage_client=MemoryStorageClient(), configuration=config)
    assert kvs1 is kvs2


async def test_unique_storage_by_storage_type(tmp_path: Path) -> None:
    config = Configuration(
        purge_on_start=True,
    )
    config.storage_dir = str(tmp_path)
    storage_client = MemoryStorageClient()

    kvs = await KeyValueStore.open(storage_client=storage_client, configuration=config)
    dataset = await Dataset.open(storage_client=storage_client, configuration=config)
    assert kvs is not dataset


async def test_unique_storage_by_name() -> None:
    """Test that StorageInstanceManager support different storage clients at the same time."""
    storage_client = MemoryStorageClient()

    kvs1 = await KeyValueStore.open(storage_client=storage_client, name='kvs1')
    kvs2 = await KeyValueStore.open(storage_client=storage_client, name='kvs2')
    assert kvs1 is not kvs2


async def test_unique_storage_by_unique_cache_key_different_path(tmp_path: Path) -> None:
    """Test that StorageInstanceManager support unique cache key. Difference in storage_dir."""
    path_1 = tmp_path / 'dir1'
    path_2 = tmp_path / 'dir2'
    path_1.mkdir()
    path_2.mkdir()

    config_1 = Configuration()
    config_1.storage_dir = str(path_1)

    config_2 = Configuration()
    config_2.storage_dir = str(path_2)

    storage_client = FileSystemStorageClient()

    kvs1 = await KeyValueStore.open(storage_client=storage_client, configuration=config_1)
    kvs2 = await KeyValueStore.open(storage_client=storage_client, configuration=config_2)
    assert kvs1 is not kvs2


async def test_unique_storage_by_unique_cache_key_same_path(tmp_path: Path) -> None:
    """Test that StorageInstanceManager support unique cache key. Different configs with same storage_dir create same
    storage."""
    config_1 = Configuration()
    config_1.storage_dir = str(tmp_path)

    config_2 = Configuration()
    config_2.storage_dir = str(tmp_path)

    storage_client = FileSystemStorageClient()

    kvs1 = await KeyValueStore.open(storage_client=storage_client, configuration=config_1)
    kvs2 = await KeyValueStore.open(storage_client=storage_client, configuration=config_2)
    assert kvs1 is kvs2


async def test_identical_storage_default_config() -> None:
    """Test that StorageInstanceManager correctly caches storage based on the storage client."""
    storage_client = MemoryStorageClient()

    kvs1 = await KeyValueStore.open(storage_client=storage_client)
    kvs2 = await KeyValueStore.open(storage_client=storage_client)
    assert kvs1 is kvs2


async def test_identical_storage_default_storage() -> None:
    """Test that StorageInstanceManager correctly caches storage based on the storage client."""
    kvs1 = await KeyValueStore.open()
    kvs2 = await KeyValueStore.open()
    assert kvs1 is kvs2


async def test_identical_storage_clear_cache() -> None:
    kvs1 = await KeyValueStore.open()
    service_locator.storage_instance_manager.clear_cache()
    kvs2 = await KeyValueStore.open()
    assert kvs1 is not kvs2


async def test_identical_storage_remove_from_cache() -> None:
    kvs1 = await KeyValueStore.open()
    service_locator.storage_instance_manager.remove_from_cache(kvs1)
    kvs2 = await KeyValueStore.open()
    assert kvs1 is not kvs2
