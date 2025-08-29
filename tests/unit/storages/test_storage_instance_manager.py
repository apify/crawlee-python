from pathlib import Path

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient
from crawlee.storages import Dataset, KeyValueStore


async def test_unique_storage_by_storage_client(tmp_path: Path) -> None:
    config = Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )

    kvs1 = await KeyValueStore.open(storage_client=MemoryStorageClient(configuration=config))
    kvs2 = await KeyValueStore.open(storage_client=FileSystemStorageClient(configuration=config))
    assert kvs1 is not kvs2


async def test_unique_storage_by_storage_client_of_same_type(tmp_path: Path) -> None:
    config = Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )

    kvs1 = await KeyValueStore.open(storage_client=MemoryStorageClient(configuration=config))
    kvs2 = await KeyValueStore.open(storage_client=MemoryStorageClient(configuration=config))
    assert kvs1 is not kvs2


async def test_unique_storage_by_storage_type(tmp_path: Path) -> None:
    config = Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )
    storage_client = MemoryStorageClient(configuration=config)

    kvs = await KeyValueStore.open(storage_client=storage_client)
    dataset = await Dataset.open(storage_client=storage_client)
    assert kvs is not dataset


async def test_unique_storage_by_name(tmp_path: Path) -> None:
    """Test that StorageInstanceManager support different storage clients at the same time."""
    config = Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )
    storage_client = FileSystemStorageClient(configuration=config)

    kvs1 = await KeyValueStore.open(storage_client=storage_client, name='kvs1')
    kvs2 = await KeyValueStore.open(storage_client=storage_client, name='kvs2')
    assert kvs1 is not kvs2


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
