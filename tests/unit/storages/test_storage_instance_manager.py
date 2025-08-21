from pathlib import Path

from crawlee.configuration import Configuration
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient
from crawlee.storages import Dataset, KeyValueStore


async def test_unique_storage_by_storage_client(tmp_path: Path) -> None:
    config = Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )

    kvs1 = await KeyValueStore.open(storage_client=MemoryStorageClient(), configuration=config)

    kvs2 = await KeyValueStore.open(
        storage_client=FileSystemStorageClient(),
        configuration=config,
    )
    assert kvs1 is not kvs2


async def test_unique_storage_by_storage_type(tmp_path: Path) -> None:
    config = Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )

    kvs = await KeyValueStore.open(configuration=config)
    dataset = await Dataset.open(configuration=config)
    assert kvs is not dataset


async def test_unique_storage_by_name(tmp_path: Path) -> None:
    """Test that StorageInstanceManager support different storage clients at the same time."""
    config = Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )

    kvs1 = await KeyValueStore.open(configuration=config, name='kvs1')
    kvs2 = await KeyValueStore.open(storage_client=FileSystemStorageClient(), configuration=config, name='kvs2')
    assert kvs1 is not kvs2


async def test_identical_storage(tmp_path: Path) -> None:
    """Test that StorageInstanceManager correctly caches storage based on the storage client."""
    config = Configuration(
        crawlee_storage_dir=str(tmp_path),  # type: ignore[call-arg]
        purge_on_start=True,
    )

    kvs1 = await KeyValueStore.open(storage_client=MemoryStorageClient(), configuration=config)

    kvs2 = await KeyValueStore.open(
        storage_client=MemoryStorageClient(),
        configuration=config,
    )
    assert kvs1 is kvs2
