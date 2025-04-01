from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.storage_clients._memory import DatasetClient


@pytest.fixture
async def dataset_client(memory_storage_client: MemoryStorageClient) -> DatasetClient:
    datasets_client = memory_storage_client.datasets()
    dataset_info = await datasets_client.get_or_create(name='test')
    return memory_storage_client.dataset(dataset_info.id)


async def test_nonexistent(memory_storage_client: MemoryStorageClient) -> None:
    dataset_client = memory_storage_client.dataset(id='nonexistent-id')
    assert await dataset_client.get() is None
    with pytest.raises(ValueError, match='Dataset with id "nonexistent-id" does not exist.'):
        await dataset_client.update(name='test-update')

    with pytest.raises(ValueError, match='Dataset with id "nonexistent-id" does not exist.'):
        await dataset_client.list_items()

    with pytest.raises(ValueError, match='Dataset with id "nonexistent-id" does not exist.'):
        await dataset_client.push_items([{'abc': 123}])
    await dataset_client.delete()


async def test_not_implemented(dataset_client: DatasetClient) -> None:
    with pytest.raises(NotImplementedError, match='This method is not supported in memory storage.'):
        await dataset_client.stream_items()
    with pytest.raises(NotImplementedError, match='This method is not supported in memory storage.'):
        await dataset_client.get_items_as_bytes()


async def test_get(dataset_client: DatasetClient) -> None:
    await asyncio.sleep(0.1)
    info = await dataset_client.get()
    assert info is not None
    assert info.id == dataset_client.id
    assert info.accessed_at != info.created_at


async def test_update(dataset_client: DatasetClient) -> None:
    new_dataset_name = 'test-update'
    await dataset_client.push_items({'abc': 123})

    old_dataset_info = await dataset_client.get()
    assert old_dataset_info is not None
    old_dataset_directory = Path(dataset_client._memory_storage_client.datasets_directory, old_dataset_info.name or '')
    new_dataset_directory = Path(dataset_client._memory_storage_client.datasets_directory, new_dataset_name)
    assert (old_dataset_directory / '000000001.json').exists() is True
    assert (new_dataset_directory / '000000001.json').exists() is False

    await asyncio.sleep(0.1)
    updated_dataset_info = await dataset_client.update(name=new_dataset_name)
    assert (old_dataset_directory / '000000001.json').exists() is False
    assert (new_dataset_directory / '000000001.json').exists() is True
    # Only modified_at and accessed_at should be different
    assert old_dataset_info.created_at == updated_dataset_info.created_at
    assert old_dataset_info.modified_at != updated_dataset_info.modified_at
    assert old_dataset_info.accessed_at != updated_dataset_info.accessed_at

    # Should fail with the same name
    with pytest.raises(ValueError, match='Dataset with name "test-update" already exists.'):
        await dataset_client.update(name=new_dataset_name)


async def test_delete(dataset_client: DatasetClient) -> None:
    await dataset_client.push_items({'abc': 123})
    dataset_info = await dataset_client.get()
    assert dataset_info is not None
    dataset_directory = Path(dataset_client._memory_storage_client.datasets_directory, dataset_info.name or '')
    assert (dataset_directory / '000000001.json').exists() is True
    await dataset_client.delete()
    assert (dataset_directory / '000000001.json').exists() is False
    # Does not crash when called again
    await dataset_client.delete()


async def test_push_items(dataset_client: DatasetClient) -> None:
    await dataset_client.push_items('{"test": "JSON from a string"}')
    await dataset_client.push_items({'abc': {'def': {'ghi': '123'}}})
    await dataset_client.push_items(['{"test-json-parse": "JSON from a string"}' for _ in range(10)])
    await dataset_client.push_items([{'test-dict': i} for i in range(10)])

    list_page = await dataset_client.list_items()
    assert list_page.items[0]['test'] == 'JSON from a string'
    assert list_page.items[1]['abc']['def']['ghi'] == '123'
    assert list_page.items[11]['test-json-parse'] == 'JSON from a string'
    assert list_page.items[21]['test-dict'] == 9
    assert list_page.count == 22


async def test_list_items(dataset_client: DatasetClient) -> None:
    item_count = 100
    used_offset = 10
    used_limit = 50
    await dataset_client.push_items([{'id': i} for i in range(item_count)])
    # Test without any parameters
    list_default = await dataset_client.list_items()
    assert list_default.count == item_count
    assert list_default.offset == 0
    assert list_default.items[0]['id'] == 0
    assert list_default.desc is False
    # Test offset
    list_offset_10 = await dataset_client.list_items(offset=used_offset)
    assert list_offset_10.count == item_count - used_offset
    assert list_offset_10.offset == used_offset
    assert list_offset_10.total == item_count
    assert list_offset_10.items[0]['id'] == used_offset
    # Test limit
    list_limit_50 = await dataset_client.list_items(limit=used_limit)
    assert list_limit_50.count == used_limit
    assert list_limit_50.limit == used_limit
    assert list_limit_50.total == item_count
    # Test desc
    list_desc_true = await dataset_client.list_items(desc=True)
    assert list_desc_true.items[0]['id'] == 99
    assert list_desc_true.desc is True


async def test_iterate_items(dataset_client: DatasetClient) -> None:
    item_count = 100
    await dataset_client.push_items([{'id': i} for i in range(item_count)])
    actual_items = []
    async for item in dataset_client.iterate_items():
        assert 'id' in item
        actual_items.append(item)
    assert len(actual_items) == item_count
    assert actual_items[0]['id'] == 0
    assert actual_items[99]['id'] == 99


async def test_reuse_dataset(dataset_client: DatasetClient, memory_storage_client: MemoryStorageClient) -> None:
    item_count = 10
    await dataset_client.push_items([{'id': i} for i in range(item_count)])

    memory_storage_client.datasets_handled = []  # purge datasets loaded to test create_dataset_from_directory
    datasets_client = memory_storage_client.datasets()
    dataset_info = await datasets_client.get_or_create(name='test')
    assert dataset_info.item_count == item_count
