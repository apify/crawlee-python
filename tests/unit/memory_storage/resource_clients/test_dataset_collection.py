from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from apify._memory_storage import MemoryStorageClient
    from apify._memory_storage.resource_clients import DatasetCollectionClient


@pytest.fixture()
def datasets_client(memory_storage_client: MemoryStorageClient) -> DatasetCollectionClient:
    return memory_storage_client.datasets()


async def test_get_or_create(datasets_client: DatasetCollectionClient) -> None:
    dataset_name = 'test'
    # A new dataset gets created
    dataset_info = await datasets_client.get_or_create(name=dataset_name)
    assert dataset_info['name'] == dataset_name

    # Another get_or_create call returns the same dataset
    dataset_info_existing = await datasets_client.get_or_create(name=dataset_name)
    assert dataset_info['id'] == dataset_info_existing['id']
    assert dataset_info['name'] == dataset_info_existing['name']
    assert dataset_info['createdAt'] == dataset_info_existing['createdAt']


async def test_list(datasets_client: DatasetCollectionClient) -> None:
    assert (await datasets_client.list()).count == 0
    dataset_info = await datasets_client.get_or_create(name='dataset')
    dataset_list = await datasets_client.list()
    assert dataset_list.count == 1
    assert dataset_list.items[0]['name'] == dataset_info['name']

    # Test sorting behavior
    newer_dataset_info = await datasets_client.get_or_create(name='newer-dataset')
    dataset_list_sorting = await datasets_client.list()
    assert dataset_list_sorting.count == 2
    assert dataset_list_sorting.items[0]['name'] == dataset_info['name']
    assert dataset_list_sorting.items[1]['name'] == newer_dataset_info['name']
