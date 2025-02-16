from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from crawlee.storage_clients import MemoryStorageClient
    from crawlee.storage_clients._memory import DatasetCollectionClient


@pytest.fixture
def datasets_client(memory_storage_client: MemoryStorageClient) -> DatasetCollectionClient:
    return memory_storage_client.datasets()


async def test_get_or_create(datasets_client: DatasetCollectionClient) -> None:
    dataset_name = 'test'
    # A new dataset gets created
    dataset_info = await datasets_client.get_or_create(name=dataset_name)
    assert dataset_info.name == dataset_name

    # Another get_or_create call returns the same dataset
    dataset_info_existing = await datasets_client.get_or_create(name=dataset_name)
    assert dataset_info.id == dataset_info_existing.id
    assert dataset_info.name == dataset_info_existing.name
    assert dataset_info.created_at == dataset_info_existing.created_at


async def test_list(datasets_client: DatasetCollectionClient) -> None:
    dataset_list_1 = await datasets_client.list()
    assert dataset_list_1.count == 0

    dataset_info = await datasets_client.get_or_create(name='dataset')
    dataset_list_2 = await datasets_client.list()

    assert dataset_list_2.count == 1
    assert dataset_list_2.items[0].name == dataset_info.name

    # Test sorting behavior
    newer_dataset_info = await datasets_client.get_or_create(name='newer-dataset')
    dataset_list_sorting = await datasets_client.list()
    assert dataset_list_sorting.count == 2
    assert dataset_list_sorting.items[0].name == dataset_info.name
    assert dataset_list_sorting.items[1].name == newer_dataset_info.name
