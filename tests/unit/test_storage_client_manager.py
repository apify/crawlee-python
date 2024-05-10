from __future__ import annotations

from unittest.mock import Mock

import pytest

from crawlee.base_storage_client import BaseStorageClient
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.storage_client_manager import StorageClientManager


def test_returns_memory_storage_client_as_default() -> None:
    manager = StorageClientManager()
    storage_client = manager.get_storage_client()
    assert isinstance(storage_client, MemoryStorageClient), 'Should return the memory storage client by default'


def test_returns_provided_local_client_for_non_cloud_environment() -> None:
    local_client = Mock(spec=BaseStorageClient)
    manager = StorageClientManager(local_client=local_client)
    storage_client = manager.get_storage_client()
    assert storage_client == local_client, 'Should return the local client when not in cloud'


def test_returns_provided_cloud_client_for_cloud_environment() -> None:
    cloud_client = Mock(spec=BaseStorageClient)
    manager = StorageClientManager(cloud_client=cloud_client)
    storage_client = manager.get_storage_client(in_cloud=True)
    assert storage_client == cloud_client, 'Should return the cloud client when in cloud'


def test_raises_error_when_no_cloud_client_provided() -> None:
    manager = StorageClientManager()
    with pytest.raises(RuntimeError, match='cloud client was not provided'):
        manager.get_storage_client(in_cloud=True)
