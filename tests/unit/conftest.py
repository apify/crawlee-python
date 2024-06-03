from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

from crawlee.configuration import Configuration
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.storage_client_manager import StorageClientManager
from crawlee.storages import _creation_management

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(autouse=True)
def _isolate_test_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Isolate tests by resetting the storage clients, clearing caches, and setting the environment variables.

    The fixture is applied automatically to all test cases.

    Args:
        monkeypatch: Test utility provided by pytest.
        tmp_path: A unique temporary directory path provided by pytest for test isolation.
    """
    # Set the environment variable for the local storage directory to the temporary path
    monkeypatch.setenv('CRAWLEE_LOCAL_STORAGE_DIR', str(tmp_path))

    # Reset the local and cloud clients in StorageClientManager
    StorageClientManager._local_client = MemoryStorageClient()
    StorageClientManager._cloud_client = None

    # Clear creation-related caches to ensure no state is carried over between tests
    monkeypatch.setattr(_creation_management, '_cache_dataset_by_id', {})
    monkeypatch.setattr(_creation_management, '_cache_dataset_by_name', {})
    monkeypatch.setattr(_creation_management, '_cache_kvs_by_id', {})
    monkeypatch.setattr(_creation_management, '_cache_kvs_by_name', {})
    monkeypatch.setattr(_creation_management, '_cache_rq_by_id', {})
    monkeypatch.setattr(_creation_management, '_cache_rq_by_name', {})

    # Verify that the environment variable is set correctly
    assert os.environ.get('CRAWLEE_LOCAL_STORAGE_DIR') == str(tmp_path)


@pytest.fixture()
def memory_storage_client(tmp_path: Path) -> MemoryStorageClient:
    cfg = Configuration(
        write_metadata=True,
        persist_storage=True,
        crawlee_local_storage_dir=str(tmp_path),  # type: ignore
    )
    return MemoryStorageClient(cfg)


@pytest.fixture()
def httpbin() -> str:
    return os.environ.get('HTTPBIN_URL', 'https://httpbin.org')
