from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import pytest

from crawlee._utils.env_vars import CrawleeEnvVars
from crawlee.memory_storage import MemoryStorageClient
from crawlee.storages.key_value_store import KeyValueStore
from crawlee.storages.storage_client_manager import StorageClientManager

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture()
def reset_default_instances(monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
    def reset() -> None:
        # monkeypatch.setattr(Dataset, '_cache_by_id', None)
        # monkeypatch.setattr(Dataset, '_cache_by_name', None)
        monkeypatch.setattr(KeyValueStore, '_cache_by_id', None)
        monkeypatch.setattr(KeyValueStore, '_cache_by_name', None)
        # monkeypatch.setattr(RequestQueue, '_cache_by_id', None)
        # monkeypatch.setattr(RequestQueue, '_cache_by_name', None)
        monkeypatch.setattr(StorageClientManager, '_default_instance', None)

    return reset


# To isolate the tests, we need to reset the used singletons before each test case
# We also set the MemoryStorageClient to use a temp path
@pytest.fixture(autouse=True)
def _reset_and_patch_default_instances(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    reset_default_instances: Callable[[], None],
) -> None:
    reset_default_instances()

    # This forces the MemoryStorageClient to use tmp_path for its storage dir
    monkeypatch.setenv(CrawleeEnvVars.LOCAL_STORAGE_DIR, str(tmp_path))


@pytest.fixture()
def memory_storage_client() -> MemoryStorageClient:
    return MemoryStorageClient(write_metadata=True, persist_storage=True)
