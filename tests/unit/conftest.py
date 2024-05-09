from __future__ import annotations

from typing import TYPE_CHECKING, Callable

import pytest

from crawlee._utils.env_vars import CrawleeEnvVars
from crawlee.configuration import Configuration
from crawlee.memory_storage_client import MemoryStorageClient
from crawlee.storages import _creation_management

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture()
def reset_default_instances(monkeypatch: pytest.MonkeyPatch) -> Callable[[], None]:
    def reset() -> None:
        monkeypatch.setattr(_creation_management, '_cache_dataset_by_id', {})
        monkeypatch.setattr(_creation_management, '_cache_dataset_by_name', {})
        monkeypatch.setattr(_creation_management, '_cache_kvs_by_id', {})
        monkeypatch.setattr(_creation_management, '_cache_kvs_by_name', {})
        monkeypatch.setattr(_creation_management, '_cache_rq_by_id', {})
        monkeypatch.setattr(_creation_management, '_cache_rq_by_name', {})

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
def memory_storage_client(tmp_path: Path) -> MemoryStorageClient:
    cfg = Configuration(write_metadata=True, persist_storage=True, local_storage_dir=str(tmp_path))
    return MemoryStorageClient(cfg)
