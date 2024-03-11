from __future__ import annotations

import pytest
from apify._memory_storage import MemoryStorageClient


@pytest.fixture()
def memory_storage_client() -> MemoryStorageClient:
    return MemoryStorageClient(write_metadata=True, persist_storage=True)
