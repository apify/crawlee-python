import pytest

from crawlee import service_locator
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient, SqlStorageClient, StorageClient


@pytest.fixture(params=['memory', 'file_system', 'sql'])
def storage_client(request: pytest.FixtureRequest) -> StorageClient:
    """Parameterized fixture to test with different storage clients."""
    storage_client: StorageClient
    if request.param == 'memory':
        storage_client = MemoryStorageClient()
    elif request.param == 'sql':
        storage_client = SqlStorageClient()
    else:
        storage_client = FileSystemStorageClient()
    service_locator.set_storage_client(storage_client)
    return storage_client
