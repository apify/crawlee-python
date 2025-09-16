import pytest

from crawlee import service_locator
from crawlee.storage_clients import FileSystemStorageClient, MemoryStorageClient, StorageClient


@pytest.fixture(params=['memory', 'file_system'])
def storage_client(request: pytest.FixtureRequest) -> StorageClient:
    """Parameterized fixture to test with different storage clients."""
    storage_client: StorageClient
    storage_client = MemoryStorageClient() if request.param == 'memory' else FileSystemStorageClient()
    service_locator.set_storage_client(storage_client)
    return storage_client
