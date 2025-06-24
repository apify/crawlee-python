from crawlee._service_locator import service_locator
from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset

# Create custom storage client (using MemoryStorageClient as example).
storage_client = MemoryStorageClient()

# Register it either with the service locator.
service_locator.set_storage_client(storage_client)

# Or pass it directly to the crawler.
crawler = ParselCrawler(storage_client=storage_client)


# Or just provide it when opening a storage (e.g. dataset).
async def example_usage() -> None:
    dataset = await Dataset.open(
        name='my_dataset',
        storage_client=storage_client,
    )
