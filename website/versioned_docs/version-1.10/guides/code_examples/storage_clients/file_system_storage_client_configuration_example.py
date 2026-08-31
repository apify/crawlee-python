from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import FileSystemStorageClient

# Create a new instance of storage client.
storage_client = FileSystemStorageClient()

# Create a configuration with custom settings.
configuration = Configuration(
    storage_dir='./my_storage',
    purge_on_start=False,
)

# And pass them to the crawler.
crawler = ParselCrawler(
    storage_client=storage_client,
    configuration=configuration,
)
