from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import FileSystemStorageClient

# Create configuration with custom settings.
configuration = Configuration(
    storage_dir='./my_storage',
    purge_on_start=False,
)

storage_client = FileSystemStorageClient()

crawler = ParselCrawler(
    storage_client=storage_client,
    configuration=configuration,
)
