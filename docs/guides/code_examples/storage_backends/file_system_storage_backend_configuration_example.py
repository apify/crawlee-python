from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import FileSystemStorageBackend

# Create a new instance of storage backend.
storage_backend = FileSystemStorageBackend()

# Create a configuration with custom settings.
configuration = Configuration(
    storage_dir='./my_storage',
    purge_on_start=False,
)

# And pass them to the crawler.
crawler = ParselCrawler(
    storage_backend=storage_backend,
    configuration=configuration,
)
