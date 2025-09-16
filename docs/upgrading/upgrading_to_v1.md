---
id: upgrading-to-v1
title: Upgrading to v1
---

This page summarizes the breaking changes between Crawlee for Python v0.6 and v1.0.

## Terminology change: "browser" in different contexts

The word "browser" is now used distinctly in two contexts:

- **Playwright context** - Refers to Playwright-supported browsers (`chromium`, `firefox`, `webkit`, `edge`).
- **Fingerprinting context** - Refers to browsers supported by fingerprint generation (`chrome`, `firefox`, `safari`, `edge`).

The type of `HeaderGeneratorOptions.browsers` has changed accordingly:

**Before (v0.6):**

```python
from crawlee.fingerprint_suite import HeaderGeneratorOptions

HeaderGeneratorOptions(browsers=['chromium'])
HeaderGeneratorOptions(browsers=['webkit'])
```

**Now (v1.0):**

```python
from crawlee.fingerprint_suite import HeaderGeneratorOptions

HeaderGeneratorOptions(browsers=['chrome'])
HeaderGeneratorOptions(browsers=['safari'])
```

## New default HTTP client

Crawlee v1.0 now uses `ImpitHttpClient` (based on [impit](https://apify.github.io/impit/) library) as the **default HTTP client**, replacing `HttpxHttpClient` (based on [httpx](https://www.python-httpx.org/) library).

If you want to keep using `HttpxHttpClient`, install Crawlee with `httpx` extra, e.g. using pip:

```bash
pip install 'crawlee[httpx]'
```

And then provide the HTTP client explicitly to the crawler:

```python
from crawlee.crawlers import HttpCrawler
from crawlee.http_clients import HttpxHttpClient

client = HttpxHttpClient()
crawler = HttpCrawler(http_client=client)
```

See the [HTTP clients guide](https://crawlee.dev/python/docs/guides/http-clients) for all options.

## Changes in storages

In Crawlee v1.0, the `Dataset`, `KeyValueStore`, and `RequestQueue` storage APIs have been updated for consistency and simplicity. Below is a detailed overview of what's new, what's changed, and what's been removed.

See the [Storages guide](https://crawlee.dev/python/docs/guides/storages) for more details.

### Dataset

The `Dataset` API now includes several new methods, such as:

- `get_metadata` - retrieves metadata information for the dataset.
- `purge` - completely clears the dataset, including all items (keeps the metadata only).
- `list_items` - returns the dataset's items in a list format.

Some older methods have been removed or replaced:

- `from_storage_object` constructor has been removed. You should now use the `open` method with either a `name` or `id` parameter.
- `get_info` method and the `storage_object` property have been replaced by the new `get_metadata` method.
- `set_metadata` method has been removed.
- `write_to_json` and `write_to_csv` methods have been removed; instead, use the `export_to` method for exporting data in different formats.

### Key-value store

The `KeyValueStore` API now includes several new methods, such as:

- `get_metadata` - retrieves metadata information for the key-value store.
- `purge` - completely clears the key-value store, removing all keys and values (keeps the metadata only).
- `delete_value` - deletes a specific key and its associated value.
- `list_keys` - lists all keys in the key-value store.

Some older methods have been removed or replaced:

- `from_storage_object` - removed; use the `open` method with either a `name` or `id` instead.
- `get_info` and `storage_object` - replaced by the new `get_metadata` method.
- `set_metadata` method has been removed.

### Request queue

The `RequestQueue` API now includes several new methods, such as:

- `get_metadata` - retrieves metadata information for the request queue.
- `purge` - completely clears the request queue, including all pending and processed requests (keeps the metadata only).
- `add_requests` - replaces the previous `add_requests_batched` method, offering the same functionality under a simpler name.

Some older methods have been removed or replaced:

- `from_storage_object` - removed; use the `open` method with either a `name` or `id` instead.
- `get_info` and `storage_object` - replaced by the new `get_metadata` method.
- `get_request` has argument `unique_key` instead of `request_id` as the `id` field was removed from the `Request`.
- `set_metadata` method has been removed.

Some changes in the related model classes:

- `resource_directory` in `RequestQueueMetadata` - removed; use the corresponding `path_to_*` property instead.
- `stats` field in `RequestQueueMetadata` - removed as it was unused.
- `RequestQueueHead` - replaced by `RequestQueueHeadWithLocks`.

## New architecture of storage clients

In v1.0, the storage client system has been completely reworked to simplify implementation and make custom storage clients easier to write.

See the [Storage clients guide](https://crawlee.dev/python/docs/guides/storage-clients) for more details.

### New dedicated storage clients

Previously, `MemoryStorageClient` handled both in-memory storage and optional file system persistence. This has now been split into two distinct storage clients:

- **`MemoryStorageClient`** - Stores all data in memory only.
- **`FileSystemStorageClient`** - Persists data on the file system, with in-memory caching for better performance.

**Before (v0.6):**

```python
from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient

# In-memory only
configuration = Configuration(persist_storage=False)
storage_client = MemoryStorageClient.from_config(configuration)

# File-system persistence
configuration = Configuration(persist_storage=True)
storage_client = MemoryStorageClient.from_config(configuration)
```

**Now (v1.0):**

```python
from crawlee.storage_clients import MemoryStorageClient, FileSystemStorageClient

# In-memory only
storage_client = MemoryStorageClient()

# File-system persistence
storage_client = FileSystemStorageClient()
```

### Registering a storage client

The way you register a storage client remains unchanged:

```python
from crawlee import service_locator
from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset

# Create custom storage client
storage_client = MemoryStorageClient()

# Then register it globally
service_locator.set_storage_client(storage_client)

# Or use it for a single crawler only
crawler = ParselCrawler(storage_client=storage_client)

# Or use it for a single storage only
dataset = await Dataset.open(
    name='my_dataset',
    storage_client=storage_client,
)
```

### Instance caching

Instance caching of `Dataset.open`, `KeyValueStore.open`, and `RequestQueue.open` now return the same instance for the same arguments. Direct calls to `StorageClient.open_*` always return new instances.

### Writing custom storage clients

The interface for custom storage clients has been simplified:

- One storage client per storage type (`RequestQueue`, `KeyValueStore`, `Dataset`).
- Collection storage clients have been removed.
- The number of methods that have to be implemented have been reduced.

## ServiceLocator changes

### ServiceLocator is stricter with registering services
You can register the services just once, and you can no longer override already registered services.

**Before (v0.6):**
```python
from crawlee import service_locator
from crawlee.storage_clients import MemoryStorageClient

service_locator.set_storage_client(MemoryStorageClient())
service_locator.set_storage_client(MemoryStorageClient())
```
**Now (v1.0):**

```python
from crawlee import service_locator
from crawlee.storage_clients import MemoryStorageClient

service_locator.set_storage_client(MemoryStorageClient())
service_locator.set_storage_client(MemoryStorageClient())  # Raises an error
```

### BasicCrawler has its own instance of ServiceLocator to track its own services
Explicitly passed services to the crawler can be different the global ones accessible in `crawlee.service_locator`. `BasicCrawler` no longer causes the global services in `service_locator` to be set to the crawler's explicitly passed services.

**Before (v0.6):**
```python
from crawlee import service_locator
from crawlee.crawlers import BasicCrawler
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset


async def main() -> None:
    custom_storage_client = MemoryStorageClient()
    crawler = BasicCrawler(storage_client=custom_storage_client)

    assert service_locator.get_storage_client() is custom_storage_client
    assert await crawler.get_dataset() is await Dataset.open()
```
**Now (v1.0):**

```python
from crawlee import service_locator
from crawlee.crawlers import BasicCrawler
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset


async def main() -> None:
    custom_storage_client = MemoryStorageClient()
    crawler = BasicCrawler(storage_client=custom_storage_client)

    assert service_locator.get_storage_client() is not custom_storage_client
    assert await crawler.get_dataset() is not await Dataset.open()
```

This allows two crawlers with different services at the same time.

**Now (v1.0):**

```python
from crawlee.crawlers import BasicCrawler
from crawlee.storage_clients import MemoryStorageClient, FileSystemStorageClient
from crawlee.configuration import Configuration
from crawlee.events import LocalEventManager

custom_configuration_1 = Configuration()
custom_event_manager_1 = LocalEventManager.from_config(custom_configuration_1)
custom_storage_client_1 = MemoryStorageClient()

custom_configuration_2 = Configuration()
custom_event_manager_2 = LocalEventManager.from_config(custom_configuration_2)
custom_storage_client_2 = FileSystemStorageClient()

crawler_1 = BasicCrawler(
    configuration=custom_configuration_1,
    event_manager=custom_event_manager_1,
    storage_client=custom_storage_client_1,
)

crawler_2 = BasicCrawler(
    configuration=custom_configuration_2,
    event_manager=custom_event_manager_2,
    storage_client=custom_storage_client_2,
  )

# use crawlers without runtime crash...
```

## Other smaller updates

There are more smaller updates.

### Python version support

We drop support for Python 3.9. The minimum supported version is now Python 3.10.

### Changes in Configuration

The fields `persist_storage` and `persist_metadata` have been removed from the `Configuration`. Persistence is now determined only by which storage client class you use.

### Changes in Request

`Request` objects no longer have `id` field and all its usages have been transferred to `unique_key` field.

### Changes in HttpResponse

The method `HttpResponse.read` is now asynchronous. This affects all HTTP-based crawlers.

**Before (v0.6):**

```python
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext

async def main() -> None:
    crawler = ParselCrawler()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        # highlight-next-line
        content = context.http_response.read()
        # ...

    await crawler.run(['https://crawlee.dev/'])
```

**Now (v1.0):**

```python
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext

async def main() -> None:
    crawler = ParselCrawler()

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        # highlight-next-line
        content = await context.http_response.read()
        # ...

    await crawler.run(['https://crawlee.dev/'])
```
