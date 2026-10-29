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

Crawlee v1.0 now uses `ImpitHttpClient` (based on [impit](https://github.com/apify/impit) library) as the **default HTTP client**, replacing `HttpxHttpClient` (based on [httpx](https://www.python-httpx.org/) library).

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

## New architecture of storage backends

In v1.0, the storage backend system has been completely reworked to simplify implementation and make custom storage backends easier to write.

See the [Storage backends guide](https://crawlee.dev/python/docs/guides/storage-backends) for more details.

### New dedicated storage backends

Previously, `MemoryStorageBackend` handled both in-memory storage and optional file system persistence. This has now been split into two distinct storage backends:

- **`MemoryStorageBackend`** - Stores all data in memory only.
- **`FileSystemStorageBackend`** - Persists data on the file system, with in-memory caching for better performance.

**Before (v0.6):**

```python
from crawlee.configuration import Configuration
from crawlee.storage_backends import MemoryStorageBackend

# In-memory only
configuration = Configuration(persist_storage=False)
storage_backend = MemoryStorageBackend.from_config(configuration)

# File-system persistence
configuration = Configuration(persist_storage=True)
storage_backend = MemoryStorageBackend.from_config(configuration)
```

**Now (v1.0):**

```python
from crawlee.storage_backends import MemoryStorageBackend, FileSystemStorageBackend

# In-memory only
storage_backend = MemoryStorageBackend()

# File-system persistence
storage_backend = FileSystemStorageBackend()
```

### Registering a storage backend

The way you register a storage backend remains unchanged:

```python
from crawlee import service_locator
from crawlee.crawlers import ParselCrawler
from crawlee.storage_backends import MemoryStorageBackend
from crawlee.storages import Dataset

# Create custom storage backend
storage_backend = MemoryStorageBackend()

# Then register it globally
service_locator.set_storage_backend(storage_backend)

# Or use it for a single crawler only
crawler = ParselCrawler(storage_backend=storage_backend)

# Or use it for a single storage only
dataset = await Dataset.open(
    name='my-dataset',
    storage_backend=storage_backend,
)
```

### Instance caching

Instance caching of `Dataset.open`, `KeyValueStore.open`, and `RequestQueue.open` now return the same instance for the same arguments. Direct calls to `StorageBackend.open_*` always return new instances.

### Writing custom storage backends

The interface for custom storage backends has been simplified:

- One storage backend per storage type (`RequestQueue`, `KeyValueStore`, `Dataset`).
- Collection storage backends have been removed.
- The number of methods that have to be implemented have been reduced.

## ServiceLocator changes

### ServiceLocator is stricter with registering services

You can register the services just once, and you can no longer override already registered services.

**Before (v0.6):**

```python
from crawlee import service_locator
from crawlee.storage_backends import MemoryStorageBackend

service_locator.set_storage_backend(MemoryStorageBackend())
service_locator.set_storage_backend(MemoryStorageBackend())
```

**Now (v1.0):**

```python
from crawlee import service_locator
from crawlee.storage_backends import MemoryStorageBackend

service_locator.set_storage_backend(MemoryStorageBackend())
service_locator.set_storage_backend(MemoryStorageBackend())  # Raises an error
```

### BasicCrawler has its own instance of ServiceLocator to track its own services

Explicitly passed services to the crawler can be different the global ones accessible in `crawlee.service_locator`. `BasicCrawler` no longer causes the global services in `service_locator` to be set to the crawler's explicitly passed services.

**Before (v0.6):**

```python
from crawlee import service_locator
from crawlee.crawlers import BasicCrawler
from crawlee.storage_backends import MemoryStorageBackend
from crawlee.storages import Dataset


async def main() -> None:
    custom_storage_backend = MemoryStorageBackend()
    crawler = BasicCrawler(storage_backend=custom_storage_backend)

    assert service_locator.get_storage_backend() is custom_storage_backend
    assert await crawler.get_dataset() is await Dataset.open()
```

**Now (v1.0):**

```python
from crawlee import service_locator
from crawlee.crawlers import BasicCrawler
from crawlee.storage_backends import MemoryStorageBackend
from crawlee.storages import Dataset


async def main() -> None:
    custom_storage_backend = MemoryStorageBackend()
    crawler = BasicCrawler(storage_backend=custom_storage_backend)

    assert service_locator.get_storage_backend() is not custom_storage_backend
    assert await crawler.get_dataset() is not await Dataset.open()
```

This allows two crawlers with different services at the same time.

**Now (v1.0):**

```python
from crawlee.crawlers import BasicCrawler
from crawlee.storage_backends import MemoryStorageBackend, FileSystemStorageBackend
from crawlee.configuration import Configuration
from crawlee.events import LocalEventManager

custom_configuration_1 = Configuration()
custom_event_manager_1 = LocalEventManager.from_config(custom_configuration_1)
custom_storage_backend_1 = MemoryStorageBackend()

custom_configuration_2 = Configuration()
custom_event_manager_2 = LocalEventManager.from_config(custom_configuration_2)
custom_storage_backend_2 = FileSystemStorageBackend()

crawler_1 = BasicCrawler(
    configuration=custom_configuration_1,
    event_manager=custom_event_manager_1,
    storage_backend=custom_storage_backend_1,
)

crawler_2 = BasicCrawler(
    configuration=custom_configuration_2,
    event_manager=custom_event_manager_2,
    storage_backend=custom_storage_backend_2,
)

# use crawlers without runtime crash...
```

## Other smaller updates

There are more smaller updates.

### Python version support

We drop support for Python 3.9. The minimum supported version is now Python 3.10.

### Changes in Configuration

The fields `persist_storage` and `persist_metadata` have been removed from the `Configuration`. Persistence is now determined only by which storage backend class you use.

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

### New storage naming restrictions

We've introduced naming restrictions for storages to ensure compatibility with Apify Platform requirements and prevent potential conflicts. Storage names may include only letters (a–z, A–Z), digits (0–9), and hyphens (-), with hyphens allowed only in the middle of the name (for example, my-storage-1).
