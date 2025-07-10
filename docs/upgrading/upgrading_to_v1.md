---
id: upgrading-to-v1
title: Upgrading to v1
---

This page summarizes the breaking changes between Crawlee for Python v0.6 and v1.0.

## Distinct use of word `browser` in similar contexts

Two different contexts:
- Playwright related browser
- fingerprinting related browser

Type of `HeaderGeneratorOptions.browsers` changed from `Literal['chromium', 'firefox', 'webkit', 'edge']` to `Literal['chrome', 'firefox', 'safari', 'edge']` as it is related to the fingerprinting context and not to the Playwright context.

Before:

```python
from crawlee.fingerprint_suite import HeaderGeneratorOptions

HeaderGeneratorOptions(browsers=['chromium'])
HeaderGeneratorOptions(browsers=['webkit'])
```

Now:

```python
from crawlee.fingerprint_suite import HeaderGeneratorOptions

HeaderGeneratorOptions(browsers=['chrome'])
HeaderGeneratorOptions(browsers=['safari'])
```


## Storage clients

In v1.0, we are introducing a new storage clients system. We have completely reworked their interface,
making it much simpler to write your own storage clients. This allows you to easily store your request queues,
key-value stores, and datasets in various destinations.

### New storage clients

Previously, the `MemoryStorageClient` handled both in-memory storage and file system persistence, depending
on configuration. In v1.0, we've split this into two dedicated classes:

- `MemoryStorageClient` - stores all data in memory only.
- `FileSystemStorageClient` - persists data on the file system, with in-memory caching for improved performance.

For details about the new interface, see the `BaseStorageClient` documentation. You can also check out
the [Storage clients guide](https://crawlee.dev/python/docs/guides/) for more information on available
storage clients and instructions on writing your own.

### Memory storage client

Before:

```python
from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient

configuration = Configuration(persist_storage=False)
storage_client = MemoryStorageClient.from_config(configuration)
```

Now:

```python
from crawlee.storage_clients import MemoryStorageClient

storage_client = MemoryStorageClient()
```

### File-system storage client

Before:

```python
from crawlee.configuration import Configuration
from crawlee.storage_clients import MemoryStorageClient

configuration = Configuration(persist_storage=True)
storage_client = MemoryStorageClient.from_config(configuration)
```

Now:

```python
from crawlee.storage_clients import FileSystemStorageClient

storage_client = FileSystemStorageClient()
```

The way you register storage clients remains the same:

```python
from crawlee import service_locator
from crawlee.crawlers import ParselCrawler
from crawlee.storage_clients import MemoryStorageClient
from crawlee.storages import Dataset

# Create custom storage client, MemoryStorageClient for example.
storage_client = MemoryStorageClient()

# Register it globally via the service locator.
service_locator.set_storage_client(storage_client)

# Or pass it directly to the crawler, it will be registered globally
# to the service locator under the hood.
crawler = ParselCrawler(storage_client=storage_client)

# Or just provide it when opening a storage (e.g. dataset), it will be used
# for this storage only, not globally.
dataset = await Dataset.open(
    name='my_dataset',
    storage_client=storage_client,
)
```

### Breaking changes

The `persist_storage` and `persist_metadata` fields have been removed from the `Configuration` class.
Persistence is now determined solely by the storage client class you use.

The `read` method for `HttpResponse` has been changed from synchronous to asynchronous.

### Storage client instance behavior

Instance caching is implemented for the storage open methods: `Dataset.open()`, `KeyValueStore.open()`,
and `RequestQueue.open()`. This means that when you call these methods with the same arguments,
the same instance is returned each time.

In contrast, when using client methods such as `StorageClient.open_dataset_client()`, each call creates
a new `DatasetClient` instance, even if the arguments are identical. These methods do not use instance caching.

This usage pattern is not common, and it is generally recommended to open storages using the standard storage
open methods rather than the storage client methods.

### Writing custom storage clients

The storage client interface has been fully reworked. Collection storage clients have been removed - now there is
one storage client class per storage type (`RequestQueue`, `KeyValueStore`, and `Dataset`). Writing your own storage
clients is now much simpler, allowing you to store your request queues, key-value stores, and datasets in any
destination you choose.

## Dataset

- There are a few new methods:
  - `get_metadata`
  - `purge`
  - `list_items`
- The `from_storage_object` method has been removed - use the `open` method with `name` or `id` instead.
- The `get_info` and `storage_object` properties have been replaced by the new `get_metadata` method.
- The `set_metadata` method has been removed.
- The `write_to_json` and `write_to_csv` methods have been removed - use `export_to` instead.

## Key-value store

- There are a few new methods:
  - `get_metadata`
  - `purge`
  - `delete_value`
  - `list_keys`
- The `from_storage_object` method has been removed - use the `open` method with `name` or `id` instead.
- The `get_info` and `storage_object` properties have been replaced by the new `get_metadata` method.
- The `set_metadata` method has been removed.

## Request queue

- There are a few new methods:
  - `get_metadata`
  - `purge`
  - `add_requests` (renamed from `add_requests_batched`)
- The `from_storage_object` method has been removed - use the `open` method with `name` or `id` instead.
- The `get_info` and `storage_object` properties have been replaced by the new `get_metadata` method.
- The `set_metadata` method has been removed.
- `resource_directory` from `RequestQueueMetadata` removed – use `path_to_...` property.
- `RequestQueueHead` model replaced with `RequestQueueHeadWithLocks`.
