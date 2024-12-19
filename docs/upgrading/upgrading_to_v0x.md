---
id: upgrading-to-v0x
title: Upgrading to v0.x
---

This page summarizes the breaking changes between Crawlee for Python zero-based versions.

## Upgrading to v0.5

This section summarizes the breaking changes between v0.4.x and v0.5.0.

### BeautifulSoupParser

- Renamed `BeautifulSoupParser` to `BeautifulSoupParserType`. Probably used only in type hints. Please replace previous usages of `BeautifulSoupParser` by `BeautifulSoupParserType`.
- `BeautifulSoupParser` is now a new class that is used in refactored class `BeautifulSoupCrawler`.

### Service locator

- The `crawlee.service_container` was completely refactored and renamed to `crawlee.service_locator`.

### Statistics

- The `crawlee.statistics.Statistics` class do not accept an event manager as an input argument anymore. It uses the default, global one.

### Request

- Removed properties `json_` and `order_no`.

### PlaywrightCrawler

- The `PlaywrightPreNavigationContext` was renamed to `PlaywrightPreNavCrawlingContext`.

## Upgrading to v0.4

This section summarizes the breaking changes between v0.3.x and v0.4.0.

### Request model

- The `Request.query_params` field has been removed. Please add query parameters directly to the URL, which was possible before as well, and is now the only supported approach.
- The `Request.payload` and `Request.data` fields have been consolidated. Now, only `Request.payload` remains, and it should be used for all payload data in requests.

### Extended unique key computation

- The computation of `extended_unique_key` now includes HTTP headers. While this change impacts the behavior, the interface remains the same.

## Upgrading to v0.3

This section summarizes the breaking changes between v0.2.x and v0.3.0.

### Public and private interface declaration

In previous versions, the majority of the package was fully public, including many elements intended for internal use only. With the release of v0.3, we have clearly defined the public and private interface of the package. As a result, some imports have been updated (see below). If you are importing something now designated as private, we recommend reconsidering its use or discussing your use case with us in the discussions/issues.

Here is a list of the updated public imports:

```diff
- from crawlee.enqueue_strategy import EnqueueStrategy
+ from crawlee import EnqueueStrategy
```

```diff
- from crawlee.models import Request
+ from crawlee import Request
```

```diff
- from crawlee.basic_crawler import Router
+ from crawlee.router import Router
```

### Request queue

There were internal changes that should not affect the intended usage:

- The unused `BaseRequestQueueClient.list_requests()` method was removed
- `RequestQueue` internals were updated to match the "Request Queue V2" implementation in Crawlee for JS

### Service container

A new module, `crawlee.service_container`, was added to allow management of "global instances" - currently it contains `Configuration`, `EventManager` and `BaseStorageClient`. The module also replaces the `StorageClientManager` static class. It is likely that its interface will change in the future. If your use case requires working with it, please get in touch - we'll be glad to hear any feedback.
