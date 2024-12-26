---
id: upgrading-to-v0x
title: Upgrading to v0.x
---

This page summarizes the breaking changes between Crawlee for Python zero-based versions.

## Upgrading to v0.5

This section summarizes the breaking changes between v0.4.x and v0.5.0.

### Crawlers & CrawlingContexts

- All crawler and crawling context classes have been consolidated into a single sub-package called `crawlers`.
- The affected classes include: `AbstractHttpCrawler`, `AbstractHttpParser`, `BasicCrawler`, `BasicCrawlerOptions`, `BasicCrawlingContext`, `BeautifulSoupCrawler`, `BeautifulSoupCrawlingContext`, `BeautifulSoupParserType`, `ContextPipeline`, `HttpCrawler`, `HttpCrawlerOptions`, `HttpCrawlingContext`, `HttpCrawlingResult`, `ParsedHttpCrawlingContext`, `ParselCrawler`, `ParselCrawlingContext`, `PlaywrightCrawler`, `PlaywrightCrawlingContext`, `PlaywrightPreNavCrawlingContext`.

Example update:
```diff
- from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
+ from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
```

### Storage clients

- All storage client classes have been moved into a single sub-package called `storage_clients`.
- The affected classes include: `MemoryStorageClient`, `BaseStorageClient`.

Example update:
```diff
- from crawlee.memory_storage_client import MemoryStorageClient
+ from crawlee.storage_clients import MemoryStorageClient
```

### CurlImpersonateHttpClient

- The `CurlImpersonateHttpClient` changed its import location.

Example update:
```diff
- from crawlee.http_clients.curl_impersonate import CurlImpersonateHttpClient
+ from crawlee.http_clients import CurlImpersonateHttpClient
```

### BeautifulSoupParser

- Renamed `BeautifulSoupParser` to `BeautifulSoupParserType`. Probably used only in type hints. Please replace previous usages of `BeautifulSoupParser` by `BeautifulSoupParserType`.
- `BeautifulSoupParser` is now a new class that is used in refactored class `BeautifulSoupCrawler`.

### Service locator

- The `crawlee.service_container` was completely refactored and renamed to `crawlee.service_locator`.

### Statistics

- The `crawlee.statistics.Statistics` class do not accept an event manager as an input argument anymore. It uses the default, global one.

### Request

- Removed properties `json_` and `order_no`.

### Request storages and loaders

- The `request_provider` parameter of `BasicCrawler.__init__` has been renamed to `request_manager`
- The `BasicCrawler.get_request_provider` method has been renamed to `BasicCrawler.get_request_manager` and it does not accept the `id` and `name` arguments anymore
    - If using a specific request queue is desired, pass it as the `request_manager` on `BasicCrawler` creation
- The `RequestProvider` interface has been renamed to `RequestManager` and moved to the `crawlee.request_loaders` package
- `RequestList` has been moved to the `crawlee.request_loaders` package
- `RequestList` does not support `.drop()`, `.reclaim_request()`, `.add_request()` and `add_requests_batched()` anymore
    - It implements the new `RequestLoader` interface instead of `RequestManager`
    - `RequestManagerTandem` with a `RequestQueue` should be used to enable passing a `RequestList` (or any other `RequestLoader` implementation) as a `request_manager`, `await list.to_tandem()` can be used as a shortcut

### PlaywrightCrawler

- The `PlaywrightPreNavigationContext` was renamed to `PlaywrightPreNavCrawlingContext`.
- The input arguments in `PlaywrightCrawler.__init__` have been renamed:
    - `browser_options` is now `browser_launch_options`,
    - `page_options` is now `browser_new_context_options`.
- These argument renaming changes have also been applied to `BrowserPool`, `PlaywrightBrowserPlugin`, and `PlaywrightBrowserController`.

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
