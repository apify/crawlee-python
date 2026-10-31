---
id: upgrading-to-v2
title: Upgrading to v2
---

This page summarizes the breaking changes between Crawlee for Python v1 and v2.

## Python version support

We drop support for Python 3.10, which reached its end of life in October 2026. The minimum supported version is now Python 3.11.

## Storage clients are renamed to storage backends

The pluggable storage implementations are called storage backends, and the `crawlee.storage_clients` package is now `crawlee.storage_backends`. The v1 names don't exist in v2, so update your imports and arguments:

- `StorageClient`, `MemoryStorageClient`, `FileSystemStorageClient`, `SqlStorageClient` and `RedisStorageClient` are now `StorageBackend`, `MemoryStorageBackend`, `FileSystemStorageBackend`, `SqlStorageBackend` and `RedisStorageBackend`.
- `DatasetClient`, `KeyValueStoreClient` and `RequestQueueClient` are now `DatasetBackend`, `KeyValueStoreBackend` and `RequestQueueBackend`. Their built-in implementations follow the same pattern, for example `FileSystemDatasetClient` is now `FileSystemDatasetBackend`.
- The `storage_client` argument is now `storage_backend`. That covers `BasicCrawler`, `ServiceLocator`, `Dataset.open`, `KeyValueStore.open` and `RequestQueue.open`. The `to_kvs_storage_client` argument of `Dataset.export_to` is now `to_kvs_storage_backend`. A custom request manager opener passed to `ThrottlingRequestManager` must accept `storage_backend`.
- `ServiceLocator.get_storage_client` and `ServiceLocator.set_storage_client` are now `get_storage_backend` and `set_storage_backend`.

For example:

```python
# v1
from crawlee import service_locator
from crawlee.storage_clients import MemoryStorageClient

service_locator.set_storage_client(MemoryStorageClient())
crawler = ParselCrawler(storage_client=MemoryStorageClient())

# v2
from crawlee import service_locator
from crawlee.storage_backends import MemoryStorageBackend

service_locator.set_storage_backend(MemoryStorageBackend())
crawler = ParselCrawler(storage_backend=MemoryStorageBackend())
```

A custom storage backend has more names to update. The `StorageBackend` methods `create_dataset_client`, `create_kvs_client`, `create_rq_client` and `get_storage_client_cache_key` are now `create_dataset_backend`, `create_kvs_backend`, `create_rq_backend` and `get_storage_backend_cache_key`. The `client` argument of the `Dataset`, `KeyValueStore` and `RequestQueue` constructors is now `backend`.

## `Event` is a `StrEnum`

`Event` now subclasses `enum.StrEnum`. Calling `str()` on a member or formatting it in an f-string returns its value, for example `'systemInfo'`, where v1 returned `'Event.SYSTEM_INFO'`. Comparisons with plain strings work the same way as in v1.

## `StatisticsState.crawler_runtime` is read-only

`StatisticsState.crawler_runtime` is computed from the runtime accumulated by previous runs and the start timestamp of the run in progress. In v1, assigning to it only emitted a `DeprecationWarning` and the value was ignored. In v2, the property has no setter, so an assignment raises `AttributeError`. Remove any such assignments.

## `StatisticsState.crawler_runtime_for_serialization` is removed

`StatisticsState.crawler_runtime_for_serialization` is removed, and the persisted `crawlerRuntimeMillis` value is a number of milliseconds instead of an ISO 8601 duration string. Read the runtime from the `crawler_runtime` property instead. Statistics persisted by v1 are still restored.

## `RequestQueueBackend.is_finished` is abstract

`RequestQueueBackend.is_finished` is now an abstract method. In v1, it fell back to `is_empty`, which ignores requests still being processed and could report the queue as finished too early. A subclass that doesn't implement it raises `TypeError` on instantiation. If you have a custom request queue backend, implement `is_finished` so that it returns `True` only when the queue is empty and no requests are in progress. The built-in backends already implement it.

## Keyword-only arguments

Secondary arguments of the public API are now keyword-only. The main argument of each call, such as a key, a value, a selector or a path, stays positional. A call that passes any of the following arguments positionally raises `TypeError`, so pass them by name:

- `context.push_data(data, *, dataset_id, dataset_name, dataset_alias)`, and the same for `RequestHandlerRunResult.push_data`.
- `context.add_requests(requests, *, rq_id, rq_name, rq_alias)`, and the same for `RequestHandlerRunResult.add_requests`.
- `context.block_requests(*, url_patterns, extra_url_patterns)`, and `block_requests(page, *, url_patterns, extra_url_patterns)`.
- `BasicCrawlingContext.create_modified_copy(*, push_data, add_requests, get_key_value_store)`.
- `BasicCrawler.get_data(*, dataset_id, dataset_name, dataset_alias, ...)` and `BasicCrawler.export_data(path, *, dataset_id, dataset_name, dataset_alias, ...)`.
- `KeyValueStore.set_value(key, value, *, content_type)`, also on the key-value store returned by `context.get_key_value_store()`.
- `KeyValueStore.iterate_keys`, `iterate_values`, `iterate_entries` and `list_keys`, which take `exclusive_start_key` and `limit` by keyword only.
- `Dataset.export_to(key, *, content_type, to_kvs_id, ...)`.
- `ConcurrencySettings(*, min_concurrency, max_concurrency, max_tasks_per_minute, desired_concurrency)`.
- `RequestList(requests, *, name, persist_state_key, persist_requests_key)`.
- `ProxyConfiguration.new_url(*, session_id, request)` and `ProxyConfiguration.new_proxy_info(*, session_id, request)`. Both arguments of `new_proxy_info` now default to `None`.
- `AdaptivePlaywrightCrawler.with_beautifulsoup_static_parser(*, ...)` and `with_parsel_static_parser(*, ...)`.
- `AdaptivePlaywrightCrawlingContext.wait_for_selector`, `query_selector_one`, `query_selector_all` and `parse_with_static_parser`, which take `timeout` by keyword only.
- `HeaderGenerator.get_specific_headers(header_names, *, browser_type)`.
- `BrowserController.new_page(*, browser_new_context_options, proxy_info)`, including the Playwright and Stagehand controllers.
- `PlaywrightPersistentBrowser(browser_type, user_data_dir, *, browser_launch_options)`.
- `AbstractHttpParser.find_links(parsed_content, selector, *, attribute)`, including the built-in parsers. Custom parsers keep working, since the crawler passes `attribute` by keyword.

For example:

```python
# v1
await kvs.set_value('report', data, 'application/json')
h2 = await context.query_selector_one('h2', timedelta(seconds=5))

# v2
await kvs.set_value('report', data, content_type='application/json')
h2 = await context.query_selector_one('h2', timeout=timedelta(seconds=5))
```

## `tiered_proxy_urls` is removed from `ProxyConfiguration`

The `tiered_proxy_urls` option is removed, together with `ProxyInfo.proxy_tier`, `Request.last_proxy_tier` and the `proxy_tier` argument of `ProxyConfiguration.new_url` and `ProxyConfiguration.new_proxy_info`. Crawlee JS v4 removed proxy tiers too.

To move from a cheap proxy to a more reliable one when a request fails, pass a `new_url_function` that picks the proxy by the request's `retry_count`:

```python
# v1
proxy_configuration = ProxyConfiguration(
    tiered_proxy_urls=[
        [None],
        ['http://cheap-datacenter-proxy.com/'],
        ['http://expensive-residential-proxy.com/'],
    ]
)


# v2
def pick_proxy_url(session_id=None, request=None):
    if request is None or request.retry_count == 0:
        return None
    if request.retry_count == 1:
        return 'http://cheap-datacenter-proxy.com/'
    return 'http://expensive-residential-proxy.com/'


proxy_configuration = ProxyConfiguration(new_url_function=pick_proxy_url)
```

Unlike tiers, this function doesn't track blocking per domain, so every request starts again from the first proxy. For details, see [Switching proxies on retries](../guides/proxy-management#switching-proxies-on-retries).

## `Request.forefront` is removed

The `Request.forefront` property is removed. Proxy tiers were the only code that set it. The crawler now reclaims a failed request for a retry at the back of the queue, and a request deferred by a `RequestThrottledError` at the front. To enqueue a request at the front, pass `forefront=True` to `add_requests` or `reclaim_request`.

## Unique keys keep the URL casing

The default `Request.unique_key` lowercases only the scheme and host of the URL. The path, query and fragment keep their casing, since they're case-sensitive. In v1, the whole URL was lowercased, so URLs that differed only in casing, such as `https://example.com/Product/ABC` and `https://example.com/product/abc`, got the same unique key, and the crawler visited only one of them.

As a result, a crawl can visit more pages than in v1. Requests stored in a request queue by v1 keep their lowercased unique keys, so adding the same URL again in v2 can create a duplicate request. To keep case-insensitive deduplication, pass an explicit `unique_key`. With `enqueue_links`, set `unique_key` to the lowercased URL in its `transform_request_function`, and leave `url` as it is, so the crawler still fetches the original casing.

## Storage data uses camelCase keys

The storage backends now write their JSON data with camelCase keys, the same as Crawlee JS. This covers the metadata, request and record metadata files of `FileSystemStorageBackend`, the request and record metadata JSON stored by `RedisStorageBackend`, and the request JSON stored by `SqlStorageBackend`. For example, `item_count` in a dataset's `__metadata__.json` becomes `itemCount`. The storage metadata documents of `RedisStorageBackend` keep their snake_case keys.

Data written by v1 still loads, so an existing storage directory, Redis instance or database keeps working. Any tooling that reads these files directly needs to handle the camelCase keys.

## Crawler `id` is a string

The `id` argument of `BasicCrawler` and its subclasses is now a `str`, so pass a string wherever you passed an integer. `StatisticsState.stats_id` and `EventCrawlerStatusData.crawler_id` are strings too, and `crawler_id` holds the crawler's `id`. In v1, it held the Python object ID of the crawler.

The statistics a crawler creates by default now use the crawler's `id`. Crawlers that share an `id` therefore share both the state returned by `use_state` and the persisted statistics. In v1, they shared only the state.

A crawler without an `id` gets its creation index, such as `'0'`, and crawlers with an explicit `id` now count towards that index too. If you resume a v1 run with `purge_on_start=False` and mix both kinds of crawlers, a generated `id` can differ from the one v1 assigned, and the crawler won't find the state that `use_state` stored under the old one.

## Concurrency is managed by `ConcurrencySystem`

Load monitoring and concurrency scaling moved from `AutoscaledPool` to the new `ConcurrencySystem`. A crawler builds a default system from its `concurrency_settings` for each run, so a crawler configured through `concurrency_settings` works as before. To cap the combined concurrency of several crawlers, pass one shared `ConcurrencySystem` to all of them through the new `concurrency_system` option.

The autoscaling classes now live in the public `crawlee.autoscaling` package, which replaces the private `crawlee._autoscaling`. Import `AutoscaledPool`, `ConcurrencySystem`, `ConcurrencyConsumer`, `SystemInfo` and `LoadRatioInfo` from there. `Snapshotter` and `SystemStatus` stay internal.

`AutoscaledPool` now only runs the task loop. If you create a pool yourself, note that:

- `system_status` and `concurrency_settings` are replaced by `concurrency_system`, which has to be active when the pool runs, and `consumer`, which identifies the pool to the system.
- `run_task_function` receives the id the task is booked under.
- `current_concurrency` comes from the concurrency system, so a shared system may count the tasks of other pools too. The pool's own tasks are counted by the new `own_concurrency`.
- `is_finished_function` is called only when no new task can start and none of the pool's own tasks are in flight, and never while the pool is paused. In v1, it was called on every loop iteration, so a paused pool could finish. A pool whose `is_task_ready_function` keeps resolving to `True` doesn't finish, so make it resolve to `False` once the work runs out.

For example:

```python
# v1
from crawlee._autoscaling import AutoscaledPool, Snapshotter, SystemStatus

async with Snapshotter.from_config() as snapshotter:
    pool = AutoscaledPool(
        system_status=SystemStatus(snapshotter),
        concurrency_settings=ConcurrencySettings(max_concurrency=10),
        run_task_function=run_task,
        is_task_ready_function=is_task_ready,
        is_finished_function=is_finished,
    )
    await pool.run()

# v2
from crawlee.autoscaling import AutoscaledPool, ConcurrencyConsumer, ConcurrencySystem

settings = ConcurrencySettings(max_concurrency=10)

async with ConcurrencySystem(settings) as concurrency_system:
    pool = AutoscaledPool(
        concurrency_system=concurrency_system,
        consumer=ConcurrencyConsumer(id='my-pool'),
        run_task_function=run_task,  # Now called with the task id.
        is_task_ready_function=is_task_ready,
        is_finished_function=is_finished,
    )
    await pool.run()
```

## `max_tasks_per_minute` counts the last minute

`ConcurrencySettings.max_tasks_per_minute` now caps the tasks started over the last 60 seconds, the same as Crawlee JS. In v1, the pool spaced task starts evenly, `60 / max_tasks_per_minute` seconds apart. In v2, tasks start as fast as the concurrency allows until the cap is reached. The next ones wait until the oldest starts age out of the window. The rate over a minute stays the same, but tasks start in bursts. For details, see [Max tasks per minute](../guides/scaling-crawlers#max-tasks-per-minute).

The value has to be a whole number of at least 1, or infinity, which is the default. A fractional value, such as `0.5` or `7.5`, now raises `ValueError`.

## Only the first crawler uses the default request queue

Only the first crawler created in a process opens the default request queue, the same as Crawlee JS. Every later crawler without a `request_manager` opens its own queue under the alias `__default_{id}__`. A crawler without an `id` uses its creation index there, such as `__default_1__`. In v1, all these crawlers shared the default queue, so a URL one crawler handled was skipped by the others.

To reach a crawler's queue, call `crawler.get_request_manager()` instead of `RequestQueue.open()`. If your crawlers relied on the shared queue, open it yourself and pass it to each of them:

```python
queue = await RequestQueue.open()
crawler_1 = BasicCrawler(request_manager=queue)
crawler_2 = BasicCrawler(request_manager=queue)
```

## Repeated `run()` calls no longer purge the request queue

In v1, calling `crawler.run()` again on the same instance purged the request queue, unless the queue was named or the previous run had failed. In v2, nothing is purged between runs, the same as Crawlee JS. A repeated `run()` continues with the same request manager and skips the requests an earlier run handled, failed ones included.

The `purge_request_queue` argument of `run()` is removed. If you passed `purge_request_queue=False`, drop the argument. To crawl the same requests again, purge the request manager yourself:

```python
# v1
await crawler.run(urls)
await crawler.run(urls)

# v2
await crawler.run(urls)
request_manager = await crawler.get_request_manager()
await request_manager.purge()
await crawler.run(urls)
```

`RequestManagerTandem.purge()` now purges the manager it wraps, where v1 did nothing. The loader isn't purged, so requests it has already passed to the manager don't come back.

Instead of purging, you can create a new crawler for each crawl. Every crawler after the first opens a request queue of its own, as described in [Only the first crawler uses the default request queue](#only-the-first-crawler-uses-the-default-request-queue). Crawlers created with the same `id` share that queue, so leave `id` unset or give each crawler a different one:

```python
for urls in batches:
    crawler = BasicCrawler()
    await crawler.run(urls)
```

`Configuration.purge_on_start` isn't affected. Unnamed storages are still purged once per process, when they're first opened.

## `RequestQueue.purge` raises when the storage backend can't purge

In v1, `RequestQueue.purge()` worked around a storage backend that didn't support purging. It dropped an unnamed queue and opened the default queue in its place, and it skipped a named queue with a warning. In v2, it raises the backend's `NotImplementedError`, the same as Crawlee JS. The Apify platform's storage backend doesn't support purging, so on the platform, call `drop()` or open a new queue instead.

## `check_readiness` replaces `is_empty` and `is_finished`

`RequestLoader` and `RequestManager` no longer have `is_empty` and `is_finished`. A single `check_readiness` method replaces them and returns one of four statuses:

- `ReadyStatus`: the next `fetch_next_request` call is expected to return a request.
- `WaitingStatus`: nothing can be fetched right now, but the source isn't done. Requests are in progress, are being added in the background, or are held back until `ready_at`.
- `StalledStatus`: the source holds requests it can't make progress on, as explained in `reason`. `ThrottlingRequestManager` reports it, and `RequestManagerTandem` passes it on from the manager it wraps.
- `FinishedStatus`: every request has been handled.

Each of these classes also has a `status` attribute set to `'ready'`, `'waiting'`, `'stalled'` or `'finished'`, so a check doesn't need an import.

The change covers `RequestQueue`, `RequestList`, `SitemapRequestLoader`, `RequestManagerTandem` and `ThrottlingRequestManager`. `ThrottlingRequestManager.get_stall_reason` is removed too, since a stall is now reported as `StalledStatus`. Unlike `get_stall_reason`, the manager reports a stall only while no request outside the stalled domains is in progress or still being added, since such a request may still add work. Storage backends keep `is_empty` and `is_finished`.

```python
# v1
has_requests = not await rq.is_empty()
done = await rq.is_finished()

# v2
readiness = await rq.check_readiness()
has_requests = readiness.status == 'ready'
done = readiness.status == 'finished'
```

If you have a custom request loader or manager, replace `is_empty` and `is_finished` with `check_readiness`. Return `ReadyStatus` as soon as a request can be fetched, before checking anything else, since the crawler calls the method often. Never return `FinishedStatus` while requests are still pending or in progress. A loader doesn't return `StalledStatus`.

A `None` from `fetch_next_request` no longer means the loader is finished. It only means nothing is pending right now. `RequestManagerTandem` now moves a request from its loader only when the loader is ready, so a `SitemapRequestLoader` that's still loading sitemaps no longer holds up the requests already in the manager.

The crawler now reads a stall from the status, so it also sees a `ThrottlingRequestManager` wrapped in a `RequestManagerTandem`. Such a crawl now ends with `PersistentRateLimitError` once a domain stalls, as it already did with a `ThrottlingRequestManager` passed directly. Before, the crawler didn't detect the stall.

## HTTP clients

`HttpClient` follows the design of Crawlee JS v4. A client sends a single request, and the base class handles redirects and cookies the same way for every client.

### Responses are read lazily

`HttpClient.crawl` and `HttpClient.stream` are removed. `send_request` returns the response as soon as its headers arrive and reads the body only on `read` or `read_stream`. A response whose body you don't read keeps its connection until you call `aclose`. Use the response as an async context manager to close it:

```python
# v1
async with http_client.stream(url) as response:
    async for chunk in response.read_stream():
        ...

# v2
async with await http_client.send_request(url) as response:
    async for chunk in response.read_stream():
        ...
```

Responses returned by `context.send_request` are closed after the request is processed, so a request handler doesn't need to close them.

### `timeout` covers the redirects and the body

The `timeout` of `send_request` is one deadline for the whole redirect chain and for reading the body, the same as Crawlee JS. A body read past it raises `TimeoutError`. Without a `timeout`, the default of the underlying library applies. It's 30 seconds for `ImpitHttpClient`, including reading the body, and for `CurlImpersonateHttpClient`, and 5 seconds per operation for `HttpxHttpClient`. In v1, `ImpitHttpClient` and `CurlImpersonateHttpClient` disabled the timeout altogether in that case. To change the default, pass `timeout` when you create the client.

The crawlers pass the remaining `navigation_timeout` to the HTTP client, so it bounds reading the body too. With `FileDownloadCrawler(stream=True)`, the deadline also covers the body streamed in the request handler, with every HTTP client. In v1, only `ImpitHttpClient` bounded the streamed body, while `HttpxHttpClient` and `CurlImpersonateHttpClient` bounded only the connection. For long downloads, raise `navigation_timeout` together with `request_handler_timeout`.

### `send_request` follows at most 10 redirects

`send_request` follows up to 10 redirects and raises `RuntimeError` when a server redirects more often. The limit isn't configurable. In v1, each client raised the `TooManyRedirects` error of its library instead, after 20 redirects with `ImpitHttpClient` and `HttpxHttpClient` and after 30 with `CurlImpersonateHttpClient`. Every client now takes `follow_redirects`. With `follow_redirects=False`, `send_request` returns the first response as it is. The redirect options of the underlying libraries are ignored with a warning. For the list, see [Some client arguments are ignored](#some-client-arguments-are-ignored).

### Client headers follow the redirect rules

The `headers` argument of a client still applies to every request. Its `Authorization`, `Proxy-Authorization` and `Cookie` headers are dropped when a redirect leaves the origin, the same as the headers passed with a request. With `HttpxHttpClient`, the headers of the client now win over the generated browser headers. In v1, the generated browser headers replaced them.

### Some client arguments are ignored

The following keyword arguments of the underlying libraries are now ignored with a warning:

- `cookies` of every client and `cookie_jar` of `ImpitHttpClient`, since cookies are kept in sessions.
- `auth` and `params` of `HttpxHttpClient` and `CurlImpersonateHttpClient`, which would reach every request of a redirect chain. Pass the credentials in the `Authorization` header and the query in the URL instead.
- `max_redirects` of every client and `allow_redirects` of `CurlImpersonateHttpClient`, since `send_request` follows the redirects.

### Custom clients implement `_fetch`

A custom client implements `_fetch` and `cleanup` instead of `crawl`, `send_request` and `stream`. `_fetch` sends one request and returns the response without following redirects or keeping cookies. The client inherits `send_request` from `HttpClient`.

For example:

```python
# v1
class MyHttpClient(HttpClient):
    async def crawl(self, request, **kwargs): ...

    async def send_request(self, url, **kwargs): ...

    def stream(self, url, **kwargs): ...

    async def cleanup(self): ...


# v2
class MyHttpClient(HttpClient):
    async def _fetch(self, url, *, method, headers, payload, proxy_info, timeout): ...

    async def cleanup(self): ...
```

`HttpClient.__init__` is no longer abstract, so a custom client doesn't have to define it. A custom `__init__` must call `super().__init__()`, because the base `__init__` stores the `follow_redirects` and `headers` options that `send_request` reads.

`HttpResponse` is now an abstract base class instead of a protocol, so a custom response subclasses it. It has three new abstract members, `url`, `headers_set_cookie` and `aclose`, and works as an async context manager that closes the response.

## Logs changed

If you configure loggers by name or match log messages, update them for the following changes.

### Logger names

The autoscaling loggers moved together with the package. `crawlee._autoscaling.autoscaled_pool`, `crawlee._autoscaling.snapshotter` and `crawlee._autoscaling.system_status` are now `crawlee.autoscaling._autoscaled_pool`, `crawlee.autoscaling._snapshotter` and `crawlee.autoscaling._system_status`. The state log, along with the messages about scaling and free capacity, now comes from the new `crawlee.autoscaling._concurrency_system` logger. To configure all of them at once, use the `crawlee.autoscaling` logger.

### Log levels

The periodic log of the autoscaling state and the `Waiting for remaining tasks to finish` message are now logged at DEBUG instead of INFO. The crawler's status message at INFO still reports the desired concurrency. To read the concurrency values in code, use `crawler.concurrency_system`.

### Log messages

In v1, the autoscaling state log listed raw ratios for every resource. In v2, it names only the overloaded resources, with the share of time each spent over its threshold and its limit:

```text
current concurrency 3, desired 5, overloaded: cpu 58% of the time (limit 40%)
```

The INFO messages the crawler logs after `stop()` is called or `max_requests_per_crawl` is reached have new wording:

```text
# v1
No new requests are allowed because crawler `stop` method was called. Ongoing requests will be allowed to complete.
The crawler will finish any remaining ongoing requests and shut down.

# v2
No new requests are allowed because the `stop()` method has been called. Ongoing requests will be allowed to complete.
The crawler has finished all the remaining ongoing requests and will shut down now.
```

The crawler logs the first message once per run, the next time it looks for a new request after the stop. It logs the second one once the ongoing requests have finished, where v1 logged it right after the stop. In `Crawler.stop() was called with following reason: ...`, a trailing period or space in the reason is dropped, so the default reason and the `max_requests_per_crawl` one no longer end with a double period.

When a crawl processes no requests because its request manager holds only handled ones, the crawler logs a new warning. It starts with `This crawl processed no requests` and explains how to crawl the requests again.

Messages that mentioned a storage client name the storage backend instead. The DEBUG message about the implicit default backend now reads `No storage backend set, implicitly creating and using default FileSystemStorageBackend.` The warning about unsupported arguments of a dataset backend ends with the backend class name, for example `... of get_data are not supported by MemoryDatasetBackend.`

`RequestQueue.purge()` no longer logs the `Storage client "..." does not support purging the request queue.` warnings. It raises `NotImplementedError` instead. For details, see [`RequestQueue.purge` raises when the storage backend can't purge](#requestqueuepurge-raises-when-the-storage-backend-cant-purge).

The ERROR message the crawler logs before raising `PersistentRateLimitError` now reads "Giving up the crawl because the request manager can't make progress." instead of "Giving up the crawl because a domain keeps rate-limiting every request."

The two DEBUG messages that `RequestQueue.is_finished` logged, "Background add requests tasks are still in progress." and "The request queue is finished.", are removed together with the method.

## Session retirement is permanent

In v1, `Session.retire()` only added `max_error_score` to the error score, so a later `mark_good()` could lower it again and make the retired session usable. In v2, a separate `retired` flag tracks retirement, exposed as the `Session.retired` property, and a retired session is never usable again. Create a new session to replace it.

On a retired session, `mark_good()` does nothing, so its error score and usage count stay as they are. Calling `retire()` on a session that is already retired also leaves the error score unchanged.

When `mark_good()` or `mark_bad()` retires a session because it expired, reached its usage limit, or reached its error score limit, it only sets the flag. In v1, it also added `max_error_score`. As a result, such sessions have a lower error score in v2, and `is_blocked` is `True` only when the error score itself reached `max_error_score`.

Session state, including the persisted session pool state, has a new `retired` field. Sessions saved by v1 load with `retired=False` and keep their error score.
