---
id: upgrading-to-v2
title: Upgrading to v2
---

This page summarizes the breaking changes between Crawlee for Python v1 and v2.

## Python version support

We drop support for Python 3.10, which reached its end of life in October 2026. The minimum supported version is now Python 3.11.

## `Event` is a `StrEnum`

`Event` now subclasses `enum.StrEnum`. Calling `str()` on a member or formatting it in an f-string returns its value, for example `'systemInfo'`, where v1 returned `'Event.SYSTEM_INFO'`. Comparisons with plain strings work the same way as in v1.

## `StatisticsState.crawler_runtime` is read-only

`StatisticsState.crawler_runtime` is computed from the crawler's start and finish timestamps. In v1, assigning to it only emitted a `DeprecationWarning` and the value was ignored. In v2, the property has no setter, so an assignment raises `AttributeError`. Remove any such assignments.

## `RequestQueueClient.is_finished` is abstract

`RequestQueueClient.is_finished` is now an abstract method. In v1, it fell back to `is_empty`, which ignores requests still being processed and could report the queue as finished too early. A subclass that doesn't implement it raises `TypeError` on instantiation. If you have a custom request queue client, implement `is_finished` so that it returns `True` only when the queue is empty and no requests are in progress. The built-in clients already implement it.

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
- `ProxyConfiguration.new_url(*, session_id, request, proxy_tier)` and `ProxyConfiguration.new_proxy_info(*, session_id, request, proxy_tier)`. All three arguments of `new_proxy_info` now default to `None`.
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

## Storage data uses camelCase keys

The storage clients now write their JSON data with camelCase keys, the same as Crawlee JS. This covers the metadata, request and record metadata files of `FileSystemStorageClient`, the request and record metadata JSON stored by `RedisStorageClient`, and the request JSON stored by `SqlStorageClient`. For example, `item_count` in a dataset's `__metadata__.json` becomes `itemCount`. The storage metadata documents of `RedisStorageClient` keep their snake_case keys.

Data written by v1 still loads, so an existing storage directory, Redis instance or database keeps working. Any tooling that reads these files directly needs to handle the camelCase keys.

## `check_readiness` replaces `is_empty` and `is_finished`

`RequestLoader` and `RequestManager` no longer have `is_empty` and `is_finished`. A single `check_readiness` method replaces them and returns one of four statuses:

- `ReadyStatus`: the next `fetch_next_request` call is expected to return a request.
- `WaitingStatus`: nothing can be fetched right now, but the source isn't done. Requests are in progress, are being added in the background, or are held back until `ready_at`.
- `StalledStatus`: the source holds requests it can't make progress on, as explained in `reason`. `ThrottlingRequestManager` reports it, and `RequestManagerTandem` passes it on from the manager it wraps.
- `FinishedStatus`: every request has been handled.

The change covers `RequestQueue`, `RequestList`, `SitemapRequestLoader`, `RequestManagerTandem` and `ThrottlingRequestManager`. `ThrottlingRequestManager.get_stall_reason` is removed too, since a stall is now reported as `StalledStatus`. Storage clients keep `is_empty` and `is_finished`.

```python
from crawlee.request_loaders import FinishedStatus, ReadyStatus

# v1
has_requests = not await rq.is_empty()
done = await rq.is_finished()

# v2
has_requests = isinstance(await rq.check_readiness(), ReadyStatus)
done = isinstance(await rq.check_readiness(), FinishedStatus)
```

If you have a custom request loader or manager, replace `is_empty` and `is_finished` with `check_readiness`. Return `ReadyStatus` as soon as a request can be fetched, before checking anything else, since the crawler calls the method often. Never return `FinishedStatus` while requests are still pending or in progress. A loader doesn't return `StalledStatus`.

A `None` from `fetch_next_request` no longer means the loader is finished. It only means nothing is pending right now. `RequestManagerTandem` now moves a request from its loader only when the loader is ready, so a `SitemapRequestLoader` that's still loading sitemaps no longer holds up the requests already in the manager.

The crawler now reads a stall from the status, so it also sees a `ThrottlingRequestManager` wrapped in a `RequestManagerTandem`. Such a crawl now ends with `PersistentRateLimitError` once a domain stalls, as it already did with a `ThrottlingRequestManager` passed directly. Before, the crawler didn't detect the stall.
