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

## Storage data uses camelCase keys

The storage clients now write their JSON data with camelCase keys, the same as Crawlee JS. This covers the metadata, request and record metadata files of `FileSystemStorageClient`, the request and record metadata JSON stored by `RedisStorageClient`, and the request JSON stored by `SqlStorageClient`. For example, `item_count` in a dataset's `__metadata__.json` becomes `itemCount`. The storage metadata documents of `RedisStorageClient` keep their snake_case keys.

Data written by v1 still loads, so an existing storage directory, Redis instance or database keeps working. Any tooling that reads these files directly needs to handle the camelCase keys.
