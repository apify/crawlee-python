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

`StatisticsState.crawler_runtime` is computed from the runtime accumulated by previous runs and the start timestamp of the run in progress. In v1, assigning to it only emitted a `DeprecationWarning` and the value was ignored. In v2, the property has no setter, so an assignment raises `AttributeError`. Remove any such assignments.

## `StatisticsState.crawler_runtime_for_serialization` is removed

`StatisticsState.crawler_runtime_for_serialization` is removed, and the persisted `crawlerRuntimeMillis` value is a number of milliseconds instead of an ISO 8601 duration string. Read the runtime from the `crawler_runtime` property instead.

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

## Autoscaling logs changed

The periodic log of the autoscaling state and the `Waiting for remaining tasks to finish` message are now logged at DEBUG instead of INFO. The crawler's status message at INFO still reports the desired concurrency.

The state log also has a new format. Instead of raw ratios for every resource, it names only the overloaded ones, with the share of time each spent over its threshold and its limit:

```text
current concurrency 3, desired 5, overloaded: cpu 58% of the time (limit 40%)
```

To read the concurrency values in code, use `crawler.concurrency_system`.

The autoscaling loggers moved together with the package. `crawlee._autoscaling.autoscaled_pool`, `crawlee._autoscaling.snapshotter` and `crawlee._autoscaling.system_status` are now `crawlee.autoscaling._autoscaled_pool`, `crawlee.autoscaling._snapshotter` and `crawlee.autoscaling._system_status`. The state log, along with the messages about scaling and free capacity, now comes from the new `crawlee.autoscaling._concurrency_system` logger. To configure all of them at once, use the `crawlee.autoscaling` logger.
