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
