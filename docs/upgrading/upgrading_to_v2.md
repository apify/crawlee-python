---
id: upgrading-to-v2
title: Upgrading to v2
---

This page summarizes the breaking changes between Crawlee for Python v1.x and v2.0.

## Statistics

The runtime tracking in `StatisticsState` was reworked to fix `crawler_runtime` miscounting after migrations and resurrections. The runtime accumulated by previous runs is now restored from the persisted `crawlerRuntimeMillis` value, so it survives any number of interruptions, and the downtime between the runs is no longer counted.

- `StatisticsState.crawler_runtime` is a read-only property now. The deprecated setter was removed; assigning to it raises `AttributeError` instead of emitting a `DeprecationWarning`.
- `StatisticsState.crawler_runtime_for_serialization` was removed. The persisted state still contains `crawlerRuntimeMillis`, now written by the `runtime_offset` field, which also restores the value when a state is loaded.
- `crawlerRuntimeMillis` is serialized as a number of milliseconds, consistent with the other `*Millis` fields and with Crawlee for JavaScript, instead of an ISO 8601 duration string. States persisted by v1.x load correctly.
- The unused state fields `errors`, `retry_errors`, `requests_finished_per_minute` and `requests_failed_per_minute` were removed from `StatisticsState`. They were never populated. The `FinalStatistics` fields of the same names are unaffected, and the per-minute rates in the statistics logs are still computed.
