from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from crawlee.storage_clients.models import KeyValueStoreRecordMetadata


def batch_records_by_size(
    records: Iterable[KeyValueStoreRecordMetadata],
    *,
    max_records: int,
    max_bytes: int,
) -> Iterator[list[KeyValueStoreRecordMetadata]]:
    """Group record metadata into batches bounded by record count and by total record size.

    Used by storage clients that read the values of several records at once, so that a store with large values does
    not load too many of them into memory in a single read. A record whose size alone exceeds `max_bytes` is still
    yielded, but alone in its batch. A record with unknown size counts as empty.
    """
    batch: list[KeyValueStoreRecordMetadata] = []
    batch_size = 0

    for record in records:
        record_size = record.size or 0
        if batch and (len(batch) >= max_records or batch_size + record_size > max_bytes):
            yield batch
            batch, batch_size = [], 0

        batch.append(record)
        batch_size += record_size

    if batch:
        yield batch
