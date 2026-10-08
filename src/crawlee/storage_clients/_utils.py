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

    Each batch is meant to be read in a single call, so the size bound keeps a store with large values from loading
    too many of them into memory at once. A record whose size alone exceeds `max_bytes` is still yielded, but alone in
    its batch. A record with unknown size counts as empty.
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
