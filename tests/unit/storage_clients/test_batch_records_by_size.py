from __future__ import annotations

from crawlee.storage_clients._utils import batch_records_by_size
from crawlee.storage_clients.models import KeyValueStoreRecordMetadata


def make_record(key: str, size: int | None) -> KeyValueStoreRecordMetadata:
    return KeyValueStoreRecordMetadata(key=key, content_type='application/octet-stream', size=size)


def batch_keys(batches: list[list[KeyValueStoreRecordMetadata]]) -> list[list[str]]:
    return [[record.key for record in batch] for batch in batches]


def test_batches_are_bounded_by_record_count() -> None:
    """A new batch starts once the current one reaches `max_records`."""
    records = [make_record(f'k{i}', 1) for i in range(5)]

    batches = list(batch_records_by_size(records, max_records=2, max_bytes=1000))

    assert batch_keys(batches) == [['k0', 'k1'], ['k2', 'k3'], ['k4']]


def test_batches_are_bounded_by_total_size() -> None:
    """A new batch starts when the next record would push the total size over `max_bytes`."""
    records = [make_record('a', 4), make_record('b', 4), make_record('c', 4), make_record('d', 4)]

    batches = list(batch_records_by_size(records, max_records=100, max_bytes=10))

    assert batch_keys(batches) == [['a', 'b'], ['c', 'd']]


def test_oversized_record_is_alone_in_its_batch() -> None:
    """A record larger than `max_bytes` is yielded alone in its batch."""
    records = [make_record('small1', 2), make_record('large', 100), make_record('small2', 2), make_record('small3', 2)]

    batches = list(batch_records_by_size(records, max_records=100, max_bytes=10))

    assert batch_keys(batches) == [['small1'], ['large'], ['small2', 'small3']]


def test_unknown_size_counts_as_empty() -> None:
    """A record with unknown size does not count toward `max_bytes`."""
    records = [make_record('a', None), make_record('b', None), make_record('c', 10)]

    batches = list(batch_records_by_size(records, max_records=100, max_bytes=10))

    assert batch_keys(batches) == [['a', 'b', 'c']]


def test_no_records_yield_no_batches() -> None:
    """An empty input yields no batches."""
    assert list(batch_records_by_size([], max_records=100, max_bytes=10)) == []
