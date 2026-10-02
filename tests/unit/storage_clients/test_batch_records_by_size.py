from __future__ import annotations

from crawlee.storage_clients._utils import batch_records_by_size
from crawlee.storage_clients.models import KeyValueStoreRecordMetadata


def _record(key: str, size: int | None) -> KeyValueStoreRecordMetadata:
    return KeyValueStoreRecordMetadata(key=key, content_type='application/octet-stream', size=size)


def _keys(batches: list[list[KeyValueStoreRecordMetadata]]) -> list[list[str]]:
    return [[record.key for record in batch] for batch in batches]


def test_batches_are_bounded_by_record_count() -> None:
    records = [_record(f'k{i}', 1) for i in range(5)]

    batches = list(batch_records_by_size(records, max_records=2, max_bytes=1000))

    assert _keys(batches) == [['k0', 'k1'], ['k2', 'k3'], ['k4']]


def test_batches_are_bounded_by_total_size() -> None:
    records = [_record('a', 4), _record('b', 4), _record('c', 4), _record('d', 4)]

    batches = list(batch_records_by_size(records, max_records=100, max_bytes=10))

    assert _keys(batches) == [['a', 'b'], ['c', 'd']]


def test_oversized_record_is_alone_in_its_batch() -> None:
    records = [_record('small1', 2), _record('large', 100), _record('small2', 2), _record('small3', 2)]

    batches = list(batch_records_by_size(records, max_records=100, max_bytes=10))

    assert _keys(batches) == [['small1'], ['large'], ['small2', 'small3']]


def test_unknown_size_counts_as_empty() -> None:
    records = [_record('a', None), _record('b', None), _record('c', 10)]

    batches = list(batch_records_by_size(records, max_records=100, max_bytes=10))

    assert _keys(batches) == [['a', 'b', 'c']]


def test_no_records_yield_no_batches() -> None:
    assert list(batch_records_by_size([], max_records=100, max_bytes=10)) == []
