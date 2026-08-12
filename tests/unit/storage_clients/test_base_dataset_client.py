from __future__ import annotations

import pytest

from crawlee.storage_clients._base import DatasetClient
from crawlee.storage_clients._memory import MemoryDatasetClient


@pytest.mark.parametrize(
    ('data', 'expected'),
    [
        pytest.param([{'a': 1}, {'a': 2}], True, id='sequence of items'),
        pytest.param([], True, id='empty sequence'),
        pytest.param({'a': 1}, False, id='single item'),
        pytest.param({}, False, id='empty single item'),
    ],
)
def test_is_sequence_of_items_distinguishes_payload_shapes(
    data: list[dict[str, int]] | dict[str, int],
    *,
    expected: bool,
) -> None:
    """`_is_sequence_of_items` reports whether a `push_data` payload holds many items or just one."""
    assert DatasetClient._is_sequence_of_items(data) is expected


def test_is_sequence_of_items_is_inherited_by_concrete_clients() -> None:
    """Concrete clients inherit `_is_sequence_of_items`, the way the Apify SDK up to 4.0.1 calls it on itself."""
    assert MemoryDatasetClient._is_sequence_of_items([{'a': 1}]) is True
    assert MemoryDatasetClient._is_sequence_of_items({'a': 1}) is False
