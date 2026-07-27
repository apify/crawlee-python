from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
from typing import TYPE_CHECKING, cast

import pytest

from crawlee._utils.file import export_csv_to_stream, json_dumps, validate_subdirectory

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Mapping
    from pathlib import Path

    from crawlee._types import JsonSerializable


async def test_json_dumps() -> None:
    assert await json_dumps({'key': 'value'}) == '{\n  "key": "value"\n}'
    assert await json_dumps(['one', 2, 3.0]) == '[\n  "one",\n  2,\n  3.0\n]'
    assert await json_dumps('string') == '"string"'
    assert await json_dumps(123) == '123'
    assert await json_dumps(datetime(2022, 1, 1, tzinfo=timezone.utc)) == '"2022-01-01 00:00:00+00:00"'


# Tests for export_csv_to_stream (dataset CSV export).


async def async_iter(
    items: list[Mapping[str, JsonSerializable]],
) -> AsyncIterator[Mapping[str, JsonSerializable]]:
    for item in items:
        yield item


async def test_export_csv_to_stream_keeps_columns_aligned_for_heterogeneous_items() -> None:
    """Values must be written under their own header column even when items have different key orders/sets."""
    dst = StringIO()
    await export_csv_to_stream(
        async_iter(
            [
                {'name': 'Alice', 'age': 30},
                {'name': 'Bob', 'city': 'NYC', 'age': 25},
                {'age': 40, 'name': 'Carol'},
            ]
        ),
        dst,
        lineterminator='\n',
    )

    assert dst.getvalue() == 'name,age\nAlice,30\nBob,25\nCarol,40\n'


async def test_export_csv_to_stream_collects_all_keys_when_requested() -> None:
    """All item keys are included when key collection is enabled."""
    dst = StringIO()
    await export_csv_to_stream(
        async_iter([{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'city': 'NYC', 'age': 25}]),
        dst,
        collect_all_keys=True,
        lineterminator='\n',
    )

    assert dst.getvalue() == 'name,age,city\nAlice,30,\nBob,25,NYC\n'


async def test_export_csv_to_stream_skips_empty_items() -> None:
    """Empty mappings are skipped and do not define or shift the header."""
    dst = StringIO()
    await export_csv_to_stream(
        async_iter([{}, {'id': 1, 'name': 'Item 1'}, {}, {'id': 2, 'name': 'Item 2'}]),
        dst,
        lineterminator='\n',
    )

    assert dst.getvalue() == 'id,name\n1,Item 1\n2,Item 2\n'


async def test_export_csv_to_stream_handles_empty_iterator() -> None:
    """An empty iterator produces no CSV content."""
    dst = StringIO()
    await export_csv_to_stream(async_iter([]), dst)

    assert dst.getvalue() == ''


async def test_export_csv_to_stream_writes_before_consuming_all_items() -> None:
    """The default export writes its header before requesting the second item."""
    dst = StringIO()

    async def items() -> AsyncIterator[Mapping[str, JsonSerializable]]:
        yield {'id': 1}
        assert dst.getvalue() == 'id\n1\n'
        yield {'id': 2}

    await export_csv_to_stream(items(), dst, lineterminator='\n')

    assert dst.getvalue() == 'id\n1\n2\n'


async def test_export_csv_to_stream_preserves_non_json_values() -> None:
    """CSV values do not pass through JSON serialization."""
    dst = StringIO()
    value = datetime(2020, 1, 1, tzinfo=timezone.utc)

    await export_csv_to_stream(
        async_iter([{'created_at': cast('JsonSerializable', value)}]),
        dst,
        lineterminator='\n',
    )

    assert dst.getvalue() == 'created_at\n2020-01-01 00:00:00+00:00\n'


# Tests for validate_subdirectory (storage name/alias directory validation).


@pytest.mark.parametrize(
    'subdirectory',
    [
        pytest.param('my-store', id='simple'),
        pytest.param('store_with_underscores', id='underscores'),
        pytest.param('store.with.dots', id='dots'),
        pytest.param('__default__', id='reserved-default'),
    ],
)
def test_validate_subdirectory_accepts_safe_segments(tmp_path: Path, subdirectory: str) -> None:
    base_dir = tmp_path / 'key_value_stores'
    result = validate_subdirectory(base_dir, subdirectory)
    # The resolved path must be a direct child of the base directory.
    assert result.parent == base_dir


@pytest.mark.parametrize(
    'subdirectory',
    [
        pytest.param('../outside', id='parent-ref'),
        pytest.param('../../outside', id='deep-parent-ref'),
        pytest.param('..', id='bare-parent'),
        pytest.param('.', id='bare-current'),
        pytest.param('a/../../outside', id='mixed-parent-ref'),
        pytest.param('/etc/passwd', id='absolute-path'),
        pytest.param('', id='empty'),
        pytest.param('nested/inside', id='nested-path'),
        pytest.param('with/slash', id='with-slash'),
    ],
)
def test_validate_subdirectory_rejects_invalid_segments(tmp_path: Path, subdirectory: str) -> None:
    base_dir = tmp_path / 'key_value_stores'
    with pytest.raises(ValueError, match='Invalid storage name or alias'):
        validate_subdirectory(base_dir, subdirectory)
