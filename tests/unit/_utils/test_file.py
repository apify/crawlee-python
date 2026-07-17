from __future__ import annotations

from datetime import datetime, timezone
from io import StringIO
from typing import TYPE_CHECKING

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


async def _async_iter(
    items: list[Mapping[str, JsonSerializable]],
) -> AsyncIterator[Mapping[str, JsonSerializable]]:
    for item in items:
        yield item


async def test_export_csv_to_stream_keeps_columns_aligned_for_heterogeneous_items() -> None:
    """Values must be written under their own header column even when items have different key orders/sets."""
    dst = StringIO()
    await export_csv_to_stream(
        _async_iter(
            [
                {'name': 'Alice', 'age': 30},
                {'name': 'Bob', 'city': 'NYC', 'age': 25},
                {'age': 40, 'name': 'Carol'},
            ]
        ),
        dst,
        lineterminator='\n',
    )

    assert dst.getvalue() == 'name,age,city\nAlice,30,\nBob,25,NYC\nCarol,40,\n'


async def test_export_csv_to_stream_skips_empty_items() -> None:
    """Empty mappings are skipped and do not define or shift the header."""
    dst = StringIO()
    await export_csv_to_stream(
        _async_iter([{}, {'id': 1, 'name': 'Item 1'}, {}, {'id': 2, 'name': 'Item 2'}]),
        dst,
        lineterminator='\n',
    )

    assert dst.getvalue() == 'id,name\n1,Item 1\n2,Item 2\n'


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
