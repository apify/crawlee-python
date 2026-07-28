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


@pytest.mark.parametrize(
    'collect_all_keys',
    [
        pytest.param(False, id='header from first item'),
        pytest.param(True, id='all keys collected'),
    ],
)
async def test_export_csv_to_stream_skips_empty_items(*, collect_all_keys: bool) -> None:
    """Empty mappings are skipped and neither define the header nor emit a blank row, in either column mode."""
    dst = StringIO()
    await export_csv_to_stream(
        async_iter([{}, {'id': 1, 'name': 'Item 1'}, {}, {'id': 2, 'name': 'Item 2'}]),
        dst,
        collect_all_keys=collect_all_keys,
        lineterminator='\n',
    )

    assert dst.getvalue() == 'id,name\n1,Item 1\n2,Item 2\n'


async def test_export_csv_to_stream_handles_empty_iterator() -> None:
    """An empty iterator produces no CSV content."""
    dst = StringIO()
    await export_csv_to_stream(async_iter([]), dst)

    assert dst.getvalue() == ''


@pytest.mark.parametrize(
    'items',
    [
        pytest.param([], id='no items'),
        pytest.param([{}, {}], id='only empty items'),
    ],
)
async def test_export_csv_to_stream_collects_all_keys_without_writable_items(
    items: list[Mapping[str, JsonSerializable]],
) -> None:
    """Key collection produces no CSV content, not a bare header line, when there is nothing to write."""
    dst = StringIO()
    await export_csv_to_stream(async_iter(items), dst, collect_all_keys=True)

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


async def test_export_csv_to_stream_warns_about_dropped_keys(caplog: pytest.LogCaptureFixture) -> None:
    """Keys dropped because they are absent from the first item are reported once, naming every dropped key."""
    dst = StringIO()
    with caplog.at_level('WARNING', logger='crawlee._utils.file'):
        await export_csv_to_stream(
            async_iter([{'name': 'Alice'}, {'name': 'Bob', 'city': 'NYC'}, {'name': 'Carol', 'age': 40}]),
            dst,
            lineterminator='\n',
        )

    assert dst.getvalue() == 'name\nAlice\nBob\nCarol\n'
    assert len(caplog.records) == 1
    assert 'age, city' in caplog.records[0].message
    assert 'collect_all_keys=True' in caplog.records[0].message


async def test_export_csv_to_stream_does_not_warn_when_nothing_is_dropped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Collecting all keys drops nothing, so it must not warn even for items with differing key sets."""
    dst = StringIO()
    with caplog.at_level('WARNING', logger='crawlee._utils.file'):
        await export_csv_to_stream(
            async_iter([{'name': 'Alice'}, {'name': 'Bob', 'city': 'NYC'}]),
            dst,
            collect_all_keys=True,
            lineterminator='\n',
        )

    assert dst.getvalue() == 'name,city\nAlice,\nBob,NYC\n'
    assert caplog.records == []


@pytest.mark.parametrize(
    'collect_all_keys',
    [
        pytest.param(False, id='header from first item'),
        pytest.param(True, id='all keys collected'),
    ],
)
async def test_export_csv_to_stream_rejects_invalid_writer_options_for_empty_iterator(
    *, collect_all_keys: bool
) -> None:
    """Writer options are validated up front, so a misconfigured export fails even when there is nothing to write."""
    with pytest.raises(TypeError, match='must be a 1-character string'):
        await export_csv_to_stream(async_iter([]), StringIO(), delimiter='ab', collect_all_keys=collect_all_keys)


async def test_export_csv_to_stream_honors_restval() -> None:
    """`restval` fills the cells of columns an item does not contain."""
    dst = StringIO()
    await export_csv_to_stream(
        async_iter([{'name': 'Alice'}, {'name': 'Bob', 'city': 'NYC'}]),
        dst,
        collect_all_keys=True,
        restval='N/A',
        lineterminator='\n',
    )

    assert dst.getvalue() == 'name,city\nAlice,N/A\nBob,NYC\n'


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
