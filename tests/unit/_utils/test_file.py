from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from crawlee._utils.file import json_dumps, validate_subdirectory

if TYPE_CHECKING:
    from pathlib import Path


async def test_json_dumps() -> None:
    assert await json_dumps({'key': 'value'}) == '{\n  "key": "value"\n}'
    assert await json_dumps(['one', 2, 3.0]) == '[\n  "one",\n  2,\n  3.0\n]'
    assert await json_dumps('string') == '"string"'
    assert await json_dumps(123) == '123'
    assert await json_dumps(datetime(2022, 1, 1, tzinfo=timezone.utc)) == '"2022-01-01 00:00:00+00:00"'


# Tests for validate_subdirectory (storage name/alias directory validation).


@pytest.mark.parametrize(
    'subdirectory',
    [
        pytest.param('my-store', id='simple'),
        pytest.param('store_with_underscores', id='underscores'),
        pytest.param('store.with.dots', id='dots'),
        pytest.param('__default__', id='reserved-default'),
        pytest.param('nested/inside', id='nested-but-contained'),
    ],
)
def test_validate_subdirectory_accepts_safe_segments(tmp_path: Path, subdirectory: str) -> None:
    base_dir = tmp_path / 'key_value_stores'
    result = validate_subdirectory(base_dir, subdirectory)
    # The resolved path must stay within the base directory.
    assert result.parent == base_dir or base_dir in result.parents


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
    ],
)
def test_validate_subdirectory_rejects_invalid_segments(tmp_path: Path, subdirectory: str) -> None:
    base_dir = tmp_path / 'key_value_stores'
    with pytest.raises(ValueError, match='Invalid storage name or alias'):
        validate_subdirectory(base_dir, subdirectory)
