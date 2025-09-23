from contextlib import nullcontext
from typing import Any

import pytest

from crawlee._utils.limit_kwargs import limit_kwarg_count


@pytest.mark.parametrize(
    ('kwargs', 'should_raise'),
    [
        ({'alias': 'alias', 'name': None, 'id': None}, False),
        ({'alias': None, 'name': 'name', 'id': None}, False),
        ({'alias': None, 'name': None, 'id': 'id'}, False),
        ({'alias': 'alias', 'name': 'name', 'id': None}, True),
        ({'alias': 'alias', 'name': None, 'id': 'id'}, True),
        ({'alias': None, 'name': 'name', 'id': 'id'}, True),
        ({'alias': 'alias', 'name': 'name', 'id': 'id'}, True),
        ({'alias': None, 'name': None, 'id': None}, False),
    ],
)
def test_limit_kwargs_default(kwargs: dict[str, Any], *, should_raise: bool) -> None:
    context = pytest.raises(ValueError, match=r'^Only one of .*') if should_raise else nullcontext()
    with context:
        limit_kwarg_count(**kwargs)


@pytest.mark.parametrize(
    ('kwargs', 'should_raise'),
    [
        ({'alias': 'alias', 'name': 'name', 'id': 'id'}, True),
        ({'alias': 'alias', 'name': 'name', 'id': None}, False),
    ],
)
def test_limit_kwargs(kwargs: dict[str, Any], *, should_raise: bool) -> None:
    context = pytest.raises(ValueError, match=r'^Only one of .*') if should_raise else nullcontext()
    with context:
        limit_kwarg_count(max_kwargs=2, **kwargs)
