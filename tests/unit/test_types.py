from __future__ import annotations

import pytest

from crawlee import ConcurrencySettings


@pytest.mark.parametrize(
    ('kwargs', 'expected_desired_concurrency'),
    [
        pytest.param({}, 10, id='no args keeps default'),
        pytest.param({'max_concurrency': 5}, 5, id='clamped down to max'),
        pytest.param({'min_concurrency': 20}, 20, id='clamped up to min'),
        pytest.param({'max_concurrency': 50}, 10, id='default fits unchanged'),
        pytest.param({'max_concurrency': 10}, 10, id='max equals default'),
        pytest.param({'min_concurrency': 3, 'max_concurrency': 3}, 3, id='min equals max'),
    ],
)
def test_concurrency_settings_default_desired_concurrency(
    kwargs: dict[str, int], expected_desired_concurrency: int
) -> None:
    """Test that the implicit `desired_concurrency` default is clamped to the min/max bounds."""
    settings = ConcurrencySettings(**kwargs)
    assert settings.desired_concurrency == expected_desired_concurrency


@pytest.mark.parametrize(
    ('kwargs', 'match'),
    [
        pytest.param(
            {'max_concurrency': 5, 'desired_concurrency': 8},
            'desired_concurrency cannot be greater than max_concurrency',
            id='explicit above max',
        ),
        pytest.param(
            {'min_concurrency': 5, 'desired_concurrency': 2},
            'desired_concurrency cannot be less than min_concurrency',
            id='explicit below min',
        ),
    ],
)
def test_concurrency_settings_explicit_desired_concurrency_out_of_bounds(kwargs: dict[str, int], match: str) -> None:
    """Test that an explicitly passed `desired_concurrency` outside the min/max bounds raises `ValueError`."""
    with pytest.raises(ValueError, match=match):
        ConcurrencySettings(**kwargs)
