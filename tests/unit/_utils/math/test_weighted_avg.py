from __future__ import annotations

import pytest

from crawlee._utils.math import get_weighted_avg


@pytest.mark.parametrize(
    ('values', 'weights', 'expected'),
    [
        ([20, 40, 50], [2, 3, 5], 41),
        ([1, 2, 3], [0.5, 0.25, 0.25], 1.75),
        ([4, 4, 4], [1, 0, 1], 4.0),
        ([1, 2, 3], [0.33, 0.33, 0.33], 2),
        ([1, 2, 3], [0.2, -0.3, 0.5], 2.75),
    ],
    ids=['basic', 'fractional_weights', 'zero_weight', 'all_equal_weights', 'negative_weights'],
)
def test__weighted_avg__basic(values: list[float], weights: list[float], expected: float) -> None:
    assert get_weighted_avg(values, weights) == expected


def test__weighted_avg__empty() -> None:
    values: list[float] = []
    weights: list[float] = []
    with pytest.raises(ValueError, match='Values and weights lists must not be empty'):
        get_weighted_avg(values, weights)


@pytest.mark.parametrize(
    ('values', 'weights'),
    [
        ([3, 2], [10]),
        ([2], [1, 5, 7]),
    ],
)
def test__weighted_avg__unequal_length_lists(values: list[float], weights: list[float]) -> None:
    with pytest.raises(ValueError, match='Values and weights must be of equal length'):
        get_weighted_avg(values, weights)


def test__weighted_avg__zero_total_weight() -> None:
    values: list[float] = [1, 2, 3]
    weights: list[float] = [0, 0, 0]
    with pytest.raises(ValueError, match='Total weight cannot be zero'):
        get_weighted_avg(values, weights)
