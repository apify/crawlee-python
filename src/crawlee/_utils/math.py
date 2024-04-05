from __future__ import annotations


def compute_weighted_avg(values: list[float], weights: list[float]) -> float:
    """Computes a weighted average of an array of numbers, complemented by an array of weights.

    Args:
        values: List of values.
        weights: List of weights.

    Raises:
        ValueError: If total weight is zero.

    Returns:
        float: Weighted average.
    """
    if not values or not weights:
        raise ValueError('Values and weights lists must not be empty')

    if len(values) != len(weights):
        raise ValueError('Values and weights must be of equal length')

    # zip(..., strict=True) can be used once support for Python 3.9 is dropped
    result = sum(value * weight for value, weight in zip(values, weights))
    total_weight = sum(weights)

    if total_weight == 0:
        raise ValueError('Total weight cannot be zero')

    return result / total_weight
