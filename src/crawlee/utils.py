from __future__ import annotations


def weighted_avg(values: list[float], weights: list[float]) -> float:
    """Computes a weighted average of an array of numbers, complemented by an array of weights.

    Args:
        values: List of values.
        weights: List of weights.

    Raises:
        ValueError: If total weight is zero.

    Returns:
        float: Weighted average.
    """
    result = sum(value * weight for value, weight in zip(values, weights, strict=True))
    total_weight = sum(weights)

    if total_weight == 0:
        raise ValueError('Total weight cannot be zero')

    return result / total_weight
