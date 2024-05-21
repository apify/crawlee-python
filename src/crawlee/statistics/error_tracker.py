# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/utils/src/internals/error_tracker.ts

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass


@dataclass(frozen=True, unsafe_hash=True)
class ErrorGroup:
    """Identifies a group of similar errors."""

    class_name: str | None


class ErrorTracker:
    """Track errors and aggregates their counts by similarity."""

    def __init__(self) -> None:
        self._errors = Counter[ErrorGroup]()

    def add(self, error: Exception) -> None:
        """Include an error in the statistics."""
        error_group = ErrorGroup(class_name=error.__class__.__name__)
        self._errors[error_group] += 1

    @property
    def unique_error_count(self) -> int:
        """Number of distinct kinds of errors."""
        return len(self._errors)

    @property
    def total(self) -> int:
        """Total number of errors."""
        return sum(self._errors.values())
