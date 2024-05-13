# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/utils/src/internals/error_tracker.ts

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, unsafe_hash=True)
class ErrorGroup:
    class_name: str | None


class ErrorTracker:
    def __init__(self) -> None:
        self._errors = dict[ErrorGroup, int]()

    def add(self, error: Exception) -> None:
        error_group = ErrorGroup(class_name=error.__class__.__name__)
        self._errors.setdefault(error_group, 0)
        self._errors[error_group] += 1

    @property
    def unique_error_count(self) -> int:
        return len(self._errors)

    @property
    def total(self) -> int:
        return sum(self._errors.values())
