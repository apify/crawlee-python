# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/utils/src/internals/error_tracker.ts

from __future__ import annotations

import traceback
from collections import Counter, defaultdict
from dataclasses import dataclass



# Errors are internally tracked in deeply nested dict where individual error groups count can be accessed like this:
# count = ErrorStackTraceGroups["some_stack_trace"]["some_error_code"]["some_error_name"]["some_error_message"]
# if any of the filtering keys was not used it will be `None` instead.
# count = ErrorStackTraceGroups["some_stack_trace"][None]["some_error_name"][None]

ErrorMessageGroups = Counter[str|None]
ErrorTypeGroups = dict[str|None, ErrorMessageGroups]
#ErrorCodeGroups = dict[str|None, ErrorNameGroups] # No error codes in Python
ErrorStackTraceGroups = dict[str|None, ErrorTypeGroups]



class ErrorTracker:
    """Track errors and aggregates their counts by similarity."""

    def __init__(self,
                 show_error_name: bool = True,
                 show_stack_trace: bool = True,
                 show_full_stack: bool = False, # Todo, what does it do in JS?
                 show_error_message: bool = True,
                 show_full_message: bool = False, # Todo, seems like only first line otherwise
                 save_error_snapshots: bool = False, # Todo, what does it do in JS?
    ) -> None:
        self.show_error_name = show_error_name
        self.show_stack_trace = show_stack_trace
        self.show_full_stack = show_full_stack
        self.show_error_message = show_error_message
        self.show_full_message = show_full_message

        self._errors: ErrorStackTraceGroups = defaultdict(lambda: defaultdict(Counter))

    def add(self, error: Exception) -> None:
        """Include an error in the statistics."""
        # Check for similar
        error_group_name = error.__class__.__name__ if self.show_error_name else None
        error_group_message = str(error) if self.show_error_message else None
        if self.show_stack_trace:
            error_traceback = traceback.extract_tb(error.__traceback__)
            error_group_stack_trace = f"{error_traceback[0].filename}:{error_traceback[0].lineno}"
            # More extraction if full stack...
        else:
            error_group_stack_trace = None

        # TODO perform special message grouping
        # First just exact match

        self._errors[error_group_stack_trace][error_group_name].update([error_group_message])

    @property
    def unique_error_count(self) -> int:
        """Number of distinct kinds of errors."""
        return len(self._errors)

    @property
    def total(self) -> int:
        """Total number of errors."""
        return sum(self._errors.values())

    def _add_to_existing_similar_error_group(self):
        pass

    def _are_errors_similar(self) -> bool:
        pass
