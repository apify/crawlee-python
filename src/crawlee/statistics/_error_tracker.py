# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/utils/src/internals/error_tracker.ts

from __future__ import annotations

import traceback
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import zip_longest

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

        # All groups, except the lowest level is matched.
        specific_groups = self._errors[error_group_stack_trace][error_group_name]

        if error_group_message in specific_groups:
            # Exact match
            specific_groups.update([error_group_message])
        else:
            for known_error_group_message in specific_groups:
                if new_group_name:=self._create_generic_message(known_error_group_message, error_group_message):
                    # Replace old name
                    specific_groups[new_group_name]=specific_groups.pop(known_error_group_message)
                    # Increment
                    specific_groups.update([new_group_name])
                    break
            else:
                # No similar message found. Create new group.
                self._errors[error_group_stack_trace][error_group_name].update([error_group_message])


    @property
    def unique_error_count(self) -> int:
        """Number of distinct kinds of errors."""
        unique_error_count = 0
        for stack_group in self._errors.values():
            for name_group in stack_group.values():
                unique_error_count+=len(name_group)
        return unique_error_count

    @property
    def total(self) -> int:
        """Total number of errors."""
        error_count = 0
        for stack_group in self._errors.values():
            for name_group in stack_group.values():
                error_count+=sum(name_group.values())
        return error_count

    def get_most_popular_errors(self, n=3) -> str:

    def _add_to_existing_similar_message_group(self):
        # Check to if exactly same error group already exists
        # If not, try to assign to similar group. Iterate one by one and try to merge if possible
        pass

    def _create_generic_message(self, message_1: str, message_2: str) -> str:
        replacement_symbol = "_"
        replacement_count = 0
        message_1_parts = message_1.split(" ")
        message_2_parts = message_2.split(" ")
        generic_message_parts = []
        parts_count = max(len(message_1_parts), len(message_2_parts))

        for message_1_part, message_2_part in zip_longest(message_1_parts, message_2_parts, fillvalue=""):
            if message_1_part != message_2_part:
                generic_message_parts.append(replacement_symbol)
                replacement_count+=1
                if replacement_count >= parts_count/2:
                    # Messages are too different.
                    return ""
            else:
                generic_message_parts.append(message_1_part)
        return " ".join(generic_message_parts)

