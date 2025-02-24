# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/utils/src/internals/error_tracker.ts

from __future__ import annotations

import traceback
from collections import Counter, defaultdict
from itertools import zip_longest
from typing import Union

GroupName = Union[str, None]
ErrorMessageGroups = Counter[GroupName]
ErrorTypeGroups = dict[GroupName, ErrorMessageGroups]
ErrorStackTraceGroups = dict[GroupName, ErrorTypeGroups]


class ErrorTracker:
    """Track errors and aggregates their counts by similarity."""

    def __init__(
        self,
        *,
        show_error_name: bool = True,
        show_stack_trace: bool = True,
        show_error_message: bool = True,
        show_full_message: bool = False,
    ) -> None:
        self.show_error_name = show_error_name
        self.show_stack_trace = show_stack_trace
        self.show_error_message = show_error_message
        if show_full_message and not show_error_message:
            raise ValueError('`show_error_message` must be `True` if `show_full_message` is set to `True`')
        self.show_full_message = show_full_message
        self._errors: ErrorStackTraceGroups = defaultdict(lambda: defaultdict(Counter))

    def add(self, error: Exception) -> None:
        """Include an error in the statistics."""
        error_group_name = error.__class__.__name__ if self.show_error_name else None
        error_group_message = self._get_error_message(error)
        error_group_stack_trace = self._get_traceback_text(error)

        # First two levels are grouped only in case of exact match.
        specific_groups = self._errors[error_group_stack_trace][error_group_name]

        # Lowest level group is matched by similarity.
        if error_group_message in specific_groups:
            # Exact match.
            specific_groups.update([error_group_message])
        else:
            for existing_error_group_message in specific_groups:
                # Add to first group with similar text. Modify text with wildcard characters if necessary.
                if new_error_group_message := self._create_generic_message(
                    existing_error_group_message, error_group_message
                ):
                    # Replace old name.
                    specific_groups[new_error_group_message] = specific_groups.pop(existing_error_group_message)
                    # Increment.
                    specific_groups.update([new_error_group_message])
                    break
            else:
                # No similar message found. Create new group.
                self._errors[error_group_stack_trace][error_group_name].update([error_group_message])

    def _get_traceback_text(self, error: Exception) -> str | None:
        if self.show_stack_trace:
            error_traceback = traceback.extract_tb(error.__traceback__)
            return f'{error_traceback[0].filename.split("/")[-1]}:{error_traceback[0].lineno}'
        return None

    def _get_error_message(self, error: Exception) -> str | None:
        if self.show_error_message:
            if self.show_full_message:
                return str(error.args[0])
            return str(error.args[0]).split('\n')[0]
        return None

    @property
    def unique_error_count(self) -> int:
        """Number of distinct kinds of errors."""
        unique_error_count = 0
        for stack_group in self._errors.values():
            for name_group in stack_group.values():
                unique_error_count += len(name_group)
        return unique_error_count

    @property
    def total(self) -> int:
        """Total number of errors."""
        error_count = 0
        for stack_group in self._errors.values():
            for name_group in stack_group.values():
                error_count += sum(name_group.values())
        return error_count

    def get_most_common_errors(self, n: int = 3) -> list[tuple[str | None, int]]:
        """Return n most common errors."""
        all_errors: ErrorMessageGroups = Counter()
        for stack_group_name, stack_group in self._errors.items():
            for name_group_name, name_group in stack_group.items():
                for message_group_name, count in name_group.items():
                    all_errors[self._get_error_repr(stack_group_name, name_group_name, message_group_name)] = count
        return all_errors.most_common(n)

    def _get_error_repr(self, stack: str | None, name: str | None, message: str | None) -> str:
        """Get the most specific error representation."""
        stack_part = f'{stack}:' if stack else ''
        name_part = f'{name}:' if name else ''
        message_part = f'{message}' if message else ''
        return f'{stack_part}{name_part}{message_part}'

    @staticmethod
    def _create_generic_message(message_1: str | None, message_2: str | None) -> str | None:
        """Create a generic error message from two messages, if they are similar enough.

        Different parts of similar messages are replaced by `_`.
        """
        if message_1 is None or message_2 is None:
            return None

        replacement_symbol = '_'
        replacement_count = 0

        generic_message_parts = []
        message_1_parts = message_1.split(' ')
        message_2_parts = message_2.split(' ')
        parts_count = max(len(message_1_parts), len(message_2_parts))

        for message_1_part, message_2_part in zip_longest(message_1_parts, message_2_parts, fillvalue=''):
            if message_1_part != message_2_part:
                generic_message_parts.append(replacement_symbol)
                replacement_count += 1
                if replacement_count >= parts_count / 2:
                    # Messages are too different.
                    return ''
            else:
                generic_message_parts.append(message_1_part)
        return ' '.join(generic_message_parts)
