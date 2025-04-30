# Inspiration: https://github.com/apify/crawlee/blob/v3.9.2/packages/utils/src/internals/error_tracker.ts

from __future__ import annotations

import traceback
from collections import Counter, defaultdict
from itertools import zip_longest
from logging import getLogger
from typing import TYPE_CHECKING, Union

from crawlee.statistics._error_snapshotter import ErrorSnapshotter

if TYPE_CHECKING:
    from crawlee._types import BasicCrawlingContext

GroupName = Union[str, None]
ErrorFilenameGroups = dict[GroupName, dict[GroupName, Counter[GroupName]]]


logger = getLogger(__name__)


class ErrorTracker:
    """Track errors and aggregates their counts by similarity."""

    def __init__(
        self,
        *,
        snapshot_kvs_name: str | None = None,
        show_error_name: bool = True,
        show_file_and_line_number: bool = True,
        show_error_message: bool = True,
        show_full_message: bool = False,
        save_error_snapshots: bool = False,
    ) -> None:
        self.error_snapshotter = ErrorSnapshotter(snapshot_kvs_name=snapshot_kvs_name) if save_error_snapshots else None
        self.show_error_name = show_error_name
        self.show_file_and_line_number = show_file_and_line_number
        self.show_error_message = show_error_message
        if show_full_message and not show_error_message:
            raise ValueError('`show_error_message` must be `True` if `show_full_message` is set to `True`')
        self.show_full_message = show_full_message
        self._errors: ErrorFilenameGroups = defaultdict(lambda: defaultdict(Counter))
        self._early_reported_errors = set[int]()

    async def add(
        self,
        error: Exception,
        *,
        context: BasicCrawlingContext | None = None,
        early: bool = False,
    ) -> None:
        """Add an error in the statistics.

        Args:
            error: Error to be added to statistics.
            context: Context used to collect error snapshot.
            early: Flag indicating that the error is added earlier than usual to have access to resources that will be
             closed before normal error collection. This prevents double reporting during normal error collection.
        """
        if id(error) in self._early_reported_errors:
            # Error had to be collected earlier before relevant resources are closed.
            self._early_reported_errors.remove(id(error))
            return

        if early:
            self._early_reported_errors.add(id(error))

        error_group_name = error.__class__.__name__ if self.show_error_name else None
        error_group_message = self._get_error_message(error)
        new_error_group_message = ''  # In case of wildcard similarity match
        error_group_file_and_line = self._get_file_and_line(error)

        # First two levels are grouped only in case of exact match.
        specific_groups = self._errors[error_group_file_and_line][error_group_name]

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
                self._errors[error_group_file_and_line][error_group_name].update([error_group_message])

        if (
            self._errors[error_group_file_and_line][error_group_name][new_error_group_message or error_group_message]
            == 1
            and context is not None
        ):
            # Save snapshot only on the first occurrence of the error and only if context and kvs was passed as well.
            await self._capture_error_snapshot(
                error_message=new_error_group_message or error_group_message,
                file_and_line=error_group_file_and_line,
                context=context,
            )

    async def _capture_error_snapshot(
        self, error_message: str, file_and_line: str, context: BasicCrawlingContext
    ) -> None:
        if self.error_snapshotter:
            try:
                await self.error_snapshotter.capture_snapshot(
                    error_message=error_message, file_and_line=file_and_line, context=context
                )
            except Exception:
                logger.exception(f'Error when trying to collect error snapshot for exception: {error_message}')

    def _get_file_and_line(self, error: Exception) -> str:
        if self.show_file_and_line_number:
            error_traceback = traceback.extract_tb(error.__traceback__)
            # Show only the most specific frame.
            return f'{error_traceback[-1].filename.split("/")[-1]}:{error_traceback[-1].lineno}'
        return ''

    def _get_error_message(self, error: Exception) -> str:
        if self.show_error_message:
            error_content = error.args[0] if error.args else error.__context__
            error_content = str(error_content) if error_content else error.__class__.__name__
            if self.show_full_message:
                return error_content
            return error_content.split('\n')[0]
        return ''

    @property
    def unique_error_count(self) -> int:
        """Number of distinct kinds of errors."""
        unique_error_count = 0
        for file_and_line_group in self._errors.values():
            for name_group in file_and_line_group.values():
                unique_error_count += len(name_group)
        return unique_error_count

    @property
    def total(self) -> int:
        """Total number of errors."""
        error_count = 0
        for file_and_line_group in self._errors.values():
            for name_group in file_and_line_group.values():
                error_count += sum(name_group.values())
        return error_count

    def get_most_common_errors(self, n: int = 3) -> list[tuple[str | None, int]]:
        """Return n most common errors."""
        all_errors: Counter[GroupName] = Counter()
        for file_and_line_group_name, file_and_line_group in self._errors.items():
            for name_group_name, name_group in file_and_line_group.items():
                for message_group_name, count in name_group.items():
                    all_errors[self._get_error_repr(file_and_line_group_name, name_group_name, message_group_name)] = (
                        count
                    )
        return all_errors.most_common(n)

    def _get_error_repr(self, file_and_line: str | None, name: str | None, message: str | None) -> str:
        """Get the most specific error representation."""
        file_and_line_part = f'{file_and_line}:' if file_and_line else ''
        name_part = f'{name}:' if name else ''
        message_part = f'{message}' if message else ''
        return f'{file_and_line_part}{name_part}{message_part}'

    @staticmethod
    def _create_generic_message(message_1: str | None, message_2: str | None) -> str:
        """Create a generic error message from two messages, if they are similar enough.

        Different parts of similar messages are replaced by `***`.
        """
        if message_1 is None or message_2 is None:
            return ''

        replacement_string = '***'
        replacement_count = 0

        generic_message_parts = []
        message_1_parts = message_1.split(' ')
        message_2_parts = message_2.split(' ')
        parts_count = min(len(message_1_parts), len(message_2_parts))

        for message_1_part, message_2_part in zip_longest(message_1_parts, message_2_parts, fillvalue=''):
            if message_1_part != message_2_part:
                generic_message_parts.append(replacement_string)
                replacement_count += 1
                if replacement_count >= parts_count / 2:
                    # Messages are too different.
                    return ''
            else:
                generic_message_parts.append(message_1_part)
        return ' '.join(generic_message_parts)
