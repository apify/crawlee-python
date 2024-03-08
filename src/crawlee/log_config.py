from __future__ import annotations

import json
import logging
import textwrap
import traceback
from typing import Any

from colorama import Fore, Style, just_fix_windows_console

just_fix_windows_console()

_LOG_NAME_COLOR = Fore.LIGHTBLACK_EX

_LOG_LEVEL_COLOR = {
    logging.DEBUG: Fore.BLUE,
    logging.INFO: Fore.GREEN,
    logging.WARNING: Fore.YELLOW,
    logging.ERROR: Fore.RED,
    logging.CRITICAL: Fore.RED,
}

_LOG_LEVEL_SHORT_ALIAS = {
    logging.DEBUG: 'DEBUG',
    logging.INFO: 'INFO ',
    logging.WARNING: 'WARN ',
    logging.ERROR: 'ERROR',
}

# So that all the log messages have the same alignment
_LOG_MESSAGE_INDENT = ' ' * 6


class CrawleeLogFormatter(logging.Formatter):
    """Log formatter that prints out the log message nicely formatted, with colored level and stringified extra fields.

    It formats the log records so that they:
        - start with the level (colorized, and padded to 5 chars so that it is nicely aligned)
        - then have the actual log message, if it's multiline then it's nicely indented
        - then have the stringified extra log fields
        - then, if an exception is a part of the log record, prints the formatted exception.
    """

    # The fields that are added to the log record with `logger.log(..., extra={...})` are just merged in the log record
    # with the other log record properties, and you can't get them in some nice, isolated way. So, to get the extra
    # fields, we just compare all the properties present in the log record with properties present in an empty log
    # record, and extract all the extra ones not present in the empty log record.
    empty_record = logging.LogRecord('dummy', 0, 'dummy', 0, 'dummy', None, None)

    def __init__(
        self,
        include_logger_name: bool = True,  # noqa: FBT001, FBT002
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Create a new instance.

        Args:
            include_logger_name: Include logger name at the beginning of the log line.
            args: Arguments passed to the parent class.
            kwargs: Keyword arguments passed to the parent class.
        """
        super().__init__(*args, **kwargs)
        self.include_logger_name = include_logger_name

    def _get_extra_fields(self, record: logging.LogRecord) -> dict[str, Any]:
        extra_fields: dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key not in self.empty_record.__dict__:
                extra_fields[key] = value

        return extra_fields

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record nicely.

        This formats the log record so that it:
            - starts with the level (colorized, and padded to 5 chars so that it is nicely aligned)
            - then has the actual log message, if it's multiline then it's nicely indented
            - then has the stringified extra log fields
            - then, if an exception is a part of the log record, prints the formatted exception.
        """
        logger_name_string = f'{_LOG_NAME_COLOR}[{record.name}]{Style.RESET_ALL} '

        # Colorize the log level, and shorten it to 6 chars tops
        level_color_code = _LOG_LEVEL_COLOR.get(record.levelno, '')
        level_short_alias = _LOG_LEVEL_SHORT_ALIAS.get(record.levelno, record.levelname)
        level_string = f'{level_color_code}{level_short_alias}{Style.RESET_ALL} '

        # Format the exception, if there is some
        # Basically just print the traceback and indent it a bit
        exception_string = ''
        if record.exc_info:
            exc_info = record.exc_info
            record.exc_info = None
            exception_string = ''.join(traceback.format_exception(*exc_info)).rstrip()
            exception_string = '\n' + textwrap.indent(exception_string, _LOG_MESSAGE_INDENT)

        # Format the extra log record fields, if there were some
        # Just stringify them to JSON and color them gray
        extra_string = ''
        extra = self._get_extra_fields(record)
        if extra:
            extra_string = (
                f' {Fore.LIGHTBLACK_EX}({json.dumps(extra, ensure_ascii=False, default=str)}){Style.RESET_ALL}'
            )

        # Format the actual log message, and indent everything but the first line
        log_string = super().format(record)
        log_string = textwrap.indent(log_string, _LOG_MESSAGE_INDENT).lstrip()

        if self.include_logger_name:
            # Include logger name at the beginning of the log line
            return f'{logger_name_string}{level_string}{log_string}{extra_string}{exception_string}'

        return f'{level_string}{log_string}{extra_string}{exception_string}'
