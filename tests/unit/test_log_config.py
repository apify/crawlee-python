from __future__ import annotations

import logging
import sys

import pytest

from crawlee._log_config import CrawleeLogFormatter


def get_log_record(level: int, msg: str, exc_info: logging._SysExcInfoType | None = None) -> logging.LogRecord:
    return logging.LogRecord(
        name='test',
        level=level,
        pathname=__file__,
        lineno=0,
        msg=msg,
        args=(),
        exc_info=exc_info,
    )


@pytest.mark.parametrize(
    ('level', 'msg', 'expected'),
    [
        (logging.DEBUG, 'Debug message', '\x1b[90m[test]\x1b[0m \x1b[34mDEBUG\x1b[0m Debug message'),
        (logging.INFO, 'Info message', '\x1b[90m[test]\x1b[0m \x1b[32mINFO \x1b[0m Info message'),
        (logging.WARNING, 'Warning message', '\x1b[90m[test]\x1b[0m \x1b[33mWARN \x1b[0m Warning message'),
        (logging.ERROR, 'Error message', '\x1b[90m[test]\x1b[0m \x1b[31mERROR\x1b[0m Error message'),
    ],
    ids=['debug', 'info', 'warning', 'error'],
)
def test_formatted_message(level: int, msg: str, expected: str) -> None:
    formatter = CrawleeLogFormatter()
    record = get_log_record(level, msg)
    formatted_message = formatter.format(record)
    assert formatted_message == expected


def test_formatting_with_exception() -> None:
    formatter = CrawleeLogFormatter()
    try:
        raise ValueError('This is a test exception')

    except ValueError:
        exc_info = sys.exc_info()
        record = get_log_record(logging.ERROR, 'Exception occurred', exc_info=exc_info)
        formatted_message = formatter.format(record)

        assert '\x1b[90m[test]\x1b[0m \x1b[31mERROR\x1b[0m Exception occurred' in formatted_message
        assert 'ValueError: This is a test exception' in formatted_message


def test_formatter_without_name() -> None:
    formatter = CrawleeLogFormatter(include_logger_name=False)
    record = get_log_record(logging.INFO, 'Info message without name')
    formatted_message = formatter.format(record)
    assert formatted_message == '\x1b[32mINFO \x1b[0m Info message without name'
