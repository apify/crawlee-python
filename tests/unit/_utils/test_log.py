from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from crawlee._utils.log import LoggerOnce

if TYPE_CHECKING:
    import pytest


def test_first_call_logs(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger('crawlee.tests.log_dedup_first')
    logger_once = LoggerOnce(logger)
    with caplog.at_level(logging.INFO, logger=logger.name):
        logger_once.log('first', key='k1')
    assert [r.getMessage() for r in caplog.records] == ['first']


def test_duplicate_key_is_suppressed(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger('crawlee.tests.log_dedup_duplicate')
    logger_once = LoggerOnce(logger)
    with caplog.at_level(logging.INFO, logger=logger.name):
        logger_once.log('first', key='k1')
        logger_once.log('second', key='k1')
    assert [r.getMessage() for r in caplog.records] == ['first']


def test_distinct_keys_each_log(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger('crawlee.tests.log_dedup_distinct')
    logger_once = LoggerOnce(logger)
    with caplog.at_level(logging.INFO, logger=logger.name):
        logger_once.log('msg-a', key='k1')
        logger_once.log('msg-b', key='k2')
    assert [r.getMessage() for r in caplog.records] == ['msg-a', 'msg-b']


def test_separate_instances_have_independent_state(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger('crawlee.tests.log_dedup_independent')
    logger_once_a = LoggerOnce(logger)
    logger_once_b = LoggerOnce(logger)
    with caplog.at_level(logging.INFO, logger=logger.name):
        logger_once_a.log('from-a', key='k1')
        logger_once_b.log('from-b', key='k1')
    assert [r.getMessage() for r in caplog.records] == ['from-a', 'from-b']


def test_default_level_is_info(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger('crawlee.tests.log_dedup_default_level')
    logger_once = LoggerOnce(logger)
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        logger_once.log('msg', key='k')
    assert caplog.records[-1].levelno == logging.INFO


def test_log_emits_at_specified_level(caplog: pytest.LogCaptureFixture) -> None:
    logger = logging.getLogger('crawlee.tests.log_dedup_levels')
    logger_once = LoggerOnce(logger)
    with caplog.at_level(logging.DEBUG, logger=logger.name):
        logger_once.log('warn-msg', key='k_warn', level=logging.WARNING)
        logger_once.log('error-msg', key='k_error', level=logging.ERROR)

    levels = {r.getMessage(): r.levelno for r in caplog.records}
    assert levels['warn-msg'] == logging.WARNING
    assert levels['error-msg'] == logging.ERROR
