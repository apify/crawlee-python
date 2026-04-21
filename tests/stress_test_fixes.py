from __future__ import annotations

import asyncio

import pytest

from typing import Any

from crawlee import Request
from crawlee.crawlers import BasicCrawler
from crawlee.errors import CriticalError, NonRetryableError


async def test_non_retryable_error_not_retried() -> None:
    """Stress test: Ensure NonRetryableError prevents subsequent retries instantly."""
    runs = 0

    async def _handler(context: Any) -> None:
        nonlocal runs
        runs += 1
        raise NonRetryableError("This request should not be retried under any circumstances.")

    crawler = BasicCrawler(
        request_handler=_handler,
        max_request_retries=5,
    )

    await crawler.run(['http://tests.crawlee.com/non-retryable'])

    # The crawler should process the URL exactly once, ignoring max_request_retries.
    assert runs == 1, f"Expected 1 run, but handler was executed {runs} times."


async def test_critical_error_aborts_crawler() -> None:
    """Stress test: Ensure CriticalError aborts the entire crawler immediately."""
    runs = 0

    async def _handler(context: Any) -> None:
        nonlocal runs
        runs += 1
        raise CriticalError("System-level critical failure simulation.")

    crawler = BasicCrawler(
        request_handler=_handler,
        max_request_retries=3,
    )

    # A CriticalError should escape the internal loop and cause the run to fail by surfacing
    with pytest.raises(CriticalError, match="System-level critical failure simulation."):
        await crawler.run(['http://tests.crawlee.com/critical'])

    assert runs == 1, f"Expected crawler to abort instantly, but ran {runs} times."
