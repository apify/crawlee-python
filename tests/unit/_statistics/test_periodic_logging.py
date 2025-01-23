from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import TYPE_CHECKING

from crawlee.statistics import Statistics

if TYPE_CHECKING:
    import pytest


async def test_periodic_logging(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.INFO)

    log_message = 'Periodic statistics XYZ'
    statistics = Statistics.with_default_state(log_interval=timedelta(milliseconds=50), log_message=log_message)

    async with statistics:
        await asyncio.sleep(0.1)

    matching_records = [rec for rec in caplog.records if rec.message.startswith(log_message)]
    assert len(matching_records) >= 1
