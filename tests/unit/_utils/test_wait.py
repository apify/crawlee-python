from __future__ import annotations

import asyncio
import logging
from datetime import timedelta

from crawlee._utils.wait import wait_for_all_tasks_to_finish


async def test_zero_timeout_cancels_right_away() -> None:
    """A zero timeout cancels the unfinished tasks without waiting for them."""
    task = asyncio.create_task(asyncio.Event().wait())

    # Capped, as a zero timeout taken for no limit would wait for the task forever.
    await asyncio.wait_for(
        wait_for_all_tasks_to_finish([task], logger=logging.getLogger(__name__), timeout=timedelta(0)),
        timeout=5,
    )

    assert task.cancelled()
