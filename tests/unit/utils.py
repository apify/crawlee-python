from __future__ import annotations

import asyncio
import sys
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

run_alone_on_mac = pytest.mark.run_alone if sys.platform == 'darwin' else lambda x: x


async def wait_for_condition(
    condition: Callable[[], bool],
    *,
    timeout: float = 5.0,
    poll_interval: float = 0.05,
) -> None:
    """Poll `condition` until it returns True, or raise `AssertionError` on timeout.

    Args:
        condition: A callable that returns True when the desired state is reached.
        timeout: Maximum time in seconds to wait before raising.
        poll_interval: Time in seconds between condition checks.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if condition():
            return
        await asyncio.sleep(poll_interval)
    raise AssertionError(f'Condition not met within {timeout}s')
