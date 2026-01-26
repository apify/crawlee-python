import asyncio
from datetime import timedelta

import pytest

from crawlee._utils.time import SharedTimeout, measure_time


async def test_shared_timeout_tracks_elapsed_time() -> None:
    timeout_duration = timedelta(seconds=1)
    shared_timeout = SharedTimeout(timeout_duration)

    # First usage
    async with shared_timeout:
        await asyncio.sleep(0.2)

    # Second usage - should have less time remaining
    async with shared_timeout as remaining:
        assert remaining < timedelta(seconds=0.85)
        assert remaining > timedelta(seconds=0)


async def test_shared_timeout_expires() -> None:
    timeout_duration = timedelta(seconds=0.1)
    shared_timeout = SharedTimeout(timeout_duration)

    with measure_time() as elapsed, pytest.raises(asyncio.TimeoutError):
        async with shared_timeout:
            await asyncio.sleep(0.5)

    assert elapsed.wall is not None
    assert elapsed.wall < 0.3


async def test_shared_timeout_cannot_be_nested() -> None:
    timeout_duration = timedelta(seconds=1)
    shared_timeout = SharedTimeout(timeout_duration)

    async with shared_timeout:
        with pytest.raises(RuntimeError, match='cannot be entered twice'):
            async with shared_timeout:
                pass


async def test_shared_timeout_multiple_sequential_uses() -> None:
    """Test that SharedTimeout can be used multiple times sequentially."""
    timeout_duration = timedelta(seconds=1)
    shared_timeout = SharedTimeout(timeout_duration)

    for _ in range(5):
        async with shared_timeout:
            await asyncio.sleep(0.05)

    # Should have consumed roughly 0.25 seconds
    async with shared_timeout as remaining:
        assert remaining < timedelta(seconds=0.8)
        assert remaining > timedelta(seconds=0)
