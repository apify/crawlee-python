import asyncio
from contextvars import copy_context
from datetime import timedelta

import pytest

from crawlee._utils.time import SharedTimeout, _active_timeout_extenders, extend_active_timeouts


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

    # The body outlasts the budget a hundredfold, so the `TimeoutError` alone shows the budget cut it short. A
    # wall-clock bound would also measure how long the event loop was starved on a loaded runner.
    with pytest.raises(TimeoutError):
        async with shared_timeout:
            await asyncio.sleep(10)


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


async def test_extend_adds_to_budget() -> None:
    """Test that the extension also counts towards the later uses of the timeout."""
    shared_timeout = SharedTimeout(timedelta(seconds=1))

    async with shared_timeout:
        extend_active_timeouts(timedelta(seconds=5))

    async with shared_timeout as remaining:
        assert remaining > timedelta(seconds=5)


async def test_extend_outside_is_noop() -> None:
    """Test that extending outside an entered timeout leaves the timeout as it is."""
    shared_timeout = SharedTimeout(timedelta(seconds=1))

    extend_active_timeouts(timedelta(seconds=5))

    async with shared_timeout as remaining:
        assert remaining == timedelta(seconds=1)


async def test_extend_reaches_enclosing() -> None:
    """Test that the timeouts enclosing the innermost one are extended as well."""
    async with SharedTimeout(timedelta(seconds=0.2)), SharedTimeout(timedelta(seconds=0.2)):
        extend_active_timeouts(timedelta(seconds=5))
        await asyncio.sleep(0.4)


async def test_extend_stops_at_expired() -> None:
    """Test that extending an expired timeout does nothing and leaves the enclosing timeouts as they are."""

    async def extend_after_expiry() -> None:
        async with SharedTimeout(timedelta(seconds=0.1)):
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                extend_active_timeouts(timedelta(seconds=5))
                raise

    async def outlive_enclosing_timeout() -> None:
        async with SharedTimeout(timedelta(seconds=0.3)):
            with pytest.raises(TimeoutError):
                await extend_after_expiry()

            await asyncio.sleep(1)

    with pytest.raises(TimeoutError):
        await outlive_enclosing_timeout()


async def test_extend_after_exit_budget_only() -> None:
    """Test that extending through a copied context of an exited timeout only adds to its budget."""
    outer_timeout = SharedTimeout(timedelta(seconds=1))
    inner_timeout = SharedTimeout(timedelta(seconds=1))

    async with outer_timeout:
        async with inner_timeout:
            context = copy_context()

        context.run(extend_active_timeouts, timedelta(seconds=5))

    async with inner_timeout as inner_remaining:
        assert inner_remaining > timedelta(seconds=5)

    async with outer_timeout as outer_remaining:
        assert outer_remaining <= timedelta(seconds=1)


async def test_extend_from_thread_raises() -> None:
    """Test that extending from another thread raises and keeps the timeout armed."""

    async def extend_from_thread() -> None:
        async with SharedTimeout(timedelta(seconds=0.3)):
            with pytest.raises(RuntimeError, match='other threads'):
                await asyncio.to_thread(extend_active_timeouts, timedelta(seconds=5))

            await asyncio.sleep(1)

    with pytest.raises(TimeoutError):
        await extend_from_thread()


async def test_expired_timeout_is_removed() -> None:
    """Test that a timeout that expired is removed from the active timeouts of the task."""

    async def expire() -> None:
        async with SharedTimeout(timedelta(seconds=0.1)):
            await asyncio.sleep(1)

    with pytest.raises(TimeoutError):
        await expire()

    assert _active_timeout_extenders.get() == ()
