# ruff: noqa: FBT003 # Boolean positional value in function call

from __future__ import annotations

import asyncio
import logging
from itertools import chain, repeat
from typing import TYPE_CHECKING, TypeVar
from unittest.mock import AsyncMock, Mock

import pytest

from crawlee import ConcurrencySettings
from crawlee._utils.time import measure_time
from crawlee.autoscaling import AutoscaledPool, ConcurrencyConsumer
from crawlee.autoscaling._types import ConcurrencySystemInterface
from tests.unit.utils import poll_until_condition

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from crawlee.autoscaling import ConcurrencySystem


T = TypeVar('T')

CONSUMER = ConcurrencyConsumer(id='test-pool')


def future(value: T, /) -> Awaitable[T]:
    """Wrap a value in an already resolved future, for the pool's synchronous predicates."""
    f = asyncio.Future[T]()
    f.set_result(value)
    return f


async def test_refuses_inactive_system(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """The pool cannot start a system it does not own, so it refuses to run against one that was never entered."""
    pool = AutoscaledPool(
        concurrency_system=make_concurrency_system(),
        consumer=CONSUMER,
        run_task_function=lambda _: future(None),
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(False),
    )

    with pytest.raises(RuntimeError, match='not active'):
        await pool.run()


async def test_reports_system_concurrency() -> None:
    """Both concurrency numbers come from the system, so that a shared budget is reported as one."""
    concurrency_system = Mock(spec=ConcurrencySystemInterface)
    concurrency_system.desired_concurrency = 7
    concurrency_system.current_concurrency = 3

    pool = AutoscaledPool(
        concurrency_system=concurrency_system,
        consumer=CONSUMER,
        run_task_function=lambda _: future(None),
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(False),
    )

    assert pool.concurrency_system is concurrency_system
    assert pool.desired_concurrency == 7
    assert pool.current_concurrency == 3

    # A pool that has not run yet has nothing of its own in flight, whatever the system reports.
    assert pool.own_concurrency == 0


async def test_retries_refused_booking() -> None:
    """A slot taken by another pool between the capacity check and the booking only defers the task."""
    concurrency_system = Mock(spec=ConcurrencySystemInterface)
    concurrency_system.active = True
    concurrency_system.has_capacity_for_task.return_value = True
    attempts = 0
    done_count = 0

    def try_register_task_start(*_args: object) -> bool:
        """Accept the second booking attempt only, so that the first task start is refused and later retried."""
        nonlocal attempts
        attempts += 1
        return attempts == 2

    concurrency_system.try_register_task_start.side_effect = try_register_task_start

    async def run(_task_id: str) -> None:
        nonlocal done_count
        done_count += 1

    pool = AutoscaledPool(
        concurrency_system=concurrency_system,
        consumer=CONSUMER,
        run_task_function=run,
        is_task_ready_function=lambda: future(done_count < 1),
        is_finished_function=lambda: future(done_count >= 1),
    )

    await asyncio.wait_for(pool.run(), timeout=5)

    assert done_count == 1
    assert attempts >= 2


async def test_own_concurrency_excludes_others(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """A shared budget counts every booking, but a pool's own concurrency counts only the tasks it started."""
    release = asyncio.Event()

    async def run(_task_id: str) -> None:
        await release.wait()

    settings = ConcurrencySettings(min_concurrency=2, max_concurrency=2, desired_concurrency=2)

    async with make_concurrency_system(settings) as concurrency_system:
        # Another pool sharing the system holds one of the two slots.
        assert concurrency_system.try_register_task_start(ConcurrencyConsumer(id='other-pool'), '0')

        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(not release.is_set()),
            is_finished_function=lambda: future(release.is_set()),
        )

        pool_run_task = asyncio.create_task(pool.run(), name='pool run task')
        try:
            assert await poll_until_condition(lambda: pool.own_concurrency == 1, timeout=5.0)

            assert pool.current_concurrency == 2
            assert pool.own_concurrency == 1
        finally:
            release.set()
            await asyncio.gather(pool_run_task, return_exceptions=True)


async def test_finishes_only_when_idle(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """The pool asks whether it's finished only while none of its tasks are in flight."""
    started = 0
    own_concurrency_at_checks = list[int]()

    async def run(_task_id: str) -> None:
        nonlocal started
        started += 1
        # The second task outlives the first, so the pool looks for new work while a task is still running.
        await asyncio.sleep(0.01 if started == 1 else 0.1)

    async def is_finished() -> bool:
        own_concurrency_at_checks.append(pool.own_concurrency)
        return True

    async with make_concurrency_system() as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=AsyncMock(side_effect=chain([True, True], repeat(False))),
            is_finished_function=is_finished,
        )

        await asyncio.wait_for(pool.run(), timeout=5)

    assert started == 2
    assert own_concurrency_at_checks == [0]


async def test_finishes_while_system_saturated(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A pool out of work finishes even while another pool keeps a shared system saturated."""
    settings = ConcurrencySettings(min_concurrency=1, max_concurrency=1, desired_concurrency=1)
    is_task_ready = AsyncMock(return_value=False)

    async with make_concurrency_system(settings) as concurrency_system:
        # Another pool sharing the system holds the only slot.
        assert concurrency_system.try_register_task_start(ConcurrencyConsumer(id='other-pool'), '0')

        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=AsyncMock(),
            is_task_ready_function=is_task_ready,
            is_finished_function=lambda: future(True),
        )

        await asyncio.wait_for(pool.run(), timeout=5)

    # The pool never had capacity to ask for a task, so it finished from the saturated branch.
    is_task_ready.assert_not_awaited()


async def test_does_not_finish_while_paused(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A paused pool with no work left finishes only once it's resumed."""
    is_finished = AsyncMock(return_value=True)

    async with make_concurrency_system() as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=AsyncMock(),
            is_task_ready_function=lambda: future(False),
            is_finished_function=is_finished,
        )
        pool.pause()

        pool_run_task = asyncio.create_task(pool.run(), name='pool run task')
        try:
            await asyncio.sleep(0.1)
            is_finished.assert_not_awaited()

            pool.resume()
            await asyncio.wait_for(pool_run_task, timeout=5)
        finally:
            pool_run_task.cancel()
            await asyncio.gather(pool_run_task, return_exceptions=True)

    is_finished.assert_awaited_once()


@pytest.mark.run_alone
async def test_runs_concurrently(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """The pool fills the whole budget the system allows, rather than running the tasks one after another."""
    done_count = 0
    running_count = 0
    max_running_count = 0

    async def run(_task_id: str) -> None:
        nonlocal done_count, running_count, max_running_count
        running_count += 1
        max_running_count = max(max_running_count, running_count)
        await asyncio.sleep(0.1)
        running_count -= 1
        done_count += 1

    settings = ConcurrencySettings(min_concurrency=10, max_concurrency=10)

    async with make_concurrency_system(settings) as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(done_count < 10),
            is_finished_function=lambda: future(done_count >= 10),
        )

        await pool.run()

    # Assert the tasks actually ran concurrently by checking the peak number that overlapped in time.
    # A sequential (or only partially concurrent) pool would peak below the configured concurrency.
    assert max_running_count == 10
    assert done_count >= 10


async def test_passes_task_id_to_task(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """Every task is told the id it was booked under, and the ids of concurrent tasks differ."""
    task_ids = list[str]()

    async def run(task_id: str) -> None:
        task_ids.append(task_id)
        await asyncio.sleep(0.05)

    async with make_concurrency_system(
        ConcurrencySettings(min_concurrency=3, max_concurrency=3, desired_concurrency=3)
    ) as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(len(task_ids) < 3),
            is_finished_function=lambda: future(len(task_ids) >= 3),
        )

        await pool.run()

    assert len(set(task_ids)) == 3


async def test_abort_works(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """An abort interrupts the tasks in progress and returns their slots to the system."""

    async def run(_task_id: str) -> None:
        await asyncio.sleep(60)

    settings = ConcurrencySettings(min_concurrency=10, max_concurrency=10)

    async with make_concurrency_system(settings) as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(True),
            is_finished_function=lambda: future(False),
        )

        with measure_time() as elapsed:
            run_task = asyncio.create_task(pool.run(), name='pool run task')
            await asyncio.sleep(0.1)
            assert pool.current_concurrency == 10
            await pool.abort()
            assert pool.current_concurrency == 0
            await run_task

    assert elapsed.wall is not None
    assert elapsed.wall < 5


async def test_abort_releases_unstarted_slot(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A task aborted before its first step still returns its slot."""
    run = AsyncMock()
    abort_tasks = list[asyncio.Task]()

    async def is_task_ready() -> bool:
        # Abort while this check is pending, so that the task it books is cancelled before it starts.
        abort_tasks.append(asyncio.create_task(pool.abort()))
        await asyncio.sleep(0)
        return True

    async with make_concurrency_system() as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=is_task_ready,
            is_finished_function=lambda: future(False),
        )

        await pool.run()
        await asyncio.gather(*abort_tasks)

        run.assert_not_called()
        assert concurrency_system.current_concurrency == 0


async def test_propagates_exceptions(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A failing task stops the pool, returns its slot, and its error reaches the caller of `run`."""
    done_count = 0

    async def run(_task_id: str) -> None:
        await asyncio.sleep(0.1)
        nonlocal done_count
        done_count += 1

        if done_count > 5:
            raise RuntimeError('Scheduled crash')

    settings = ConcurrencySettings(min_concurrency=10, max_concurrency=10)

    async with make_concurrency_system(settings) as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(done_count < 20),
            is_finished_function=lambda: future(done_count >= 20),
        )

        with pytest.raises(RuntimeError, match=r'Scheduled crash'):
            await pool.run()

        assert concurrency_system.current_concurrency == 0

    assert done_count < 20


async def test_orchestrator_error_awaits_workers(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """A scheduling error reaches the caller only after the worker tasks that were still running have finished."""
    worker_started = asyncio.Event()
    worker_finished = asyncio.Event()
    check_failed = asyncio.Event()
    release_worker = asyncio.Event()
    checks = 0

    async def run(_task_id: str) -> None:
        worker_started.set()
        await release_worker.wait()
        worker_finished.set()

    async def is_task_ready() -> bool:
        nonlocal checks
        checks += 1
        if checks > 1:
            await worker_started.wait()
            check_failed.set()
            raise RuntimeError('Queue status unavailable')
        return True

    async with make_concurrency_system() as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=is_task_ready,
            is_finished_function=lambda: future(False),
        )
        pool_run_task = asyncio.create_task(pool.run())
        try:
            await asyncio.wait_for(check_failed.wait(), timeout=5)
            assert not pool_run_task.done()
            release_worker.set()
            with pytest.raises(RuntimeError, match='Queue status unavailable'):
                await asyncio.wait_for(pool_run_task, timeout=5)
            assert worker_finished.is_set()
        finally:
            release_worker.set()
            await asyncio.gather(pool_run_task, return_exceptions=True)


async def test_orchestrator_error_logged_on_cancel(
    make_concurrency_system: type[ConcurrencySystem], caplog: pytest.LogCaptureFixture
) -> None:
    """A scheduling error raised while the run is being cancelled is logged, as the cancelled result cannot carry it."""

    async def is_finished() -> bool:
        pool_run_task.cancel()
        raise RuntimeError('Queue status unavailable')

    async with make_concurrency_system() as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=lambda _: future(None),
            is_task_ready_function=lambda: future(False),
            is_finished_function=is_finished,
        )

        with caplog.at_level(logging.ERROR, logger='crawlee.autoscaling._autoscaled_pool'):
            pool_run_task = asyncio.create_task(pool.run())
            with pytest.raises(asyncio.CancelledError):
                await pool_run_task

    assert 'Unpropagated exception in worker task orchestrator' in caplog.text
    assert 'Queue status unavailable' in caplog.text


async def test_propagates_exceptions_after_finished(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A task that fails once no work is left still fails the run, instead of the pool just finishing."""
    is_finished = AsyncMock(return_value=True)

    async def run(_task_id: str) -> None:
        await asyncio.sleep(0.1)
        raise RuntimeError('Scheduled crash')

    async with make_concurrency_system() as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=AsyncMock(side_effect=chain([True], repeat(False))),
            is_finished_function=is_finished,
        )

        with pytest.raises(RuntimeError, match=r'Scheduled crash'):
            await asyncio.wait_for(pool.run(), timeout=5)

    # The pool never asked whether it was finished while the failing task was still running.
    is_finished.assert_not_awaited()


async def test_propagates_task_timeout_error(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """A `TimeoutError` raised by the task function propagates instead of being swallowed by the pool."""
    started_count = 0

    async def run(_task_id: str) -> None:
        nonlocal started_count
        started_count += 1
        raise TimeoutError('Fetching a session from the pool timed out after 10 seconds')

    async with make_concurrency_system(
        ConcurrencySettings(min_concurrency=1, max_concurrency=1, desired_concurrency=1)
    ) as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(started_count == 0),
            is_finished_function=lambda: future(started_count > 0),
        )

        with pytest.raises(TimeoutError, match=r'Fetching a session from the pool timed out'):
            await pool.run()


async def test_allows_multiple_run_calls(make_concurrency_system: type[ConcurrencySystem]) -> None:
    """A finished pool can be run again against the same system."""
    done_count = 0

    async def run(_task_id: str) -> None:
        nonlocal done_count
        done_count += 1
        await asyncio.sleep(0.1)

    async with make_concurrency_system(
        ConcurrencySettings(min_concurrency=4, max_concurrency=4, desired_concurrency=4)
    ) as concurrency_system:
        pool = AutoscaledPool(
            concurrency_system=concurrency_system,
            consumer=CONSUMER,
            run_task_function=run,
            is_task_ready_function=lambda: future(done_count < 4),
            is_finished_function=lambda: future(done_count >= 4),
        )

        await pool.run()
        assert done_count == 4

        done_count = 0

        await pool.run()
        assert done_count == 4


async def test_fails_when_system_closes(
    make_concurrency_system: type[ConcurrencySystem],
) -> None:
    """Closing a system under a running pool is a lifecycle error, so the run fails instead of crawling blind."""
    settings = ConcurrencySettings(min_concurrency=1, max_concurrency=1, desired_concurrency=1)
    concurrency_system = make_concurrency_system(settings)

    async def run(_task_id: str) -> None:
        await asyncio.sleep(0.05)

    await concurrency_system.__aenter__()

    pool = AutoscaledPool(
        concurrency_system=concurrency_system,
        consumer=CONSUMER,
        run_task_function=run,
        is_task_ready_function=lambda: future(True),
        is_finished_function=lambda: future(False),
    )

    pool_run_task = asyncio.create_task(pool.run(), name='pool run task')
    await asyncio.sleep(0.1)
    await concurrency_system.__aexit__(None, None, None)

    with pytest.raises(RuntimeError, match='not active'):
        await asyncio.wait_for(pool_run_task, timeout=5)
