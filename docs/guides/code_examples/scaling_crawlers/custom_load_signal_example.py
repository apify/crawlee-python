import asyncio
import shutil
from contextlib import suppress
from datetime import timedelta

from crawlee.autoscaling import (
    ConcurrencySystem,
    LoadSignalsOptions,
    LoadSignalStartContext,
    LoadSnapshot,
    SnapshotStore,
)
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


class DiskSpaceSignal:
    """Reports overload while the disk the crawl writes to is nearly full."""

    name = 'disk_space'
    overloaded_ratio = 0.5

    def __init__(self, path: str, *, min_free_ratio: float = 0.1) -> None:
        self._path = path
        self._min_free_ratio = min_free_ratio
        self._store = SnapshotStore[LoadSnapshot]()
        self._task: asyncio.Task | None = None

    async def start(self, context: LoadSignalStartContext) -> None:
        # Keep the window the signal is sampled over,
        # and drop anything measured before a restart.
        self._store.use_sample_window(context.max_sample_window)
        self._store.clear()
        self._task = asyncio.create_task(self._measure())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task

    def get_sample(self, duration: timedelta | None = None) -> list[LoadSnapshot]:
        return self._store.get_sample(duration)

    async def _measure(self) -> None:
        while True:
            usage = shutil.disk_usage(self._path)
            is_overloaded = usage.free / usage.total < self._min_free_ratio
            self._store.push(LoadSnapshot(is_overloaded=is_overloaded))
            await asyncio.sleep(1)


async def main() -> None:
    concurrency_system = ConcurrencySystem(
        load_signals=LoadSignalsOptions(custom=[DiskSpaceSignal('.')]),
    )

    async with concurrency_system:
        crawler = BeautifulSoupCrawler(concurrency_system=concurrency_system)

        @crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
            context.log.info(f'Processing {context.request.url}...')

        await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
