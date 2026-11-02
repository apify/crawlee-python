import asyncio
import dataclasses
from datetime import datetime, timedelta

from crawlee.autoscaling import (
    ConcurrencySystem,
    CpuLoadSignal,
    LoadSignalsOptions,
    LoadSignalStartContext,
    LoadSnapshot,
)
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


class StickyCpuSignal:
    """Keeps reporting CPU overload for a while after the CPU recovers."""

    def __init__(self, cooldown: timedelta) -> None:
        # The built-in signal still does all the measuring,
        # this one only reinterprets what it measured.
        self._cpu = CpuLoadSignal()
        self._cooldown = cooldown

        # Taking the built-in name over is allowed only because
        # the built-in signal is switched off below.
        self.name = self._cpu.name
        self.overloaded_ratio = self._cpu.overloaded_ratio

    async def start(self, context: LoadSignalStartContext) -> None:
        await self._cpu.start(context)

    async def stop(self) -> None:
        await self._cpu.stop()

    def get_sample(self, duration: timedelta | None = None) -> list[LoadSnapshot]:
        overloaded_until: datetime | None = None
        sample = []

        # The whole history is marked, so that an overload older than `duration`
        # still extends the cooldown.
        for snapshot in self._cpu.get_sample():
            if snapshot.is_overloaded:
                overloaded_until = snapshot.created_at + self._cooldown

            # Scaling up right after the CPU recovers would overload it again.
            is_overloaded = (
                overloaded_until is not None and snapshot.created_at < overloaded_until
            )
            sample.append(dataclasses.replace(snapshot, is_overloaded=is_overloaded))

        if duration is None or not sample:
            return sample

        latest = sample[-1].created_at
        return [
            snapshot for snapshot in sample if latest - snapshot.created_at <= duration
        ]


async def main() -> None:
    concurrency_system = ConcurrencySystem(
        load_signals=LoadSignalsOptions(
            cpu=False,
            custom=[StickyCpuSignal(cooldown=timedelta(seconds=10))],
        ),
    )

    async with concurrency_system:
        crawler = BeautifulSoupCrawler(concurrency_system=concurrency_system)

        @crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
            context.log.info(f'Processing {context.request.url}...')

        await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
