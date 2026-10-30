import asyncio
from datetime import timedelta

from crawlee.autoscaling import (
    ConcurrencySystem,
    EventLoopLoadSignalOptions,
    LoadSignalsOptions,
)
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    concurrency_system = ConcurrencySystem(
        load_signals=LoadSignalsOptions(
            # The local storage client reports no rate-limit errors,
            # so there is no point in polling it every second.
            storage_client=False,
            # HTTP crawling barely blocks the event loop,
            # so measure it less often and tolerate more.
            event_loop=EventLoopLoadSignalOptions(
                snapshot_interval=timedelta(seconds=2),
                overloaded_ratio=0.7,
            ),
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
