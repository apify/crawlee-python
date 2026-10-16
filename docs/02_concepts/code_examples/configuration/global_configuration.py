import asyncio
from datetime import timedelta

from crawlee import service_locator
from crawlee.configuration import Configuration
from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    # Register the configuration before any Crawlee component reads it.
    configuration = Configuration(
        log_level='WARNING',
        persist_state_interval=timedelta(seconds=30),
    )
    service_locator.set_configuration(configuration)

    # Any later lookup returns the same instance.
    global_configuration = Configuration.get_global_configuration()
    print(f'Same instance: {global_configuration is configuration}')

    # The crawler picks up the global configuration.
    crawler = ParselCrawler(max_requests_per_crawl=1)

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        # Logged at INFO level, so the WARNING threshold filters it out.
        context.log.info(f'Processing {context.request.url} ...')
        await context.push_data({'title': context.selector.css('title::text').get()})

    await crawler.run(['https://crawlee.dev'])

    data = await crawler.get_data()
    print(data.items)


if __name__ == '__main__':
    asyncio.run(main())
