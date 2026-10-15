import asyncio

from crawlee import ConcurrencySettings
from crawlee.crawlers import (
    ParselCrawler,
    ParselCrawlingContext,
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
)
from crawlee.sessions import SessionPool
from crawlee.storages import RequestQueue


async def main() -> None:
    # Open request queues for both crawlers with different aliases
    playwright_rq = await RequestQueue.open(alias='playwright-requests')
    parsel_rq = await RequestQueue.open(alias='parsel-requests')

    # Use a shared session pool between both crawlers
    async with SessionPool() as session_pool:
        playwright_crawler = PlaywrightCrawler(
            # Set the request queue for Playwright crawler
            request_manager=playwright_rq,
            session_pool=session_pool,
            # Configure concurrency settings for Playwright crawler
            concurrency_settings=ConcurrencySettings(
                max_concurrency=5, desired_concurrency=5
            ),
            # Set `keep_alive`` so that the crawler does not stop working when there are
            # no requests in the queue.
            keep_alive=True,
        )

        parsel_crawler = ParselCrawler(
            # Set the request queue for Parsel crawler
            request_manager=parsel_rq,
            session_pool=session_pool,
            # Configure concurrency settings for Parsel crawler
            concurrency_settings=ConcurrencySettings(
                max_concurrency=10, desired_concurrency=10
            ),
            # Set maximum requests per crawl for Parsel crawler
            max_requests_per_crawl=50,
        )

        @playwright_crawler.router.default_handler
        async def handle_playwright(context: PlaywrightCrawlingContext) -> None:
            context.log.info(f'Playwright Processing {context.request.url}...')

            title = await context.page.title()
            # Push the extracted data to the dataset for Playwright crawler
            await context.push_data(
                {'title': title, 'url': context.request.url, 'source': 'playwright'},
                dataset_name='playwright-data',
            )

        @parsel_crawler.router.default_handler
        async def handle_parsel(context: ParselCrawlingContext) -> None:
            context.log.info(f'Parsel Processing {context.request.url}...')

            title = context.parsed_content.css('title::text').get()
            # Push the extracted data to the dataset for Parsel crawler
            await context.push_data(
                {'title': title, 'url': context.request.url, 'source': 'parsel'},
                dataset_name='parsel-data',
            )

            # Enqueue links to the Playwright request queue for blog pages
            await context.enqueue_links(
                selector='a[href*="/blog/"]', rq_alias='playwright-requests'
            )
            # Enqueue other links to the Parsel request queue
            await context.enqueue_links(selector='a:not([href*="/blog/"])')

        # Start the Playwright crawler in the background
        background_crawler_task = asyncio.create_task(playwright_crawler.run([]))

        # Run the Parsel crawler with the initial URL and wait for it to finish
        await parsel_crawler.run(['https://crawlee.dev/blog'])

        # Wait for the Playwright crawler to finish processing all requests
        while not await playwright_rq.is_empty():
            playwright_crawler.log.info('Waiting for Playwright crawler to finish...')
            await asyncio.sleep(5)

        # Stop the Playwright crawler after all requests are processed
        playwright_crawler.stop()

        # Wait for the background Playwright crawler task to complete
        await background_crawler_task


if __name__ == '__main__':
    asyncio.run(main())
