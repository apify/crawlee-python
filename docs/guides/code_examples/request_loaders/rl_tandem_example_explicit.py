import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.request_loaders import RequestList, RequestManagerTandem
from crawlee.storages import RequestQueue


async def main() -> None:
    # Create a static request list.
    request_list = RequestList(['https://crawlee.dev', 'https://apify.com'])

    # Open the default request queue.
    request_queue = await RequestQueue.open()

    # And combine them together to a sinhle request manager.
    request_manager = RequestManagerTandem(request_list, request_queue)

    # Create a crawler and pass the request manager to it.
    crawler = ParselCrawler(
        request_manager=request_manager,
        max_requests_per_crawl=10,  # Limit the max requests per crawl.
    )

    @crawler.router.default_handler
    async def handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # New links will be enqueued directly to the queue.
        await context.enqueue_links()

        # Extract data using Parsel's XPath and CSS selectors.
        data = {
            'url': context.request.url,
            'title': context.selector.xpath('//title/text()').get(),
        }

        # Push extracted data to the dataset.
        await context.push_data(data)

    await crawler.run()


if __name__ == '__main__':
    asyncio.run(main())
