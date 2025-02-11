import asyncio

from crawlee._types import BasicCrawlingContext
from crawlee.crawlers import BeautifulSoupCrawler


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Keep the crawler alive even when there are no requests to be processed now.
        keep_alive=True,
    )

    def stop_crawler_if_url_visited(context: BasicCrawlingContext) -> None:
        """Stop crawler once specific url is visited.

        Example of guard condition to stop the crawler."""
        if context.request.url == 'https://crawlee.dev/docs/examples':
            crawler.stop(
                'Stop crawler that was in keep_alive state after specific url was visite'
            )
        else:
            context.log.info('keep_alive=True, waiting for more requests to come.')

    async def add_request_later(url: str, after_s: int) -> None:
        """Add requests to the queue after some time. Can be done by external code."""
        # Just an example of request being added to the crawler later,
        # when it is waiting due to `keep_alive=True`.
        await asyncio.sleep(after_s)
        await crawler.add_requests([url])

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BasicCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Stop crawler if some guard condition has been met.
        stop_crawler_if_url_visited(context)

    # Start some tasks that will add some requests later to simulate real situation,
    # where requests are added later by external code.
    add_request_later_task1 = asyncio.create_task(
        add_request_later(url='https://crawlee.dev', after_s=1)
    )
    add_request_later_task2 = asyncio.create_task(
        add_request_later(url='https://crawlee.dev/docs/examples', after_s=5)
    )

    # Run the crawler without the initial list of requests.
    # Wait for more requests to be added to the queue later due to `keep_alive=True`.
    await crawler.run()

    await asyncio.gather(add_request_later_task1, add_request_later_task2)


if __name__ == '__main__':
    asyncio.run(main())
