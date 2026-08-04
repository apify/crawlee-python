import asyncio

from crawlee import Request
from crawlee.crawlers import BasicCrawlingContext, HttpCrawler, HttpCrawlingContext
from crawlee.errors import ProxyError


async def main() -> None:
    # Set how many session rotations will happen before calling the error handler
    # when ProxyError occurs
    crawler = HttpCrawler(max_session_rotations=5, max_request_retries=6)

    # For this example, we'll create a proxy error in our handler
    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        raise ProxyError('Simulated proxy error')

    # This handler runs after all retry attempts are exhausted
    @crawler.failed_request_handler
    async def failed_handler(context: BasicCrawlingContext, error: Exception) -> None:
        context.log.error(f'Failed request {context.request.url}, after 5 rotations')
        request = context.request
        # For proxy errors, we can add a new `Request` to try again
        if isinstance(error, ProxyError) and not request.unique_key.startswith('retry'):
            context.log.info(f'Retrying {request.url} ...')
            # Create a new `Request` with a modified key to avoid deduplication
            new_request = Request.from_url(
                request.url, unique_key=f'retry{request.unique_key}'
            )

            # Add the new `Request` to the `Queue`
            rq = await crawler.get_request_manager()
            await rq.add_request(new_request)

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
