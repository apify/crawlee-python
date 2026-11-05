import asyncio

from crawlee.crawlers import BasicCrawlingContext, HttpCrawler, HttpCrawlingContext
from crawlee.errors import HttpStatusCodeError, SessionError


async def main() -> None:
    crawler = HttpCrawler(max_request_retries=5)

    # Create a parsing error for demonstration
    @crawler.router.default_handler
    async def default_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')
        raise ValueError('Simulated parsing error')

    # This handler runs before any retry attempts
    @crawler.error_handler
    async def retry_handler(context: BasicCrawlingContext, error: Exception) -> None:
        context.log.error(f'Failed request {context.request.url}')
        # Only allow retries for network-related errors
        if not isinstance(error, (SessionError, HttpStatusCodeError)):
            context.log.error('Non-network error detected')
            # Stop further retry attempts for this `Request`
            context.request.no_retry = True

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
