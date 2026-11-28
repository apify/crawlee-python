import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.proxy_configuration import ProxyConfiguration
from crawlee.sessions import SessionPool


async def main() -> None:
    # To use the proxy IP session rotation logic, you must turn the proxy usage on.
    proxy_configuration = ProxyConfiguration(
        # options
    )

    session_pool = SessionPool(max_pool_size=100, proxy_configuration=proxy_configuration)

    # Initialize crawler with a custom SessionPool configuration
    # to manage concurrent sessions and proxy rotation
    crawler = PlaywrightCrawler(
        # Overrides default Session pool configuration.
        session_pool=session_pool,
    )

    # Define the default request handler that manages session states
    # based on the response content and potential blocking
    @crawler.router.default_handler
    async def default_handler(context: PlaywrightCrawlingContext) -> None:
        title = await context.page.title()

        if title == 'Blocked':
            context.session.retire()
        elif title == 'Not sure if blocked, might also be a connection error':
            context.session.mark_bad()
        else:
            context.session.mark_good()  # BasicCrawler handles this automatically.

    async with session_pool:
        await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
