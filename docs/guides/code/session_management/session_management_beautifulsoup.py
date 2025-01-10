import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.proxy_configuration import ProxyConfiguration
from crawlee.sessions import SessionPool


async def main() -> None:
    # To use the proxy IP session rotation logic, you must turn the proxy usage on.
    proxy_configuration = ProxyConfiguration(
        # options
    )

    # Initialize crawler with a custom SessionPool configuration
    # to manage concurrent sessions and proxy rotation
    crawler = BeautifulSoupCrawler(
        proxy_configuration=proxy_configuration,
        # Activates the Session pool (default is true).
        use_session_pool=True,
        # Overrides default Session pool configuration.
        session_pool=SessionPool(max_pool_size=100),
    )

    # Define the default request handler that manages session states
    # based on the response content and potential blocking
    @crawler.router.default_handler
    async def default_handler(context: BeautifulSoupCrawlingContext) -> None:
        title = context.soup.title.get_text() if context.soup.title else None

        if context.session:
            if title == 'Blocked':
                context.session.retire()
            elif title == 'Not sure if blocked, might also be a connection error':
                context.session.mark_bad()
            else:
                context.session.mark_good()  # BasicCrawler handles this automatically.

    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
