import asyncio

from crawlee import Request
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.proxy_configuration import ProxyConfiguration

CHEAP_PROXY_URL = 'http://cheap-datacenter-proxy.com/'
PREMIUM_PROXY_URL = 'http://expensive-residential-proxy.com/'


def pick_proxy_url(
    session_id: str | None = None,  # noqa: ARG001
    request: Request | None = None,
) -> str | None:
    # The first attempt goes without a proxy.
    if request is None or request.retry_count == 0:
        return None

    # The first retry uses the cheap proxy, any further retry the expensive one.
    return CHEAP_PROXY_URL if request.retry_count == 1 else PREMIUM_PROXY_URL


async def main() -> None:
    # Create a ProxyConfiguration object and pass it to the crawler.
    proxy_configuration = ProxyConfiguration(new_url_function=pick_proxy_url)
    crawler = PlaywrightCrawler(proxy_configuration=proxy_configuration)

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def default_handler(context: PlaywrightCrawlingContext) -> None:
        # Log the proxy used for the current request.
        context.log.info(f'Proxy for the current request: {context.proxy_info}')

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
