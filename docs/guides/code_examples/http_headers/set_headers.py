import asyncio

from crawlee import HttpHeaders, Request
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext
from crawlee.http_clients import ImpitHttpClient


async def main() -> None:
    # Set default headers on the client. They are sent on every request.
    http_client = ImpitHttpClient(headers={'X-Api-Key': 'secret'})

    crawler = HttpCrawler(http_client=http_client)

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        # `httpbin.org/headers` echoes the received request headers back.
        response = (await context.http_response.read()).decode()
        context.log.info(response)

    # Add a header for this request only. It merges with the client defaults.
    request = Request.from_url(
        'https://httpbin.org/headers',
        headers=HttpHeaders({'Accept': 'application/json'}),
        # Both requests target the same URL. Without a distinct `unique_key`,
        # deduplication would drop this one.
        unique_key='set-headers-example',
    )

    await crawler.run(['https://httpbin.org/headers', request])


if __name__ == '__main__':
    asyncio.run(main())
