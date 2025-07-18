import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext
from crawlee.http_clients import CurlImpersonateHttpClient


async def main() -> None:
    http_client = CurlImpersonateHttpClient(
        # Optional additional keyword arguments for `curl_cffi.requests.AsyncSession`.
        timeout=10,
        impersonate='chrome131',
    )

    crawler = ParselCrawler(
        http_client=http_client,
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Enqueue all links from the page.
        await context.enqueue_links()

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': context.selector.css('title::text').get(),
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
