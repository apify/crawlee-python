import asyncio

from crawlee import service_locator
from crawlee.crawlers import (
    BeautifulSoupCrawler,
    BeautifulSoupCrawlingContext,
)

# Disable clearing the `Queue` and `Dataset` on each run
# This allows you to continue from where you left off in the previous run
configuration = service_locator.get_configuration()
configuration.purge_on_start = False


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Limit the number of requests per run
        max_requests_per_crawl=10,
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from HTML
        data = {
            'url': context.request.url,
            'title': context.soup.title.string if context.soup.title else None,
            'h1s': [h1.text for h1 in context.soup.find_all('h1')],
            'h2s': [h2.text for h2 in context.soup.find_all('h2')],
            'h3s': [h3.text for h3 in context.soup.find_all('h3')],
        }

        # Save data to `Dataset`
        await context.push_data(data)

        # Extract links from HTML and add them to the `Queue`
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev'])

    # Export data from `Dataset` to `data.json` file
    # After each run, you will get previous results along with new ones
    await crawler.export_data_json('data.json')


if __name__ == '__main__':
    asyncio.run(main())
