import asyncio

from apify import Actor

from crawlee import Glob
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    async with Actor:
        crawler = BeautifulSoupCrawler()

        @crawler.router.default_handler
        async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
            url = context.request.url

            # Extract HTML title of the page.
            title_element = context.soup.find('title')
            title = title_element.text if title_element else ''
            context.log.info(f'Title of {url}: {title}')

            # Add URLs that match the provided pattern.
            await context.enqueue_links(include=[Glob('https://www.iana.org/*')])

            # Save extracted data to dataset.
            await context.push_data({'url': url, 'title': title})

        # Enqueue the initial request and run the crawler
        await crawler.run(['https://www.iana.org/'])


asyncio.run(main())
