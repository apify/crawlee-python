import asyncio

from pydantic import ValidationError
from scrapling.parser import Selector
from yarl import URL

from crawlee import Request
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    crawler = HttpCrawler(
        max_request_retries=1,
        max_requests_per_crawl=10,
    )

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Parse the HTML content using Scrapling.
        page = Selector(await context.http_response.read(), url=context.request.url)

        # Extract data using Xpath selectors with .get_all_text method for full text
        # content.
        title_el = page.xpath_first('//title')
        data = {
            'url': context.request.url,
            'title': title_el.text if isinstance(title_el, Selector) else title_el,
            'h1s': [
                h1.get_all_text() if isinstance(h1, Selector) else h1
                for h1 in page.xpath('//h1')
            ],
            'h2s': [
                h2.get_all_text() if isinstance(h2, Selector) else h2
                for h2 in page.xpath('//h2')
            ],
            'h3s': [
                h3.get_all_text() if isinstance(h3, Selector) else h3
                for h3 in page.xpath('//h3')
            ],
        }
        await context.push_data(data)

        # Css selector to extract valid href attributes.
        links_selector = (
            'a[href]:not([href^="#"]):not([href^="javascript:"]):not([href^="mailto:"])'
        )
        base_url = URL(context.request.url)
        extracted_requests = []

        # Extract links.
        for item in page.css(links_selector):
            href = item.attrib.get('href') if isinstance(item, Selector) else None
            if not href:
                continue

            # Convert relative URLs to absolute if needed.
            url = str(base_url.join(URL(href)))
            try:
                request = Request.from_url(url)
            except ValidationError as exc:
                context.log.warning(f'Skipping invalid URL "{url}": {exc}')
                continue
            extracted_requests.append(request)

        # Add extracted requests to the queue with the same-domain strategy.
        await context.add_requests(extracted_requests, strategy='same-domain')

    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
