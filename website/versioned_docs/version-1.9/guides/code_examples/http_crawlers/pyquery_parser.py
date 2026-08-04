import asyncio

from pydantic import ValidationError
from pyquery import PyQuery
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

        # Parse the HTML content using PyQuery.
        parsed_html = PyQuery(await context.http_response.read())

        # Extract data using jQuery-style selectors.
        data = {
            'url': context.request.url,
            'title': parsed_html('title').text(),
            'h1s': [h1.text() for h1 in parsed_html('h1').items()],
            'h2s': [h2.text() for h2 in parsed_html('h2').items()],
            'h3s': [h3.text() for h3 in parsed_html('h3').items()],
        }
        await context.push_data(data)

        # Css selector to extract valid href attributes.
        links_selector = (
            'a[href]:not([href^="#"]):not([href^="javascript:"]):not([href^="mailto:"])'
        )
        base_url = URL(context.request.url)

        extracted_requests = []

        # Extract links.
        for item in parsed_html(links_selector).items():
            href = item.attr('href')
            if not href:
                continue

            # Convert relative URLs to absolute if needed.
            url = str(base_url.join(URL(str(href))))
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
