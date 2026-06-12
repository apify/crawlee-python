import asyncio

from pydantic import ValidationError
from selectolax.lexbor import LexborHTMLParser
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

        # Parse the HTML content using Selectolax with Lexbor backend.
        parsed_html = LexborHTMLParser(await context.http_response.read())

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': parsed_html.css_first('title').text(),
            'h1s': [h1.text() for h1 in parsed_html.css('h1')],
            'h2s': [h2.text() for h2 in parsed_html.css('h2')],
            'h3s': [h3.text() for h3 in parsed_html.css('h3')],
        }
        await context.push_data(data)

        # Css selector to extract valid href attributes.
        links_selector = (
            'a[href]:not([href^="#"]):not([href^="javascript:"]):not([href^="mailto:"])'
        )
        base_url = URL(context.request.url)
        extracted_requests = []

        # Extract links.
        for item in parsed_html.css(links_selector):
            href = item.attributes.get('href')
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
