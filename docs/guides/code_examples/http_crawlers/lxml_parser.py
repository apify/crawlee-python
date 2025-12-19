import asyncio

from lxml import html
from pydantic import ValidationError

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

        # Parse the HTML content using lxml.
        parsed_html = html.fromstring(await context.http_response.read())

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': parsed_html.findtext('.//title'),
            'h1s': [h1.text_content() for h1 in parsed_html.findall('.//h1')],
            'h2s': [h2.text_content() for h2 in parsed_html.findall('.//h2')],
            'h3s': [h3.text_content() for h3 in parsed_html.findall('.//h3')],
        }
        await context.push_data(data)

        # Convert relative URLs to absolute before extracting links.
        parsed_html.make_links_absolute(context.request.url, resolve_base_href=True)

        # Xpath 1.0 selector for extracting valid href attributes.
        links_xpath = (
            '//a/@href[not(starts-with(., "#")) '
            'and not(starts-with(., "javascript:")) '
            'and not(starts-with(., "mailto:"))]'
        )

        extracted_requests = []

        # Extract links.
        for url in parsed_html.xpath(links_xpath):
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
