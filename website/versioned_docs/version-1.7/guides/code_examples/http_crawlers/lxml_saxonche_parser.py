import asyncio

from lxml import html
from pydantic import ValidationError
from saxonche import PySaxonProcessor

from crawlee import Request
from crawlee.crawlers import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    crawler = HttpCrawler(
        max_request_retries=1,
        max_requests_per_crawl=10,
    )

    # Create Saxon processor once and reuse across requests.
    saxon_proc = PySaxonProcessor(license=False)
    xpath_proc = saxon_proc.new_xpath_processor()

    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Parse HTML with lxml.
        parsed_html = html.fromstring(await context.http_response.read())
        # Convert relative URLs to absolute before extracting links.
        parsed_html.make_links_absolute(context.request.url, resolve_base_href=True)
        # Convert parsed HTML to XML for Saxon processing.
        xml = html.tostring(parsed_html, encoding='unicode', method='xml')
        # Parse XML with Saxon.
        parsed_xml = saxon_proc.parse_xml(xml_text=xml)
        # Set the parsed context for XPath evaluation.
        xpath_proc.set_context(xdm_item=parsed_xml)

        # Extract data using XPath 2.0 string() function.
        data = {
            'url': context.request.url,
            'title': xpath_proc.evaluate_single('.//title/string()'),
            'h1s': [str(h) for h in (xpath_proc.evaluate('//h1/string()') or [])],
            'h2s': [str(h) for h in (xpath_proc.evaluate('//h2/string()') or [])],
            'h3s': [str(h) for h in (xpath_proc.evaluate('//h3/string()') or [])],
        }
        await context.push_data(data)

        # XPath 2.0 with distinct-values() to get unique links and remove fragments.
        links_xpath = """
            distinct-values(
                for $href in //a/@href[
                    not(starts-with(., "#"))
                    and not(starts-with(., "javascript:"))
                    and not(starts-with(., "mailto:"))
                ]
                return replace($href, "#.*$", "")
            )
        """

        extracted_requests = []

        # Extract links.
        for item in xpath_proc.evaluate(links_xpath) or []:
            url = item.string_value
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
