import asyncio
import io
import logging
from functools import partial
from pathlib import Path

from playwright.async_api import Request
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from crawlee.crawlers import (
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)


async def archive_response(
    request: Request, writer: WARCWriter, logger: logging.Logger
) -> None:
    """Helper function for archiving response in WARC format."""
    response = await request.response()
    if not response:
        logger.warning(f'Could not get response {request.url}')
        return
    try:
        response_body = await response.body()
    except Exception as e:
        logger.warning(f'Could not get response body for {response.url}: {e}')
        return
    logger.info(f'Archiving resource {response.url}')
    response_payload_stream = io.BytesIO(response_body)
    response_headers = StatusAndHeaders(
        str(response.status), response.headers, protocol='HTTP/1.1'
    )
    response_record = writer.create_warc_record(
        response.url,
        'response',
        payload=response_payload_stream,
        length=len(response_body),
        http_headers=response_headers,
    )
    writer.write_record(response_record)


async def main() -> None:
    crawler = PlaywrightCrawler(
        max_requests_per_crawl=1,
        headless=False,
    )

    # Create a WARC archive file a prepare the writer.
    archive = Path('example.warc.gz')
    with archive.open('wb') as output:
        writer = WARCWriter(output, gzip=True)

        # Create a WARC info record to store metadata about the archive.
        warcinfo_payload = {
            'software': 'Crawlee',
            'format': 'WARC/1.1',
            'description': 'Example archive created with PlaywrightCrawler',
        }
        writer.write_record(writer.create_warcinfo_record(archive.name, warcinfo_payload))

        @crawler.pre_navigation_hook
        async def archiving_hook(context: PlaywrightPreNavCrawlingContext) -> None:
            # Ensure that all responses with additional resources are archived
            context.page.on(
                'requestfinished',
                partial(archive_response, logger=context.log, writer=writer),
            )

        @crawler.router.default_handler
        async def request_handler(context: PlaywrightCrawlingContext) -> None:
            # For some sites, where the content loads dynamically,
            # it is needed to scroll the page to load all content.
            # It slows down the crawling, but ensures that all content is loaded.
            await context.infinite_scroll()
            await context.enqueue_links(strategy='same-domain')

        await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
