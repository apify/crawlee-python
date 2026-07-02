import asyncio
import io
from pathlib import Path

from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def archive_response(context: ParselCrawlingContext, writer: WARCWriter) -> None:
    """Helper function for archiving response in WARC format."""
    # Create WARC records for response
    response_body = await context.http_response.read()
    response_payload_stream = io.BytesIO(response_body)

    response_headers = StatusAndHeaders(
        str(context.http_response.status_code),
        context.http_response.headers,
        protocol='HTTP/1.1',
    )
    response_record = writer.create_warc_record(
        context.request.url,
        'response',
        payload=response_payload_stream,
        length=len(response_body),
        http_headers=response_headers,
    )
    writer.write_record(response_record)


async def main() -> None:
    crawler = ParselCrawler(
        max_requests_per_crawl=10,
    )

    # Create a WARC archive file a prepare the writer.
    archive = Path('example.warc.gz')
    with archive.open('wb') as output:
        writer = WARCWriter(output, gzip=True)

        # Create a WARC info record to store metadata about the archive.
        warcinfo_payload = {
            'software': 'Crawlee',
            'format': 'WARC/1.1',
            'description': 'Example archive created with ParselCrawler',
        }
        writer.write_record(writer.create_warcinfo_record(archive.name, warcinfo_payload))

        # Define the default request handler, which will be called for every request.
        @crawler.router.default_handler
        async def request_handler(context: ParselCrawlingContext) -> None:
            context.log.info(f'Archiving {context.request.url} ...')
            await archive_response(context=context, writer=writer)
            await context.enqueue_links(strategy='same-domain')

        await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
