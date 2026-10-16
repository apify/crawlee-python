import asyncio
from datetime import timedelta
from pathlib import Path

from yarl import URL

from crawlee.crawlers import FileDownloadCrawler, FileDownloadCrawlingContext

DOWNLOAD_DIR = Path('downloads')


async def main() -> None:
    # With stream=True, the request handler receives a response whose body has not
    # been read yet.
    crawler = FileDownloadCrawler(
        stream=True,
        # Bounds establishing the connection and receiving the response headers.
        navigation_timeout=timedelta(minutes=5),
        # The body is downloaded inside the handler, so this bounds the transfer itself.
        request_handler_timeout=timedelta(minutes=5),
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        context.log.info(f'Downloading {context.request.url} ...')

        file_name = URL(context.request.url).name

        # Write each chunk to disk as it arrives, without buffering the whole file.
        with (DOWNLOAD_DIR / file_name).open('wb') as file:
            async for chunk in context.http_response.read_stream():
                file.write(chunk)

        context.log.info(f'Saved {file_name}')

    # Run the crawler with the list of files to download.
    await crawler.run(
        [
            'https://samplelib.com/mp4/sample-15s-720p.mp4',
        ]
    )


if __name__ == '__main__':
    DOWNLOAD_DIR.mkdir(exist_ok=True)
    asyncio.run(main())
