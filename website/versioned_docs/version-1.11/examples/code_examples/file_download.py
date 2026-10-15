import asyncio

from yarl import URL

from crawlee.crawlers import FileDownloadCrawler, FileDownloadCrawlingContext


async def main() -> None:
    # FileDownloadCrawler downloads files with plain HTTP requests and accepts
    # any content type.
    crawler = FileDownloadCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: FileDownloadCrawlingContext) -> None:
        context.log.info(f'Downloading {context.request.url} ...')

        # Read the whole file into memory.
        content = await context.http_response.read()

        # Save the file to the default key-value store with the server's content type.
        kvs = await context.get_key_value_store()
        file_name = URL(context.request.url).name
        await kvs.set_value(
            key=file_name,
            value=content,
            content_type=context.http_response.headers.get('content-type'),
        )

    # Run the crawler with the list of files to download.
    await crawler.run(
        [
            'https://pdfobject.com/pdf/sample.pdf',
            'https://crawlee.dev/assets/images/gradcracker-scraper-caefb62d1c150c4209a6e564c052fa41.webp',
        ]
    )


if __name__ == '__main__':
    asyncio.run(main())
