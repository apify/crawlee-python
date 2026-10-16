import asyncio

from crawlee.crawlers import BasicCrawler, BasicCrawlingContext


async def main() -> None:
    crawler = BasicCrawler(max_request_retries=2)

    @crawler.router.default_handler
    async def request_handler(context: BasicCrawlingContext) -> None:
        attempt = context.request.retry_count + 1
        context.log.info(f'Attempt {attempt} for {context.request.url}')

        # Buffered until the handler finishes without an exception.
        await context.push_data({'url': context.request.url, 'attempt': attempt})

        # Fail on the first attempt, so the crawler retries the request.
        if attempt == 1:
            raise RuntimeError('Simulated failure')

    await crawler.run(['https://crawlee.dev/'])

    # Only the item from the successful attempt is stored.
    data = await crawler.get_data()
    print(data.items)


if __name__ == '__main__':
    asyncio.run(main())
