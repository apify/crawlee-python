import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=10)

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        # Every call returns the same dictionary. Crawlee saves it to the default
        # key-value store on each persist state event and at the end of the run.
        state = await context.use_state()
        state[context.request.url] = context.selector.css('title::text').get()

        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev'])

    # The crawler exposes the same state outside the request handlers.
    state = await crawler.use_state()
    print(f'Title of the start page: {state["https://crawlee.dev"]}')


if __name__ == '__main__':
    asyncio.run(main())
