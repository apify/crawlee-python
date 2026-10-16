import asyncio

from crawlee.crawlers import ParselCrawler, ParselCrawlingContext


async def main() -> None:
    crawler = ParselCrawler(max_requests_per_crawl=5)

    @crawler.router.default_handler
    async def request_handler(context: ParselCrawlingContext) -> None:
        # The request being processed and the crawler's logger.
        context.log.info(f'Processing {context.request.url} ...')

        # Store a result in the default dataset.
        await context.push_data(
            {
                'url': context.request.url,
                'title': context.selector.css('title::text').get(),
            }
        )

        # Store a value in the default key-value store.
        kvs = await context.get_key_value_store()
        await kvs.set_value('last-url', context.request.url)

        # Keep a counter in the crawler state shared by all handler calls.
        state = await context.use_state({'pages': 0})
        pages = state['pages']
        if isinstance(pages, int):
            state['pages'] = pages + 1

        # Add links found on the page to the request queue.
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev/'])

    kvs = await crawler.get_key_value_store()
    print(f'Last URL: {await kvs.get_value("last-url")}')

    state = await crawler.use_state()
    print(f'Pages counted in state: {state["pages"]}')

    data = await crawler.get_data()
    print(f'Items in dataset: {len(data.items)}')


if __name__ == '__main__':
    asyncio.run(main())
