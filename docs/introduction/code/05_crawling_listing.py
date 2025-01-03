import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # Wait for the category cards to render on the page. This ensures that
        # the elements we want to interact with are present in the DOM.
        await context.page.wait_for_selector('.collection-block-item')

        # Enqueue links found within elements that match the specified selector.
        # These links will be added to the crawling queue with the label CATEGORY.
        await context.enqueue_links(
            selector='.collection-block-item',
            label='CATEGORY',
        )

    await crawler.run(['https://warehouse-theme-metal.myshopify.com/collections'])


if __name__ == '__main__':
    asyncio.run(main())
