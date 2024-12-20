import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url}')

        # We're not processing detail pages yet, so we just pass.
        if context.request.label == 'DETAIL':
            pass

        # We are now on a category page. We can use this to paginate through and
        # enqueue all products, as well as any subsequent pages we find.
        elif context.request.label == 'CATEGORY':
            # Wait for the product items to render.
            await context.page.wait_for_selector('.product-item > a')

            # Enqueue links found within elements matching the provided selector.
            # These links will be added to the crawling queue with the label DETAIL.
            await context.enqueue_links(
                selector='.product-item > a',
                label='DETAIL',
            )

            # Find the "Next" button to paginate through the category pages.
            next_button = await context.page.query_selector('a.pagination__next')

            # If a "Next" button is found, enqueue the next page of results.
            if next_button:
                await context.enqueue_links(
                    selector='a.pagination__next',
                    label='CATEGORY',
                )

        # This indicates we're on the start page with no specific label.
        # On the start page, we want to enqueue all the category pages.
        else:
            # Wait for the collection cards to render.
            await context.page.wait_for_selector('.collection-block-item')

            # Enqueue links found within elements matching the provided selector.
            # These links will be added to the crawling queue with the label CATEGORY.
            await context.enqueue_links(
                selector='.collection-block-item',
                label='CATEGORY',
            )

    await crawler.run(['https://warehouse-theme-metal.myshopify.com/collections'])


if __name__ == '__main__':
    asyncio.run(main())
