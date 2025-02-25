import asyncio

# Instead of BeautifulSoupCrawler let's use Playwright to be able to render JavaScript.
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        # Wait for the collection cards to render on the page. This ensures that
        # the elements we want to interact with are present in the DOM.
        await context.page.wait_for_selector('.collection-block-item')

        # Execute a function within the browser context to target the collection
        # card elements and extract their text content, trimming any leading or
        # trailing whitespace.
        category_texts = await context.page.eval_on_selector_all(
            '.collection-block-item',
            '(els) => els.map(el => el.textContent.trim())',
        )

        # Log the extracted texts.
        for i, text in enumerate(category_texts):
            context.log.info(f'CATEGORY_{i + 1}: {text}')

    await crawler.run(['https://warehouse-theme-metal.myshopify.com/collections'])


if __name__ == '__main__':
    asyncio.run(main())
