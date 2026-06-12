import asyncio

from crawlee.crawlers import (
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPostNavCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)
from crawlee.errors import SessionError


async def main() -> None:
    crawler = PlaywrightCrawler(max_requests_per_crawl=10)

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        await context.enqueue_links()

    @crawler.pre_navigation_hook
    async def configure_page(context: PlaywrightPreNavCrawlingContext) -> None:
        context.log.info(f'Navigating to {context.request.url} ...')

        # block stylesheets, images, fonts and other static assets
        # to speed up page loading
        await context.block_requests()

    @crawler.post_navigation_hook
    async def custom_captcha_check(context: PlaywrightPostNavCrawlingContext) -> None:
        # check if the page contains a captcha
        captcha_element = context.page.locator('input[name="captcha"]').first
        if await captcha_element.is_visible():
            context.log.warning('Captcha detected! Skipping the page.')
            raise SessionError('Captcha detected')

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
