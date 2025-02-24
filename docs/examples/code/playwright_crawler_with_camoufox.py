import asyncio

#  Camoufox is external package and needs to be installed. It is not included in crawlee.
from camoufox import AsyncNewBrowser
from typing_extensions import override

from crawlee.browsers import (
    BrowserPool,
    PlaywrightBrowserController,
    PlaywrightBrowserPlugin,
)
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


class CamoufoxPlugin(PlaywrightBrowserPlugin):
    """Example browser plugin that uses Camoufox browser,
    but otherwise keeps the functionality of PlaywrightBrowserPlugin.
    """

    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        return PlaywrightBrowserController(
            browser=await AsyncNewBrowser(
                self._playwright, **self._browser_launch_options
            ),
            # Increase, if camoufox can handle it in your use case.
            max_open_pages_per_browser=1,
            # This turns off the crawlee header_generation. Camoufox has its own.
            header_generator=None,
        )


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
        # Custom browser pool. Gives users full control over browsers used by the crawler.
        browser_pool=BrowserPool(plugins=[CamoufoxPlugin()]),
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract some data from the page using Playwright's API.
        posts = await context.page.query_selector_all('.athing')
        for post in posts:
            # Get the HTML elements for the title and rank within each post.
            title_element = await post.query_selector('.title a')

            # Extract the data we want from the elements.
            title = await title_element.inner_text() if title_element else None

        # Push the extracted data to the default dataset.
        await context.push_data({'title': title})

        # Find a link to the next page and enqueue it if it exists.
        await context.enqueue_links(selector='.morelink')

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://news.ycombinator.com/'])


if __name__ == '__main__':
    asyncio.run(main())
