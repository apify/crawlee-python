import asyncio

from camoufox import AsyncNewBrowser  #  This external package needs to be installed. It is not included in crawlee.
from typing_extensions import override

from crawlee.browsers import BrowserPool, PlaywrightBrowserController, PlaywrightBrowserPlugin
from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext, PlaywrightPreNavigationContext


class ExampleCamoufoxPlugin(PlaywrightBrowserPlugin):
    """Example browser plugin that uses Camoufox Browser, but otherwise keeps the functionality of
    PlaywrightBrowserPlugin."""

    @override
    async def new_browser(self) -> PlaywrightBrowserController:
        if not self._playwright:
            raise RuntimeError('Playwright browser plugin is not initialized.')

        return PlaywrightBrowserController(
            browser=await AsyncNewBrowser(self._playwright, **self._browser_options),
            max_open_pages_per_browser=self._max_open_pages_per_browser,
            header_generator=None,  #  This turns off the crawlee header_generation. Camoufox has its own.
        )


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
        # Custom browser pool. This gives users full control over browsers used by the crawler.
        browser_pool=BrowserPool(plugins=[ExampleCamoufoxPlugin(browser_options={'headless': True})]),
    )

    # Define the default request handler, which will be called for every request.
    # The handler receives a context parameter, providing various properties and
    # helper methods. Here are a few key ones we use for demonstration:
    # - request: an instance of the Request class containing details such as the URL
    #   being crawled and the HTTP method used.
    # - page: Playwright's Page object, which allows interaction with the web page
    #   (see https://playwright.dev/python/docs/api/class-page for more details).
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page using Playwright's API.
        posts = await context.page.query_selector_all('.athing')
        data = []

        for post in posts:
            # Get the HTML elements for the title and rank within each post.
            title_element = await post.query_selector('.title a')
            rank_element = await post.query_selector('.rank')

            # Extract the data we want from the elements.
            title = await title_element.inner_text() if title_element else None
            rank = await rank_element.inner_text() if rank_element else None
            href = await title_element.get_attribute('href') if title_element else None

            data.append({'title': title, 'rank': rank, 'href': href})

        # Push the extracted data to the default dataset. In local configuration,
        # the data will be stored as JSON files in ./storage/datasets/default.
        await context.push_data(data)

        # Find a link to the next page and enqueue it if it exists.
        await context.enqueue_links(selector='.morelink')

    # Define a hook that will be called each time before navigating to a new URL.
    # The hook receives a context parameter, providing access to the request and
    # browser page among other things. In this example, we log the URL being
    # navigated to.
    @crawler.pre_navigation_hook
    async def log_navigation_url(context: PlaywrightPreNavigationContext) -> None:
        context.log.info(f'Navigating to {context.request.url} ...')

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://news.ycombinator.com/'])


if __name__ == '__main__':
    asyncio.run(main())
