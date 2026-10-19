import asyncio

from crawlee.crawlers import (
    PlaywrightCrawler,
    PlaywrightCrawlingContext,
    PlaywrightPreNavCrawlingContext,
)


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
        # Headless mode, set to False to see the browser in action.
        headless=False,
        # Browser types supported by Playwright.
        browser_type='chromium',
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
    async def log_navigation_url(context: PlaywrightPreNavCrawlingContext) -> None:
        context.log.info(f'Navigating to {context.request.url} ...')

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://news.ycombinator.com/'])


if __name__ == '__main__':
    asyncio.run(main())
