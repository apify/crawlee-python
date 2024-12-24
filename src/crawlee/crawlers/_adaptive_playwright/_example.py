import asyncio

from crawlee.crawlers import PlaywrightCrawlingContext
from crawlee.crawlers._adaptive_playwright._adaptive_playwright_crawler import AdaptivePlaywrightCrawler




async def main():
    crawler = AdaptivePlaywrightCrawler()

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


    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://news.ycombinator.com/'])


if __name__ == '__main__':
    asyncio.run(main())
