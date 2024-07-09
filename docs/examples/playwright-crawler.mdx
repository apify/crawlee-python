---
id: playwright-crawler
title: Playwright crawler
---

This example demonstrates how to use `PlaywrightCrawler` to recursively scrape the Hacker news website using headless Chromium and Playwright.

The `PlaywrightCrawler` manages the browser and page instances, simplifying the process of interacting with web pages. In the request handler, Playwright's API is used to extract data from each post on the page. Specifically, it retrieves the title, rank, and URL of each post. Additionally, the handler enqueues links to the next pages to ensure continuous scraping. This setup is ideal for scraping dynamic web pages where JavaScript execution is required to render the content.

```python
import asyncio

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext


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
            title_element = await post.query_selector('.title a')
            rank_element = await post.query_selector('.rank')

            title = await context.page.evaluate('(element) => element.innerText', title_element) if title_element else None
            rank = await context.page.evaluate('(element) => element.innerText', rank_element) if rank_element else None
            href = await context.page.evaluate('(element) => element.href', title_element) if title_element else None

            data.append(
                {
                    'title': title,
                    'rank': rank,
                    'href': href,
                }
            )

        # Push the extracted data to the default dataset. In local configuration,
        # the data will be stored as JSON files in ./storage/datasets/default.
        await context.push_data(data)

        # Find a link to the next page and enqueue it if it exists.
        await context.enqueue_links(selector='.morelink')

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://news.ycombinator.com/'])


if __name__ == '__main__':
    asyncio.run(main())
```
