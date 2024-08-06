---
id: capture-screenshots-using-playwright
title: Capture screenshots using Playwright
---

This example demonstrates how to capture screenshots of web pages using `PlaywrightCrawler` and store them in the key-value store.

The `PlaywrightCrawler` is configured to automate the browsing and interaction with web pages. It uses headless Chromium as the browser type to perform these tasks. Each web page specified in the initial list of URLs is visited sequentially, and a screenshot of the page is captured using Playwright's `page.screenshot()` method.

The captured screenshots are stored in the key-value store, which is suitable for managing and storing files in various formats. In this case, screenshots are stored as PNG images with a unique key generated from the URL of the page.

```python
import asyncio

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext
from crawlee.storages import KeyValueStore


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
        # Headless mode, set to False to see the browser in action.
        headless=False,
        # Browser types supported by Playwright.
        browser_type='chromium',
    )

    # Open the default key-value store.
    kvs = await KeyValueStore.open()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Capture the screenshot of the page using Playwright's API.
        screenshot = await context.page.screenshot()
        name = context.request.url.split('/')[-1]

        # Store the screenshot in the key-value store.
        await kvs.set_value(
            key=f'screenshot-{name}',
            value=screenshot,
            content_type='image/png',
        )

    # Run the crawler with the initial list of URLs.
    await crawler.run(
        [
            'https://crawlee.dev',
            'https://apify.com',
            'https://example.com',
        ]
    )


if __name__ == '__main__':
    asyncio.run(main())
```
