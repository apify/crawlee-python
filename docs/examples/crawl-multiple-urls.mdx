---
id: crawl-multiple-urls
title: Crawl multiple URLs
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This example demonstrates how to crawl a specified list of URLs using different crawlers. You'll learn how to set up the crawler, define a request handler, and run the crawler with multiple URLs. This setup is useful for scraping data from multiple pages or websites concurrently.

<Tabs groupId="main">
<TabItem value="BeautifulSoupCrawler" label="BeautifulSoupCrawler">

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    # Run the crawler with the initial list of requests.
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

</TabItem>
<TabItem value="PlaywrightCrawler" label="PlaywrightCrawler">

```python
import asyncio

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

    # Run the crawler with the initial list of requests.
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

</TabItem>
</Tabs>
