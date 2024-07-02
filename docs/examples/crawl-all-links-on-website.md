---
id: crawl-all-links-on-website
title: Crawl all links on website
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This example uses the `enqueue_links()` helper to add new links to the `RequestQueue` as the crawler navigates from page to page.

:::tip

If no options are given, by default the method will only add links that are under the same subdomain. This behavior can be controlled with the `strategy` option. You can find more info about this option in the [Crawl website with relative links](./crawl-website-with-relative-links) example.

:::

<Tabs groupId="main">
<TabItem value="BeautifulSoupCrawler" label="BeautifulSoupCrawler">

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Limit the crawl to only 10 requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Enqueue all links (matching the 'a' selector) found on the page.
        await context.enqueue_links()

    # Run the crawler with the initial request.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>
<TabItem value="PlaywrightCrawler" label="PlaywrightCrawler">

```python
import asyncio

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Limit the crawl to only 10 requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Enqueue all links (matching the 'a' selector) found on the page.
        await context.enqueue_links()

    # Run the crawler with the initial request.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>
</Tabs>
