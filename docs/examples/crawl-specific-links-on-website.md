---
id: crawl-specific-links-on-website
title: Crawl specific links on website
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- TODO: jeste vylepsit text -->

This example demonstrates how to crawl the web with matching specific pattern of links. You can pass `include` or `exclude` parameters to the `enqueue_links()` helper. Found links will be added to the `RequestQueue` queue only if they match the specified pattern. Both include and excludes support list of globs or regular expressions for filtering.

<Tabs groupId="main">
<TabItem value="BeautifulSoupCrawler" label="BeautifulSoupCrawler">

```python
import asyncio

from crawlee import Glob
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

        # Enqueue all the documentation links found on the page, except for the examples.
        await context.enqueue_links(
            include=[Glob('https://crawlee.dev/docs/**')],
            exclude=[Glob('https://crawlee.dev/docs/examples')],
        )

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>
<TabItem value="PlaywrightCrawler" label="PlaywrightCrawler">

```python
import asyncio

from crawlee import Glob
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

        # Enqueue all the documentation links found on the page, except for the examples.
        await context.enqueue_links(
            include=[Glob('https://crawlee.dev/docs/**')],
            exclude=[Glob('https://crawlee.dev/docs/examples')],
        )

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>
</Tabs>
