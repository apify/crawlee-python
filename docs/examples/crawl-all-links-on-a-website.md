---
id: crawl-all-links-on-a-website
title: Crawl all links on a website
---

This example uses the `enqueue_links()` helper to add new links to the `RequestQueue` as the crawler navigates from page to page.

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
