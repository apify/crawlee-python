---
id: crawl-all-links-on-a-website
title: Crawl all links on a website
---

This example uses the `enqueue_links()` helper to add new links to the RequestQueue as the crawler navigates from page to page.

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

crawler = BeautifulSoupCrawler(
    max_requests_per_crawl=10,  # Limitation for only 10 requests (do not use if you want to crawl all links)
)

# Function called for each URL
@crawler.router.default_handler
async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
    context.log.info(context.request.url)

    # Enqueue all links (`a` selector) on the page
    await context.enqueue_links()

async def main() -> None:
    # Run the crawler with initial request
    await crawler.run([
        'https://crawlee.dev',
    ])

asyncio.run(main())
```
