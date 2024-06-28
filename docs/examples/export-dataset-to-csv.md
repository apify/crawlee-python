---
id: export-dataset-to-csv
title: Export a dataset to a single CSV
---


```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

crawler = BeautifulSoupCrawler(
    max_requests_per_crawl=10,  # Limitation for only 10 requests (do not use if you want to crawl all links)
)

# Function called for each URL
@crawler.router.default_handler
async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
    await context.push_data({
        'url': context.request.url,
        'title': context.soup.title.string if context.soup.title else None,
    })

async def main() -> None:
    # Run the crawler
    await crawler.run([
        'https://crawlee.dev',
    ])

    # Export the data
    await crawler.export_data("results.csv")  # You can also export a JSON file by changing the extension to '.json'

asyncio.run(main())
```
