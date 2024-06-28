---
id: add-data-to-dataset
title: Add data to dataset
---

This example saves data to the default dataset. If the dataset doesn't exist, it will be created. You can save data to custom datasets by passing `dataset_id` or `dataset_name` to `push_data`.

```python
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

crawler = BeautifulSoupCrawler();

# Function called for each URL
@crawler.router.default_handler
async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
    await context.push_data({
        "url": context.request.url,
        "html": context.http_response.text(),
    })

# Run the crawler
await crawler.run([
    'http://www.example.com/page-1',
    'http://www.example.com/page-2',
    'http://www.example.com/page-3',
]);
```
