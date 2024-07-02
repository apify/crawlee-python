---
id: export-entire-dataset-to-file
title: Export entire dataset to file
---

<!-- TODO: jeste vylepsit text -->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

This dataset example uses the `export_data()` method of the crawler to export the entire default dataset to a single file. You can use either CSV or JSON.

:::note

For these examples, we are using the `BeautifulSoupCrawler`. However the same method is available for the `PlaywrightCrawler` as well. You can use it the exact same way.

:::

<Tabs groupId="main">
<TabItem value="json" label="JSON">

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

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': context.soup.title.string if context.soup.title else None,
        }

        # Enqueue all links found on the page.
        await context.enqueue_links()

        # Push the extracted data to the default dataset.
        await context.push_data(data)

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])

    # Export the entire dataset to a JSON file.
    await crawler.export_data('results.json')


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>

<TabItem value="csv" label="CSV">

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

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': context.soup.title.string if context.soup.title else None,
        }

        # Enqueue all links found on the page.
        await context.enqueue_links()

        # Push the extracted data to the default dataset.
        await context.push_data(data)

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])

    # Export the entire dataset to a CSV file.
    await crawler.export_data('results.csv')


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>
</Tabs>
