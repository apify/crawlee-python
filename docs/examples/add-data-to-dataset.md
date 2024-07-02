---
id: add-data-to-dataset
title: Add data to dataset
---

This example demonstrates how to save data to the default dataset using the context helper `context.push_data()`. If the dataset doesn't exist, it will be created automatically. You can also save data to custom datasets by passing `dataset_id` or `dataset_name` to `push_data` method.

```python
import asyncio

from crawlee.http_crawler import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    # We are going to use the HttpCrawler for this case.
    crawler = HttpCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'html': context.http_response.text[:1000],
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

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

Each item in this dataset will be saved to its own file in the following directory:

```text
{PROJECT_FOLDER}/storage/datasets/default/
```

You can also open a dataset manually, and interact with it directly, using an asynchronous constructor `open()`:

```python
from crawlee.storages import Dataset

# ...

async def main() -> None:
    # Open dataset manually using asynchronous constructor open().
    dataset = await Dataset.open()

    # Interact with dataset directly.
    await dataset.push_data({'key': 'value'})

# ...
```
