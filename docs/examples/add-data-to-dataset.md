---
id: add-data-to-dataset
title: Add data to dataset
---

This example demonstrates how to save data to the default dataset using the context helper `context.push_data()`. If the dataset doesn't exist, it will be created automatically. You can also save data to custom datasets by passing `dataset_id` or `dataset_name` to `push_data` method.

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        data = {
            'url': context.request.url,
            'html': context.http_response.text[:1000],
        }
        await context.push_data(data)

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
