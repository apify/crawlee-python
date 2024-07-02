---
id: crawl-website-with-relative-links
title: Crawl website with relative links
---

<!-- TODO: jeste vylepsit text -->

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

When crawling a website, you may encounter different types of links present that you may want to crawl. To facilitate the easy crawling of such links, we provide the `enqueue_links()` method on the crawler context, which will automatically find links and add them to the crawler's `RequestQueue`.

We provide 4 different strategies for crawling relative links:

- `EnqueueStrategy.All` - which will enqueue all links found, regardless of the domain they point to.
- `EnqueueStrategy.SAME_DOMAIN` which will enqueue all links found that have the same domain name, including links from any possible subdomain. They are sharing the same top-level domain and base domain.
- `EnqueueStrategy.SAME_HOSTNAME` which will enqueue all links found for the exact same hostname. This is the **default** strategy.
- `EnqueueStrategy.SAME_ORIGIN` which will enqueue all links found for the same origin. The same origin refers to URLs sharing the same protocol, domain, and port.

:::note

For these examples, we are using the `BeautifulSoupCrawler`. However the same method is available for the `PlaywrightCrawler` as well. You can use it the exact same way.

:::

<Tabs groupId="main">
<TabItem value="all_links" label="All links">

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.enqueue_strategy import EnqueueStrategy


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Limit the crawl to only 10 requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Enqueue all links found on the page. Any URLs found will be matched by this strategy,
        # even if they go off of the site you are currently crawling.
        await context.enqueue_links(strategy=EnqueueStrategy.ALL)

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>
<TabItem value="same_domain" label="Same domain">

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.enqueue_strategy import EnqueueStrategy


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Limit the crawl to only 10 requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Setting the strategy to SAME_DOMAIN will enqueue all links found that
        # are on the same hostname as request.loaded_url or request.url.
        await context.enqueue_links(strategy=EnqueueStrategy.SAME_DOMAIN)

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>
<TabItem value="same_hostname" label="Same hostname">

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.enqueue_strategy import EnqueueStrategy


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Limit the crawl to only 10 requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Setting the strategy to SAME_HOSTNAME will enqueue all links found that are on
        # the same hostname (including subdomains) as request.loaded_url or request.url.
        await context.enqueue_links(strategy=EnqueueStrategy.SAME_HOSTNAME)

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>
<TabItem value="same_origin" label="Same origin">

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.enqueue_strategy import EnqueueStrategy


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Limit the crawl to only 10 requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Setting the strategy to SAME_ORIGIN will enqueue all links found that are on
        # the same origin as request.loaded_url or request.url.
        await context.enqueue_links(strategy=EnqueueStrategy.SAME_ORIGIN)

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

</TabItem>
</Tabs>
