---
id: adding-more-urls
title: Adding more URLs
---

Previously you've built a very simple crawler that downloads HTML of a single page, reads its title and prints it to the console. This is the original source code:

```python
import asyncio
import logging
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

logger = logging.getLogger(__name__)

async def main() -> None:
    crawler = BeautifulSoupCrawler()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        url = context.request.url
        title = context.soup.title.string if context.soup.title else ''
        logger.info(f'The title of {url} is: {title}.')

    await crawler.run(['https://crawlee.dev/'])

if __name__ == '__main__':
    asyncio.run(main())
```

Now you'll use the example from the previous section and improve on it. You'll add more URLs to the queue and thanks to that the crawler will keep going, finding new links, enqueuing them into the `RequestQueue` and then scraping them.

## How crawling works

The process is simple:

1. Find new links on the page.
2. Filter only those pointing to the same domain, in this case [crawlee.dev](https://crawlee.dev/).
3. Enqueue (add) them to the `RequestQueue`.
4. Visit the newly enqueued links.
5. Repeat the process.

In the following paragraphs you will learn about the `enqueue_links` function which simplifies crawling to a single function call.

:::tip context awareness

The `enqueue_links` function is context aware. It means that it will read the information about the currently crawled page from the context, and you don't need to explicitly provide any arguments. However, you can specify filtering criteria or an enqueuing strategy if desired. It will find the links and automatically add the links to the running crawler's `RequestQueue`.

:::

## Limit your crawls

When you're just testing your code or when your crawler could potentially find millions of links, it's very useful to set a maximum limit of crawled pages. The option is called `max_requests_per_crawl`, is available in all crawlers, and you can set it like this:

```python
crawler = BeautifulSoupCrawler(max_requests_per_crawl=20)
```

This means that no new requests will be started after the 20th request is finished. The actual number of processed requests might be a little higher thanks to parallelization, because the running requests won't be forcefully aborted. It's not even possible in most cases.

## Finding new links

There are numerous approaches to finding links to follow when crawling the web. For our purposes, we will be looking for `<a>` elements that contain the `href` attribute because that's what you need in most cases. For example:

```html
<a href="https://crawlee.dev/docs/introduction">This is a link to Crawlee introduction</a>
```

Since this is the most common case, it is also the `enqueue_links` default.

```python
import asyncio
import logging
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

logger = logging.getLogger(__name__)

async def main() -> None:
    # Let's limit our crawls to make our tests shorter and safer.
    crawler = BeautifulSoupCrawler(max_requests_per_crawl=20)

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        url = context.request.url
        title = context.soup.title.string if context.soup.title else ''
        logger.info(f'The title of {url} is: {title}.')

        # The enqueue_links function is available as one of the fields of the context.
        # It is also context aware, so it does not require any parameters.
        await context.enqueue_links()

    await crawler.run(['https://crawlee.dev/'])

if __name__ == '__main__':
    asyncio.run(main())
```

If you need to override the default selection of elements in `enqueue_links`, you can use the `selector` argument.

```python
await context.enqueue_links(selector='div.has-link')
```

## Filtering links to same domain

Websites typically contain a lot of links that lead away from the original page. This is normal, but when crawling a website, we usually want to crawl that one site and not let our crawler wander away to Google, Facebook and Twitter. Therefore, we need to filter out the off-domain links and only keep the ones that lead to the same domain.

```python
# The default behavior of enqueue_links is to stay on the same hostname, so it does not require
# any parameters. This will ensure the subdomain stays the same.
await context.enqueue_links()
```

The default behavior of `enqueue_links` is to stay on the same hostname. This **does not include subdomains**. To include subdomains in your crawl, use the `strategy` argument.

```python
# See the EnqueueStrategy object for more strategy options.
await enqueue_links(strategy='same-domain')
```

When you run the code, you will see the crawler log the **title** of the first page, then the **enqueueing** message showing number of URLs, followed by the **title** of the first enqueued page and so on and so on.

## Skipping duplicate URLs

Skipping of duplicate URLs is critical, because visiting the same page multiple times would lead to duplicate results. This is automatically handled by the `RequestQueue` which deduplicates requests using their `unique_key`. This `unique_key` is automatically generated from the request's URL by lowercasing the URL, lexically ordering query parameters, removing fragments and a few other tweaks that ensure the queue only includes unique URLs.

## Advanced filtering arguments

While the defaults for `enqueue_links` can be often exactly what you need, it also gives you fine-grained control over which URLs should be enqueued. One way we already mentioned above. It is using the `EnqueueStrategy`. You can use the `all` strategy if you want to follow every single link, regardless of its domain, or you can enqueue links that target the same domain name with the `same_domain` strategy.

```python
# Wanders the internet.
await enqueue_links(strategy='all')
```

### Filter URLs with patterns

For even more control, you can use the `include` or `exclude` parameters, either as glob patterns or regular expressions, to filter the URLs. Refer to the API documentation for `enqueue_links` for detailed information on these and other available options.

```python
from crawlee import Glob

await enqueue_links(
    include=[Glob('https://someplace.com/**/cats')],
    exclude=[Glob('https://**/archive/**')],
)
```

<!-- TODO:
### Transform requests

...
-->

## Next steps

Next, you will start your project of scraping a production website and learn some more Crawlee tricks in the process.
