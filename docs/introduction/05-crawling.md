---
id: crawling
title: Crawling
---

To crawl the whole [Warehouse store example](https://warehouse-theme-metal.myshopify.com/collections) and find all the data, you first need to visit all the pages with products - going through all categories available and also all the product detail pages.

## Crawling the listing pages

In previous lessons, you used the `enqueue_links()` function like this:

```python
await enqueue_links()
```

While useful in that scenario, you need something different now. Instead of finding all the `<a href="..">` elements with links to the same hostname, you need to find only the specific ones that will take your crawler to the next page of results. Otherwise, the crawler will visit a lot of other pages that you're not interested in. Using the power of DevTools and yet another `enqueue_links()` parameter, this becomes fairly easy.

```python
import asyncio
import logging

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

logger = logging.getLogger(__name__)


async def main() -> None:
    crawler = PlaywrightCrawler()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        logger.info(f'Processing {context.request.url}')

        # Wait for the category cards to render on the page. This ensures that
        # the elements we want to interact with are present in the DOM.
        await context.page.wait_for_selector('.collection-block-item')

        # Enqueue links found within elements that match the specified selector.
        # These links will be added to the crawling queue with the label CATEGORY.
        await context.enqueue_links(
            selector='.collection-block-item',
            label='CATEGORY',
        )

    await crawler.run(['https://warehouse-theme-metal.myshopify.com/collections'])


if __name__ == '__main__':
    asyncio.run(main())
```

The code should look pretty familiar to you. It's a very simple request handler where we log the currently processed URL to the console and enqueue more links. But there are also a few new, interesting additions. Let's break it down.

### The `selector` parameter of `enqueue_links()`

When you previously used `enqueue_links()`, you were not providing any `selector` parameter, and it was fine, because you wanted to use the default value, which is `a` - finds all `<a>` elements. But now, you need to be more specific. There are multiple `<a>` links on the `Categories` page, and you're only interested in those that will take your crawler to the available list of results. Using the DevTools, you'll find that you can select the links you need using the `.collection-block-item` selector, which selects all the elements that have the `class=collection-block-item` attribute.

### The `label` of `enqueue_links()`

You will see `label` used often throughout Crawlee, as it's a convenient way of labelling a `Request` instance for quick identification later. You can access it with `request.label` and it's a `string`. You can name your requests any way you want. Here, we used the label `CATEGORY` to note that we're enqueueing pages that represent a category of products. The `enqueue_links()` function will add this label to all requests before enqueueing them to the `RequestQueue`. Why this is useful will become obvious in a minute.

## Crawling the detail pages

In a similar fashion, you need to collect all the URLs to the product detail pages, because only from there you can scrape all the data you need. The following code only repeats the concepts you already know for another set of links.

```python
import asyncio
import logging

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

logger = logging.getLogger(__name__)


async def main() -> None:
    crawler = PlaywrightCrawler()

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        logger.info(f'Processing {context.request.url}')

        # We're not processing detail pages yet, so we just pass.
        if context.request.label == 'DETAIL':
            pass

        # We are now on a category page. We can use this to paginate through and
        # enqueue all products, as well as any subsequent pages we find.
        elif context.request.label == 'CATEGORY':
            # Wait for the product items to render.
            await context.page.wait_for_selector('.product-item > a')

            # Enqueue links found within elements matching the provided selector.
            # These links will be added to the crawling queue with the label DETAIL.
            await context.enqueue_links(
                selector='.product-item > a',
                label='DETAIL',
            )

            # Find the "Next" button to paginate through the category pages.
            next_button = await context.page.query_selector('a.pagination__next')

            # If a "Next" button is found, enqueue the next page of results.
            if next_button:
                await context.enqueue_links(
                    selector='a.pagination__next',
                    label='CATEGORY',
                )

        # This indicates we're on the start page with no specific label.
        # On the start page, we want to enqueue all the category pages.
        else:
            # Wait for the collection cards to render.
            await context.page.wait_for_selector('.collection-block-item')

            # Enqueue links found within elements matching the provided selector.
            # These links will be added to the crawling queue with the label CATEGORY.
            await context.enqueue_links(
                selector='.collection-block-item',
                label='CATEGORY',
            )

    await crawler.run(['https://warehouse-theme-metal.myshopify.com/collections'])


if __name__ == '__main__':
    asyncio.run(main())
```

The crawling code is now complete. When you run the code, you'll see the crawler visit all the listing URLs and all the detail URLs.

## Next steps

This concludes the Crawling lesson, because you have taught the crawler to visit all the pages it needs. Let's continue with scraping data.
