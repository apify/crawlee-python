---
id: first-crawler
title: First crawler
---

Now, you will build your first crawler. But before you do, let's briefly introduce the Crawlee classes involved in the process.

## How Crawlee works

There are 2 main crawler classes available for use in Crawlee.

- `BeautifulSoupCrawler`
- `PlaywrightCrawler`

We'll talk about their differences later. Now, let's talk about what they have in common.

The general idea of each crawler is to go to a web page, open it, do some stuff there, save some results, continue to the next page, and repeat this process until the crawler's done its job. So the crawler always needs to find answers to two questions: _Where should I go?_ and _What should I do there?_ Answering those two questions is the only required setup. The crawlers have reasonable defaults for everything else.

### The where - `Request` and `RequestQueue`

All crawlers use instances of the `Request` class to determine where they need to go. Each request may hold a lot of information, but at the very least, it must hold a URL - a web page to open. But having only one URL would not make sense for crawling. Sometimes you have a pre-existing list of your own URLs that you wish to visit, perhaps a thousand. Other times you need to build this list dynamically as you crawl, adding more and more URLs to the list as you progress. Most of the time, you will use both options.

The requests are stored in a `RequestQueue`, a dynamic queue of `Request` instances. You can seed it with start URLs and also add more requests while the crawler is running. This allows the crawler to open one page, extract interesting data, such as links to other pages on the same domain, add them to the queue (called _enqueuing_) and repeat this process to build a queue of virtually unlimited number of URLs.

### The what - request handler

In the request handler you tell the crawler what to do at each and every page it visits. You can use it to handle extraction of data from the page, processing the data, saving it, calling APIs, doing calculations and so on.

The request handler is a user-defined function, invoked automatically by the crawler for each `Request` from the `RequestQueue`. It always receives a single argument - `CrawlingContext`. Its properties change depending on the crawler class used, but it always includes the `request` property, which represents the currently crawled URL and related metadata.

## Building a crawler

Let's put the theory into practice and start with something easy. Visit a page and get its HTML title. In this tutorial, you'll scrape the Crawlee website [https://crawlee.dev](https://crawlee.dev), but the same code will work for any website.

### Adding requests to the crawling queue

Earlier you learned that the crawler uses a queue of requests as its source of URLs to crawl. Let's create it and add the first request.

```python
import asyncio
from crawlee.storages import RequestQueue

async def main() -> None:
    # First you create the request queue instance.
    rq = await RequestQueue.open()

    # And then you add one or more requests to it.
    await rq.add_request('https://crawlee.dev')

if __name__ == '__main__':
    asyncio.run(main())
```

The `RequestQueue.add_request()` method automatically converts the object with URL string to a `Request` instance. So now you have a `RequestQueue` that holds one request which points to `https://crawlee.dev`.

:::tip Bulk add requests

The code above is for illustration of the request queue concept. Soon you'll learn about the `Crawler.add_requests()` method which allows you to skip this initialization code, and it also supports adding a large number of requests without blocking.

:::

### Building a BeautifulSoupCrawler

Crawlee comes with two main crawler classes: `BeautifulSoupCrawler`, and `PlaywrightCrawler`. You can read their short descriptions in the [Quick start](../quick-start) lesson.

Unless you have a good reason to start with a different one, you should try building a `BeautifulSoupCrawler` first. It is an HTTP crawler with HTTP2 support, anti-blocking features and integrated HTML parser - [BeautifulSoup](https://pypi.org/project/beautifulsoup4/). It's fast, simple, cheap to run and does not require complicated dependencies. The only downside is that it won't work out of the box for websites which require JavaScript rendering. But you might not need JavaScript rendering at all, because many modern websites use server-side rendering.

Let's continue with the earlier `RequestQueue` example.

```python
import asyncio
# Add import of crawler and crawling context.
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import RequestQueue

async def main() -> None:
    # First you create the request queue instance.
    rq = await RequestQueue.open()

    # And then you add one or more requests to it.
    await rq.add_request('https://crawlee.dev')

    crawler = BeautifulSoupCrawler(request_provider=rq)

    # Define a request handler and attach it to the crawler using the decorator.
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        # Extract <title> text with BeautifulSoup.
        # See BeautifulSoup documentation for API docs.
        url = context.request.url
        title = context.soup.title.string if context.soup.title else ''
        context.log.info(f'The title of {url} is: {title}.')

    await crawler.run()

if __name__ == '__main__':
    asyncio.run(main())
```

When you run the example, you will see the title of https://crawlee.dev printed to the log. What really happens is that `BeautifulSoupCrawler` first makes an HTTP request to `https://crawlee.dev`, then parses the received HTML with BeautifulSoup and makes it available as the `context` argument of the request handler.

```log
[__main__] INFO  The title of "https://crawlee.dev" is "Crawlee · Build reliable crawlers. Fast. | Crawlee".
```

### Add requests faster

Earlier we mentioned that you'll learn how to use the `Crawler.add_requests()` method to skip the request queue initialization. It's simple. Every crawler has an implicit `RequestQueue` instance, and you can add requests to it with the `Crawler.add_requests()` method. In fact, you can go even further and just use the first parameter of `crawler.run()`!

```python
import asyncio
# You don't need to import RequestQueue anymore
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

async def main() -> None:
    crawler = BeautifulSoupCrawler()

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        url = context.request.url
        title = context.soup.title.string if context.soup.title else ''
        context.log.info(f'The title of {url} is: {title}.')

    # Start the crawler with the provided URLs
    await crawler.run(['https://crawlee.dev/'])

if __name__ == '__main__':
    asyncio.run(main())
```

When you run this code, you'll see exactly the same output as with the earlier, longer example. The `RequestQueue` is still there, it's just managed by the crawler automatically.

:::info

This method not only makes the code shorter, it will help with performance too! Internally it calls `RequestQueue.add_requests_batched()` method. It will wait only for the initial batch of 1000 requests to be added to the queue before resolving, which means the processing will start almost instantly. After that, it will continue adding the rest of the requests in the background (again, in batches of 1000 items, once every second).

:::

## Next steps

Next, you'll learn about crawling links. That means finding new URLs on the pages you crawl and adding them to the `RequestQueue` for the crawler to visit.
