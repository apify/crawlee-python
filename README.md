<h1 align="center">
    <a href="https://crawlee.dev">
        <picture>
          <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apify/crawlee/master/website/static/img/crawlee-dark.svg?sanitize=true">
          <img alt="Crawlee" src="https://raw.githubusercontent.com/apify/crawlee/master/website/static/img/crawlee-light.svg?sanitize=true" width="500">
        </picture>
    </a>
    <br>
    <small>A web scraping and browser automation library</small>
</h1>

Crawlee covers your crawling and scraping end-to-end and **helps you build reliable scrapers. Fast.**

Your crawlers will appear almost human-like and fly under the radar of modern bot protections even with
the default configuration. Crawlee gives you the tools to crawl the web for links, scrape data, and
store it to disk or cloud while staying configurable to suit your project's needs.

We also have a TypeScript implementation of the Crawlee, which you can explore and utilize for your projects.
Visit our GitHub repository for more information [Crawlee on GitHub](https://github.com/apify/crawlee).

## Installation

Crawlee is available as the [`crawlee`](https://pypi.org/project/crawlee/) PyPI package.

```bash
pip install crawlee
```

Additional, optional dependencies unlocking more features are shipped as package extras.

If you plan to use `BeautifulSoupCrawler`, install `crawlee` with `beautifulsoup` extra:

```bash
pip install 'crawlee[beautifulsoup]'
```

If you plan to use `PlaywrightCrawler`, install `crawlee` with the `playwright` extra:

```bash
pip install 'crawlee[playwright]'
```

Then, install the Playwright dependencies:

```bash
playwright install
```

You can install multiple extras at once by using a comma as a separator:

```bash
pip install 'crawlee[beautifulsoup,playwright]'
```

## Features

- Unified interface for **HTTP and headless browser** crawling.
- Persistent **queue** for URLs to crawl (breadth & depth-first).
- Pluggable **storage** of both tabular data and files.
- Automatic **scaling** with available system resources.
- Integrated **proxy rotation** and session management.
- Configurable **request routing** - directing URLs to appropriate handlers.
- Robust **error handling**.
- Automatic **retries** when getting blocked.
- Written in Python with **type hints**, which means better DX and fewer bugs.

## Introduction

Crawlee covers your crawling and scraping end-to-end and helps you build reliable scrapers. Fast.

Your crawlers will appear human-like and fly under the radar of modern bot protections even with the default
configuration. Crawlee gives you the tools to crawl the web for links, scrape data and persistently store it
in machine-readable formats, without having to worry about the technical details. And thanks to rich configuration
options, you can tweak almost any aspect of Crawlee to suit your project's needs if the default settings
don't cut it.

### Crawlers

Crawlee offers a framework for parallel web crawling through a variety of crawler classes, each designed to meet different crawling needs.

#### HttpCrawler

[`HttpCrawler`](https://github.com/apify/crawlee-py/tree/master/src/crawlee/http_crawler) provides a framework
for the parallel crawling of web pages using plain HTTP requests. The URLs to crawl are fed from a request provider.
It enables the recursive crawling of websites. The parsing of obtained HTML is the user's responsibility.

Since `HttpCrawler` uses raw HTTP requests to download web pages, it is very fast and efficient on data
bandwidth. However, if the target website requires JavaScript to display the content, you might need to use
some browser crawler instead, e.g. `PlaywrightCrawler`, because it loads the pages using a full-featured headless Chrome browser.

`HttpCrawler` downloads each URL using a plain HTTP request, obtain the response and then invokes the
user-provided request handler to extract page data.

The source URLs are represented using the
[`Request`](https://github.com/apify/crawlee-py/blob/master/src/crawlee/models.py) objects that are fed from
[`RequestList`](https://github.com/apify/crawlee-py/blob/master/src/crawlee/storages/request_list.py)
or [`RequestQueue`](https://github.com/apify/crawlee-py/blob/master/src/crawlee/storages/request_queue.py)
instances provided by the request provider option.

The crawler finishes when there are no more Request objects to crawl.

If you want to parse data using [BeautifulSoup](https://pypi.org/project/beautifulsoup4/) see
the `BeautifulSoupCrawler` section.

Example usage:

```python
import asyncio

from crawlee.http_crawler import HttpCrawler, HttpCrawlingContext


async def main() -> None:
    # Create a HttpCrawler instance and provide a starting requests
    crawler = HttpCrawler()

    # Define a handler for processing requests
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        # Crawler will provide a HttpCrawlingContext instance,
        # from which you can access the request and response data
        data = {
            'url': context.request.url,
            'status_code': context.http_response.status_code,
            'headers': dict(context.http_response.headers),
            'response': context.http_response.read().decode()[:1000],
        }
        # Extract the record and push it to the dataset
        await context.push_data(data)

    # Run the crawler
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

For further explanation of storages (dataset, request queue) see the storages section.

#### BeautifulSoupCrawler

[`BeautifulSoupCrawler`](https://github.com/apify/crawlee-py/tree/master/src/crawlee/beautifulsoup_crawler) extends
the `HttpCrawler`. It provides the same features and on top of that, it uses
[BeautifulSoup](https://www.crummy.com/software/BeautifulSoup/) HTML parser.

Same as for `HttpCrawler`, since `BeautifulSoupCrawler` uses raw HTTP requests to download web pages,
it is very fast and efficient on data bandwidth. However, if the target website requires JavaScript to display
the content, you might need to use `PlaywrightCrawler` instead, because it loads the pages using
a full-featured headless browser (Chrome, Firefox or others).

`BeautifulSoupCrawler` downloads each URL using a plain HTTP request, parses the HTML content using BeautifulSoup
and then invokes the user-provided request handler to extract page data using an interface to the
parsed HTML DOM.

Example usage:

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    # Create a BeautifulSoupCrawler instance and provide a request provider
    crawler = BeautifulSoupCrawler()

    # Define a handler for processing requests
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        # Crawler will provide a BeautifulSoupCrawlingContext instance,
        # from which you can access the request and response data
        data = {
            'title': context.soup.title.text,
            'url': context.request.url,
        }
        # Extract the record and push it to the dataset
        await context.push_data(data)

    # Run the crawler
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

`BeautifulSoupCrawler` also provides a helper for enqueuing links in the currently crawling website.
See the following example with the updated request handler:

```python
from crawlee.enqueue_strategy import EnqueueStrategy

# ...

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        # Use enqueue links helper to enqueue all links from the page with the same domain
        await context.enqueue_links(strategy=EnqueueStrategy.SAME_DOMAIN)

        data = {
            'title': context.soup.title.text,
            'url': context.request.url,
        }

        await context.push_data(data)
```

#### PlaywrightCrawler

[`PlaywrightCrawler`](https://github.com/apify/crawlee-py/tree/master/src/crawlee/playwright_crawler) extends
the `BasicCrawler`. It provides the same features and on top of that, it uses
[Playwright](https://playwright.dev/python) browser automation tool.

This crawler provides a straightforward framework for parallel web page crawling using headless versions of Chromium,
Firefox, and Webkit browsers through Playwright. URLs to be crawled are supplied by a request provider, which can be
either a `RequestList` containing a static list of URLs or a dynamic `RequestQueue`.

Using a headless browser to download web pages and extract data, `PlaywrightCrawler` is ideal for crawling
websites that require JavaScript execution. For websites that do not require JavaScript, consider using
the `BeautifulSoupCrawler`, which utilizes raw HTTP requests and will be much faster.

Example usage:

```python
import asyncio

from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    # Create a crawler instance and provide a request provider (and other optional arguments)
    crawler = PlaywrightCrawler(
        # headless=False,
        # browser_type='firefox',
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        data = {
            'request_url': context.request.url,
            'page_url': context.page.url,
            'page_title': await context.page.title(),
            'page_content': (await context.page.content())[:10000],
        }
        await context.push_data(data)

    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

Example usage with custom browser pool:

```python
import asyncio

from crawlee.browsers import BrowserPool, PlaywrightBrowserPlugin
from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    # Create a browser pool with a Playwright browser plugin
    browser_pool = BrowserPool(
        plugins=[
            PlaywrightBrowserPlugin(
                browser_type='firefox',
                browser_options={'headless': False},
                page_options={'viewport': {'width': 1920, 'height': 1080}},
            )
        ]
    )

    # Create a crawler instance and provide a browser pool and request provider
    crawler = PlaywrightCrawler(browser_pool=browser_pool)

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        data = {
            'request_url': context.request.url,
            'page_url': context.page.url,
            'page_title': await context.page.title(),
            'page_content': (await context.page.content())[:10000],
        }
        await context.push_data(data)

    await crawler.run(['https://apify.com', 'https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

### Storages

Crawlee introduces several result storage types that are useful for specific tasks. The storing of underlying data
is realized by the storage client. Currently, only a memory storage client is implemented. Using this, the data
are stored either in the memory or persisted on the disk.

By default, the data are stored in the directory specified by the `CRAWLEE_STORAGE_DIR` environment variable.
With default `.storage/`.

#### Dataset

A [`Dataset`](https://github.com/apify/crawlee-py/blob/master/src/crawlee/storages/dataset.py) is a type
of storage mainly suitable for storing tabular data.

Datasets are used to store structured data where each object stored has the same attributes, such as online store
products or real estate offers. The dataset can be imagined as a table, where each object is a row and its attributes
are columns. The dataset is an append-only storage - we can only add new records to it, but we cannot modify or
remove existing records.

Each Crawlee project run is associated with a default dataset. Typically, it is used to store crawling results
specific to the crawler run. Its usage is optional.

The data are persisted as follows:

```
{CRAWLEE_STORAGE_DIR}/datasets/{DATASET_ID}/{INDEX}.json
```

The following code demonstrates the basic operations of the dataset:

```python
import asyncio

from crawlee.storages import Dataset


async def main() -> None:
    # Open a default dataset
    dataset = await Dataset.open()

    # Push a single record
    await dataset.push_data({'key1': 'value1'})

    # Get records from the dataset
    data = await dataset.get_data()
    print(f'Dataset data: {data.items}')  # Dataset data: [{'key1': 'value1'}]

    # Open a named dataset
    dataset_named = await Dataset.open(name='some-name')

    # Push multiple records
    await dataset_named.push_data([{'key2': 'value2'}, {'key3': 'value3'}])


if __name__ == '__main__':
    asyncio.run(main())
```

<!-- TODO: link to a real-world example -->

#### Key-value store

The [`KeyValueStore`](https://github.com/apify/crawlee-py/blob/master/src/crawlee/storages/key_value_store.py)
is used for saving and reading data records or files. Each data record is represented by a unique
key and associated with a MIME content type. Key-value stores are ideal for saving screenshots of web pages, and PDFs
or to persist the state of crawlers.

Each Crawlee project run is associated with a default key-value store. By convention, the project input and output are stored in the default key-value store under the `INPUT` and `OUTPUT` keys respectively. Typically, both input
and output are `JSON` files, although they could be any other format.

The data are persisted as follows:

```
{CRAWLEE_STORAGE_DIR}/key_value_stores/{STORE_ID}/{KEY}.{EXT}
```

The following code demonstrates the basic operations of key-value stores:

```python
import asyncio

from crawlee.storages import KeyValueStore


async def main() -> None:
    kvs = await KeyValueStore.open()  # Open a default key-value store

    # Write the OUTPUT to the default key-value store
    await kvs.set_value('OUTPUT', {'my_result': 123})

    # Read the OUTPUT from the default key-value store
    value = await kvs.get_value('OUTPUT')
    print(f'Value of OUTPUT: {value}')  # Value of OUTPUT: {'my_result': 123}

    # Open a named key-value store
    kvs_named = await KeyValueStore.open(name='some-name')

    # Write a record to the named key-value store
    await kvs_named.set_value('some-key', {'foo': 'bar'})

    # Delete a record from the named key-value store
    await kvs_named.set_value('some-key', None)


if __name__ == '__main__':
    asyncio.run(main())
```

<!-- TODO: link to a real-world example -->

#### Request queue

The [`RequestQueue`](https://github.com/apify/crawlee-py/blob/master/src/crawlee/storages/request_queue.py)
is a storage of URLs (requests) to crawl. The queue is used for the deep crawling of websites,
where we start with several URLs and then recursively follow links to other pages. The data structure supports both
breadth-first and depth-first crawling orders.

Each Crawlee project run is associated with a default request queue. Typically, it is used to store URLs to crawl in the specific crawler run. Its usage is optional.

The data are persisted as follows:

```
{CRAWLEE_STORAGE_DIR}/request_queues/{QUEUE_ID}/entries.json
```

The following code demonstrates the basic usage of the request queue:

```python
import asyncio

from crawlee.storages import RequestQueue


async def main() -> None:
    # Open a default request queue
    rq = await RequestQueue.open()

    # Add a single request
    await rq.add_request('https://crawlee.dev')

    # Open a named request queue
    rq_named = await RequestQueue.open(name='some-name')

    # Add multiple requests
    await rq_named.add_requests_batched(['https://apify.com', 'https://example.com'])

    # Fetch the next request
    request = await rq_named.fetch_next_request()
    print(f'Next request: {request.url}')  # Next request: https://apify.com


if __name__ == '__main__':
    asyncio.run(main())
```

For an example of usage of the request queue with a crawler see the `BeautifulSoupCrawler` example.

### Session Management

[​SessionPool](https://github.com/apify/crawlee-py/blob/master/src/crawlee/sessions/session_pool.py)
is a class that allows us to handle the rotation of proxy IP addresses along with cookies and other custom
settings in Crawlee.

The main benefit of using a session pool is that we can filter out blocked or non-working proxies,
so our actor does not retry requests over known blocked/non-working proxies. Another benefit of using
the session pool is that we can store information tied tightly to an IP address, such as cookies, auth tokens,
and particular headers. Having our cookies and other identifiers used only with a specific IP will reduce
the chance of being blocked. The last but not least benefit is the even rotation of IP addresses - the session
pool picks the session randomly, which should prevent burning out a small pool of available IPs.

To use a default session pool with automatic session rotation use the `use_session_pool` option for the crawler.

```python
from crawlee.http_crawler import HttpCrawler

crawler = HttpCrawler(use_session_pool=True)
```

If you want to configure your own session pool, instantiate it and provide it directly to the crawler.

```python
import asyncio
from datetime import timedelta

from crawlee.http_crawler import HttpCrawler
from crawlee.sessions import Session, SessionPool


async def main() -> None:
    # Use dict as args for new sessions
    session_pool_v1 = SessionPool(
        max_pool_size=10,
        create_session_settings = {'max_age': timedelta(minutes=10)},
    )

    # Use lambda creation function for new sessions
    session_pool_v2 = SessionPool(
        max_pool_size=10,
        create_session_function=lambda _: Session(max_age=timedelta(minutes=10)),
    )

    crawler = HttpCrawler(session_pool=session_pool_v1, use_session_pool=True)


if __name__ == '__main__':
    asyncio.run(main())
```

<!--
### Browser Management

- TODO
- Write once browser rotation and/or other features are ready
- Update PlaywrightCrawler according to this
-->

## Running on the Apify platform

Crawlee is open-source and runs anywhere, but since it's developed by [Apify](https://apify.com), it's easy to set up on the Apify platform and run in the cloud. Visit the [Apify SDK website](https://sdk.apify.com) to learn more about deploying Crawlee to the Apify platform.

## Support

If you find any bug or issue with Crawlee, please [submit an issue on GitHub](https://github.com/apify/crawlee-py/issues). For questions, you can ask on [Stack Overflow](https://stackoverflow.com/questions/tagged/apify), in GitHub Discussions or you can join our [Discord server](https://discord.com/invite/jyEM2PRvMU).

## Contributing

Your code contributions are welcome, and you'll be praised for eternity! If you have any ideas for improvements, either submit an issue or create a pull request. For contribution guidelines and the code of conduct, see [CONTRIBUTING.md](https://github.com/apify/crawlee-py/blob/master/CONTRIBUTING.md).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE.md](https://github.com/apify/crawlee-py/blob/master/LICENSE.md) file for details.
