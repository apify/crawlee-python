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

Your crawlers will appear human-like and fly under the radar of modern bot protections even with the default configuration. Crawlee gives you the tools to crawl the web for links, scrape data, and store it to disk or cloud while staying configurable to suit your project's needs.

We have also a TypeScript implementation, see [github.com/apify/crawlee](https://github.com/apify/crawlee).

## Installation

Crawlee is available as the [`crawlee`](https://pypi.org/project/crawlee/) PyPI package.

```
pip install crawlee
```

## Features

- Single interface for **HTTP and headless browser** crawling.
- Persistent **queue** for URLs to crawl (breadth & depth-first).
- Pluggable **storage** of both tabular data and files.
- Automatic **scaling** with available system resources.
- Integrated **proxy rotation** and session management.
- Configurable **routing**, **error handling** and **retries**.
- Written in Python with **type hints**.

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
for the parallel crawling of web pages using plain HTTP requests.
The URLs to crawl are fed either from a static list of URLs or from a dynamic queue of URLs enabling recursive
crawling of websites. The parsing of obtained HTML is the user's responsibility.
A [HTTPX](https://pypi.org/project/httpx/) library is used for making HTTP requests.

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
from crawlee.storages import Dataset, RequestQueue


async def main() -> None:
    # Open a default request queue and add requests to it
    rq = await RequestQueue.open()
    await rq.add_request('https://crawlee.dev')

    # Open a default dataset for storing results
    dataset = await Dataset.open()

    # Create a HttpCrawler instance and provide a request provider
    crawler = HttpCrawler(request_provider=rq)

    # Define a handler for processing requests
    @crawler.router.default_handler
    async def request_handler(context: HttpCrawlingContext) -> None:
        # Crawler will provide a HttpCrawlingContext instance, from which you can access
        # the request and response data
        record = {
            'url': context.request.url,
            'status_code': context.http_response.status_code,
            'headers': dict(context.http_response.headers),
            'response': context.http_response.read().decode()[:1000],
        }
        # Extract the record and push it to the dataset
        await dataset.push_data(record)

    # Run the crawler
    await crawler.run()


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
a full-featured headless Chrome browser.

`BeautifulSoupCrawler` downloads each URL using a plain HTTP request, parses the HTML content using BeautifulSoup
and then invokes the user-provided request handler to extract page data using an interface to the
parsed HTML DOM.

Example usage:

```python
import asyncio

from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler, BeautifulSoupCrawlingContext
from crawlee.storages import Dataset, RequestQueue


async def main() -> None:
    # Open a default request queue and add requests to it
    rq = await RequestQueue.open()
    await rq.add_request('https://crawlee.dev')

    # Open a default dataset for storing results
    dataset = await Dataset.open()

    # Create a BeautifulSoupCrawler instance and provide a request provider
    crawler = BeautifulSoupCrawler(request_provider=rq)

    # Define a handler for processing requests
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        # Crawler will provide a BeautifulSoupCrawlingContext instance, from which you can access
        # the request and response data
        record = {
            'title': context.soup.title.text if context.soup.title else '',
            'url': context.request.url,
        }
        # Extract the record and push it to the dataset
        await dataset.push_data(record)

    # Run the crawler
    await crawler.run()


if __name__ == '__main__':
    asyncio.run(main())
```

`BeautifulSoupCrawler` also provides a helper for enqueuing links in the currently crawling website.
See the following example with the updated request handler:

```python
    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        # Use enqueue links helper to enqueue all links from the page with the same domain
        await context.enqueue_links(strategy=EnqueueStrategy.SAME_DOMAIN)
        record = {
            'title': context.soup.title.text if context.soup.title else '',
            'url': context.request.url,
        }
        await dataset.push_data(record)
```

#### PlaywrightCrawler

- TODO

### Storages

Crawlee introduces several result storage types that are useful for specific tasks. The storing of underlying data
is realized by the storage client. Currently, only a memory storage client is implemented. Using this, the data
are stored either in the memory or persisted on the disk.

#### Dataset

A [`Dataset`](https://github.com/apify/crawlee-py/blob/master/src/crawlee/storages/dataset.py) is a type
of storage mainly suitable for storing tabular data.

Datasets are used to store structured data where each object stored has the same attributes, such as online store
products or real estate offers. The dataset can be imagined as a table, where each object is a row and its attributes
are columns. The dataset is an append-only storage - we can only add new records to it, but we cannot modify or
remove existing records.

Each Crawlee project run is associated with a default dataset. Typically, it is used to store crawling results
specific to the crawler run. Its usage is optional.

By default, the data is stored in the directory specified by the `CRAWLEE_STORAGE_DIR` environment variable
as follows:

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
    dataset_named = await Dataset.open('some-name')

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

By default, the data is stored in the directory specified by the `CRAWLEE_STORAGE_DIR` environment variable
as follows:

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
    kvs_named = await KeyValueStore.open('some-name')

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

By default, the data is stored in the directory specified by the `CRAWLEE_STORAGE_DIR` environment variable
as follows:

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
    rq_named = await RequestQueue.open('some-name')

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
from crawlee.http_crawler import HttpCrawler
from crawlee.sessions import SessionPool

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
```

## Running on the Apify platform

Crawlee is open-source and runs anywhere, but since it's developed by [Apify](https://apify.com), it's easy to set up on the Apify platform and run in the cloud. Visit the [Apify SDK website](https://sdk.apify.com) to learn more about deploying Crawlee to the Apify platform.

## Support

If you find any bug or issue with Crawlee, please [submit an issue on GitHub](https://github.com/apify/crawlee-py/issues). For questions, you can ask on [Stack Overflow](https://stackoverflow.com/questions/tagged/apify), in GitHub Discussions or you can join our [Discord server](https://discord.com/invite/jyEM2PRvMU).

## Contributing

Your code contributions are welcome, and you'll be praised for eternity! If you have any ideas for improvements, either submit an issue or create a pull request. For contribution guidelines and the code of conduct, see [CONTRIBUTING.md](https://github.com/apify/crawlee-py/blob/master/CONTRIBUTING.md).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE.md](https://github.com/apify/crawlee-py/blob/master/LICENSE.md) file for details.
