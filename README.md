<h1 align="center">
    <a href="https://crawlee.dev">
        <picture>
          <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/apify/crawlee-python/master/website/static/img/crawlee-dark.svg?sanitize=true">
          <img alt="Crawlee" src="https://raw.githubusercontent.com/apify/crawlee-python/master/website/static/img/crawlee-light.svg?sanitize=true" width="500">
        </picture>
    </a>
    <br>
    <small>A web scraping and browser automation library</small>
</h1>

<p align=center>
    <a href="https://trendshift.io/repositories/11169" target="_blank"><img src="https://trendshift.io/api/badge/repositories/11169" alt="apify%2Fcrawlee-python | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</p>

<p align=center>
    <a href="https://badge.fury.io/py/crawlee" rel="nofollow">
        <img src="https://badge.fury.io/py/crawlee.svg" alt="PyPI version" style="max-width: 100%;">
    </a>
    <a href="https://pypi.org/project/crawlee/" rel="nofollow">
        <img src="https://img.shields.io/pypi/dm/crawlee" alt="PyPI - Downloads" style="max-width: 100%;">
    </a>
    <a href="https://pypi.org/project/crawlee/" rel="nofollow">
        <img src="https://img.shields.io/pypi/pyversions/crawlee" alt="PyPI - Python Version" style="max-width: 100%;">
    </a>
    <a href="https://discord.gg/jyEM2PRvMU" rel="nofollow">
        <img src="https://img.shields.io/discord/801163717915574323?label=discord" alt="Chat on discord" style="max-width: 100%;">
    </a>
</p>

Crawlee covers your crawling and scraping end-to-end and **helps you build reliable scrapers. Fast.**

> 🚀 Crawlee for Python is open to early adopters!

Your crawlers will appear almost human-like and fly under the radar of modern bot protections even with the default configuration. Crawlee gives you the tools to crawl the web for links, scrape data and persistently store it in machine-readable formats, without having to worry about the technical details. And thanks to rich configuration options, you can tweak almost any aspect of Crawlee to suit your project's needs if the default settings don't cut it.

> 👉 **View full documentation, guides and examples on the [Crawlee project website](https://crawlee.dev/python/)** 👈

We also have a TypeScript implementation of the Crawlee, which you can explore and utilize for your projects. Visit our GitHub repository for more information [Crawlee for JS/TS on GitHub](https://github.com/apify/crawlee).

## Installation

We recommend visiting the [Introduction tutorial](https://crawlee.dev/python/docs/introduction) in Crawlee documentation for more information.

Crawlee is available as [`crawlee`](https://pypi.org/project/crawlee/) package on PyPI. This package includes the core functionality, while additional features are available as optional extras to keep dependencies and package size minimal.

To install Crawlee with all features, run the following command:

```sh
python -m pip install 'crawlee[all]'
```

Then, install the [Playwright](https://playwright.dev/) dependencies:

```sh
playwright install
```

Verify that Crawlee is successfully installed:

```sh
python -c 'import crawlee; print(crawlee.__version__)'
```

For detailed installation instructions see the [Setting up](https://crawlee.dev/python/docs/introduction/setting-up) documentation page.

### With Crawlee CLI

The quickest way to get started with Crawlee is by using the Crawlee CLI and selecting one of the prepared templates. First, ensure you have [uv](https://pypi.org/project/uv/) installed:

```sh
uv --help
```

If [uv](https://pypi.org/project/uv/) is not installed, follow the official [installation guide](https://docs.astral.sh/uv/getting-started/installation/).

Then, run the CLI and choose from the available templates:

```sh
uvx 'crawlee[cli]' create my-crawler
```

If you already have `crawlee` installed, you can spin it up by running:

```sh
crawlee create my-crawler
```

## Examples

Here are some practical examples to help you get started with different types of crawlers in Crawlee. Each example demonstrates how to set up and run a crawler for specific use cases, whether you need to handle simple HTML pages or interact with JavaScript-heavy sites. A crawler run will create a `storage/` directory in your current working directory.

### BeautifulSoupCrawler

The [`BeautifulSoupCrawler`](https://crawlee.dev/python/api/class/BeautifulSoupCrawler) downloads web pages using an HTTP library and provides HTML-parsed content to the user. By default it uses [`HttpxHttpClient`](https://crawlee.dev/python/api/class/HttpxHttpClient) for HTTP communication and [BeautifulSoup](https://pypi.org/project/beautifulsoup4/) for parsing HTML. It is ideal for projects that require efficient extraction of data from HTML content. This crawler has very good performance since it does not use a browser. However, if you need to execute client-side JavaScript, to get your content, this is not going to be enough and you will need to use [`PlaywrightCrawler`](https://crawlee.dev/python/api/class/PlaywrightCrawler). Also if you want to use this crawler, make sure you install `crawlee` with `beautifulsoup` extra.

```python
import asyncio

from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext


async def main() -> None:
    crawler = BeautifulSoupCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
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

        # Push the extracted data to the default dataset.
        await context.push_data(data)

        # Enqueue all links found on the page.
        await context.enqueue_links()

    # Run the crawler with the initial list of URLs.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

### PlaywrightCrawler

The [`PlaywrightCrawler`](https://crawlee.dev/python/api/class/PlaywrightCrawler) uses a headless browser to download web pages and provides an API for data extraction. It is built on [Playwright](https://playwright.dev/), an automation library designed for managing headless browsers. It excels at retrieving web pages that rely on client-side JavaScript for content generation, or tasks requiring interaction with JavaScript-driven content. For scenarios where JavaScript execution is unnecessary or higher performance is required, consider using the [`BeautifulSoupCrawler`](https://crawlee.dev/python/api/class/BeautifulSoupCrawler). Also if you want to use this crawler, make sure you install `crawlee` with `playwright` extra.

```python
import asyncio

from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Limit the crawl to max requests. Remove or increase it for crawling all links.
        max_requests_per_crawl=10,
    )

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': await context.page.title(),
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

        # Enqueue all links found on the page.
        await context.enqueue_links()

    # Run the crawler with the initial list of requests.
    await crawler.run(['https://crawlee.dev'])


if __name__ == '__main__':
    asyncio.run(main())
```

### More examples

Explore our [Examples](https://crawlee.dev/python/docs/examples) page in the Crawlee documentation for a wide range of additional use cases and demonstrations.

## Features

Why Crawlee is the preferred choice for web scraping and crawling?

### Why use Crawlee instead of just a random HTTP library with an HTML parser?

- Unified interface for **HTTP & headless browser** crawling.
- Automatic **parallel crawling** based on available system resources.
- Written in Python with **type hints** - enhances DX (IDE autocompletion) and reduces bugs (static type checking).
- Automatic **retries** on errors or when you’re getting blocked.
- Integrated **proxy rotation** and session management.
- Configurable **request routing** - direct URLs to the appropriate handlers.
- Persistent **queue for URLs** to crawl.
- Pluggable **storage** of both tabular data and files.
- Robust **error handling**.

### Why to use Crawlee rather than Scrapy?

- **Asyncio-based** – Leveraging the standard [Asyncio](https://docs.python.org/3/library/asyncio.html) library, Crawlee delivers better performance and seamless compatibility with other modern asynchronous libraries.
- **Type hints** – Newer project built with modern Python, and complete type hint coverage for a better developer experience.
- **Simple integration** – Crawlee crawlers are regular Python scripts, requiring no additional launcher executor. This flexibility allows to integrate a crawler directly into other applications.
- **State persistence** – Supports state persistence during interruptions, saving time and costs by avoiding the need to restart scraping pipelines from scratch after an issue.
- **Organized data storages** – Allows saving of multiple types of results in a single scraping run. Offers several storing options (see [datasets](https://crawlee.dev/python/api/class/Dataset) & [key-value stores](https://crawlee.dev/python/api/class/KeyValueStore)).

## Running on the Apify platform

Crawlee is open-source and runs anywhere, but since it's developed by [Apify](https://apify.com), it's easy to set up on the Apify platform and run in the cloud. Visit the [Apify SDK website](https://docs.apify.com/sdk/python/) to learn more about deploying Crawlee to the Apify platform.

## Support

If you find any bug or issue with Crawlee, please [submit an issue on GitHub](https://github.com/apify/crawlee-python/issues). For questions, you can ask on [Stack Overflow](https://stackoverflow.com/questions/tagged/apify), in GitHub Discussions or you can join our [Discord server](https://discord.com/invite/jyEM2PRvMU).

## Contributing

Your code contributions are welcome, and you'll be praised for eternity! If you have any ideas for improvements, either submit an issue or create a pull request. For contribution guidelines and the code of conduct, see [CONTRIBUTING.md](https://github.com/apify/crawlee-python/blob/master/CONTRIBUTING.md).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](https://github.com/apify/crawlee-python/blob/master/LICENSE) file for details.
