---
id: setting-up
title: Setting up
---

To run Crawlee on your computer, ensure you meet the following requirements:

1. [Python](https://www.python.org/) 3.9 or higher installed,
2. [Pip](https://pip.pypa.io/en/stable/) installed.

You can verify these by running the following commands:

```sh
python --version
```

```sh
pip --version
```

## Installation

Crawlee is available as the [`crawlee`](https://pypi.org/project/crawlee/) PyPI package.

```sh
pip install crawlee
```

Additional, optional dependencies unlocking more features are shipped as package extras.

If you plan to use `BeautifulSoupCrawler`, install `crawlee` with `beautifulsoup` extra:

```sh
pip install 'crawlee[beautifulsoup]'
```

If you plan to use `PlaywrightCrawler`, install `crawlee` with the `playwright` extra:

```sh
pip install 'crawlee[playwright]'
```

Then, install the Playwright dependencies:

```sh
playwright install
```

You can install multiple extras at once by using a comma as a separator:

```sh
pip install 'crawlee[beautifulsoup,playwright]'
```

Verify that Crawlee is successfully installed:

```sh
python -c 'import crawlee; print(crawlee.__version__)'
```

## With Crawlee CLI

The quickest way to get started with Crawlee is by using the Crawlee CLI and selecting one of the prepared templates. First, ensure you have [Pipx](https://pipx.pypa.io/) installed:

```sh
pipx --help
```

Then, run the CLI and choose from the available templates:

```sh
pipx run crawlee create my-crawler
```

## Next steps

Next, you will learn how to create a very simple crawler and u Crawlee components while building it.
