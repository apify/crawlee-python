---
id: setting-up
title: Setting up
---

To run Crawlee on your computer, ensure you meet the following requirements:

1. [Python](https://www.python.org/) 3.9 or higher installed,
2. [Pip](https://pip.pypa.io/en/stable/) installed.

You can verify these by running the following commands:

```bash
python --version
```

```bash
pip --version
```

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

Verify that Crawlee is successfully installed:

```bash
python -c 'import crawlee; print(crawlee.__version__)'
```

<!-- TODO: Update this section once templates are ready -->

## Next steps

Next, you will learn how to create a very simple crawler and u Crawlee components while building it.
