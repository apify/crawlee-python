# Crawlee Python - Tax Data Collection

A specialized fork of Crawlee Python focused on tax-related data collection and processing.

This specialized version of Crawlee is designed for collecting tax-related data from various sources while maintaining compliance with data protection regulations. It provides robust scraping capabilities with built-in error handling, retry mechanisms, and data validation specifically tailored for tax data processing workflows.

## Features

- **Tax-specific data extraction** - Optimized for common tax document formats and structures
- **Compliance-focused** - Built-in safeguards for sensitive financial data handling
- **Robust error handling** - Comprehensive retry and recovery mechanisms for reliable data collection
- **Multiple crawler types** - BeautifulSoup, Playwright, and specialized crawlers for different use cases

## Installation

This project requires Python 3.10 or later and uses `uv` for dependency management.

### Development Setup

1. Clone this repository:
```sh
git clone <repository-url>
cd crawlee-python-tax
```

2. Install development dependencies:
```sh
make install-dev
```

3. Verify installation:
```sh
python -c 'import crawlee; print(crawlee.__version__)'
```

## Usage Examples

### Basic Tax Document Scraper

```python
import asyncio
from crawlee.crawlers import BeautifulSoupCrawler, BeautifulSoupCrawlingContext

async def main() -> None:
    crawler = BeautifulSoupCrawler(
        max_requests_per_crawl=50,
    )

    @crawler.router.default_handler
    async def request_handler(context: BeautifulSoupCrawlingContext) -> None:
        context.log.info(f'Processing tax document from {context.request.url} ...')
        
        # Extract tax-specific data
        tax_data = {
            'url': context.request.url,
            'document_type': context.soup.select_one('.document-type')?.get_text(),
            'tax_year': context.soup.select_one('.tax-year')?.get_text(),
            'filing_date': context.soup.select_one('.filing-date')?.get_text(),
        }
        
        # Store the extracted tax data
        await context.push_data(tax_data)

if __name__ == '__main__':
    asyncio.run(main())
```

### Browser-based Tax Form Processing

```python
import asyncio
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

async def main() -> None:
    crawler = PlaywrightCrawler(
        max_requests_per_crawl=10,
    )

    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing interactive tax form from {context.request.url} ...')
        
        # Wait for dynamic content to load
        await context.page.wait_for_selector('.tax-form-data')
        
        # Extract data from interactive elements
        form_data = {
            'url': context.request.url,
            'form_type': await context.page.locator('.form-type').text_content(),
            'tax_year': await context.page.locator('.tax-year').text_content(),
            'last_updated': await context.page.locator('.last-updated').text_content(),
        }
        
        await context.push_data(form_data)

if __name__ == '__main__':
    asyncio.run(main())
```

## Development

### Running Tests

```sh
# Run all tests
make check-code

# Run only unit tests
make unit-tests

# Run tests with coverage
make unit-tests-cov
```

### Code Quality

```sh
# Format code
make format

# Lint code
make lint

# Type check
make type-check
```

## Contributing

Contributions are welcome! Please read the development guidelines in CLAUDE.md and follow the established code style.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Attribution

This project is based on [Crawlee for Python](https://github.com/apify/crawlee-python) by Apify Technologies s.r.o. See [ATTRIBUTION.md](ATTRIBUTION.md) for complete attribution details.