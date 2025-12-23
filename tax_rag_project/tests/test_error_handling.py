"""Test script for error handling and retry mechanisms."""

import asyncio

from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler


async def main() -> None:
    """Test error handling with a mix of valid and invalid URLs."""
    test_urls = [
        # Valid URL
        'https://www.canada.ca/en/revenue-agency/services/forms-publications.html',
        # Invalid URLs to test error handling
        'https://www.canada.ca/en/revenue-agency/this-page-does-not-exist-404.html',
        'https://invalid-domain-that-does-not-exist-12345.com',
    ]

    print('Testing error handling with valid and invalid URLs...\n')

    crawler = TaxDataCrawler(max_depth=0)
    await crawler.run(test_urls)

    print('\n[SUCCESS] Error handling test complete.')
    print('Check logs above to verify:')
    print('  - Valid URL processed successfully')
    print('  - 404 URL logged warning (no crash)')
    print('  - Invalid domain retried 3 times then failed gracefully')


if __name__ == '__main__':
    asyncio.run(main())
