import asyncio

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler


async def main():
    """Test deep crawling and multi-site support"""
    # Configure for moderate crawling
    settings = Settings(
        MAX_REQUESTS_PER_CRAWL=50,  # Limit total requests
        MAX_CONCURRENCY=3,
        MAX_REQUESTS_PER_MINUTE=30,  # Be respectful
        RESPECT_ROBOTS_TXT=True,
    )

    # Start with CRA forms page (likely has many links)
    test_urls = [
        'https://www.canada.ca/en/revenue-agency/services/forms-publications/forms.html',
    ]

    print('Testing deep crawling and site-specific handlers...')
    print('Max crawl depth: 2')
    print(f'Max requests: {settings.MAX_REQUESTS_PER_CRAWL}')
    print(f'Starting from: {test_urls[0]}\n')

    crawler = TaxDataCrawler(settings=settings, max_depth=2)
    await crawler.run(test_urls)

    print('\n[SUCCESS] Deep crawling test complete.')
    print('\nExpected behavior:')
    print('  - Depth 0: Processed seed URL (forms page)')
    print('  - Depth 1: Discovered and processed links from seed page')
    print('  - Depth 2: Discovered and processed links from depth 1 pages')
    print('  - CRA handler extracted structured data (title, tax year, document type)')
    print('  - Statistics show multiple URLs processed')
    print('\nCheck storage/datasets/default/ for extracted documents')


if __name__ == '__main__':
    asyncio.run(main())
