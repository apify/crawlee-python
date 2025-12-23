import asyncio
import logging

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler

logger = logging.getLogger(__name__)


async def main() -> None:
    """Test deep crawling and multi-site support."""
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

    logger.info('Testing deep crawling and site-specific handlers...')
    logger.info('Max crawl depth: 2')
    logger.info('Max requests: %d', settings.MAX_REQUESTS_PER_CRAWL)
    logger.info('Starting from: %s\n', test_urls[0])

    crawler = TaxDataCrawler(settings=settings, max_depth=2)
    await crawler.run(test_urls)

    logger.info('\n[SUCCESS] Deep crawling test complete.')
    logger.info('\nExpected behavior:')
    logger.info('  - Depth 0: Processed seed URL (forms page)')
    logger.info('  - Depth 1: Discovered and processed links from seed page')
    logger.info('  - Depth 2: Discovered and processed links from depth 1 pages')
    logger.info('  - CRA handler extracted structured data (title, tax year, document type)')
    logger.info('  - Statistics show multiple URLs processed')
    logger.info('\nCheck storage/datasets/default/ for extracted documents')


if __name__ == '__main__':
    asyncio.run(main())
