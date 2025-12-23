"""Simple runner for testing base_crawler.py directly."""

import asyncio
import logging
import sys
from pathlib import Path

# Add src to path so imports work
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

# Import after path setup - this is required for the crawler module
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler  # noqa: E402

logger = logging.getLogger(__name__)


async def main() -> None:
    """Run a simple crawl with base_crawler."""
    test_urls = [
        'https://www.canada.ca/en/revenue-agency.html',
        'https://www.canada.ca/en/revenue-agency/services/tax.html',
    ]

    logger.info('Starting base_crawler test...')
    logger.info('Processing %d URLs\n', len(test_urls))

    crawler = TaxDataCrawler()
    await crawler.run(test_urls)

    logger.info('\n[SUCCESS] Base crawler test complete!')


if __name__ == '__main__':
    asyncio.run(main())
