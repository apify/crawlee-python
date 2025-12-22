"""Simple runner for testing base_crawler.py directly."""

import asyncio
import sys
from pathlib import Path

# Add src to path so imports work
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler


async def main():
    """Run a simple crawl with base_crawler."""
    test_urls = [
        'https://www.canada.ca/en/revenue-agency.html',
        'https://www.canada.ca/en/revenue-agency/services/tax.html',
    ]

    print('Starting base_crawler test...')
    print(f'Processing {len(test_urls)} URLs\n')

    crawler = TaxDataCrawler()
    await crawler.run(test_urls)

    print('\n[SUCCESS] Base crawler test complete!')


if __name__ == '__main__':
    asyncio.run(main())
