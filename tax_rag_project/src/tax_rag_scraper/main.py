"""Main entry point for the tax documentation crawler."""

import asyncio

from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler


async def main():
    """Run the base crawler test."""
    # Test with CRA website (Canadian Revenue Agency - public info pages)
    test_urls = [
        'https://www.canada.ca/en/revenue-agency/services/forms-publications.html'
    ]

    crawler = TaxDataCrawler()
    await crawler.run(test_urls)

    print("[OK] Base crawler test complete. Check storage/datasets/default/ for results.")


if __name__ == '__main__':
    asyncio.run(main())
