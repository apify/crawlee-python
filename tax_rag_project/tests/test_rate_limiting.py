"""Test script for rate limiting and security features."""

import asyncio
from datetime import UTC, datetime

from tax_rag_scraper.config.settings import Settings
from tax_rag_scraper.crawlers.base_crawler import TaxDataCrawler


async def main() -> None:
    """Test rate limiting and security features."""

    # Configure for aggressive rate limiting (easy to observe)
    settings = Settings(
        MAX_REQUESTS_PER_MINUTE=10,  # Only 10 requests/minute
        MAX_CONCURRENCY=2,
        RESPECT_ROBOTS_TXT=True,
    )

    test_urls = [
        'https://www.canada.ca/en/revenue-agency.html',
        'https://www.canada.ca/en/revenue-agency/services/tax.html',
        'https://www.canada.ca/en/revenue-agency/services/forms-publications.html',
        'https://www.canada.ca/en/revenue-agency/corporate/about-canada-revenue-agency-cra.html',
        'https://www.canada.ca/en/revenue-agency/services/e-services.html',
    ]

    print('Testing rate limiting and security features...')
    print(f'Rate limit: {settings.MAX_REQUESTS_PER_MINUTE} requests/minute')
    print(f'Robots.txt respect: {settings.RESPECT_ROBOTS_TXT}')
    print(f'\nStarting crawl at {datetime.now(UTC).strftime("%H:%M:%S")}')
    print('Watch for delayed requests (should take ~30 seconds for 5 URLs)\n')

    start_time = datetime.now(UTC)

    crawler = TaxDataCrawler(settings=settings, max_depth=0)
    await crawler.run(test_urls)

    end_time = datetime.now(UTC)
    duration = (end_time - start_time).total_seconds()

    print(f'\nCrawl finished at {end_time.strftime("%H:%M:%S")}')
    print(f'Total duration: {duration:.2f} seconds')
    print(f'Average: {duration / len(test_urls):.2f} seconds per URL')

    # Verify rate limiting worked
    expected_min_duration = (len(test_urls) - 1) * (60 / settings.MAX_REQUESTS_PER_MINUTE)
    if duration >= expected_min_duration:
        print(f'\n[SUCCESS] Rate limiting working correctly (expected minimum: {expected_min_duration:.2f}s)')
    else:
        print('\n[WARNING] Rate limiting may not be working (duration too short)')


if __name__ == '__main__':
    asyncio.run(main())
