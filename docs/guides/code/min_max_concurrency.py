from crawlee import ConcurrencySettings
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler

concurrency_settings = ConcurrencySettings(
    # Start the crawler right away with 8 concurrent tasks, if they are available
    desired_concurrency=8,
    # Ensure there will always be minimum 5 concurrent tasks at any time
    min_concurrency=5,
    # Ensure the crawler doesn't exceed 10 concurrent tasks at any time
    max_concurrency=10,
)

crawler = BeautifulSoupCrawler(
    # Pass the concurrency setting to the crawler
    concurrency_settings=concurrency_settings,
)
