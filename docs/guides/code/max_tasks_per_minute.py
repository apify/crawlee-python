from crawlee import ConcurrencySettings
from crawlee.beautifulsoup_crawler import BeautifulSoupCrawler

concurrency_settings = ConcurrencySettings(
    # Let the crawler know it can run up to 100 requests concurrently at any time
    max_concurrency=100,
    # ...but also ensure the crawler never exceeds 250 requests per minute
    max_tasks_per_minute=10,
)

crawler = BeautifulSoupCrawler(
    # Pass the concurrency setting to the crawler
    concurrency_settings=concurrency_settings,
)
