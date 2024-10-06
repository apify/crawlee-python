from crawlee.playwright_crawler import PlaywrightCrawler
from crawlee.browsers import BrowserPool

# Create a browser pool with use_fingerprints set to False
browser_pool = BrowserPool.with_default_plugin(
    headless=True,
    kwargs={
        'use_fingerprints': False,
    },
)

# Instantiate the PlaywrightCrawler with the customized browser pool
crawler = PlaywrightCrawler(
    browser_pool=browser_pool,
    # ...
)
