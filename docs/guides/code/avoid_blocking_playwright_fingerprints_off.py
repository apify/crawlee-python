from crawlee.browsers import BrowserPool
from crawlee.playwright_crawler import PlaywrightCrawler

# Create a browser pool with use_fingerprints set to False
browser_pool = BrowserPool.with_default_plugin(
    headless=True,
    use_fingerprints=False,
)

# Instantiate the PlaywrightCrawler with the customized browser pool
crawler = PlaywrightCrawler(
    browser_pool=browser_pool,
    # Additional parameters if needed
)
