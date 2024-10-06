from crawlee import PuppeteerCrawler
from crawlee.browsers import BrowserPool

# Create a browser pool with use_fingerprints set to False
browser_pool = BrowserPool.with_default_plugin(
    headless=True,
    kwargs={
        'use_fingerprints': False,
    },
)

# Instantiate the PuppeteerCrawler with the customized browser pool
crawler = PuppeteerCrawler(
    browser_pool=browser_pool,
    # ...
)
