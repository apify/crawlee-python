from crawlee.browsers import BrowserPool
from crawlee.browsers._types import BrowserType  # This contains the literal for browser types
from crawlee.playwright_crawler import PlaywrightCrawler

# Create the browser pool with customized fingerprint options
browser_pool = BrowserPool.with_default_plugin(
    headless=True,
    browser_type='chromium',  # Use 'chromium', 'firefox', or 'webkit'
    kwargs={
        'use_fingerprints': True,
        'fingerprint_options': {
            'fingerprint_generator_options': {
                'browsers': [
                    {
                        'name': 'chromium',  # Or 'firefox', or 'webkit'
                        'min_version': 96,
                    },
                ],
                'devices': ['desktop'],  # Specify device types directly
                'operating_systems': ['windows'],  # Specify OS types directly
            },
        },
    },
)

# Instantiate the PlaywrightCrawler with the customized browser pool
crawler = PlaywrightCrawler(
    browser_pool=browser_pool,
)
