from crawlee.browsers import BrowserPool, BrowserName, DeviceCategory, OperatingSystemsName
from crawlee.playwright_crawler import PlaywrightCrawler

# Create the browser pool with customized fingerprint options
browser_pool = BrowserPool.with_default_plugin(
    headless=True,
    browser_type=BrowserName.EDGE,
    kwargs={
        'use_fingerprints': True,  # this is the default
        'fingerprint_options': {
            'fingerprint_generator_options': {
                'browsers': [
                    {
                        'name': BrowserName.EDGE,
                        'min_version': 96,
                    },
                ],
                'devices': [DeviceCategory.DESKTOP],
                'operating_systems': [OperatingSystemsName.WINDOWS],
            },
        },
    },
)

# Instantiate the PlaywrightCrawler with the customized browser pool
crawler = PlaywrightCrawler(
    browser_pool=browser_pool,
    # Additional parameters if needed
)
