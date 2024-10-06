from crawlee import PuppeteerCrawler
from crawlee.browsers._browser_pool import BrowserPool, BrowserName, DeviceCategory, OperatingSystemsName

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

# Instantiate the PuppeteerCrawler with the customized browser pool
crawler = PuppeteerCrawler(
    browser_pool=browser_pool,
    # ...
)
