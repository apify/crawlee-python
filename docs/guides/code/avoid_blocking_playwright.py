from crawlee.browsers import BrowserPool
from crawlee.browsers._playwright_browser_plugin import PlaywrightBrowserPlugin
from crawlee.playwright_crawler import PlaywrightCrawler

# Create the PlaywrightBrowserPlugin with customized options
plugin = PlaywrightBrowserPlugin(
    browser_type='chromium',  # Use 'chromium', 'firefox', or 'webkit'
    browser_options={
        'args': [
            '--no-sandbox',
            '--disable-setuid-sandbox',
        ],
    },
    fingerprint_generator_options={
        'browsers': [
            {
                'name': 'chromium',  # Or 'firefox', or 'webkit'
                'min_version': 96,
            },
        ],
        'devices': ['desktop'],  # Specify device types directly
        'operating_systems': ['windows'],  # Specify OS types directly
    },
    use_fingerprints=True,  # Enable fingerprinting
)

# Create the browser pool with the customized plugin
browser_pool = BrowserPool(plugins=[plugin])

# Instantiate the PlaywrightCrawler with the customized browser pool
crawler = PlaywrightCrawler(
    browser_pool=browser_pool,
)
