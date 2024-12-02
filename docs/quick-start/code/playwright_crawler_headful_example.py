from crawlee.playwright_crawler import PlaywrightCrawler


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Run with a visible browser window.
        headless=False,
        # Switch to the Firefox browser.
        browser_type='firefox',
    )

    # ...
