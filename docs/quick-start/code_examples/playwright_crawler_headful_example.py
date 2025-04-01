from crawlee.crawlers import PlaywrightCrawler


async def main() -> None:
    crawler = PlaywrightCrawler(
        # Run with a visible browser window.
        # highlight-next-line
        headless=False,
        # Switch to the Firefox browser.
        browser_type='firefox',
    )

    # ...
