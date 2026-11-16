import asyncio

from crawlee.browsers import BrowserPool, PlaywrightBrowserPlugin
from crawlee.crawlers import PlaywrightCrawler


async def main() -> None:
    browser_pool = BrowserPool(
        plugins=[
            PlaywrightBrowserPlugin(
                browser_type='chromium',
                browser_launch_options={
                    'headless': False,
                    'channel': 'msedge',
                    'slow_mo': 200,
                },
                browser_new_context_options={
                    'color_scheme': 'dark',
                    'extra_http_headers': {
                        'Custom-Header': 'my-header',
                        'Accept-Language': 'en',
                    },
                    'user_agent': 'My-User-Agent',
                },
            )
        ]
    )

    crawler = PlaywrightCrawler(browser_pool=browser_pool)

    # ...

    async with browser_pool:
        await crawler.run(['https://crawlee.dev/'])


if __name__ == '__main__':
    asyncio.run(main())
