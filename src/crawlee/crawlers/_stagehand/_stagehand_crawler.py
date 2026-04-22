from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

from crawlee._utils.docs import docs_group
from crawlee.browsers import BrowserPool
from crawlee.browsers._stagehand_browser_plugin import StagehandBrowserPlugin
from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

if TYPE_CHECKING:
    from typing_extensions import Unpack

    from crawlee.browsers._types import StagehandOptions
    from crawlee.crawlers._basic import BasicCrawlerOptions
    from crawlee.statistics import StatisticsState


@docs_group('Crawlers')
class StagehandCrawler(PlaywrightCrawler):
    """A web crawler that integrates Stagehand AI-powered browser automation with Crawlee.

    Extends `PlaywrightCrawler` with a `StagehandBrowserPlugin` that manages a Stagehand
    session per browser instance. Each page in the crawling context is a `StagehandPage`,
    which exposes AI methods alongside all standard Playwright `Page` methods:

    - `page.act(**kwargs)` — perform actions using natural language
    - `page.extract(**kwargs)` — extract structured data with AI
    - `page.observe(**kwargs)` — get AI-suggested actions on the page
    - `page.execute(**kwargs)` — run an autonomous multi-step agent

    ### Usage

    ```python
    from crawlee.crawlers import StagehandCrawler
    from crawlee.crawlers._stagehand import StagehandCrawlingContext

    crawler = StagehandCrawler()

    @crawler.router.default_handler
    async def handler(context: StagehandCrawlingContext) -> None:
        await context.page.act(input='Click the login button')
        data = await context.page.extract(instruction='Get the page title')
        await context.push_data(data)

    await crawler.run(['https://example.com'])
    ```
    """

    def __init__(
        self,
        *,
        stagehand_options: StagehandOptions | None = None,
        browser_pool: BrowserPool | None = None,
        browser_new_context_options: dict[str, Any] | None = None,
        max_open_pages_per_browser: int = 20,
        **kwargs: Unpack[BasicCrawlerOptions[PlaywrightCrawlingContext, StatisticsState]],
    ) -> None:
        """Initialize a new instance.

        Args:
            stagehand_options: Stagehand-specific configuration (model, API key, env, etc.).
                Ignored if `browser_pool` is provided.
            browser_pool: A pre-configured `BrowserPool`. All plugins must be instances of
                `StagehandBrowserPlugin` (or its subclasses). If omitted, a pool is created
                automatically from `stagehand_options`.
            browser_new_context_options: Options passed to Playwright's `browser.new_context`
                after connecting via CDP. Ignored if `browser_pool` is provided.
            max_open_pages_per_browser: Maximum pages open per browser instance.
                Ignored if `browser_pool` is provided.
            kwargs: Additional keyword arguments forwarded to `BasicCrawler`.
        """
        if browser_pool is not None:
            self._validate_browser_pool(browser_pool)
            if stagehand_options is not None:
                warnings.warn(
                    '`stagehand_options` is ignored when `browser_pool` is provided.',
                    stacklevel=2,
                )
        else:
            browser_pool = BrowserPool(
                plugins=[
                    StagehandBrowserPlugin(
                        stagehand_options=stagehand_options,
                        browser_new_context_options=browser_new_context_options,
                        max_open_pages_per_browser=max_open_pages_per_browser,
                    )
                ]
            )

        super().__init__(browser_pool=browser_pool, **kwargs)

    @staticmethod
    def _validate_browser_pool(pool: BrowserPool) -> None:
        invalid = [p for p in pool.plugins if not isinstance(p, StagehandBrowserPlugin)]
        if invalid:
            raise ValueError(
                f'All BrowserPool plugins must be StagehandBrowserPlugin instances. Invalid plugins: {invalid}'
            )
