from __future__ import annotations

from typing import TYPE_CHECKING, Any

from crawlee._utils.docs import docs_group
from crawlee.browsers import BrowserPool
from crawlee.browsers._stagehand_browser_plugin import StagehandBrowserPlugin
from crawlee.crawlers import PlaywrightCrawler

from ._stagehand_crawling_context import (
    StagehandCrawlingContext,
    StagehandPostNavCrawlingContext,
    StagehandPreNavCrawlingContext,
)

if TYPE_CHECKING:
    from datetime import timedelta
    from pathlib import Path

    from typing_extensions import Unpack

    from crawlee.browsers import StagehandOptions
    from crawlee.crawlers._basic import BasicCrawlerOptions
    from crawlee.crawlers._playwright._types import GotoOptions
    from crawlee.statistics import StatisticsState


@docs_group('Crawlers')
class StagehandCrawler(
    PlaywrightCrawler[
        StagehandPreNavCrawlingContext,
        StagehandPostNavCrawlingContext,
        StagehandCrawlingContext,
    ]
):
    """A web crawler that integrates Stagehand AI-powered browser automation with Crawlee.

    `StagehandCrawler` builds on top of `PlaywrightCrawler`, inheriting all of its features.
    It uses `StagehandBrowserPlugin` to manage Stagehand sessions. Stagehand creates and manages
    the browser instance — either locally via a bundled Chromium binary, or remotely via Browserbase
    cloud — and Playwright connects to it via the Chrome DevTools Protocol (CDP).

    Because Stagehand relies on CDP, only Chromium is supported. Not all Playwright browser and
    context configuration options are available — browser settings are limited to the subset accepted
    by Stagehand's ``BrowserLaunchOptions`` (such as ``headless``, ``args``, ``viewport``, ``proxy``,
    ``locale``, and ``executable_path``). Features like fingerprint generation and incognito pages
    are not supported.

    Each page in the crawling context is a `StagehandPage`, which extends the standard Playwright
    `Page` with the following AI methods:

    - `page.act(**kwargs)` — perform an action on the page using natural language
    - `page.extract(**kwargs)` — extract structured data from the page with AI
    - `page.observe(**kwargs)` — get AI-suggested actions available on the page
    - `page.execute(**kwargs)` — run an autonomous multi-step agent

    Stagehand configuration (model, API key, environment) is provided via `stagehand_options`.
    By default, the crawler runs locally using the `openai/gpt-4.1-mini` model.

    ### Usage

    ```python
    import asyncio
    from crawlee.crawlers import StagehandCrawler
    from crawlee.crawlers._stagehand import StagehandCrawlingContext
    from crawlee.browsers import StagehandOptions

    crawler = StagehandCrawler(
        stagehand_options=StagehandOptions(
            api_key='sk-...',
            model='openai/gpt-4.1-mini',
        ),
    )

    @crawler.router.default_handler
    async def handler(context: StagehandCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Use standard Playwright methods alongside AI methods.
        await context.page.act(instruction='Click the accept cookies button if present')

        data = await context.page.extract(instruction='Get the article title and author')

        await context.push_data(data)

    asyncio.run(crawler.run(['https://example.com']))
    ```
    """

    _PRE_NAV_CONTEXT_CLASS = StagehandPreNavCrawlingContext
    _POST_NAV_CONTEXT_CLASS = StagehandPostNavCrawlingContext
    _CRAWLING_CONTEXT_CLASS = StagehandCrawlingContext

    def __init__(
        self,
        *,
        stagehand_options: StagehandOptions | None = None,
        browser_pool: BrowserPool | None = None,
        user_data_dir: str | Path | None = None,
        headless: bool | None = None,
        browser_launch_options: dict[str, Any] | None = None,
        browser_new_context_options: dict[str, Any] | None = None,
        goto_options: GotoOptions | None = None,
        navigation_timeout: timedelta | None = None,
        max_open_pages_per_browser: int | None = None,
        **kwargs: Unpack[BasicCrawlerOptions[StagehandCrawlingContext, StatisticsState]],
    ) -> None:
        """Initialize a new instance.

        Args:
            stagehand_options: Stagehand-specific configuration (model, API key, env, etc.).
                Cannot be specified if `browser_pool` is provided.
            browser_pool: A pre-configured `BrowserPool`. All plugins must be instances of
                `StagehandBrowserPlugin`. If omitted, a pool is created automatically from the
                other browser arguments.
            user_data_dir: Path to a user data directory, which stores browser session data like
                cookies and local storage. Cannot be specified if `browser_pool` is provided.
            headless: Whether to run the browser in headless mode. Defaults to the value from
                Crawlee's global `Configuration`. Cannot be specified if `browser_pool` is provided.
            browser_launch_options: Keyword arguments for browser launch passed to Stagehand's
                `BrowserLaunchOptions` (a subset of Playwright's launch options). Supported keys
                include `args`, `executable_path`, `proxy`, `viewport`, `locale`, and others.
                Cannot be specified if `browser_pool` is provided.
            browser_new_context_options: Keyword arguments for browser context creation, merged
                with `browser_launch_options`. Options that map to `BrowserLaunchOptions` take
                effect on the first page; subsequent pages reuse the existing session context.
                Cannot be specified if `browser_pool` is provided.
            goto_options: Additional options passed to Stagehand's `Page.goto()`. The `timeout`
                option is not supported — use `navigation_timeout` instead.
            navigation_timeout: Timeout for the navigation phase (from opening the page to calling
                the request handler). Defaults to one minute.
            max_open_pages_per_browser: Maximum number of pages open per browser instance.
                Cannot be specified if `browser_pool` is provided.
            kwargs: Additional keyword arguments forwarded to `BasicCrawler`.
        """
        if browser_pool is not None:
            self._validate_browser_pool(browser_pool)
            if any(
                param is not None
                for param in (
                    stagehand_options,
                    user_data_dir,
                    headless,
                    browser_launch_options,
                    browser_new_context_options,
                    max_open_pages_per_browser,
                )
            ):
                raise ValueError(
                    'Cannot specify `stagehand_options`, `user_data_dir`, `headless`, '
                    '`browser_launch_options`, `browser_new_context_options` or '
                    '`max_open_pages_per_browser` when `browser_pool` is provided.'
                )
        else:
            launch_options = dict(browser_launch_options or {})
            if headless is not None:
                launch_options['headless'] = headless

            browser_pool = BrowserPool(
                plugins=[
                    StagehandBrowserPlugin(
                        stagehand_options=stagehand_options,
                        user_data_dir=user_data_dir,
                        browser_launch_options=launch_options or None,
                        browser_new_context_options=browser_new_context_options,
                        max_open_pages_per_browser=max_open_pages_per_browser or 20,
                    )
                ]
            )

        super().__init__(
            browser_pool=browser_pool,
            goto_options=goto_options,
            navigation_timeout=navigation_timeout,
            **kwargs,
        )

    @staticmethod
    def _validate_browser_pool(pool: BrowserPool) -> None:
        invalid = [p for p in pool.plugins if not isinstance(p, StagehandBrowserPlugin)]
        if invalid:
            raise ValueError(
                f'All BrowserPool plugins must be StagehandBrowserPlugin instances. Invalid plugins: {invalid}'
            )
