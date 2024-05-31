from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from typing_extensions import Unpack

from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, BasicCrawlingContext, ContextPipeline
from crawlee.browsers import BrowserPool
from crawlee.playwright_crawler.types import PlaywrightCrawlingContext

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class PlaywrightCrawler(BasicCrawler[PlaywrightCrawlingContext]):
    """A crawler that fetches the request URL using `Playwright`."""

    def __init__(
        self,
        browser_pool: BrowserPool | None = None,
        browser_type: Literal['chromium', 'firefox', 'webkit'] | None = None,
        headless: bool | None = None,
        **kwargs: Unpack[BasicCrawlerOptions[PlaywrightCrawlingContext]],
    ) -> None:
        """Create a new instance.

        Args:
            browser_pool: A `BrowserPool` instance to be used for launching the browsers and getting pages.
            browser_type: The type of browser to launch ('chromium', 'firefox', or 'webkit').
                This option should not be used if `browser_pool` is provided.
            headless: Whether to run the browser in headless mode.
                This option should not be used if `browser_pool` is provided.
            kwargs: Additional arguments to be forwarded to the underlying `BasicCrawler`.
        """
        if browser_pool:
            # Raise an exception if browser_pool is provided together with headless or browser_type arguments.
            if headless is not None or browser_type is not None:
                raise ValueError(
                    'You cannot provide `headless` or `browser_type` arguments when `browser_pool` is provided.'
                )

        # If browser_pool is not provided, create a new instance of BrowserPool with specified arguments.
        else:
            browser_pool = BrowserPool.with_default_plugin(headless=headless, browser_type=browser_type)

        self._browser_pool = browser_pool

        kwargs['_context_pipeline'] = ContextPipeline().compose(self._page_goto)
        kwargs['_additional_context_managers'] = [self._browser_pool]

        super().__init__(**kwargs)

    async def _page_goto(
        self,
        context: BasicCrawlingContext,
    ) -> AsyncGenerator[PlaywrightCrawlingContext, None]:
        if self._browser_pool is None:
            raise ValueError('Browser pool is not initialized.')

        crawlee_page = await self._browser_pool.new_page()
        await crawlee_page.page.goto(context.request.url)
        context.request.loaded_url = crawlee_page.page.url

        yield PlaywrightCrawlingContext(
            request=context.request,
            session=context.session,
            send_request=context.send_request,
            add_requests=context.add_requests,
            proxy_info=context.proxy_info,
            page=crawlee_page.page,
        )

        await crawlee_page.page.close()
