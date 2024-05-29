from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Literal

from typing_extensions import Unpack

from crawlee.basic_crawler import (
    BasicCrawler,
    BasicCrawlerOptions,
    BasicCrawlingContext,
    ContextPipeline,
)
from crawlee.browsers import BrowserPool, PlaywrightBrowserPlugin
from crawlee.playwright_crawler.types import PlaywrightCrawlingContext

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator


class PlaywrightCrawler(BasicCrawler[PlaywrightCrawlingContext]):
    """A crawler that fetches the request URL using `Playwright`."""

    def __init__(
        self,
        headless: bool | None = None,
        browser_type: Literal['chromium', 'firefox', 'webkit'] | None = None,
        **kwargs: Unpack[BasicCrawlerOptions[PlaywrightCrawlingContext]],
    ) -> None:
        """Create a new instance.

        Args:
            headless: Whether to run the browser in headless mode.
                This option should not be used if `browser_pool` is provided.
            browser_type: The type of browser to launch ('chromium', 'firefox', or 'webkit').
                This option should not be used if `browser_pool` is provided.
            kwargs: Additional arguments to be forwarded to the underlying BasicCrawler.
        """
        browser_pool = kwargs.get('browser_pool')

        if browser_pool:
            # Raise an exception if browser_pool is provided together with headless or browser_type arguments.
            if headless is not None or browser_type is not None:
                raise ValueError(
                    'You cannot provide `headless` or `browser_type` arguments when `browser_pool` is provided.'
                )

        # If browser_pool is not provided, create a new instance of BrowserPool with specified arguments.
        else:
            plugin_options: dict = defaultdict(dict)

            if headless is not None:
                plugin_options['browser_options']['headless'] = headless

            if browser_type:
                plugin_options['browser_type'] = browser_type

            browser_pool = BrowserPool(plugins=[PlaywrightBrowserPlugin(**plugin_options)])
            kwargs['browser_pool'] = browser_pool

        kwargs['use_browser_pool'] = True
        kwargs['_context_pipeline'] = ContextPipeline().compose(self._page_goto)
        super().__init__(**kwargs)

    async def _page_goto(
        self,
        context: BasicCrawlingContext,
    ) -> AsyncGenerator[PlaywrightCrawlingContext, None]:
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
