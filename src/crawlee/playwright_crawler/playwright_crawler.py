from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

from typing_extensions import Unpack

from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.browsers import BrowserPool
from crawlee.enqueue_strategy import EnqueueStrategy
from crawlee.models import BaseRequestData
from crawlee.playwright_crawler.types import PlaywrightCrawlingContext

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.basic_crawler.types import AddRequestsKwargs, BasicCrawlingContext


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

        kwargs.setdefault('_logger', logging.getLogger(__name__))

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

        async def enqueue_links(
            *,
            selector: str = 'a',
            label: str | None = None,
            user_data: dict | None = None,
            **kwargs: Unpack[AddRequestsKwargs],
        ) -> None:
            kwargs.setdefault('strategy', EnqueueStrategy.SAME_HOSTNAME)

            requests = list[BaseRequestData]()
            user_data = user_data or {}

            elements = await crawlee_page.page.query_selector_all(selector)

            for element in elements:
                href = await element.get_attribute('href')

                if href:
                    link_user_data = user_data.copy()

                    if label is not None:
                        link_user_data.setdefault('label', label)

                    request = BaseRequestData.from_url(href, user_data=link_user_data)
                    requests.append(request)

            await context.add_requests(requests, **kwargs)

        yield PlaywrightCrawlingContext(
            request=context.request,
            session=context.session,
            add_requests=context.add_requests,
            send_request=context.send_request,
            push_data=context.push_data,
            proxy_info=context.proxy_info,
            log=context.log,
            page=crawlee_page.page,
            enqueue_links=enqueue_links,
        )

        await crawlee_page.page.close()
