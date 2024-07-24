from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Literal

from typing_extensions import Unpack

from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.basic_crawler.errors import SessionError
from crawlee.browsers import BrowserPool
from crawlee.enqueue_strategy import EnqueueStrategy
from crawlee.models import BaseRequestData
from crawlee.playwright_crawler.types import PlaywrightCrawlingContext

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee.basic_crawler.types import AddRequestsKwargs, BasicCrawlingContext


class PlaywrightCrawler(BasicCrawler[PlaywrightCrawlingContext]):
    """A crawler that leverages the [Playwright](https://playwright.dev/python/) browser automation library.

    `PlaywrightCrawler` is a subclass of `BasicCrawler`, inheriting all its features, such as autoscaling of requests,
    request routing, and utilization of `RequestProvider`. Additionally, it offers Playwright-specific methods and
    properties, like the `page` property for user data extraction, and the `enqueue_links` method for crawling
    other pages.

    This crawler is ideal for crawling websites that require JavaScript execution, as it uses headless browsers
    to download web pages and extract data. For websites that do not require JavaScript, consider using
    `BeautifulSoupCrawler`, which uses raw HTTP requests, and it is much faster.

    `PlaywrightCrawler` opens a new browser page (i.e., tab) for each `Request` object and invokes the user-provided
    request handler function via the `Router`. Users can interact with the page and extract the data using
    the Playwright API.

    Note that the pool of browser instances used by `PlaywrightCrawler`, and the pages they open, is internally
    managed by the `BrowserPool`.
    """

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

        # Compose the context pipeline with the Playwright-specific context enhancer.
        kwargs['_context_pipeline'] = (
            ContextPipeline().compose(self._make_http_request).compose(self._handle_blocked_request)
        )
        kwargs['_additional_context_managers'] = [self._browser_pool]
        kwargs.setdefault('_logger', logging.getLogger(__name__))

        super().__init__(**kwargs)

    async def _make_http_request(
        self,
        context: BasicCrawlingContext,
    ) -> AsyncGenerator[PlaywrightCrawlingContext, None]:
        """Enhance the crawling context with making an HTTP request using Playwright.

        Args:
            context: The basic crawling context to be enhanced.

        Raises:
            ValueError: If the browser pool is not initialized.
            SessionError: If the URL cannot be loaded by the browser.

        Yields:
            An enhanced crawling context with Playwright-specific features.
        """
        if self._browser_pool is None:
            raise ValueError('Browser pool is not initialized.')

        # Create a new browser page
        crawlee_page = await self._browser_pool.new_page(proxy_info=context.proxy_info)

        async with crawlee_page.page:
            # Navigate to the URL and get response.
            response = await crawlee_page.page.goto(context.request.url)

            if response is None:
                raise SessionError(f'Failed to load the URL: {context.request.url}')

            # Set the loaded URL to the actual URL after redirection.
            context.request.loaded_url = crawlee_page.page.url

            async def enqueue_links(
                *,
                selector: str = 'a',
                label: str | None = None,
                user_data: dict | None = None,
                **kwargs: Unpack[AddRequestsKwargs],
            ) -> None:
                """The `PlaywrightCrawler` implementation of the `EnqueueLinksFunction` function."""
                kwargs.setdefault('strategy', EnqueueStrategy.SAME_HOSTNAME)

                requests = list[BaseRequestData]()
                user_data = user_data or {}

                elements = await crawlee_page.page.query_selector_all(selector)

                for element in elements:
                    url = await element.get_attribute('href')

                    if url:
                        url = url.strip()

                        if not is_url_absolute(url):
                            url = convert_to_absolute_url(context.request.url, url)

                        link_user_data = user_data.copy()

                        if label is not None:
                            link_user_data.setdefault('label', label)

                        request = BaseRequestData.from_url(url, user_data=link_user_data)
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
                response=response,
                enqueue_links=enqueue_links,
            )

    async def _handle_blocked_request(
        self,
        crawling_context: PlaywrightCrawlingContext,
    ) -> AsyncGenerator[PlaywrightCrawlingContext, None]:
        """Enhance the crawling context with handling of blocked requests.

        Args:
            crawling_context: The crawling context to be checked for blocking.

        Raises:
            SessionError: If the session is blocked based on the HTTP status code or the response content.

        Yields:
            The original crawling context if the session is not blocked.
        """
        if self._retry_on_blocked:
            status_code = crawling_context.response.status

            # Check if the session is blocked based on the HTTP status code.
            if crawling_context.session and crawling_context.session.is_blocked_status_code(status_code=status_code):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}.')

            matched_selectors = [
                selector for selector in RETRY_CSS_SELECTORS if (await crawling_context.page.query_selector(selector))
            ]

            # Check if the session is blocked based on the response content
            if matched_selectors:
                raise SessionError(
                    'Assuming the session is blocked - '
                    f"HTTP response matched the following selectors: {'; '.join(matched_selectors)}"
                )

        yield crawling_context
