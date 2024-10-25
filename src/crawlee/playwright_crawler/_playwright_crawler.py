from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pydantic import ValidationError
from typing_extensions import Unpack

from crawlee import EnqueueStrategy
from crawlee._request import BaseRequestData
from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee.basic_crawler import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.browsers import BrowserPool
from crawlee.errors import SessionError
from crawlee.playwright_crawler._playwright_crawling_context import PlaywrightCrawlingContext
from crawlee.playwright_crawler._utils import infinite_scroll

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from crawlee._types import AddRequestsKwargs, BasicCrawlingContext
    from crawlee.browsers._types import BrowserType


class PlaywrightCrawler(BasicCrawler[PlaywrightCrawlingContext]):
    """A web crawler that leverages the `Playwright` browser automation library.

    The `PlaywrightCrawler` builds on top of the `BasicCrawler`, which means it inherits all of its features.
    On top of that it provides a high level web crawling interface on top of the `Playwright` library. To be more
    specific, it uses the Crawlee's `BrowserPool` to manage the Playwright's browser instances and the pages they
    open. You can create your own `BrowserPool` instance and pass it to the `PlaywrightCrawler` constructor, or let
    the crawler create a new instance with the default settings.

    This crawler is ideal for crawling websites that require JavaScript execution, as it uses real browsers
    to download web pages and extract data. For websites that do not require JavaScript, consider using one of the
    HTTP client-based crawlers, such as the `HttpCrawler`, `ParselCrawler`, or `BeautifulSoupCrawler`. They use
    raw HTTP requests, which means they are much faster.

    ### Usage

    ```python
    from crawlee.playwright_crawler import PlaywrightCrawler, PlaywrightCrawlingContext

    crawler = PlaywrightCrawler()

    # Define the default request handler, which will be called for every request.
    @crawler.router.default_handler
    async def request_handler(context: PlaywrightCrawlingContext) -> None:
        context.log.info(f'Processing {context.request.url} ...')

        # Extract data from the page.
        data = {
            'url': context.request.url,
            'title': await context.page.title(),
            'response': (await context.response.text())[:100],
        }

        # Push the extracted data to the default dataset.
        await context.push_data(data)

    await crawler.run(['https://crawlee.dev/'])
    ```
    """

    def __init__(
        self,
        browser_pool: BrowserPool | None = None,
        browser_type: BrowserType | None = None,
        headless: bool | None = None,
        **kwargs: Unpack[BasicCrawlerOptions[PlaywrightCrawlingContext]],
    ) -> None:
        """A default constructor.

        Args:
            browser_pool: A `BrowserPool` instance to be used for launching the browsers and getting pages.
            browser_type: The type of browser to launch ('chromium', 'firefox', or 'webkit').
                This option should not be used if `browser_pool` is provided.
            headless: Whether to run the browser in headless mode.
                This option should not be used if `browser_pool` is provided.
            kwargs: Additional keyword arguments to pass to the underlying `BasicCrawler`.
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
        """Executes an HTTP request utilizing the `BrowserPool` and the `Playwright` library.

        Args:
            context: The basic crawling context to be enhanced.

        Raises:
            ValueError: If the browser pool is not initialized.
            SessionError: If the URL cannot be loaded by the browser.

        Yields:
            The enhanced crawling context with the Playwright-specific features (page, response, enqueue_links, and
                infinite_scroll).
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

                        try:
                            request = BaseRequestData.from_url(url, user_data=link_user_data)
                        except ValidationError as exc:
                            context.log.debug(
                                f'Skipping URL "{url}" due to invalid format: {exc}. '
                                'This may be caused by a malformed URL or unsupported URL scheme. '
                                'Please ensure the URL is correct and retry.'
                            )
                            continue

                        requests.append(request)

                await context.add_requests(requests, **kwargs)

            yield PlaywrightCrawlingContext(
                request=context.request,
                session=context.session,
                add_requests=context.add_requests,
                send_request=context.send_request,
                push_data=context.push_data,
                proxy_info=context.proxy_info,
                get_key_value_store=context.get_key_value_store,
                log=context.log,
                page=crawlee_page.page,
                infinite_scroll=lambda: infinite_scroll(crawlee_page.page),
                response=response,
                enqueue_links=enqueue_links,
            )

    async def _handle_blocked_request(
        self,
        context: PlaywrightCrawlingContext,
    ) -> AsyncGenerator[PlaywrightCrawlingContext, None]:
        """Try to detect if the request is blocked based on the HTTP status code or the response content.

        Args:
            context: The current crawling context.

        Raises:
            SessionError: If the request is considered blocked.

        Yields:
            The original crawling context if no errors are detected.
        """
        if self._retry_on_blocked:
            status_code = context.response.status

            # Check if the session is blocked based on the HTTP status code.
            if context.session and context.session.is_blocked_status_code(status_code=status_code):
                raise SessionError(f'Assuming the session is blocked based on HTTP status code {status_code}.')

            matched_selectors = [
                selector for selector in RETRY_CSS_SELECTORS if (await context.page.query_selector(selector))
            ]

            # Check if the session is blocked based on the response content
            if matched_selectors:
                raise SessionError(
                    'Assuming the session is blocked - '
                    f"HTTP response matched the following selectors: {'; '.join(matched_selectors)}"
                )

        yield context
