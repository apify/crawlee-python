from __future__ import annotations

import asyncio
import logging
import warnings
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Generic, Literal, Union
from urllib.parse import urlparse

from pydantic import ValidationError
from typing_extensions import NotRequired, TypedDict, TypeVar

from crawlee import service_locator
from crawlee._request import Request, RequestOptions
from crawlee._utils.blocked import RETRY_CSS_SELECTORS
from crawlee._utils.docs import docs_group
from crawlee._utils.robots import RobotsTxtFile
from crawlee._utils.urls import convert_to_absolute_url, is_url_absolute
from crawlee.browsers import BrowserPool
from crawlee.crawlers._basic import BasicCrawler, BasicCrawlerOptions, ContextPipeline
from crawlee.errors import SessionError
from crawlee.fingerprint_suite import DefaultFingerprintGenerator, FingerprintGenerator, HeaderGeneratorOptions
from crawlee.http_clients import HttpxHttpClient
from crawlee.sessions._cookies import PlaywrightCookieParam
from crawlee.statistics import StatisticsState

from ._playwright_crawling_context import PlaywrightCrawlingContext
from ._playwright_http_client import PlaywrightHttpClient, browser_page_context
from ._playwright_pre_nav_crawling_context import PlaywrightPreNavCrawlingContext
from ._utils import block_requests, infinite_scroll

TCrawlingContext = TypeVar('TCrawlingContext', bound=PlaywrightCrawlingContext)
TStatisticsState = TypeVar('TStatisticsState', bound=StatisticsState, default=StatisticsState)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Mapping, Sequence
    from pathlib import Path

    from playwright.async_api import Page, Route
    from playwright.async_api import Request as PlaywrightRequest
    from typing_extensions import Unpack

    from crawlee import RequestTransformAction
    from crawlee._types import (
        BasicCrawlingContext,
        EnqueueLinksFunction,
        EnqueueLinksKwargs,
        ExtractLinksFunction,
        HttpHeaders,
        HttpMethod,
        HttpPayload,
    )
    from crawlee.browsers._types import BrowserType


@docs_group('Classes')
class PlaywrightCrawler(BasicCrawler[PlaywrightCrawlingContext, StatisticsState]):
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
    from crawlee.crawlers import PlaywrightCrawler, PlaywrightCrawlingContext

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
        *,
        browser_pool: BrowserPool | None = None,
        browser_type: BrowserType | None = None,
        user_data_dir: str | Path | None = None,
        browser_launch_options: Mapping[str, Any] | None = None,
        browser_new_context_options: Mapping[str, Any] | None = None,
        fingerprint_generator: FingerprintGenerator | None | Literal['default'] = 'default',
        headless: bool | None = None,
        use_incognito_pages: bool | None = None,
        **kwargs: Unpack[BasicCrawlerOptions[PlaywrightCrawlingContext, StatisticsState]],
    ) -> None:
        """Initialize a new instance.

        Args:
            browser_pool: A `BrowserPool` instance to be used for launching the browsers and getting pages.
            user_data_dir: Path to a user data directory, which stores browser session data like cookies
                and local storage.
            browser_type: The type of browser to launch ('chromium', 'firefox', or 'webkit').
                This option should not be used if `browser_pool` is provided.
            browser_launch_options: Keyword arguments to pass to the browser launch method. These options are provided
                directly to Playwright's `browser_type.launch` method. For more details, refer to the
                [Playwright documentation](https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch).
                This option should not be used if `browser_pool` is provided.
            browser_new_context_options: Keyword arguments to pass to the browser new context method. These options
                are provided directly to Playwright's `browser.new_context` method. For more details, refer to the
                [Playwright documentation](https://playwright.dev/python/docs/api/class-browser#browser-new-context).
                This option should not be used if `browser_pool` is provided.
            fingerprint_generator: An optional instance of implementation of `FingerprintGenerator` that is used
                to generate browser fingerprints together with consistent headers.
            headless: Whether to run the browser in headless mode.
                This option should not be used if `browser_pool` is provided.
            use_incognito_pages: By default pages share the same browser context. If set to True each page uses its
                own context that is destroyed once the page is closed or crashes.
                This option should not be used if `browser_pool` is provided.
            kwargs: Additional keyword arguments to pass to the underlying `BasicCrawler`.
        """
        configuration = kwargs.pop('configuration', None)
        if configuration is not None:
            service_locator.set_configuration(configuration)

        if browser_pool:
            # Raise an exception if browser_pool is provided together with other browser-related arguments.
            if any(
                param not in [None, 'default']
                for param in (
                    user_data_dir,
                    use_incognito_pages,
                    headless,
                    browser_type,
                    browser_launch_options,
                    browser_new_context_options,
                    fingerprint_generator,
                )
            ):
                raise ValueError(
                    'You cannot provide `headless`, `browser_type`, `browser_launch_options`, '
                    '`browser_new_context_options`, `use_incognito_pages`, `user_data_dir`  or'
                    '`fingerprint_generator` arguments when `browser_pool` is provided.'
                )

        # If browser_pool is not provided, create a new instance of BrowserPool with specified arguments.
        else:
            if fingerprint_generator == 'default':
                generator_browser_type = None if browser_type is None else [browser_type]
                fingerprint_generator = DefaultFingerprintGenerator(
                    header_options=HeaderGeneratorOptions(browsers=generator_browser_type)
                )

            browser_pool = BrowserPool.with_default_plugin(
                headless=headless,
                browser_type=browser_type,
                user_data_dir=user_data_dir,
                browser_launch_options=browser_launch_options,
                browser_new_context_options=browser_new_context_options,
                use_incognito_pages=use_incognito_pages,
                fingerprint_generator=fingerprint_generator,
            )

        self._browser_pool = browser_pool

        # Compose the context pipeline with the Playwright-specific context enhancer.
        kwargs['_context_pipeline'] = (
            ContextPipeline()
            .compose(self._open_page)
            .compose(self._navigate)
            .compose(self._handle_status_code_response)
            .compose(self._handle_blocked_request_by_content)
        )
        kwargs['_additional_context_managers'] = [self._browser_pool]
        kwargs.setdefault('_logger', logging.getLogger(__name__))
        self._pre_navigation_hooks: list[Callable[[PlaywrightPreNavCrawlingContext], Awaitable[None]]] = []

        kwargs['http_client'] = PlaywrightHttpClient() if not kwargs.get('http_client') else kwargs['http_client']

        super().__init__(**kwargs)

    async def _open_page(
        self,
        context: BasicCrawlingContext,
    ) -> AsyncGenerator[PlaywrightPreNavCrawlingContext, None]:
        if self._browser_pool is None:
            raise ValueError('Browser pool is not initialized.')

        # Create a new browser page
        crawlee_page = await self._browser_pool.new_page(proxy_info=context.proxy_info)

        pre_navigation_context = PlaywrightPreNavCrawlingContext(
            request=context.request,
            session=context.session,
            add_requests=context.add_requests,
            send_request=context.send_request,
            push_data=context.push_data,
            use_state=context.use_state,
            proxy_info=context.proxy_info,
            get_key_value_store=context.get_key_value_store,
            log=context.log,
            page=crawlee_page.page,
            block_requests=partial(block_requests, page=crawlee_page.page),
        )

        async with browser_page_context(crawlee_page.page):
            for hook in self._pre_navigation_hooks:
                await hook(pre_navigation_context)
        yield pre_navigation_context

    def _prepare_request_interceptor(
        self,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
    ) -> Callable:
        """Create a request interceptor for Playwright to support non-GET methods with custom parameters.

        The interceptor modifies requests by adding custom headers and payload before they are sent.

        Args:
            method: HTTP method to use for the request.
            headers: Custom HTTP headers to send with the request.
            payload: Request body data for POST/PUT requests.
        """

        async def route_handler(route: Route, _: PlaywrightRequest) -> None:
            await route.continue_(method=method, headers=dict(headers) if headers else None, post_data=payload)

        return route_handler

    async def _navigate(
        self,
        context: PlaywrightPreNavCrawlingContext,
    ) -> AsyncGenerator[PlaywrightCrawlingContext, Exception | None]:
        """Execute an HTTP request utilizing the `BrowserPool` and the `Playwright` library.

        Args:
            context: The basic crawling context to be enhanced.

        Raises:
            ValueError: If the browser pool is not initialized.
            SessionError: If the URL cannot be loaded by the browser.

        Yields:
            The enhanced crawling context with the Playwright-specific features (page, response, enqueue_links,
                infinite_scroll and block_requests).
        """
        async with context.page:
            if context.session:
                session_cookies = context.session.cookies.get_cookies_as_playwright_format()
                await self._update_cookies(context.page, session_cookies)

            if context.request.headers:
                await context.page.set_extra_http_headers(context.request.headers.model_dump())
            # Navigate to the URL and get response.
            if context.request.method != 'GET':
                # Call the notification only once
                warnings.warn(
                    'Using other request methods than GET or adding payloads has a high impact on performance'
                    ' in recent versions of Playwright. Use only when necessary.',
                    category=UserWarning,
                    stacklevel=2,
                )

                route_handler = self._prepare_request_interceptor(
                    method=context.request.method,
                    headers=context.request.headers,
                    payload=context.request.payload,
                )

                # Set route_handler only for current request
                await context.page.route(context.request.url, route_handler)

            response = await context.page.goto(context.request.url)

            if response is None:
                raise SessionError(f'Failed to load the URL: {context.request.url}')

            # Set the loaded URL to the actual URL after redirection.
            context.request.loaded_url = context.page.url

            extract_links = self._create_extract_links_function(context)

            async with browser_page_context(context.page):
                error = yield PlaywrightCrawlingContext(
                    request=context.request,
                    session=context.session,
                    add_requests=context.add_requests,
                    send_request=context.send_request,
                    push_data=context.push_data,
                    use_state=context.use_state,
                    proxy_info=context.proxy_info,
                    get_key_value_store=context.get_key_value_store,
                    log=context.log,
                    page=context.page,
                    infinite_scroll=lambda: infinite_scroll(context.page),
                    response=response,
                    extract_links=extract_links,
                    enqueue_links=self._create_enqueue_links_function(context, extract_links),
                    block_requests=partial(block_requests, page=context.page),
                )

            if context.session:
                pw_cookies = await self._get_cookies(context.page)
                context.session.cookies.set_cookies_from_playwright_format(pw_cookies)

            # Collect data in case of errors, before the page object is closed.
            if error:
                await self.statistics.error_tracker.add(error=error, context=context, early=True)

    def _create_extract_links_function(self, context: PlaywrightPreNavCrawlingContext) -> ExtractLinksFunction:
        """Create a callback function for extracting links from context.

        Args:
            context: The current crawling context.

        Returns:
            Awaitable that is used for extracting links from context.
        """

        async def extract_links(
            *,
            selector: str = 'a',
            label: str | None = None,
            user_data: dict | None = None,
            transform_request_function: Callable[[RequestOptions], RequestOptions | RequestTransformAction]
            | None = None,
            **kwargs: Unpack[EnqueueLinksKwargs],
        ) -> list[Request]:
            """Extract links from the current page.

            The `PlaywrightCrawler` implementation of the `ExtractLinksFunction` function.
            """
            requests = list[Request]()
            skipped = list[str]()
            base_user_data = user_data or {}

            elements = await context.page.query_selector_all(selector)

            robots_txt_file = await self._get_robots_txt_file_for_url(context.request.url)

            strategy = kwargs.get('strategy', 'same-hostname')
            include_blobs = kwargs.get('include')
            exclude_blobs = kwargs.get('exclude')
            limit_requests = kwargs.get('limit')

            for element in elements:
                if limit_requests and len(requests) >= limit_requests:
                    break

                url = await element.get_attribute('href')

                if url:
                    url = url.strip()

                    if not is_url_absolute(url):
                        base_url = context.request.loaded_url or context.request.url
                        url = convert_to_absolute_url(base_url, url)

                    if robots_txt_file and not robots_txt_file.is_allowed(url):
                        skipped.append(url)
                        continue

                    if self._check_enqueue_strategy(
                        strategy,
                        target_url=urlparse(url),
                        origin_url=urlparse(context.request.url),
                    ) and self._check_url_patterns(url, include_blobs, exclude_blobs):
                        request_option = RequestOptions({'url': url, 'user_data': {**base_user_data}, 'label': label})

                        if transform_request_function:
                            transform_request_option = transform_request_function(request_option)
                            if transform_request_option == 'skip':
                                continue
                            if transform_request_option != 'unchanged':
                                request_option = transform_request_option

                        try:
                            request = Request.from_url(**request_option)
                        except ValidationError as exc:
                            context.log.debug(
                                f'Skipping URL "{url}" due to invalid format: {exc}. '
                                'This may be caused by a malformed URL or unsupported URL scheme. '
                                'Please ensure the URL is correct and retry.'
                            )
                            continue

                        requests.append(request)

            if skipped:
                skipped_tasks = [
                    asyncio.create_task(self._handle_skipped_request(request, 'robots_txt')) for request in skipped
                ]
                await asyncio.gather(*skipped_tasks)

            return requests

        return extract_links

    def _create_enqueue_links_function(
        self, context: PlaywrightPreNavCrawlingContext, extract_links: ExtractLinksFunction
    ) -> EnqueueLinksFunction:
        async def enqueue_links(
            *,
            selector: str | None = None,
            label: str | None = None,
            user_data: dict | None = None,
            transform_request_function: Callable[[RequestOptions], RequestOptions | RequestTransformAction]
            | None = None,
            requests: Sequence[str | Request] | None = None,
            **kwargs: Unpack[EnqueueLinksKwargs],
        ) -> None:
            """Extract and enqueue links from the current page.

            The `PlaywrightCrawler` implementation of the `EnqueueLinksFunction` function.
            """
            kwargs.setdefault('strategy', 'same-hostname')

            if requests:
                if any((selector, label, user_data, transform_request_function)):
                    raise ValueError(
                        'You cannot provide `selector`, `label`, `user_data` or `transform_request_function` '
                        'arguments when `requests` is provided.'
                    )
                # Add directly passed requests.
                await context.add_requests(requests or list[Union[str, Request]](), **kwargs)
            else:
                # Add requests from extracted links.
                await context.add_requests(
                    await extract_links(
                        selector=selector or 'a',
                        label=label,
                        user_data=user_data,
                        transform_request_function=transform_request_function,
                    ),
                    **kwargs,
                )

        return enqueue_links

    async def _handle_status_code_response(
        self, context: PlaywrightCrawlingContext
    ) -> AsyncGenerator[PlaywrightCrawlingContext, None]:
        """Validate the HTTP status code and raise appropriate exceptions if needed.

        Args:
            context: The current crawling context containing the response.

        Raises:
            SessionError: If the status code indicates the session is blocked.
            HttpStatusCodeError: If the status code represents a server error or is explicitly configured as an error.
            HttpClientStatusCodeError: If the status code represents a client error.

        Yields:
            The original crawling context if no errors are detected.
        """
        status_code = context.response.status
        if self._retry_on_blocked:
            self._raise_for_session_blocked_status_code(context.session, status_code)
        self._raise_for_error_status_code(status_code)
        yield context

    async def _handle_blocked_request_by_content(
        self,
        context: PlaywrightCrawlingContext,
    ) -> AsyncGenerator[PlaywrightCrawlingContext, None]:
        """Try to detect if the request is blocked based on the response content.

        Args:
            context: The current crawling context.

        Raises:
            SessionError: If the request is considered blocked.

        Yields:
            The original crawling context if no errors are detected.
        """
        if self._retry_on_blocked:
            matched_selectors = [
                selector for selector in RETRY_CSS_SELECTORS if (await context.page.query_selector(selector))
            ]

            # Check if the session is blocked based on the response content
            if matched_selectors:
                raise SessionError(
                    'Assuming the session is blocked - '
                    f'HTTP response matched the following selectors: {"; ".join(matched_selectors)}'
                )

        yield context

    def pre_navigation_hook(self, hook: Callable[[PlaywrightPreNavCrawlingContext], Awaitable[None]]) -> None:
        """Register a hook to be called before each navigation.

        Args:
            hook: A coroutine function to be called before each navigation.
        """
        self._pre_navigation_hooks.append(hook)

    async def _get_cookies(self, page: Page) -> list[PlaywrightCookieParam]:
        """Get the cookies from the page."""
        cookies = await page.context.cookies()
        return [PlaywrightCookieParam(**cookie) for cookie in cookies]

    async def _update_cookies(self, page: Page, cookies: list[PlaywrightCookieParam]) -> None:
        """Update the cookies in the page context."""
        await page.context.add_cookies([{**cookie} for cookie in cookies])

    async def _find_txt_file_for_url(self, url: str) -> RobotsTxtFile:
        """Find the robots.txt file for a given URL.

        Args:
            url: The URL whose domain will be used to locate and fetch the corresponding robots.txt file.
        """
        http_client = HttpxHttpClient() if isinstance(self._http_client, PlaywrightHttpClient) else self._http_client

        return await RobotsTxtFile.find(url, http_client=http_client)


class _PlaywrightCrawlerAdditionalOptions(TypedDict):
    """Additional arguments for the `PlaywrightCrawler` constructor.

    It is intended for typing forwarded `__init__` arguments in the subclasses.
    All arguments are `BasicCrawlerOptions` + `_PlaywrightCrawlerAdditionalOptions`
    """

    browser_pool: NotRequired[BrowserPool]
    """A `BrowserPool` instance to be used for launching the browsers and getting pages."""

    browser_type: NotRequired[BrowserType]
    """The type of browser to launch ('chromium', 'firefox', or 'webkit').
    This option should not be used if `browser_pool` is provided."""

    browser_launch_options: NotRequired[Mapping[str, Any]]
    """Keyword arguments to pass to the browser launch method. These options are provided
    directly to Playwright's `browser_type.launch` method. For more details, refer to the Playwright
    documentation: https://playwright.dev/python/docs/api/class-browsertype#browser-type-launch.
    This option should not be used if `browser_pool` is provided."""

    browser_new_context_options: NotRequired[Mapping[str, Any]]
    """Keyword arguments to pass to the browser new context method. These options are provided directly to Playwright's
    `browser.new_context` method. For more details, refer to the Playwright documentation:
    https://playwright.dev/python/docs/api/class-browser#browser-new-context. This option should not be used if
    `browser_pool` is provided."""

    headless: NotRequired[bool]
    """Whether to run the browser in headless mode. This option should not be used if `browser_pool` is provided."""


@docs_group('Data structures')
class PlaywrightCrawlerOptions(
    Generic[TCrawlingContext, TStatisticsState],
    _PlaywrightCrawlerAdditionalOptions,
    BasicCrawlerOptions[TCrawlingContext, StatisticsState],
):
    """Arguments for the `AbstractHttpCrawler` constructor.

    It is intended for typing forwarded `__init__` arguments in the subclasses.
    """
