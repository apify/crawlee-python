from __future__ import annotations

from typing import TYPE_CHECKING, Any

from playwright.async_api import APIRequestContext, APIResponse, Playwright, ProxySettings, async_playwright
from typing_extensions import override

from crawlee._types import HttpHeaders
from crawlee._utils.docs import docs_group
from crawlee.fingerprint_suite import HeaderGenerator
from crawlee.http_clients import HttpClient, HttpCrawlingResult, HttpResponse

if TYPE_CHECKING:
    from types import TracebackType

    from crawlee import Request
    from crawlee._types import HttpMethod, HttpPayload
    from crawlee.proxy_configuration import ProxyInfo
    from crawlee.sessions import Session
    from crawlee.statistics import Statistics


class _PlaywrightResponse:
    """Adapter class for `playwright.APIResponse` to conform to the `HttpResponse` protocol."""

    def __init__(self, response: APIResponse, content: bytes) -> None:
        self._response = response
        self._content = content

    @property
    def http_version(self) -> str:
        return 'unidentified'

    @property
    def status_code(self) -> int:
        return self._response.status

    @property
    def headers(self) -> HttpHeaders:
        return HttpHeaders(dict(self._response.headers))

    def read(self) -> bytes:
        return self._content


@docs_group('Classes')
class PlaywrightHttpClient(HttpClient):
    """HTTP client based on the Playwright library.

    This client uses the Playwright library to perform HTTP requests in crawlers (`BasicCrawler` subclasses)
    and to manage sessions, proxies, and error handling.

    See the `HttpClient` class for more common information about HTTP clients.

    ### Usage

    ```python
    from crawlee.crawlers import HttpCrawler  # or any other HTTP client-based crawler
    from crawlee.http_clients import PlaywrightHttpClient

    http_client = PlaywrightHttpClient()
    crawler = HttpCrawler(http_client=http_client)
    ```
    """

    _DEFAULT_HEADER_GENERATOR = HeaderGenerator()

    def __init__(
        self,
        *,
        persist_cookies_per_session: bool = True,
        header_generator: HeaderGenerator | None = _DEFAULT_HEADER_GENERATOR,
        **request_context_kwargs: Any,
    ) -> None:
        """Initialize a new instance.

        Args:
            persist_cookies_per_session: Whether to persist cookies per HTTP session.
            header_generator: Header generator instance to use for generating common headers.
            request_context_kwargs: Additional keyword arguments for Playwright's APIRequestContext.
        """
        super().__init__(
            persist_cookies_per_session=persist_cookies_per_session,
        )

        self._request_context_kwargs = request_context_kwargs
        self._header_generator = header_generator

        self._playwright_context_manager = async_playwright()
        self._playwright: Playwright | None = None

    @override
    async def crawl(
        self,
        request: Request,
        *,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
        statistics: Statistics | None = None,
    ) -> HttpCrawlingResult:
        client = await self._get_client(proxy_info, session)
        headers = self._combine_headers(request.headers)

        response = await client.fetch(
            url_or_request=request.url,
            method=request.method.lower(),
            headers=dict(headers) if headers else None,
            data=request.payload,
        )

        if statistics:
            statistics.register_status_code(response.status)

        if self._persist_cookies_per_session and session:
            await self._store_cookies_in_session(client, session)

        request.loaded_url = response.url
        content = await response.body()

        await client.dispose()

        return HttpCrawlingResult(
            http_response=_PlaywrightResponse(response, content=content),
        )

    @override
    async def send_request(
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: HttpHeaders | dict[str, str] | None = None,
        payload: HttpPayload | None = None,
        session: Session | None = None,
        proxy_info: ProxyInfo | None = None,
    ) -> HttpResponse:
        if isinstance(headers, dict) or headers is None:
            headers = HttpHeaders(headers or {})

        client = await self._get_client(proxy_info, session)
        headers = self._combine_headers(headers)

        response = await client.fetch(
            url_or_request=url, method=method.lower(), headers=dict(headers) if headers else None, data=payload
        )

        if self._persist_cookies_per_session and session:
            await self._store_cookies_in_session(client, session)

        content = await response.body()

        await client.dispose()

        return _PlaywrightResponse(response, content=content)

    def _combine_headers(self, explicit_headers: HttpHeaders | None) -> HttpHeaders | None:
        """Merge default headers with explicit headers for an HTTP request.

        Generate a final set of request headers by combining default headers, a random User-Agent header,
        and any explicitly provided headers.
        """
        common_headers = self._header_generator.get_common_headers() if self._header_generator else HttpHeaders()
        user_agent_header = (
            self._header_generator.get_random_user_agent_header() if self._header_generator else HttpHeaders()
        )
        explicit_headers = explicit_headers or HttpHeaders()
        headers = common_headers | user_agent_header | explicit_headers
        return headers if headers else None

    async def _get_client(self, proxy_info: ProxyInfo | None, session: Session | None) -> APIRequestContext:
        """Create a new Playwright APIRequestContext.

        Creates a new context for each request, configured with the appropriate
        proxy settings and cookies from the session.

        Args:
            proxy_info: The proxy configuration, if any
            session: The session object, if any

        Returns:
            A newly created Playwright APIRequestContext
        """
        kwargs: dict[str, Any] = {}

        if proxy_info:
            kwargs['proxy'] = ProxySettings(
                server=f'{proxy_info.scheme}://{proxy_info.hostname}:{proxy_info.port}',
                username=proxy_info.username,
                password=proxy_info.password,
            )

        if self._persist_cookies_per_session and session and session.cookies:
            pw_cookies = session.cookies.get_cookies_as_playwright_format()
            if pw_cookies:
                kwargs['storage_state'] = {'cookies': pw_cookies, 'origins': []}

        kwargs.update(self._request_context_kwargs)

        if not self._playwright:
            raise RuntimeError(f'The {self.__class__.__name__} is not started.')

        return await self._playwright.request.new_context(**kwargs)

    async def _store_cookies_in_session(self, client: APIRequestContext, session: Session) -> None:
        """Store cookies from the Playwright request context in the session."""
        storage_state = await client.storage_state()
        session.cookies.set_cookies_from_playwright_format(storage_state.get('cookies', []))

    @override
    async def __aenter__(self) -> PlaywrightHttpClient:
        self._playwright = await self._playwright_context_manager.__aenter__()
        return self

    @override
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        await self._playwright_context_manager.__aexit__(exc_type, exc_value, exc_traceback)
        self._playwright = None
        self._playwright_context_manager = async_playwright()
